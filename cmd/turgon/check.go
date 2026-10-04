package main

import (
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/netguard"
	"github.com/fduser123-coding/turgon/pkg/plugin"
	"github.com/fduser123-coding/turgon/pkg/store/pgstore"
)

func checkCmd() *cobra.Command {
	var tf temporalFlags
	var specPath, dbURL string
	var skipTemporal bool
	cmd := &cobra.Command{
		Use:   "check",
		Short: "Check that every connection in a spec works, and say how to fix what doesn't",
		Long: "Check connects to each system a runtime spec uses, verifies the tables, objects, fields\n" +
			"and permissions its configuration relies on, and checks Turgon's own database and the\n" +
			"Temporal cluster. Failures come with a plain-language fix. Exits non-zero on any failure.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			spec, err := loadSpec(specPath)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 60*time.Second)
			defer cancel()
			out := cmd.OutOrStdout()
			failed := 0
			report := func(scope string, r connector.CheckResult) {
				mark := "ok  "
				if !r.OK {
					mark = "FAIL"
					failed++
				}
				fmt.Fprintf(out, "%s  %-16s %s", mark, scope, r.Name)
				if r.Detail != "" {
					fmt.Fprintf(out, ": %s", r.Detail)
				}
				fmt.Fprintln(out)
				if r.Fix != "" {
					fmt.Fprintf(out, "      fix: %s\n", r.Fix)
				}
			}

			if dbURL != "" {
				pool, err := pgxpool.New(ctx, dbURL)
				if err == nil {
					err = pool.Ping(ctx)
				}
				if err == nil {
					err = pgstore.Migrate(ctx, pool)
				}
				if err != nil {
					report("turgon", connector.Fail("state database", err.Error(), connector.NetworkFix(err, "the state database")))
				} else {
					report("turgon", connector.Pass("state database", "reachable, schema up to date"))
				}
				if pool != nil {
					pool.Close()
				}
			}
			if !skipTemporal {
				c, err := tf.dial()
				if err == nil {
					_, err = c.CheckHealth(ctx, nil)
					c.Close()
				}
				if err != nil {
					report("turgon", connector.Fail("temporal", err.Error(), connector.NetworkFix(err, tf.address)))
				} else {
					report("turgon", connector.Pass("temporal", tf.address+" namespace "+tf.namespace))
				}
			}

			secretsFrom, err := openSecrets()
			if err != nil {
				report("turgon", connector.Fail("secrets", err.Error(), "Set TURGON_SECRETS to env or openbao, and the TURGON_OPENBAO_* settings for OpenBao."))
				fmt.Fprintf(out, "%d check(s) failed\n", failed)
				return errSilent
			}
			for _, r := range checkSecrets(ctx, secretsFrom, specSecretRefs(spec)) {
				report("turgon", r)
			}

			registry, err := connectorRegistry()
			if err != nil {
				report("turgon", connector.Fail("connectors", err.Error(), "Set TURGON_CONNECTORS to name=unix:///path.sock pairs, one per sidecar."))
				registry = connector.Registry{}
			}
			conns := spec.Spec.Connectors
			sort.Slice(conns, func(i, j int) bool { return conns[i].Endpoint < conns[j].Endpoint })
			for _, c := range conns {
				factory, ok := registry[c.Name]
				if !ok {
					fix := "This connector runs on another worker type; check it there."
					if c.Runtime != v1alpha1.RuntimeNative {
						fix = "This " + c.Runtime + " connector runs in a sidecar: start it (connectors/, or the Helm chart's connectors list) and set TURGON_CONNECTORS=" + c.Name + "=unix:///var/run/turgon/connectors/" + c.Name + ".sock."
					}
					report(c.Endpoint, connector.Fail("runtime", "no runtime for connector "+c.Name+" in this worker", fix))
					continue
				}
				inst, err := factory(ctx, c, secretsFrom)
				if err != nil {
					report(c.Endpoint, connector.Fail("configure", err.Error(), secretsFrom.Missing(c.SecretRef)))
					continue
				}
				if chk, ok := inst.(connector.Checker); ok {
					for _, r := range chk.Check(ctx) {
						report(c.Endpoint, r)
					}
				} else {
					report(c.Endpoint, connector.Pass("configure", "no live checks for this connector"))
				}
				inst.Close()
			}
			for _, r := range checkPlugins(ctx, spec, secretsFrom) {
				report(r.where, r.CheckResult)
			}
			for _, r := range checkSplink(ctx, spec) {
				report("turgon", r)
			}
			if failed > 0 {
				fmt.Fprintf(out, "%d check(s) failed\n", failed)
				return errSilent
			}
			fmt.Fprintln(out, "all checks passed")
			return nil
		},
	}
	tf.register(cmd)
	cmd.Flags().StringVarP(&specPath, "spec", "s", "runtime-spec.json", "compiled runtime spec")
	cmd.Flags().StringVar(&dbURL, "database-url", envOr("TURGON_DATABASE_URL", ""), "also check Turgon's state database")
	cmd.Flags().BoolVar(&skipTemporal, "skip-temporal", false, "do not check the Temporal cluster")
	return cmd
}

type pluginCheck struct {
	where string
	connector.CheckResult
}

// checkPlugins loads each logic plugin as `turgon run` will, and checks
// that its secrets can be read and the hosts it may call resolve to
// addresses it may reach.
func checkPlugins(ctx context.Context, spec *compiler.RuntimeSpec, secretsFrom secretBackend) []pluginCheck {
	var out []pluginCheck
	allow, _ := netguard.ParseAllow(os.Getenv("TURGON_PLUGIN_NETWORK_ALLOW"))
	guard := netguard.Guard{Allow: allow}
	for _, d := range spec.Spec.Plugins {
		if d.Type != v1alpha1.PluginLogic || d.Limits == nil {
			continue
		}
		where := "plugin " + d.Name
		add := func(r connector.CheckResult) { out = append(out, pluginCheck{where, r}) }
		m, err := plugin.Load(ctx, d.Module, plugin.Limits{MemoryMB: d.Limits.MemoryMB, Timeout: time.Duration(d.Limits.TimeoutMs) * time.Millisecond})
		if err != nil {
			add(connector.Fail("module", err.Error(), "Rebuild the plugin for "+strings.Join(plugin.Worlds, " or ")+" and compile the spec again."))
			continue
		}
		add(connector.Pass("module", fmt.Sprintf("%s, %d KiB, imports %s", m.World, (len(d.Module)+1023)/1024, strings.Join(m.Imports, ", "))))
		m.Close(ctx)
		for _, h := range slices.Sorted(maps.Keys(d.Secrets)) {
			if _, err := secretsFrom.Resolve(ctx, d.Secrets[h]); err != nil {
				add(connector.Fail("secret "+h, err.Error(), secretsFrom.Missing(d.Secrets[h])))
			} else {
				add(connector.Pass("secret "+h, secretsFrom.Where(d.Secrets[h])))
			}
		}
		if n := d.Grants.Network; n != nil {
			for _, host := range n.Allow {
				if err := guard.CheckURL(ctx, "https://"+host+"/"); err != nil {
					add(connector.Fail("network "+host, err.Error(), "Plugins call public addresses only; list internal ranges in TURGON_PLUGIN_NETWORK_ALLOW."))
				} else {
					add(connector.Pass("network "+host, "resolves to addresses the plugin may reach"))
				}
			}
		}
	}
	return out
}
