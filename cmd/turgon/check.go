package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/fduser123-coding/turgon/pkg/connector"
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

			registry := connectorRegistry()
			conns := spec.Spec.Connectors
			sort.Slice(conns, func(i, j int) bool { return conns[i].Endpoint < conns[j].Endpoint })
			for _, c := range conns {
				factory, ok := registry[c.Name]
				if !ok {
					report(c.Endpoint, connector.Fail("runtime", "no runtime for connector "+c.Name+" in this worker", "This connector runs on another worker type; check it there."))
					continue
				}
				inst, err := factory(ctx, c, connector.EnvSecrets{})
				if err != nil {
					report(c.Endpoint, connector.Fail("configure", err.Error(), fmt.Sprintf("Set %s to the secret for %s.", connector.EnvName(c.SecretRef), c.SecretRef)))
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
