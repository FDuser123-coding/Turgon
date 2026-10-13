package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/catalog"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/meta"
	"github.com/fduser123-coding/turgon/pkg/store/pgstore"
)

// discovery is what `turgon discover` found at one endpoint.
type discovery struct {
	Endpoint  string `json:"endpoint"`
	Connector string `json:"connector"`
	Error     string `json:"error,omitempty"`
	Objects   int    `json:"objects"`
	Fields    int    `json:"fields"`
	// Snapshot is the stored snapshot; Created, whether this discovery
	// added it (the endpoint changed, or was never discovered).
	Snapshot *pgstore.Snapshot `json:"snapshot,omitempty"`
	Created  bool              `json:"created,omitempty"`
	// Previous is the snapshot the changes are measured from.
	Previous *pgstore.Snapshot `json:"previous,omitempty"`
	Changes  []meta.Change     `json:"changes,omitempty"`
	// Missing are objects and fields the configuration uses that the
	// system lacks.
	Missing []meta.Missing `json:"missing,omitempty"`
	Catalog meta.Catalog   `json:"-"`
}

func discoverCmd() *cobra.Command {
	var specPath, dbURL string
	var endpoints, objects, catalogs []string
	var asJSON, failOnBreaking bool
	cmd := &cobra.Command{
		Use:   "discover",
		Short: "Read what each connected system holds, store it, and show what changed",
		Long: "Discover asks each endpoint's connector what the system holds (tables, sObjects, entity\n" +
			"sets, BAPIs, IDoc segments, with their fields), stores it as a snapshot in Turgon's\n" +
			"database, and lists the changes since the previous snapshot, with what uses each one:\n" +
			"the connector's operations and the spec's mappings. A breaking change something uses\n" +
			"(a removed field, a shorter or newly required one) is flagged; --fail-on-breaking exits\n" +
			"non-zero on one, for a scheduled job or a pipeline.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			spec, err := loadSpec(specPath)
			if err != nil {
				return err
			}
			extra := map[string][]string{}
			for _, o := range objects {
				ep, name, ok := strings.Cut(o, "=")
				if !ok || ep == "" || name == "" {
					return fmt.Errorf("--object %q: want endpoint=object", o)
				}
				extra[ep] = append(extra[ep], name)
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Minute)
			defer cancel()
			var store *pgstore.Store
			if dbURL != "" {
				pool, err := pgxpool.New(ctx, dbURL)
				if err != nil {
					return err
				}
				defer pool.Close()
				if err := pgstore.Migrate(ctx, pool); err != nil {
					return fmt.Errorf("state database: %w", err)
				}
				store = pgstore.New(pool)
			}
			secretsFrom, err := openSecrets()
			if err != nil {
				return err
			}
			registry, err := connectorRegistry()
			if err != nil {
				return err
			}
			others, err := sketches(catalogs)
			if err != nil {
				return err
			}
			results, err := discoverAll(ctx, spec, registry, secretsFrom, store, endpoints, extra, others...)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			if asJSON {
				enc := json.NewEncoder(out)
				enc.SetIndent("", "  ")
				if err := enc.Encode(results); err != nil {
					return err
				}
			} else {
				printDiscoveries(out, results, store != nil)
			}
			failed := false
			for _, r := range results {
				if r.Error != "" || (failOnBreaking && len(r.Missing) > 0) {
					failed = true
				}
				for _, c := range r.Changes {
					failed = failed || (failOnBreaking && c.Breaks())
				}
			}
			if failed {
				return errSilent
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&specPath, "spec", "s", "runtime-spec.json", "compiled runtime spec")
	cmd.Flags().StringVar(&dbURL, "database-url", envOr("TURGON_DATABASE_URL", ""), "Turgon's state database, where snapshots are kept (without it, nothing is stored or compared)")
	cmd.Flags().StringArrayVar(&endpoints, "endpoint", nil, "discover only this endpoint (repeatable)")
	cmd.Flags().StringArrayVar(&objects, "object", nil, "also discover an object the configuration does not use, as endpoint=object (repeatable; e.g. erp=public.invoices, erp=public.*, sap-ecc=IDOC:ORDERS05)")
	cmd.Flags().StringSliceVarP(&catalogs, "catalog", "c", nil, "also count the mappings of every recipe in these catalogs as users, compiled or not")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print the results as JSON")
	cmd.Flags().BoolVar(&failOnBreaking, "fail-on-breaking", false, "exit non-zero when a change can break a run, or a used object is missing")
	return cmd
}

// sketches are the recipes of catalogs, as far as their mappings go.
func sketches(dirs []string) ([]*compiler.RuntimeSpec, error) {
	if len(dirs) == 0 {
		return nil, nil
	}
	cat, err := catalog.Load(dirs...)
	if err != nil {
		return nil, err
	}
	var out []*compiler.RuntimeSpec
	for _, obj := range cat.All() {
		if rec, ok := obj.(*v1alpha1.Recipe); ok {
			out = append(out, compiler.Sketch(cat, rec))
		}
	}
	return out, nil
}

// discoverAll discovers each selected endpoint whose connector can, and
// compares what it holds with its latest snapshot.
func discoverAll(ctx context.Context, spec *compiler.RuntimeSpec, registry connector.Registry, secretsFrom connector.SecretResolver,
	store *pgstore.Store, only []string, extra map[string][]string, others ...*compiler.RuntimeSpec) ([]discovery, error) {
	specs := append([]*compiler.RuntimeSpec{spec}, others...)
	want := map[string]bool{}
	for _, e := range only {
		want[e] = true
	}
	for e := range extra {
		if !slices.ContainsFunc(spec.Spec.Connectors, func(c compiler.ConnectorConfig) bool { return c.Endpoint == e }) {
			return nil, fmt.Errorf("--object %s=...: the spec has no such endpoint", e)
		}
	}
	conns := append([]compiler.ConnectorConfig(nil), spec.Spec.Connectors...)
	sort.Slice(conns, func(i, j int) bool { return conns[i].Endpoint < conns[j].Endpoint })
	var out []discovery
	for _, c := range conns {
		if len(only) > 0 && !want[c.Endpoint] {
			continue
		}
		delete(want, c.Endpoint)
		d := discovery{Endpoint: c.Endpoint, Connector: c.Name}
		cat, err := discoverOne(ctx, c, registry, secretsFrom, extra[c.Endpoint])
		if err != nil {
			d.Error = err.Error()
			out = append(out, d)
			continue
		}
		d.Catalog = cat
		d.Objects = len(cat.Objects)
		for _, o := range cat.Objects {
			d.Fields += len(o.Fields)
		}
		d.Missing = meta.MissingUses(cat)
		if store != nil {
			prev, had, err := store.LatestCatalog(ctx, c.Endpoint)
			if err != nil {
				return nil, err
			}
			snap, created, err := store.SaveCatalog(ctx, cat)
			if err != nil {
				return nil, err
			}
			d.Snapshot, d.Created = &snap, created
			if had && created {
				if list, err := store.Snapshots(ctx, c.Endpoint); err == nil && len(list) > 1 {
					d.Previous = &list[1]
				}
				d.Changes = meta.Compare(prev, cat, specs...)
			}
		}
		out = append(out, d)
	}
	for e := range want {
		return nil, fmt.Errorf("--endpoint %s: the spec has no such endpoint", e)
	}
	return out, nil
}

func discoverOne(ctx context.Context, c compiler.ConnectorConfig, registry connector.Registry, secretsFrom connector.SecretResolver, objects []string) (meta.Catalog, error) {
	factory, ok := registry[c.Name]
	if !ok {
		return meta.Catalog{}, fmt.Errorf("no runtime for connector %s in this worker", c.Name)
	}
	inst, err := factory(ctx, c, secretsFrom)
	if err != nil {
		return meta.Catalog{}, err
	}
	defer inst.Close()
	d, ok := inst.(connector.Discoverer)
	if !ok {
		return meta.Catalog{}, fmt.Errorf("connector %s cannot discover", c.Name)
	}
	cat, err := d.Discover(ctx, objects)
	if err != nil {
		return meta.Catalog{}, err
	}
	cat.Endpoint, cat.Connector, cat.Version = c.Endpoint, c.Name, c.Version
	cat.Normalize()
	return cat, nil
}

func printDiscoveries(w io.Writer, rs []discovery, stored bool) {
	for i, r := range rs {
		if i > 0 {
			fmt.Fprintln(w)
		}
		if r.Error != "" {
			fmt.Fprintf(w, "%s (%s): FAIL %s\n", r.Endpoint, r.Connector, r.Error)
			continue
		}
		fmt.Fprintf(w, "%s (%s %s): %d objects, %d fields", r.Endpoint, r.Connector, r.Catalog.Version, r.Objects, r.Fields)
		switch {
		case r.Snapshot == nil:
			fmt.Fprintln(w)
		case !r.Created:
			fmt.Fprintf(w, "; unchanged since snapshot %d (%s)\n", r.Snapshot.ID, r.Snapshot.DiscoveredAt.Format(time.RFC3339))
		case r.Previous == nil:
			fmt.Fprintf(w, "; first snapshot %d\n", r.Snapshot.ID)
		default:
			fmt.Fprintf(w, "; changed: snapshot %d, previous %d (%s)\n", r.Snapshot.ID, r.Previous.ID, r.Previous.DiscoveredAt.Format(time.RFC3339))
		}
		for _, m := range r.Missing {
			fmt.Fprintf(w, "  MISSING   %s\n", m)
		}
		for _, c := range r.Changes {
			mark := "          "
			switch {
			case c.Breaks():
				mark = "  BREAKING"
			case c.Kind == meta.FieldNotSeen && len(c.UsedBy) > 0:
				mark = "  CHECK   " // a sampled field the latest records lack
			case c.Breaking:
				mark = "  breaking"
			}
			fmt.Fprintf(w, "%s %s", mark, c)
			if len(c.UsedBy) > 0 {
				fmt.Fprintf(w, "; used by %s", strings.Join(c.UsedBy, ", "))
			} else if c.Breaking {
				fmt.Fprint(w, "; nothing here uses it")
			}
			fmt.Fprintln(w)
		}
	}
	if !stored {
		fmt.Fprintln(w, "\nno --database-url: snapshots were not stored, and nothing was compared")
	}
}
