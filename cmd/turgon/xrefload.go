package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/catalog"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/identity"
	"github.com/fduser123-coding/turgon/pkg/store/pgstore"
)

// xrefLoadCmd links a system's records to their master records in bulk,
// from an export the connection defines (Salesforce's Bulk API): at
// go-live, every account that carries its ERP customer number is linked at
// once, instead of each waiting for a data steward.
func xrefLoadCmd() *cobra.Command {
	var catalogs, match []string
	var dbURL, entity, sourceField, masterField, by string
	var dryRun, replace bool
	var batchSize int
	cmd := &cobra.Command{
		Use:   "load CONNECTION EXPORT",
		Short: "Link a system's records to master records in bulk, from a connection's export",
		Long: "Read the connection's export (for Salesforce, a Bulk API query job) and link each\n" +
			"record to the master record named in one of its fields. Records already linked to\n" +
			"another master record are reported and left alone unless --replace is given.\n" +
			"With --match, the records' identifying fields are kept for matching later records.",
		Example: "  turgon xref load salesforce-prod Account.ERPNumbers -c my-catalog --entity Customer \\\n" +
			"    --master ERP_Customer_Number__c --match Name:name --dry-run",
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			out := cmd.OutOrStdout()
			system, export := args[0], args[1]
			fields, err := parseMatch(match)
			if err != nil {
				return err
			}
			cat, err := catalog.Load(catalogs...)
			if err != nil {
				return err
			}
			cfg, err := compiler.ConnectionConfig(cat, system)
			if err != nil {
				return err
			}
			registry, err := connectorRegistry()
			if err != nil {
				return err
			}
			factory, ok := registry[cfg.Name]
			if !ok {
				return fmt.Errorf("connector %s is not built into this binary", cfg.Name)
			}
			secretsFrom, err := openSecrets()
			if err != nil {
				return err
			}
			inst, err := factory(ctx, cfg, secretsFrom)
			if err != nil {
				return err
			}
			defer inst.Close()
			exp, ok := inst.(connector.Exporter)
			if !ok {
				return fmt.Errorf("connection %s: the %s connector cannot export records", system, cfg.Name)
			}
			pool, err := pgxpool.New(ctx, dbURL)
			if err != nil {
				return err
			}
			defer pool.Close()
			if err := pgstore.Migrate(ctx, pool); err != nil {
				return err
			}
			store := pgstore.New(pool)

			var total pgstore.XrefCounts
			var batch []pgstore.XrefLink
			var noSource, noMaster int
			digest := sha256.New()
			flush := func() error {
				if len(batch) == 0 {
					return nil
				}
				c, err := store.LinkMany(ctx, entity, system, batch, replace, dryRun)
				if err != nil {
					return err
				}
				total.Added += c.Added
				total.Replaced += c.Replaced
				total.Unchanged += c.Unchanged
				total.Conflicts = append(total.Conflicts, c.Conflicts...)
				conflicted := map[string]bool{}
				for _, cf := range c.Conflicts {
					conflicted[cf.Source] = true
				}
				for _, l := range batch {
					if !conflicted[l.Source] {
						fmt.Fprintf(digest, "%s\t%s\n", l.Source, l.Master)
					}
				}
				batch = batch[:0]
				return nil
			}
			checked := false
			read, err := exp.Export(ctx, export, func(rec map[string]string) error {
				if !checked {
					for _, f := range []string{sourceField, masterField} {
						if _, ok := rec[f]; !ok {
							return fmt.Errorf("export %s has no field %s (it has %s)", export, f, strings.Join(keys(rec), ", "))
						}
					}
					checked = true
				}
				src, master := rec[sourceField], strings.TrimSpace(rec[masterField])
				switch {
				case src == "":
					noSource++
					return nil
				case master == "":
					noMaster++
					return nil
				}
				doc := make(map[string]any, len(rec))
				for k, v := range rec {
					doc[k] = v
				}
				batch = append(batch, pgstore.XrefLink{Source: src, Master: master, Attributes: identity.Extract(doc, fields)})
				if len(batch) >= batchSize {
					return flush()
				}
				return nil
			})
			if err == nil {
				err = flush()
			}
			if err != nil {
				return fmt.Errorf("after %d records: %w", read, err)
			}

			verb := "linked"
			if dryRun {
				verb = "would link"
			}
			fmt.Fprintf(out, "read %d %s records from %s (%s)\n", read, entity, system, export)
			fmt.Fprintf(out, "  %s %d new, %d already linked", verb, total.Added, total.Unchanged)
			if replace {
				fmt.Fprintf(out, ", %d moved to a new master record", total.Replaced)
			}
			fmt.Fprintln(out)
			if noMaster > 0 || noSource > 0 {
				fmt.Fprintf(out, "  skipped %d without %s, %d without %s\n", noMaster, masterField, noSource, sourceField)
			}
			if n := len(total.Conflicts); n > 0 {
				fmt.Fprintf(out, "  %d already linked to another master record, left alone (--replace moves them):\n", n)
				sort.Slice(total.Conflicts, func(i, j int) bool { return total.Conflicts[i].Source < total.Conflicts[j].Source })
				for i, c := range total.Conflicts {
					if i == 10 {
						fmt.Fprintf(out, "    ... and %d more\n", n-10)
						break
					}
					fmt.Fprintf(out, "    %s: linked to %s, the export says %s\n", c.Source, c.Linked, c.Proposed)
				}
			}
			if dryRun {
				return nil
			}
			// A load decides which master records later writes go to, like a
			// steward's links: audited, with a digest of what it linked.
			_, err = pgstore.NewAuditLog(pool).Record(by, "xref.loaded", map[string]any{
				"entity": entity, "system": system, "export": export, "records": read,
				"added": total.Added, "replaced": total.Replaced, "unchanged": total.Unchanged, "conflicts": len(total.Conflicts),
				"skipped": noMaster + noSource, "linksSha256": hex.EncodeToString(digest.Sum(nil)),
			})
			return err
		},
	}
	cmd.Flags().StringSliceVarP(&catalogs, "catalog", "c", []string{"."}, "catalog directories")
	cmd.Flags().StringVar(&dbURL, "database-url", os.Getenv("TURGON_DATABASE_URL"), "Postgres URL for Turgon's state")
	cmd.Flags().StringVar(&entity, "entity", "", "semantic entity, e.g. Customer")
	cmd.Flags().StringVar(&sourceField, "source", "Id", "the export's field holding the record ID in the system")
	cmd.Flags().StringVar(&masterField, "master", "", "the export's field holding the master record ID (records without one are skipped)")
	cmd.Flags().StringSliceVar(&match, "match", nil, "FIELD:KIND identifying fields to keep for matching (kind email, domain, name or exact)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "read and compare, write nothing")
	cmd.Flags().BoolVar(&replace, "replace", false, "move records linked to another master record")
	cmd.Flags().IntVar(&batchSize, "batch", 5000, "links written per transaction")
	cmd.Flags().StringVar(&by, "by", os.Getenv("USER"), "who loads, for the audit log")
	_ = cmd.MarkFlagRequired("entity")
	_ = cmd.MarkFlagRequired("master")
	return cmd
}

func parseMatch(specs []string) ([]v1alpha1.MatchField, error) {
	var out []v1alpha1.MatchField
	for _, s := range specs {
		field, kind, ok := strings.Cut(s, ":")
		switch {
		case !ok || field == "":
			return nil, fmt.Errorf("--match %q: want FIELD:KIND", s)
		case kind != v1alpha1.MatchEmail && kind != v1alpha1.MatchDomain && kind != v1alpha1.MatchName && kind != v1alpha1.MatchExact:
			return nil, fmt.Errorf("--match %q: kind must be email, domain, name or exact", s)
		}
		out = append(out, v1alpha1.MatchField{Field: field, Kind: kind})
	}
	return out, nil
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
