package main

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/fduser123-coding/turgon/pkg/identity"
	"github.com/fduser123-coding/turgon/pkg/store/pgstore"
)

func identityCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "identity", Short: "Work with identity matching"}
	var dbURL, entity, by string
	var threshold float64
	var dryRun, force bool
	train := &cobra.Command{
		Use:   "train",
		Short: "Train the entity's matching model on the links stewards confirmed",
		Long: `Estimates how much each comparison (same email, same company domain, same or
similar name, same exact field) says about two records being the same entity,
from this deployment's own links: records linked to one master record are
matches, records of different ones are not. Estimates lean on the default
model where there is little data.

Some master records are held out; both models are evaluated on them at the
auto-match threshold, and the trained model is stored only if it is at least
as precise (--force stores it anyway). Workers use it for the entity's next
resolve steps. The training is recorded in the audit log.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			pool, err := pgxpool.New(ctx, dbURL)
			if err != nil {
				return err
			}
			defer pool.Close()
			if err := pgstore.Migrate(ctx, pool); err != nil {
				return err
			}
			st := pgstore.New(pool)
			records, err := st.LinkedRecords(ctx, entity)
			if err != nil {
				return err
			}
			if len(records) < 2 {
				return fmt.Errorf("%s has %d linked record(s) with attributes; link more before training", entity, len(records))
			}
			model, rep := identity.Train(records, identity.TrainOptions{Threshold: threshold})

			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "%s: %d records of %d master records; %d matching and %d non-matching training pairs\n\n",
				entity, rep.Records, rep.Masters, rep.Matching, rep.NonMatching)
			tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
			fmt.Fprintln(tw, "level\tm default\tm trained\tu default\tu trained\tpairs (match/non)")
			for _, l := range rep.Levels {
				fmt.Fprintf(tw, "%s\t%.3f\t%.3f\t%.4f\t%.4f\t%d/%d\n", l.Level, l.Default.M, l.Trained.M, l.Default.U, l.Trained.U, l.Matching, l.NonMatch)
			}
			_ = tw.Flush()
			fmt.Fprintf(out, "\nheld out: %d matching, %d non-matching pairs; at threshold %.2f:\n", rep.HeldOutMatching, rep.HeldOutNonMatching, rep.Threshold)
			fmt.Fprintf(out, "  default  precision %.3f  recall %.3f  false matches %d\n", rep.Default.Precision, rep.Default.Recall, rep.Default.FalseMatches)
			fmt.Fprintf(out, "  trained  precision %.3f  recall %.3f  false matches %d\n", rep.Trained.Precision, rep.Trained.Recall, rep.Trained.FalseMatches)

			if dryRun {
				fmt.Fprintln(out, "\ndry run: not stored")
				return nil
			}
			if rep.HeldOutMatching+rep.HeldOutNonMatching > 0 && rep.Trained.Precision < rep.Default.Precision && !force {
				return fmt.Errorf("the trained model is less precise than the default on held-out records; not stored (--force stores it anyway)")
			}
			if err := st.PutMatchModel(ctx, entity, model, rep, by); err != nil {
				return err
			}
			if _, err := pgstore.NewAuditLog(pool).Record(by, "identity.model.trained", map[string]any{
				"entity": entity, "records": rep.Records, "masters": rep.Masters,
				"default": rep.Default, "trained": rep.Trained, "threshold": rep.Threshold, "forced": force,
			}); err != nil {
				return err
			}
			fmt.Fprintf(out, "\nstored: workers use it for %s from their next resolve step\n", entity)
			return nil
		},
	}
	train.Flags().StringVar(&dbURL, "database-url", os.Getenv("TURGON_DATABASE_URL"), "Postgres URL for Turgon's state")
	train.Flags().StringVar(&entity, "entity", "Customer", "entity whose links to train on")
	train.Flags().Float64Var(&threshold, "threshold", 0.95, "auto-match threshold to evaluate at (a recipe's autoMatchAbove)")
	train.Flags().BoolVar(&dryRun, "dry-run", false, "report without storing the model")
	train.Flags().BoolVar(&force, "force", false, "store the model even if it is less precise than the default")
	train.Flags().StringVar(&by, "by", os.Getenv("USER"), "who trains, for the audit log")
	cmd.AddCommand(train)
	return cmd
}
