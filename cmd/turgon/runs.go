package main

import (
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/fduser123-coding/turgon/pkg/console"
)

// runsCmd lists recent runs, as the console's Runs page does: their status,
// whether one waits for a person, and why a run ended without writing.
func runsCmd() *cobra.Command {
	var tf temporalFlags
	var limit int
	var skipped bool
	cmd := &cobra.Command{
		Use:   "runs",
		Short: "List recent runs: status, approvals waiting, and why a run was skipped",
		Long: "Runs lists the most recent integration runs, newest first. A completed run that\n" +
			"wrote nothing because its record has no cross-reference (a resolve step with\n" +
			"ifUnknown: skip, such as an order made in the ERP by hand) shows why. --skipped\n" +
			"lists only those, with how many there were.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, err := tf.dial()
			if err != nil {
				return err
			}
			defer c.Close()
			runs, err := console.TemporalRuns{Client: c, Namespace: tf.namespace}.List(cmd.Context(), limit)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			n := 0
			w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
			fmt.Fprintln(w, "STATUS\tRUN\tSTARTED\tNOTE")
			for _, r := range runs {
				if skipped && r.Skipped == "" {
					continue
				}
				status, note := r.Status, ""
				switch {
				case r.Skipped != "":
					status, note = "skipped", r.Skipped
					n++
				case r.Pending != nil:
					note = "awaiting approval of " + r.Pending.Step
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", status, r.ID, r.Started.UTC().Format(time.RFC3339), note)
			}
			if err := w.Flush(); err != nil {
				return err
			}
			if skipped {
				fmt.Fprintf(out, "%d of the last %d run(s) were skipped\n", n, len(runs))
			}
			return nil
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 100, "how many recent runs to read")
	cmd.Flags().BoolVar(&skipped, "skipped", false, "only the runs that were skipped, with their count")
	tf.register(cmd)
	return cmd
}
