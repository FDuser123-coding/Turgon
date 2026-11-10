package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/fduser123-coding/turgon/pkg/engine"
	"github.com/fduser123-coding/turgon/pkg/store/pgstore"
)

// approvalsCmd lists the open requests for approval in the ledger and
// releases one whose run ended without closing it.
func approvalsCmd() *cobra.Command {
	var tf temporalFlags
	var dbURL string
	open := func(cmd *cobra.Command) (*pgxpool.Pool, client.Client, error) {
		if dbURL == "" {
			return nil, nil, errors.New("the approval ledger is in Turgon's state database: give --database-url (TURGON_DATABASE_URL)")
		}
		pool, err := pgxpool.New(cmd.Context(), dbURL)
		if err != nil {
			return nil, nil, err
		}
		if err := pgstore.Migrate(cmd.Context(), pool); err != nil {
			pool.Close()
			return nil, nil, err
		}
		c, err := tf.dial()
		if err != nil {
			pool.Close()
			return nil, nil, fmt.Errorf("temporal: %w", err)
		}
		return pool, c, nil
	}
	cmd := &cobra.Command{
		Use:   "approvals",
		Short: "List the writes waiting for approval, by the run asking and the runs waiting on it",
		Long: "Each write waiting for a person has one request, held by the run that asked. Runs for\n" +
			"later changes of the same record wait on its decision. A request whose run ended\n" +
			"without closing it (terminated, say) is orphaned: release it with `turgon approvals\n" +
			"release` so the runs waiting on it go on.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			pool, c, err := open(cmd)
			if err != nil {
				return err
			}
			defer pool.Close()
			defer c.Close()
			held, err := pgstore.New(pool).OpenApprovals()
			if err != nil {
				return err
			}
			if len(held) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "no write waits for approval")
				return nil
			}
			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			fmt.Fprintln(w, "WRITE\tHELD BY\tSTATUS\tUNTIL\tWAITING")
			for _, h := range held {
				status := runStatus(cmd, c, h.Holder)
				if status != "running" {
					status += " (orphaned)"
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", h.Key, h.Holder, status, h.Until.UTC().Format(time.RFC3339), strings.Join(h.Waiters, ", "))
			}
			return w.Flush()
		},
	}
	var holder, by, note string
	release := &cobra.Command{
		Use:   "release WRITE --holder RUN_ID --note WHY",
		Short: "Release an orphaned request for approval and wake the runs waiting on it",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if holder == "" || strings.TrimSpace(note) == "" || by == "" {
				return errors.New("--holder (the run holding the request), --note (why) and --by are required: releases are audited")
			}
			pool, c, err := open(cmd)
			if err != nil {
				return err
			}
			defer pool.Close()
			defer c.Close()
			status := runStatus(cmd, c, holder)
			if status == "running" {
				return fmt.Errorf("%s is still running: approve or reject its write instead (turgon approve %s)", holder, holder)
			}
			// Recorded before the release, so no release is ever unaudited.
			if _, err := pgstore.NewAuditLog(pool).Record(by, "approval.released", map[string]any{
				"key": args[0], "holder": holder, "holderStatus": status, "reason": strings.TrimSpace(note)}); err != nil {
				return fmt.Errorf("audit log: %w", err)
			}
			waiters, ok, err := pgstore.New(pool).ReleaseApproval(args[0], holder)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("%s does not hold the request for %s", holder, args[0])
			}
			woken := engine.WakeWaiters(cmd.Context(), c, args[0], waiters)
			fmt.Fprintf(cmd.OutOrStdout(), "released %s (held by %s, %s); woke %d of %d waiting run(s)\n", args[0], holder, status, woken, len(waiters))
			return nil
		},
	}
	release.Flags().StringVar(&holder, "holder", "", "the run holding the request, as turgon approvals lists it")
	release.Flags().StringVar(&by, "by", os.Getenv("USER"), "operator identity, recorded in the audit log")
	release.Flags().StringVar(&note, "note", "", "why the request is released")
	cmd.AddCommand(release)
	// Both commands take the Temporal flags, bound to the same values.
	tf.register(cmd)
	tf.register(release)
	cmd.PersistentFlags().StringVar(&dbURL, "database-url", os.Getenv("TURGON_DATABASE_URL"), "Postgres URL for Turgon's state")
	return cmd
}

// runStatus is a run's status as Temporal reports it, lower-case, or
// "missing" when Temporal no longer knows it.
func runStatus(cmd *cobra.Command, c client.Client, id string) string {
	d, err := c.DescribeWorkflowExecution(cmd.Context(), id, "")
	if err != nil {
		var nf *serviceerror.NotFound
		if errors.As(err, &nf) {
			return "missing"
		}
		return "unknown (" + err.Error() + ")"
	}
	s := d.GetWorkflowExecutionInfo().GetStatus().String()
	return strings.ToLower(strings.TrimPrefix(s, "WORKFLOW_EXECUTION_STATUS_"))
}
