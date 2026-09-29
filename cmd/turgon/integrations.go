package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/fduser123-coding/turgon/pkg/catalog"
	"github.com/fduser123-coding/turgon/pkg/console"
	"github.com/fduser123-coding/turgon/pkg/verifier"
)

// integrationsCmd prints what is connected to what: the catalog's systems
// and the flows between them, step by step (the console's Integrations
// page shows the same map).
func integrationsCmd() *cobra.Command {
	var catalogs []string
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "integrations",
		Short: "Show the systems a catalog connects and the flows between them",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cat, err := catalog.Load(catalogs...)
			if err != nil {
				return err
			}
			m := console.BuildIntegrations(cat, verifier.Options{})
			out := cmd.OutOrStdout()
			if asJSON {
				enc := json.NewEncoder(out)
				enc.SetIndent("", "  ")
				return enc.Encode(m)
			}
			tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
			fmt.Fprintln(tw, "SYSTEM\tROLE\tPRODUCT\tEVENTS\tOPERATIONS")
			for _, s := range m.Systems {
				var evs []string
				for _, e := range s.Events {
					evs = append(evs, e.Name+" ("+e.Delivery+")")
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", s.Name, s.Role, s.Product, strings.Join(evs, ", "), strings.Join(s.Operations, ", "))
			}
			if err := tw.Flush(); err != nil {
				return err
			}
			fmt.Fprintln(out)
			for _, f := range m.Flows {
				state := f.Level
				if !f.Deployable {
					state += ", not deployable: " + f.Problem
				}
				fmt.Fprintf(out, "%s (%s)\n", f.Name, state)
				if f.Description != "" {
					fmt.Fprintf(out, "  %s\n", f.Description)
				}
				fmt.Fprintf(out, "  when %s emits %s (%s)\n", f.Trigger.System, f.Trigger.Event, f.Trigger.Delivery)
				for _, st := range f.Steps {
					fmt.Fprintf(out, "  -> %s\n", stepText(st))
				}
				fmt.Fprintln(out)
			}
			return nil
		},
	}
	cmd.Flags().StringSliceVarP(&catalogs, "catalog", "c", []string{"."}, "catalog directories")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print JSON (what the console's /api/integrations returns)")
	return cmd
}

func stepText(st console.FlowStep) string {
	switch st.Kind {
	case "map":
		s := "map with " + st.Mapping
		if st.Fields > 0 {
			s += fmt.Sprintf(" (%d fields)", st.Fields)
		}
		return s
	case "resolve":
		return "resolve " + st.Entity + " to its master record (" + st.Strategy + ")"
	case "write":
		s := "write " + st.Operation + " to " + st.System
		var guards []string
		if st.Risk != "" {
			guards = append(guards, st.Risk+" risk")
		}
		if st.Simulation != "" {
			guards = append(guards, "dry-run first ("+st.Simulation+")")
		}
		if st.Approval != "" && st.Approval != "none" {
			guards = append(guards, "approval: "+st.Approval)
		}
		if st.Compensation != "" {
			guards = append(guards, "undone by "+st.Compensation)
		}
		if len(guards) > 0 {
			s += " [" + strings.Join(guards, "; ") + "]"
		}
		return s
	}
	return st.Kind
}
