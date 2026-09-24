// Command porter is the Porter CLI: validate specs, verify recipes and stack
// blueprints, compile them to runtime specs, and check audit logs.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/catalog"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/spec"
	"github.com/fduser123-coding/turgon/pkg/verifier"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

// errSilent signals a failure that has already been reported to the user.
var errSilent = errors.New("")

func main() {
	if err := newRoot(os.Stdout, os.Stderr).Execute(); err != nil {
		if err != errSilent {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
		os.Exit(1)
	}
}

func newRoot(stdout, stderr io.Writer) *cobra.Command {
	root := &cobra.Command{
		Use:           "porter",
		Short:         "Porter: the neutral, customer-side integrator",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.SetOut(stdout)
	root.SetErr(stderr)
	root.AddCommand(validateCmd(), verifyCmd(), compileCmd(), auditCmd(), versionCmd(),
		runCmd(), approveCmd(), pendingCmd(), retryCmd(), xrefCmd(), secretsCmd(), consoleCmd(), checkCmd(), mcpCmd())
	return root
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the CLI version",
		Run: func(cmd *cobra.Command, _ []string) {
			fmt.Fprintf(cmd.OutOrStdout(), "porter %s (api %s)\n", version, v1alpha1.APIVersion)
		},
	}
}

func validateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate [path...]",
		Short: "Check spec files against the schema",
		Long:  "Decode every spec file under the given paths (default: current directory) and report schema violations.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				args = []string{"."}
			}
			docs, err := spec.LoadPaths(args...)
			if err != nil {
				return err
			}
			out, bad := cmd.OutOrStdout(), 0
			for _, d := range docs {
				m := d.Object.GetMeta()
				id := d.Object.GetTypeMeta().Kind + "/" + m.Name
				if errs := d.Object.Validate(); len(errs) > 0 {
					bad++
					fmt.Fprintf(out, "FAIL  %s (%s)\n", id, d.Source)
					for _, e := range errs {
						fmt.Fprintf(out, "      %s\n", e)
					}
					continue
				}
				fmt.Fprintf(out, "ok    %s\n", id)
			}
			if _, err := catalog.FromDocuments(docs); err != nil {
				return err
			}
			fmt.Fprintf(out, "%d object(s), %d invalid\n", len(docs), bad)
			if bad > 0 {
				return errSilent
			}
			return nil
		},
	}
}

type targetFlags struct {
	catalogs []string
	asJSON   bool
	opts     verifier.Options
}

func (f *targetFlags) register(cmd *cobra.Command) {
	cmd.Flags().StringSliceVarP(&f.catalogs, "catalog", "c", []string{"."}, "catalog directories or files")
	cmd.Flags().BoolVar(&f.asJSON, "json", false, "print the report as JSON")
	cmd.Flags().Float64Var(&f.opts.ReviewThreshold, "review-threshold", 0, "mapping confidence below which fields need review (default 0.90)")
	cmd.Flags().Float64Var(&f.opts.WriteReviewThreshold, "write-review-threshold", 0, "threshold for mappings that feed writes (default 0.95)")
}

// resolve loads the catalog and finds the target, which is either an object
// name in the catalog or a spec file (whose first document is used).
func (f *targetFlags) resolve(target string) (*catalog.Catalog, v1alpha1.Object, error) {
	cat, err := catalog.Load(f.catalogs...)
	if err != nil {
		return nil, nil, err
	}
	if info, err := os.Stat(target); err == nil && !info.IsDir() {
		docs, err := spec.LoadFile(target)
		if err != nil {
			return nil, nil, err
		}
		if len(docs) == 0 {
			return nil, nil, fmt.Errorf("%s contains no objects", target)
		}
		return cat, docs[0].Object, nil
	}
	name, con := v1alpha1.ParseRef(target)
	obj, err := cat.Find(name)
	if err != nil {
		return nil, nil, err
	}
	if con != "" {
		obj, err = cat.Resolve(obj.GetTypeMeta().Kind, name, con)
	}
	return cat, obj, err
}

func verifyCmd() *cobra.Command {
	var f targetFlags
	cmd := &cobra.Command{
		Use:   "verify NAME|FILE",
		Short: "Verify a recipe, plugin or stack blueprint",
		Long: "Run the verifier stages (schema, resolve, permitted interfaces, slot contracts,\n" +
			"mappings, policy, capacity) and report the one-click level reached.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cat, obj, err := f.resolve(args[0])
			if err != nil {
				return err
			}
			rep := verifier.New(cat, f.opts).Verify(obj)
			if f.asJSON {
				if err := writeJSON(cmd.OutOrStdout(), rep); err != nil {
					return err
				}
			} else {
				printReport(cmd.OutOrStdout(), rep, "")
			}
			if !rep.Deployable {
				return errSilent
			}
			return nil
		},
	}
	f.register(cmd)
	return cmd
}

func compileCmd() *cobra.Command {
	var f targetFlags
	var output string
	cmd := &cobra.Command{
		Use:   "compile NAME|FILE",
		Short: "Verify and compile a recipe or blueprint into a runtime spec",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cat, obj, err := f.resolve(args[0])
			if err != nil {
				return err
			}
			rt, rep, err := compiler.Compile(cat, obj, f.opts)
			if err != nil {
				if rep != nil {
					printReport(cmd.ErrOrStderr(), rep, "")
				}
				return err
			}
			w := cmd.OutOrStdout()
			if output != "" && output != "-" {
				file, err := os.Create(output)
				if err != nil {
					return err
				}
				defer file.Close()
				w = file
			}
			if err := writeJSON(w, rt); err != nil {
				return err
			}
			if output != "" && output != "-" {
				fmt.Fprintf(cmd.ErrOrStderr(), "wrote %s (%s, level %s)\n", output, rt.Metadata.Digest, rt.Metadata.Level)
			}
			return nil
		},
	}
	f.register(cmd)
	cmd.Flags().StringVarP(&output, "output", "o", "", "write the runtime spec to a file instead of stdout")
	return cmd
}

func auditCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "audit", Short: "Work with audit logs"}
	cmd.AddCommand(&cobra.Command{
		Use:   "verify FILE",
		Short: "Check an audit log's hash chain",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			file, err := os.Open(args[0])
			if err != nil {
				return err
			}
			defer file.Close()
			last, err := audit.Verify(file)
			if err != nil {
				return err
			}
			if last == nil {
				fmt.Fprintln(cmd.OutOrStdout(), "empty log")
				return nil
			}
			fmt.Fprintf(cmd.OutOrStdout(), "ok: %d entries, head %s\n", last.Seq, last.Hash)
			return nil
		},
	})
	return cmd
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func printReport(w io.Writer, r *verifier.Report, indent string) {
	status := "deployable"
	if !r.Deployable {
		status = "BLOCKED"
	}
	fmt.Fprintf(w, "%s%s  level %s  %s\n", indent, r.Subject, r.Level, status)
	for _, f := range r.Findings {
		path := ""
		if f.Path != "" {
			path = " " + f.Path
		}
		fmt.Fprintf(w, "%s  %-7s %-10s%s: %s\n", indent, f.Severity, f.Stage, path, f.Message)
	}
	if len(r.ReviewQueue) > 0 {
		fmt.Fprintf(w, "%s  review queue:\n", indent)
		for _, it := range r.ReviewQueue {
			fmt.Fprintf(w, "%s    %s %s = %s (%s, %.2f)\n", indent, it.Mapping, it.Target, it.Expression, it.Origin, it.Confidence)
		}
	}
	for _, c := range r.Children {
		printReport(w, c, indent+strings.Repeat(" ", 2))
	}
}
