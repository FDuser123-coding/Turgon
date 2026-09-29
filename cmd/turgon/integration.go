package main

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	"github.com/fduser123-coding/turgon/pkg/compiler"
)

var k8sName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// integrationCmd prints the Integration that runs a compiled spec under the
// Turgon operator, for kubectl apply.
func integrationCmd() *cobra.Command {
	var (
		specPath, name, namespace, host string
		replicas                        int
		webhooks                        bool
	)
	cmd := &cobra.Command{
		Use:   "integration",
		Short: "Print the Integration (for the Turgon operator) that runs a compiled spec",
		Example: `  turgon compile -c catalog shop-orders-to-erp -o shop.json --sign-key signing.key
  turgon integration -s shop.json | kubectl apply -n turgon -f -`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			data, err := os.ReadFile(specPath)
			if err != nil {
				return err
			}
			rt, err := compiler.ParseSpec(data)
			if err != nil {
				return fmt.Errorf("%s: %w", specPath, err)
			}
			if name == "" {
				name = rt.Metadata.Name
			}
			if !k8sName.MatchString(name) || len(name) > 48 {
				return fmt.Errorf("%q cannot name an Integration: lower-case letters, digits and -, at most 48", name)
			}
			meta := map[string]any{"name": name}
			if namespace != "" {
				meta["namespace"] = namespace
			}
			spec := map[string]any{"runtimeSpec": json.RawMessage(data)}
			if replicas >= 0 {
				spec["replicas"] = replicas
			}
			if webhooks || host != "" {
				w := map[string]any{"enabled": true}
				if host != "" {
					w["host"] = host
				}
				spec["webhooks"] = w
			}
			doc, err := json.Marshal(map[string]any{"apiVersion": "turgon.dev/v1alpha1", "kind": "Integration", "metadata": meta, "spec": spec})
			if err != nil {
				return err
			}
			out, err := yaml.JSONToYAML(doc)
			if err != nil {
				return err
			}
			_, err = cmd.OutOrStdout().Write(out)
			return err
		},
	}
	cmd.Flags().StringVarP(&specPath, "spec", "s", "runtime-spec.json", "compiled runtime spec")
	cmd.Flags().StringVar(&name, "name", "", "the Integration's name (default: the spec's)")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace")
	cmd.Flags().IntVar(&replicas, "replicas", -1, "workers (default: the operator's)")
	cmd.Flags().BoolVar(&webhooks, "webhooks", false, "receive the spec's webhook events (a Service in front of the workers)")
	cmd.Flags().StringVar(&host, "webhook-host", "", "also route the webhook paths from an Ingress for this host")
	return cmd
}
