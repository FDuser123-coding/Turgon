package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	corev1 "k8s.io/api/core/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"

	turgonv1 "github.com/fduser123-coding/turgon/apis/operator/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/operator"
	"github.com/fduser123-coding/turgon/pkg/signing"
)

// validateManifest checks what a chart rendered for the operator: the
// worker template loads strictly and every Integration's inline spec
// passes the checks the operator makes before a rollout.
func validateManifest(path, trustedKeys string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	cfg := &operator.Config{}
	if trustedKeys != "" {
		pem, err := os.ReadFile(trustedKeys)
		if err != nil {
			return err
		}
		if cfg.TrustedKeys, err = signing.ParsePublicKeys(pem); err != nil {
			return err
		}
	}
	templates, integrations := 0, 0
	dec := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(data)))
	for {
		doc, err := dec.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		var head struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		}
		if err := yaml.Unmarshal(doc, &head); err != nil {
			return err
		}
		switch head.Kind {
		case "ConfigMap":
			var cm corev1.ConfigMap
			if err := yaml.Unmarshal(doc, &cm); err != nil {
				return err
			}
			if t, ok := cm.Data["worker-template.yaml"]; ok {
				if _, err := operator.LoadTemplate([]byte(t)); err != nil {
					return fmt.Errorf("configmap %s: %w", head.Metadata.Name, err)
				}
				templates++
			}
		case "Integration":
			var in turgonv1.Integration
			if err := yaml.UnmarshalStrict(doc, &in); err != nil {
				return fmt.Errorf("integration %s: %w", head.Metadata.Name, err)
			}
			if in.Spec.RuntimeSpec == nil {
				continue
			}
			v, err := cfg.Verify(in.Spec.RuntimeSpec.Raw)
			if err != nil {
				return fmt.Errorf("integration %s: %w", head.Metadata.Name, err)
			}
			fmt.Printf("ok  integration %s: %s level %s\n", in.Name, v.Spec.Metadata.Digest, v.Spec.Metadata.Level)
			integrations++
		}
	}
	if templates != 1 {
		return fmt.Errorf("%d worker templates, want 1", templates)
	}
	fmt.Printf("ok  worker template, %d integration(s)\n", integrations)
	return nil
}
