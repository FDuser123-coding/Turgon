package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/signing"
)

// trustedKeysPath is --trusted-keys: when set, loadSpec accepts only specs
// signed by one of its keys.
var trustedKeysPath string

func keygenCmd() *cobra.Command {
	var out string
	cmd := &cobra.Command{
		Use:   "keygen",
		Short: "Create an Ed25519 key pair for signing runtime specs",
		Long: `Writes OUT.key (the private key, for the pipeline that compiles specs; keep it
in a secret store) and OUT.pub (the public key, for workers' --trusted-keys).`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			priv, pub, err := signing.GenerateKey()
			if err != nil {
				return err
			}
			if _, err := os.Stat(out + ".key"); err == nil {
				return fmt.Errorf("%s.key exists; refusing to overwrite a signing key", out)
			}
			if err := os.WriteFile(out+".key", priv, 0o600); err != nil {
				return err
			}
			if err := os.WriteFile(out+".pub", pub, 0o644); err != nil {
				return err
			}
			keys, _ := signing.ParsePublicKeys(pub)
			fmt.Fprintf(cmd.OutOrStdout(), "wrote %s.key and %s.pub (key %s)\n", out, out, signing.KeyID(keys[0]))
			return nil
		},
	}
	cmd.Flags().StringVar(&out, "out", "turgon-signing", "file name prefix")
	return cmd
}

func signCmd() *cobra.Command {
	var key string
	cmd := &cobra.Command{
		Use:   "sign SPEC",
		Short: "Sign a compiled runtime spec in place",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := os.ReadFile(args[0])
			if err != nil {
				return err
			}
			var rt compiler.RuntimeSpec
			if err := json.Unmarshal(data, &rt); err != nil {
				return fmt.Errorf("%s: %w", args[0], err)
			}
			if err := signWith(&rt, key); err != nil {
				return err
			}
			f, err := os.Create(args[0])
			if err != nil {
				return err
			}
			defer f.Close()
			if err := writeJSON(f, &rt); err != nil {
				return err
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "signed %s (%s) with key %s\n", args[0], rt.Metadata.Digest, rt.Metadata.Signatures[len(rt.Metadata.Signatures)-1].KeyID)
			return nil
		},
	}
	cmd.Flags().StringVar(&key, "key", os.Getenv("TURGON_SIGNING_KEY"), "Ed25519 private key (PEM file)")
	_ = cmd.MarkFlagRequired("key")
	return cmd
}

func signWith(rt *compiler.RuntimeSpec, keyPath string) error {
	data, err := os.ReadFile(keyPath)
	if err != nil {
		return err
	}
	priv, err := signing.ParsePrivateKey(data)
	if err != nil {
		return err
	}
	return signing.Sign(rt, priv)
}

// verifySignature enforces --trusted-keys.
func verifySignature(path string, rt *compiler.RuntimeSpec) error {
	if trustedKeysPath == "" {
		return nil
	}
	data, err := os.ReadFile(trustedKeysPath)
	if err != nil {
		return fmt.Errorf("trusted keys: %w", err)
	}
	keys, err := signing.ParsePublicKeys(data)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return fmt.Errorf("trusted keys: %s holds no public key", trustedKeysPath)
	}
	if _, err := signing.Verify(rt, keys); err != nil {
		return fmt.Errorf("%s: %w; refusing to run it", path, err)
	}
	return nil
}
