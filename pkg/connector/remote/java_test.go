package remote

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// sidecar starts the Camel/Java SAP ECC connector with its fake ECC
// (connectors/sap-ecc-fake, built with installDist) on a Unix socket.
func sidecar(t *testing.T, bin, sock string) *exec.Cmd {
	t.Helper()
	_ = os.Remove(sock)
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), "TURGON_CONNECTOR_LISTEN=unix://"+sock, "TURGON_CONNECTOR_TOKEN=tok")
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })
	for i := 0; i < 300; i++ {
		if _, err := os.Stat(sock); err == nil {
			return cmd
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("the sidecar did not open its socket")
	return nil
}

// TestJavaSidecar drives the Java connector over the protocol: the same
// path a worker takes. Set TURGON_TEST_CONNECTOR_SAP_ECC to the demo
// distribution's start script.
func TestJavaSidecar(t *testing.T) {
	bin := os.Getenv("TURGON_TEST_CONNECTOR_SAP_ECC")
	if bin == "" {
		t.Skip("TURGON_TEST_CONNECTOR_SAP_ECC is not set (connectors: gradle :sap-ecc-fake:installDist)")
	}
	sock := filepath.Join(t.TempDir(), "sap-ecc.sock")
	cmd := sidecar(t, bin, sock)
	pool := NewPool()
	defer pool.Close()
	ctx := context.Background()
	cfg := compiler.ConnectorConfig{Endpoint: "sap-ecc", Name: "sap-ecc", Version: "0.4.0", Runtime: "camel-java",
		SecretRef: "openbao://sap/ecc/prod", Config: json.RawMessage(`{"rfc":{"provider":"fake","destination":{"ashost":"ecc-it"}}}`)}
	inst, err := pool.Factory(Address{Target: "unix://" + sock, Token: "tok"})(ctx, cfg, connector.StaticSecrets{"openbao://sap/ecc/prod": "TURGON:pw"})
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Close()

	checks := inst.(connector.Checker).Check(ctx)
	for _, c := range checks {
		if !c.OK {
			t.Fatalf("check %+v", c)
		}
	}
	if !strings.Contains(checks[0].Detail, "sap-ecc 0.4.0") || !strings.Contains(checks[1].Detail, "fake ECC") {
		t.Fatalf("checks %+v", checks)
	}

	order := json.RawMessage(`{"customerId":"1000","orderDate":"2026-10-01","currency":"EUR","lines":[{"material":"M-7","quantity":2}]}`)
	prev, err := inst.Simulate(ctx, "create-sales-order", order)
	if err != nil || !strings.Contains(string(prev), `"testRun":true`) {
		t.Fatalf("simulate %s %v", prev, err)
	}
	res, err := inst.Commit(ctx, "create-sales-order", "006A", order)
	if err != nil || !strings.Contains(string(res), `"salesOrder":"0000012000"`) {
		t.Fatalf("commit %s %v", res, err)
	}
	if err := inst.(writeguard.Confirmer).Confirm(ctx, "create-sales-order", res); err != nil {
		t.Fatal(err)
	}
	if again, err := inst.Commit(ctx, "create-sales-order", "006A", order); err != nil || !strings.Contains(string(again), `"existing":true`) {
		t.Fatalf("retried create %s %v", again, err)
	}
	if _, err := inst.Commit(ctx, "create-sales-order", "K2", json.RawMessage(strings.Replace(string(order), `"1000"`, `"1002"`, 1))); !errors.Is(err, writeguard.ErrRejected) || !strings.Contains(err.Error(), "blocked for sales") {
		t.Fatalf("blocked customer: %v", err)
	}
	if _, err := inst.Commit(ctx, "create-sales-order", "K3", json.RawMessage(`{"customerId":"1000","lines":[]}`)); !errors.Is(err, writeguard.ErrInvalid) {
		t.Fatalf("invalid: %v", err)
	}
	rec, err := inst.(writeguard.Reader).Read(ctx, "get-customer", "1000")
	if err != nil || !strings.Contains(string(rec), "Ada Lovelace GmbH") {
		t.Fatalf("read %s %v", rec, err)
	}
	if _, err := inst.(writeguard.Reader).Read(ctx, "get-customer", "4711"); !errors.Is(err, writeguard.ErrNotFound) {
		t.Fatalf("read missing: %v", err)
	}
	if _, ok := inst.(connector.Source); ok {
		t.Fatal("sap-ecc does not poll yet")
	}

	// The sidecar restarts (and its fake ECC forgets everything): the
	// worker configures its instance again on the next call.
	_ = cmd.Process.Kill()
	_, _ = cmd.Process.Wait()
	sidecar(t, bin, sock)
	var cancelled json.RawMessage
	for i := 0; i < 50; i++ { // the connection reconnects with backoff
		if cancelled, err = inst.Commit(ctx, "create-sales-order", "006B", order); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("after the restart: %v", err)
	}
	if out, err := inst.Commit(ctx, "cancel-sales-order", "006B#compensate", cancelled); err != nil || !strings.Contains(string(out), `"deleted":true`) {
		t.Fatalf("cancel %s %v", out, err)
	}
}
