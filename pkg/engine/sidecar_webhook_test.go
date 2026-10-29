package engine

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/remote"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// startJava runs a Java start script and returns the first stdout line
// that ready accepts.
func startJava(t *testing.T, env []string, ready func(string) bool, bin string, args ...string) string {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })
	lines := make(chan string)
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	timeout := time.After(60 * time.Second)
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatalf("%s exited", bin)
			}
			if ready(line) {
				go func() {
					for range lines {
					}
				}()
				return line
			}
		case <-timeout:
			t.Fatalf("%s did not start", bin)
		}
	}
}

// The whole webhook path through a Camel/Java sidecar: Stripe's signed
// invoice.paid delivery reaches the worker's listener, the stripe sidecar
// verifies it, the inbox stores it, the dispatcher starts the run without
// polling Stripe, and the run records the ERP payment and links it on the
// invoice. Set TURGON_TEST_CONNECTOR_STRIPE and TURGON_TEST_FAKE_STRIPE
// (connectors: gradle :stripe:installDist :stripe-fake:installDist).
func TestSidecarWebhooksStartRuns(t *testing.T) {
	bin, fake := os.Getenv("TURGON_TEST_CONNECTOR_STRIPE"), os.Getenv("TURGON_TEST_FAKE_STRIPE")
	if bin == "" || fake == "" {
		t.Skip("TURGON_TEST_CONNECTOR_STRIPE and TURGON_TEST_FAKE_STRIPE are not set")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	line := startJava(t, nil, func(s string) bool { return strings.HasPrefix(s, "customer ") }, fake,
		"--port", fmt.Sprint(port), "--key", "rk_test_hooks000000", "--customer", "ada@example.com=Ada Lovelace GmbH")
	customer := strings.Fields(line)[1]
	stripeURL := fmt.Sprintf("http://127.0.0.1:%d", port)

	sock := filepath.Join(t.TempDir(), "stripe.sock")
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), "TURGON_CONNECTOR_LISTEN=unix://"+sock, "TURGON_CONNECTOR_TOKEN=tok")
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })
	for i := 0; i < 600; i++ {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	pool := remote.NewPool()
	t.Cleanup(pool.Close)
	factory := pool.Factory(remote.Address{Target: "unix://" + sock, Token: "tok"})
	secrets := connector.StaticSecrets{"openbao://stripe-invoicing/restricted-key": "rk_test_hooks000000",
		"openbao://stripe-invoicing/webhook-secret": "whsec_hooks"}

	f := newFixtureReg(t, "stripe-invoicing-payments-to-erp", secrets, true, connector.Registry{"stripe": factory},
		func(cfg string) string {
			if strings.Contains(cfg, `"metadataKey"`) {
				return `{"url":"` + stripeURL + `",` + strings.TrimPrefix(cfg, "{")
			}
			return cfg
		})
	if _, ok := f.rt.Webhooks["stripe-invoicing"]["Invoice.Paid"]; !ok {
		t.Fatalf("the worker does not take Invoice.Paid by webhook: %v", f.rt.Webhooks)
	}
	ctx := context.Background()
	if err := f.store.PutXref(ctx, "Customer", "stripe-invoicing", customer, "C-100"); err != nil {
		t.Fatal(err)
	}
	h := newHooked(t, f)
	if runs := h.poll(); len(runs) != 0 { // the first pass reconciles: nothing paid yet
		t.Fatalf("runs = %d", len(runs))
	}

	// An invoice is created and paid in Stripe (through a second instance,
	// as another integration would).
	var cfg compiler.ConnectorConfig
	for _, c := range f.rt.Spec.Spec.Connectors {
		if c.Endpoint == "stripe-invoicing" {
			cfg = c
		}
	}
	other, err := factory(ctx, cfg, secrets)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	res, err := other.Commit(ctx, "create-invoice", "SO-9", json.RawMessage(`{"customerId":"`+customer+`","currency":"EUR","lines":[{"description":"Widget","amount":"49.90","quantity":1}]}`))
	if err != nil {
		t.Fatal(err)
	}
	var inv struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(res, &inv)
	if resp, err := http.Post(stripeURL+"/fake/pay?invoice="+inv.ID, "", nil); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("pay: %v %v", resp, err)
	}
	// What Stripe's webhook endpoint would send: the event, signed.
	evs, err := other.(connector.Source).Poll(ctx, "Invoice.Paid", 0, 10)
	if err != nil || len(evs) != 1 {
		t.Fatalf("event %+v %v", evs, err)
	}
	body := []byte(evs[0].Payload)
	sign := func(secret string, at time.Time) http.Header {
		mac := hmac.New(sha256.New, []byte(secret))
		fmt.Fprintf(mac, "%d.%s", at.Unix(), body)
		return http.Header{"Stripe-Signature": {fmt.Sprintf("t=%d,v1=%x", at.Unix(), mac.Sum(nil))}}
	}
	url := h.srv.URL + "/webhooks/stripe-invoicing/Invoice.Paid"
	if code, _ := post(t, url, sign("whsec_guess", time.Now()), body); code != http.StatusUnauthorized {
		t.Fatalf("forged: %d", code)
	}
	if code, _ := post(t, url, sign("whsec_hooks", time.Now().Add(-time.Hour)), body); code != http.StatusUnauthorized {
		t.Fatalf("replayed: %d", code)
	}
	if code, msg := post(t, url, sign("whsec_hooks", time.Now()), body); code != http.StatusOK || !strings.Contains(msg, `"new":1`) {
		t.Fatalf("signed: %d %s", code, msg)
	}
	if code, msg := post(t, url, sign("whsec_hooks", time.Now()), body); code != http.StatusOK || !strings.Contains(msg, `"new":0`) {
		t.Fatalf("redelivered: %d %s", code, msg)
	}

	// The clock has not reached the next reconcile: the run comes from the inbox.
	runs := h.poll()
	if len(runs) != 1 || runs[0].Event.ID != evs[0].ID {
		t.Fatalf("runs %+v", runs)
	}
	out, err, _ := h.run(runs[0])
	if err != nil || len(out.Writes) != 2 || out.Writes[0].Status != writeguard.StatusCommitted || out.Writes[1].Status != writeguard.StatusCommitted {
		t.Fatalf("run: %v %+v", err, out.Writes)
	}
	if h.payments(`external_id = '`+inv.ID+`' AND amount = 49.90`) != 1 {
		t.Fatal("no ERP payment for the invoice")
	}
	resp, err := http.Get(stripeURL + "/fake/invoices/" + inv.ID)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(b), "erp_payment_id") {
		t.Fatalf("the invoice is not linked: %s", b)
	}
	// The reconciling poll reads the same event again: no second run.
	h.clock = h.clock.Add(DefaultReconcile + time.Second)
	if runs := h.poll(); len(runs) != 0 {
		t.Fatalf("restarted %d", len(runs))
	}
}
