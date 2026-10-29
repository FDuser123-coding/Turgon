package remote

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// fakeStripe starts connectors/stripe-fake (built with installDist) with one
// customer and returns its base URL and the customer's ID.
func fakeStripe(t *testing.T, bin string) (string, string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	cmd := exec.Command(bin, "--port", fmt.Sprint(port), "--key", "rk_test_sidecar0000", "--customer", "ada@example.com=Ada Lovelace GmbH")
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
	}()
	timeout := time.After(30 * time.Second)
	for {
		select {
		case line := <-lines:
			if f := strings.Fields(line); len(f) == 3 && f[0] == "customer" {
				return fmt.Sprintf("http://127.0.0.1:%d", port), f[1]
			}
		case <-timeout:
			t.Fatal("the fake Stripe did not start")
		}
	}
}

// TestJavaSidecarStripe drives the Stripe connector over the protocol
// against the fake Stripe. Set TURGON_TEST_CONNECTOR_STRIPE to the
// connector's start script and TURGON_TEST_FAKE_STRIPE to the fake's.
func TestJavaSidecarStripe(t *testing.T) {
	bin, fake := os.Getenv("TURGON_TEST_CONNECTOR_STRIPE"), os.Getenv("TURGON_TEST_FAKE_STRIPE")
	if bin == "" || fake == "" {
		t.Skip("TURGON_TEST_CONNECTOR_STRIPE and TURGON_TEST_FAKE_STRIPE are not set (connectors: gradle :stripe:installDist :stripe-fake:installDist)")
	}
	url, customer := fakeStripe(t, fake)
	sock := filepath.Join(t.TempDir(), "stripe.sock")
	sidecar(t, bin, sock)
	pool := NewPool()
	defer pool.Close()
	ctx := context.Background()
	cfg := compiler.ConnectorConfig{Endpoint: "stripe-invoicing", Name: "stripe", Version: "1.5.0", Runtime: "camel-java",
		SecretRef: "openbao://stripe-invoicing/restricted-key",
		Config:    json.RawMessage(`{"url":"` + url + `","webhook":{"secretRef":"openbao://stripe-invoicing/webhook-secret"}}`)}
	inst, err := pool.Factory(Address{Target: "unix://" + sock, Token: "tok"})(ctx, cfg,
		connector.StaticSecrets{"openbao://stripe-invoicing/restricted-key": "rk_test_sidecar0000", "openbao://stripe-invoicing/webhook-secret": "whsec_test"})
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Close()

	for _, c := range inst.(connector.Checker).Check(ctx) {
		if !c.OK {
			t.Fatalf("check %+v", c)
		}
	}
	invoice := json.RawMessage(`{"customerId":"` + customer + `","currency":"EUR","lines":[{"description":"Widget","amount":"174.95","quantity":2}]}`)
	prev, err := inst.Simulate(ctx, "create-invoice", invoice)
	if err != nil || !strings.Contains(string(prev), `"total":349.90`) {
		t.Fatalf("simulate %s %v", prev, err)
	}
	res, err := inst.Commit(ctx, "create-invoice", "SO-1", invoice)
	if err != nil || !strings.Contains(string(res), `"status":"open"`) {
		t.Fatalf("commit %s %v", res, err)
	}
	if err := inst.(writeguard.Confirmer).Confirm(ctx, "create-invoice", res); err != nil {
		t.Fatal(err)
	}
	var created struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(res, &created)
	if again, err := inst.Commit(ctx, "create-invoice", "SO-1", invoice); err != nil || !strings.Contains(string(again), created.ID) {
		t.Fatalf("retried create %s %v", again, err)
	}
	if _, err := inst.Commit(ctx, "create-invoice", "X", json.RawMessage(`{"customerId":"cus_404","currency":"eur","lines":[{"description":"a","amount":1}]}`)); !errors.Is(err, writeguard.ErrInvalid) {
		t.Fatalf("unknown customer: %v", err)
	}
	if rec, err := inst.(writeguard.Reader).Read(ctx, "get-invoice", created.ID); err != nil || !strings.Contains(string(rec), `"total":34990`) {
		t.Fatalf("read %s %v", rec, err)
	}

	// Paid: the event arrives by polling, in the webhook's shape.
	src := inst.(connector.Source)
	fakeCall(t, "POST", url+"/fake/pay?invoice="+created.ID, "")
	evs, err := src.Poll(ctx, "Invoice.Paid", 0, 10)
	if err != nil || len(evs) != 1 || !strings.Contains(string(evs[0].Payload), `"amount_paid":34990`) || !strings.HasPrefix(evs[0].ID, "evt_") {
		t.Fatalf("poll %+v %v", evs, err)
	}
	// The same event from a webhook endpoint, signed with its secret.
	hook, err := inst.(connector.WebhookSource).Webhook("Invoice.Paid")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	mac := hmac.New(sha256.New, []byte("whsec_test"))
	fmt.Fprintf(mac, "%d.%s", now.Unix(), evs[0].Payload)
	h := http.Header{}
	h.Set("Stripe-Signature", fmt.Sprintf("t=%d,v1=%x", now.Unix(), mac.Sum(nil)))
	if got, err := hook.Receive(h, evs[0].Payload, now); err != nil || len(got) != 1 || got[0].ID != evs[0].ID {
		t.Fatalf("webhook %+v %v", got, err)
	}
	if _, err := hook.Receive(h, evs[0].Payload, now.Add(time.Hour)); !errors.Is(err, connector.ErrUnauthenticated) {
		t.Fatalf("an old signature: %v", err)
	}
	if _, err := inst.Commit(ctx, "void-invoice", "SO-1#compensate", res); !errors.Is(err, writeguard.ErrRejected) {
		t.Fatalf("voiding a paid invoice: %v", err)
	}

	link := json.RawMessage(`{"invoiceId":"` + created.ID + `","erpPaymentId":"7"}`)
	lres, err := inst.Commit(ctx, "link-invoice", "SO-1/link", link)
	if err != nil || !strings.Contains(string(lres), `"metadata":{"erp_payment_id":"7"}`) {
		t.Fatalf("link %s %v", lres, err)
	}
	if _, err := inst.Commit(ctx, "unlink-invoice", "SO-1/link#compensate", lres); err != nil {
		t.Fatal(err)
	}
	if rec, _ := inst.(writeguard.Reader).Read(ctx, "get-invoice", created.ID); strings.Contains(string(rec), "erp_payment_id") {
		t.Fatalf("unlinked invoice %s", rec)
	}
}
