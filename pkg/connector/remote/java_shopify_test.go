package remote

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
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
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// fakeShopify starts connectors/shopify-fake (built with installDist) and
// returns its base URL.
func fakeShopify(t *testing.T, bin string) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	cmd := exec.Command(bin, "--port", fmt.Sprint(port), "--shop", "turgon-demo.myshopify.com", "--token", "shpat_demo")
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })
	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	for i := 0; i < 300; i++ {
		if resp, err := http.Get(url + "/fake/orders/1"); err == nil {
			resp.Body.Close()
			return url
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("the fake Shopify did not start")
	return ""
}

func fakeCall(t *testing.T, method, url, body string) map[string]any {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if len(b) > 0 {
		if err := json.Unmarshal(b, &out); err != nil {
			t.Fatalf("%s: %v", b, err)
		}
	}
	return out
}

// TestJavaSidecarShopify drives the Shopify connector over the protocol
// against the fake Shopify. Set TURGON_TEST_CONNECTOR_SHOPIFY to the
// connector's start script and TURGON_TEST_FAKE_SHOPIFY to the fake's.
func TestJavaSidecarShopify(t *testing.T) {
	bin, fake := os.Getenv("TURGON_TEST_CONNECTOR_SHOPIFY"), os.Getenv("TURGON_TEST_FAKE_SHOPIFY")
	if bin == "" || fake == "" {
		t.Skip("TURGON_TEST_CONNECTOR_SHOPIFY and TURGON_TEST_FAKE_SHOPIFY are not set (connectors: gradle :shopify:installDist :shopify-fake:installDist)")
	}
	url := fakeShopify(t, fake)
	sock := filepath.Join(t.TempDir(), "shopify.sock")
	sidecar(t, bin, sock)
	pool := NewPool()
	defer pool.Close()
	ctx := context.Background()
	cfg := compiler.ConnectorConfig{Endpoint: "shopify-admin", Name: "shopify", Version: "2.1.0", Runtime: "camel-java",
		SecretRef: "openbao://shopify-admin/token",
		Config: json.RawMessage(`{"shop":"turgon-demo.myshopify.com","apiVersion":"2026-07","url":"` + url + `",
			"webhook":{"secretRef":"openbao://shopify-admin/client-secret"}}`)}
	inst, err := pool.Factory(Address{Target: "unix://" + sock, Token: "tok"})(ctx, cfg, connector.StaticSecrets{"openbao://shopify-admin/token": "shpat_demo",
		"openbao://shopify-admin/client-secret": "shpss_client_secret"})
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Close()

	for _, c := range inst.(connector.Checker).Check(ctx) {
		if !c.OK {
			t.Fatalf("check %+v", c)
		}
	}
	created := fakeCall(t, "POST", url+"/fake/orders", `{"email":"ada@example.com","currency":"EUR","subtotal":"349.90",
		"customer":{"email":"ada@example.com","first_name":"Ada","last_name":"Lovelace"},"line_items":[{"sku":"M-7","quantity":2,"price":"174.95"}]}`)
	id := created["legacyResourceId"].(string)

	src, ok := inst.(connector.Source)
	if !ok {
		t.Fatal("the shopify sidecar does not poll")
	}
	evs, err := src.Poll(ctx, "Order.Created", 0, 10)
	if err != nil || len(evs) != 1 || fmt.Sprint(evs[0].Position) != id || evs[0].ID != "gid://shopify/Order/"+id {
		t.Fatalf("poll %+v %v", evs, err)
	}
	var order map[string]any
	_ = json.Unmarshal(evs[0].Payload, &order)
	if order["subtotal_price"] != "349.90" || order["order_number"] != 1001.0 {
		t.Fatalf("payload %s", evs[0].Payload)
	}
	// The same order by webhook: verified by the sidecar, keyed like the poll.
	hook, err := inst.(connector.WebhookSource).Webhook("Order.Created")
	if err != nil {
		t.Fatal(err)
	}
	mac := hmac.New(sha256.New, []byte("shpss_client_secret"))
	mac.Write(evs[0].Payload)
	h := http.Header{}
	h.Set("X-Shopify-Hmac-Sha256", base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	h.Set("X-Shopify-Topic", "orders/create")
	if got, err := hook.Receive(h, evs[0].Payload, time.Now()); err != nil || len(got) != 1 || got[0].ID != evs[0].ID {
		t.Fatalf("webhook %+v %v", got, err)
	}
	h.Set("X-Shopify-Hmac-Sha256", base64.StdEncoding.EncodeToString([]byte("forged")))
	if _, err := hook.Receive(h, evs[0].Payload, time.Now()); !errors.Is(err, connector.ErrUnauthenticated) {
		t.Fatalf("forged webhook: %v", err)
	}
	if more, err := src.Poll(ctx, "Order.Created", evs[0].Position, 10); err != nil || len(more) != 0 {
		t.Fatalf("poll after %v %v", more, err)
	}
	if rec, err := inst.(writeguard.Reader).Read(ctx, "get-order", id); err != nil || !strings.Contains(string(rec), `"sku":"M-7"`) {
		t.Fatalf("read %s %v", rec, err)
	}
	if _, err := inst.(writeguard.Reader).Read(ctx, "get-order", "1"); !errors.Is(err, writeguard.ErrNotFound) {
		t.Fatalf("read missing: %v", err)
	}

	note := json.RawMessage(`{"orderId":"` + id + `","note":"ERP order 12000"}`)
	prev, err := inst.Simulate(ctx, "update-order", note)
	if err != nil || !strings.Contains(string(prev), `"proposed":{"note":"ERP order 12000"}`) {
		t.Fatalf("simulate %s %v", prev, err)
	}
	res, err := inst.Commit(ctx, "update-order", "gid://shopify/Order/"+id, note)
	if err != nil {
		t.Fatal(err)
	}
	if err := inst.(writeguard.Confirmer).Confirm(ctx, "update-order", res); err != nil {
		t.Fatal(err)
	}
	if got := fakeCall(t, "GET", url+"/fake/orders/"+id, ""); got["note"] != "ERP order 12000" {
		t.Fatalf("note %v", got["note"])
	}
	// The compensation takes the commit's result, as the write guard passes it.
	if _, err := inst.Commit(ctx, "restore-order", "gid://shopify/Order/"+id+"#compensate", res); err != nil {
		t.Fatal(err)
	}
	if got := fakeCall(t, "GET", url+"/fake/orders/"+id, ""); got["note"] != nil {
		t.Fatalf("restored note %v", got["note"])
	}

	fakeCall(t, "POST", url+"/fake/reject-update?order="+id, "")
	if _, err := inst.Commit(ctx, "update-order", "K", note); !errors.Is(err, writeguard.ErrRejected) {
		t.Fatalf("user error: %v", err)
	}
	fakeCall(t, "POST", url+"/fake/throttle?n=1", "")
	if _, err := src.Poll(ctx, "Order.Created", 0, 10); err == nil || errors.Is(err, writeguard.ErrInvalid) || errors.Is(err, writeguard.ErrRejected) {
		t.Fatalf("a throttled poll must be retried: %v", err)
	}

	// Without the client secret the orders are still polled; only the
	// webhook is refused, with the reason.
	plain, err := pool.Factory(Address{Target: "unix://" + sock, Token: "tok"})(ctx, cfg, connector.StaticSecrets{"openbao://shopify-admin/token": "shpat_demo"})
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	if _, err := plain.(connector.WebhookSource).Webhook("Order.Created"); err == nil || errors.Is(err, connector.ErrNoWebhook) ||
		!strings.Contains(err.Error(), "openbao://shopify-admin/client-secret is not set") {
		t.Fatalf("webhook without its secret: %v", err)
	}
	if evs, err := plain.(connector.Source).Poll(ctx, "Order.Created", 0, 10); err != nil || len(evs) != 1 {
		t.Fatalf("poll without the webhook secret %+v %v", evs, err)
	}

	cat, err := inst.(connector.Discoverer).Discover(ctx, nil)
	if err != nil || len(cat.Objects) != 2 || cat.Objects[0].Name != "Customer" || cat.Objects[1].Name != "Order" { // sorted by name
		t.Fatalf("discover %+v %v", cat, err)
	}
}
