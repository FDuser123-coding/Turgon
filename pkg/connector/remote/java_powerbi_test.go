package remote

import (
	"context"
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

// fakePowerBI starts connectors/powerbi-fake (built with installDist) and
// returns its base URL.
func fakePowerBI(t *testing.T, bin string) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	cmd := exec.Command(bin, "--port", fmt.Sprint(port), "--tenant", "contoso", "--client-id", "app-1", "--client-secret", "s3cret",
		"--workspace", "ws-1", "--dataset", "ds-1", "--table", "SalesOrders=OrderId,Customer,NetValue,Reversal,TurgonKey")
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })
	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	for i := 0; i < 300; i++ {
		if resp, err := http.Get(url + "/fake/tables/SalesOrders/rows"); err == nil {
			resp.Body.Close()
			return url
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("the fake Power BI did not start")
	return ""
}

func fakeRows(t *testing.T, url string) []map[string]any {
	t.Helper()
	resp, err := http.Get(url + "/fake/tables/SalesOrders/rows")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var out struct {
		Value []map[string]any `json:"value"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("%s: %v", b, err)
	}
	return out.Value
}

// TestJavaSidecarPowerBI drives the Power BI connector over the protocol
// against the fake Power BI. Set TURGON_TEST_CONNECTOR_POWERBI to the
// connector's start script and TURGON_TEST_FAKE_POWERBI to the fake's.
func TestJavaSidecarPowerBI(t *testing.T) {
	bin, fake := os.Getenv("TURGON_TEST_CONNECTOR_POWERBI"), os.Getenv("TURGON_TEST_FAKE_POWERBI")
	if bin == "" || fake == "" {
		t.Skip("TURGON_TEST_CONNECTOR_POWERBI and TURGON_TEST_FAKE_POWERBI are not set (connectors: gradle :powerbi-export:installDist :powerbi-fake:installDist)")
	}
	url := fakePowerBI(t, fake)
	sock := filepath.Join(t.TempDir(), "powerbi-export.sock")
	sidecar(t, bin, sock)
	pool := NewPool()
	defer pool.Close()
	ctx := context.Background()
	cfg := compiler.ConnectorConfig{Endpoint: "powerbi-sales", Name: "powerbi-export", Version: "1.1.0", Runtime: "camel-java",
		SecretRef: "openbao://powerbi/sales/client-secret", Config: json.RawMessage(`{"tenant":"contoso","clientId":"app-1",
			"workspace":"ws-1","dataset":"ds-1","authority":"` + url + `","api":"` + url + `",
			"tables":{"SalesOrders":{"columns":{"OrderId":"String","Customer":"String","NetValue":"Double","Reversal":"Boolean","TurgonKey":"String"},
			  "key":"TurgonKey","reverse":{"negate":["NetValue"],"set":{"Reversal":true}}}}}`)}
	inst, err := pool.Factory(Address{Target: "unix://" + sock, Token: "tok"})(ctx, cfg,
		connector.StaticSecrets{"openbao://powerbi/sales/client-secret": "s3cret"})
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
	if !strings.Contains(checks[0].Detail, "powerbi-export 1.1.0") {
		t.Fatalf("checks %+v", checks)
	}

	row := json.RawMessage(`{"OrderId":"SHOP-1001","Customer":"ada@example.com","NetValue":"249.90","Reversal":false}`)
	prev, err := inst.Simulate(ctx, "push-rows", row)
	if err != nil || !strings.Contains(string(prev), `"simulated":true`) || len(fakeRows(t, url)) != 0 {
		t.Fatalf("simulate %s %v", prev, err)
	}
	res, err := inst.Commit(ctx, "push-rows", "SHOP-1001", row)
	if err != nil {
		t.Fatal(err)
	}
	rows := fakeRows(t, url)
	if len(rows) != 1 || rows[0]["TurgonKey"] != "SHOP-1001" || rows[0]["NetValue"] != 249.9 {
		t.Fatalf("rows %v", rows)
	}
	// The compensation takes the commit's result, as the write guard passes it.
	if _, err := inst.Commit(ctx, "reverse-rows", "SHOP-1001#compensate", res); err != nil {
		t.Fatal(err)
	}
	rows = fakeRows(t, url)
	if len(rows) != 2 || rows[1]["NetValue"] != -249.9 || rows[1]["Reversal"] != true || rows[1]["TurgonKey"] != "SHOP-1001#reversed" {
		t.Fatalf("reversed %v", rows)
	}

	if _, err := inst.Commit(ctx, "push-rows", "K", json.RawMessage(`{"Discount":5}`)); !errors.Is(err, writeguard.ErrInvalid) {
		t.Fatalf("unknown column: %v", err)
	}
	resp, err := http.Post(url+"/fake/throttle?n=1", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if _, err := inst.Commit(ctx, "push-rows", "K", row); err == nil || errors.Is(err, writeguard.ErrInvalid) || errors.Is(err, writeguard.ErrRejected) ||
		!strings.Contains(err.Error(), "429") {
		t.Fatalf("throttled push must be retried: %v", err)
	}

	cat, err := inst.(connector.Discoverer).Discover(ctx, nil)
	if err != nil || len(cat.Objects) != 1 || cat.Objects[0].Name != "SalesOrders" || len(cat.Objects[0].Fields) != 5 {
		t.Fatalf("discover %+v %v", cat, err)
	}
}
