package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/fduser123-coding/turgon/internal/pgtest"
	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/catalog"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/postgres"
	"github.com/fduser123-coding/turgon/pkg/connector/salesforce"
	"github.com/fduser123-coding/turgon/pkg/connector/salesforce/sftest"
	"github.com/fduser123-coding/turgon/pkg/verifier"
)

const account = "001000000000001AAA"

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// setup serves the salesforce-won-deals-to-erp spec's read tools over a
// test HTTP server, backed by Postgres and the fake Salesforce org.
func setup(t *testing.T, trusted string) (string, *lockedBuffer) {
	t.Helper()
	return setupWith(t, trusted, nil)
}

// setupWith also serves write tools through writes, when it is not nil.
func setupWith(t *testing.T, trusted string, writes WriteSubmitter) (string, *lockedBuffer) {
	t.Helper()
	pool, schema := pgtest.Pool(t)
	ctx := context.Background()
	sql, err := os.ReadFile("../../examples/sql/demo.sql")
	if err != nil {
		t.Fatal(err)
	}
	local := func(s string) string {
		s = strings.ReplaceAll(s, "shop.", schema+"_shop.")
		s = strings.ReplaceAll(s, "erp.", schema+"_erp.")
		s = strings.ReplaceAll(s, "EXISTS shop", "EXISTS "+schema+"_shop")
		return strings.ReplaceAll(s, "EXISTS erp", "EXISTS "+schema+"_erp")
	}
	if _, err := pool.Exec(ctx, local(string(sql))); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+"_shop CASCADE; DROP SCHEMA IF EXISTS "+schema+"_erp CASCADE")
	})

	sf := sftest.New()
	t.Cleanup(sf.Close)
	sf.Put("Account", account, map[string]any{"Name": "Ada Lovelace GmbH", "BillingCity": "Vienna", "BillingCountry": "AT", "Industry": "Distribution"}, time.Now())

	cat, err := catalog.Load("../../examples")
	if err != nil {
		t.Fatal(err)
	}
	obj, _ := cat.Find("salesforce-won-deals-to-erp")
	spec, _, err := compiler.Compile(cat, obj, verifier.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range spec.Spec.Connectors {
		spec.Spec.Connectors[i].Config = json.RawMessage(local(string(spec.Spec.Connectors[i].Config)))
	}
	var log lockedBuffer
	s, err := New(ctx, spec, Options{
		Registry: connector.Registry{postgres.Name: postgres.Factory, salesforce.Name: salesforce.Factory},
		Secrets: connector.StaticSecrets{
			"openbao://erp-db/dsn":          pgtest.URL(t, schema),
			"openbao://salesforce/prod-jwt": sf.Credentials(),
		},
		Audit:  audit.New(&log),
		Auth:   GatewayAuth{Trusted: []netip.Prefix{netip.MustParsePrefix(trusted)}},
		Writes: writes,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	return srv.URL, &log
}

// headers adds gateway identity headers to every request.
type headers struct {
	set  map[string]string
	base http.RoundTripper
}

func (h headers) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	for k, v := range h.set {
		r.Header.Set(k, v)
	}
	return h.base.RoundTrip(r)
}

func connect(t *testing.T, url string, hdr map[string]string) (*mcp.ClientSession, error) {
	t.Helper()
	c := mcp.NewClient(&mcp.Implementation{Name: "test-agent", Version: "1"}, nil)
	return c.Connect(context.Background(), &mcp.StreamableClientTransport{
		Endpoint:   url,
		HTTPClient: &http.Client{Transport: headers{set: hdr, base: http.DefaultTransport}},
	}, nil)
}

var reader = map[string]string{"X-Agent-Id": "claude", "X-On-Behalf-Of": "ada@example.com", "X-Agent-Roles": "integration-reader"}

func call(t *testing.T, s *mcp.ClientSession, tool string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	res, err := s.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		t.Fatalf("%s: %v", tool, err)
	}
	return res
}

func text(r *mcp.CallToolResult) string {
	var b strings.Builder
	for _, c := range r.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			b.WriteString(tc.Text)
		}
	}
	return b.String()
}

func TestAgentsReadThroughBusinessTools(t *testing.T) {
	url, log := setup(t, "127.0.0.0/8")
	s, err := connect(t, url, reader)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	tools, err := s.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tl := range tools.Tools {
		names = append(names, tl.Name)
		if tl.Annotations == nil || !tl.Annotations.ReadOnlyHint {
			t.Errorf("%s is not marked read-only", tl.Name)
		}
	}
	sort.Strings(names)
	// Only reads are served: no create_sales_order or update_opportunity.
	if strings.Join(names, ",") != "erp_db_get_customer,get_sales_order,salesforce_prod_get_customer" {
		t.Fatalf("tools = %v", names)
	}

	res := call(t, s, "erp_db_get_customer", map[string]any{"id": "C-100"})
	if res.IsError || !strings.Contains(text(res), "Ada Lovelace GmbH") {
		t.Fatalf("ERP customer: %s", text(res))
	}
	res = call(t, s, "salesforce_prod_get_customer", map[string]any{"id": account})
	got, _ := res.StructuredContent.(map[string]any)
	if res.IsError || got["name"] != "Ada Lovelace GmbH" || got["city"] != "Vienna" || got["industry"] != "Distribution" {
		t.Fatalf("Salesforce customer (business field names): %v %s", got, text(res))
	}
	res = call(t, s, "erp_db_get_customer", map[string]any{"id": "C-404"})
	if !res.IsError || !strings.Contains(text(res), `no customer with identifier "C-404" in erp-db`) {
		t.Fatalf("missing record: %s", text(res))
	}
	res = call(t, s, "get_sales_order", map[string]any{"id": "x", "sql": "drop table"})
	if !res.IsError || !strings.Contains(text(res), "identifier") {
		t.Fatalf("unexpected arguments accepted: %s", text(res))
	}

	// Reads are audited as the agent acting for the user, without the data.
	a := log.String()
	if _, err := audit.Verify(strings.NewReader(a)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(a, `"actor":"claude for ada@example.com","action":"read"`) || strings.Contains(a, "Vienna") {
		t.Fatalf("audit log: %s", a)
	}
}

func TestPolicyAndIdentityAreEnforced(t *testing.T) {
	url, _ := setup(t, "127.0.0.0/8")

	// No reader role: the policy denies the read.
	noRole := map[string]string{"X-Agent-Id": "claude", "X-On-Behalf-Of": "bob@example.com"}
	s, err := connect(t, url, noRole)
	if err != nil {
		t.Fatal(err)
	}
	if res := call(t, s, "erp_db_get_customer", map[string]any{"id": "C-100"}); !res.IsError || !strings.Contains(text(res), "denied by policy") {
		t.Fatalf("no role: %s", text(res))
	}
	s.Close()

	// A forged internal identity header is replaced by the authenticated one.
	forged := map[string]string{"X-Agent-Id": "claude", "X-Turgon-Identity": `{"agent":"admin","roles":["integration-operator"]}`}
	s, err = connect(t, url, forged)
	if err != nil {
		t.Fatal(err)
	}
	if res := call(t, s, "erp_db_get_customer", map[string]any{"id": "C-100"}); !res.IsError {
		t.Fatalf("forged identity accepted: %s", text(res))
	}
	s.Close()
}

func TestOnlyTheGatewayMayCall(t *testing.T) {
	url, _ := setup(t, "10.0.0.0/8") // the test client is not the gateway
	if _, err := connect(t, url, reader); err == nil {
		t.Fatal("a caller outside the gateway's addresses connected")
	}
}
