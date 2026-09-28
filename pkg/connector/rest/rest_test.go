package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/connector/rest/shoptest"
	"github.com/fduser123-coding/turgon/pkg/connector/rest/stripetest"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// shopConfig is the example shopify-store connection's config.
func shopConfig(base string) Config {
	return Config{
		BaseURL:   base,
		Auth:      Auth{Type: "header", Header: "X-Shopify-Access-Token"},
		CheckPath: "/shop.json",
		Events: map[string]Event{"Order.Created": {
			Path: "/orders.json", Params: map[string]string{"since_id": "{{position}}", "limit": "{{limit}}", "status": "any"}, Items: "orders",
		}},
		Operations: map[string]Operation{
			"get-order":    {Method: "GET", Path: "/orders/{{id}}.json", Result: "order", Fields: map[string]string{"name": "number", "email": "email", "note": "note"}},
			"get-customer": {Method: "GET", Path: "/customers/{{id}}.json", Result: "customer"},
			"update-order": {
				Method: "PUT", Path: "/orders/{{orderId}}.json", Wrap: "order", Result: "order",
				Fields:  map[string]string{"note": "note"},
				Capture: &Capture{Path: "/orders/{{orderId}}.json", Result: "order"},
			},
			"restore-order": {Method: "PUT", Path: "/orders/{{orderId}}.json", Wrap: "order", Restore: true},
		},
	}
}

func newShop(t *testing.T) (*shoptest.Shop, *Conn) {
	t.Helper()
	shop := shoptest.New("shpat_test")
	t.Cleanup(shop.Close)
	c, err := New(shopConfig(shop.URL()), "shpat_test", http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	return shop, c
}

func order(n int) map[string]any {
	return map[string]any{"name": fmt.Sprintf("#%d", n), "email": "Ada@Example.com", "created_at": "2026-09-24T09:30:00-04:00",
		"subtotal_price": "310.00", "currency": "EUR", "line_items": []any{map[string]any{"sku": "M-1", "quantity": 3}}}
}

func TestPollReadsOrdersInOrderWithACursor(t *testing.T) {
	shop, c := newShop(t)
	ctx := context.Background()
	var ids []int64
	for i := 0; i < 5; i++ {
		ids = append(ids, shop.AddOrder(order(1001+i)))
	}
	first, err := c.Poll(ctx, "Order.Created", 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 3 || first[0].Position != ids[0] || first[0].ID != fmt.Sprint(ids[0]) || first[2].Position != ids[2] {
		t.Fatalf("first page %+v", first)
	}
	var payload map[string]any
	_ = json.Unmarshal(first[0].Payload, &payload)
	if payload["name"] != "#1001" || payload["subtotal_price"] != "310.00" {
		t.Fatalf("payload %v", payload)
	}
	rest, err := c.Poll(ctx, "Order.Created", first[2].Position, 10)
	if err != nil || len(rest) != 2 || rest[0].Position != ids[3] {
		t.Fatalf("second page %+v %v", rest, err)
	}
	if none, _ := c.Poll(ctx, "Order.Created", ids[4], 10); len(none) != 0 {
		t.Fatalf("read past the end: %+v", none)
	}
}

func TestReadsReturnBusinessFields(t *testing.T) {
	shop, c := newShop(t)
	ctx := context.Background()
	id := shop.AddOrder(order(1001))
	shop.AddCustomer(7, map[string]any{"email": "ada@example.com", "first_name": "Ada"})
	got, err := c.Read(ctx, "get-order", fmt.Sprint(id))
	if err != nil {
		t.Fatal(err)
	}
	var rec map[string]any
	_ = json.Unmarshal(got, &rec)
	if rec["number"] != "#1001" || rec["email"] != "Ada@Example.com" || rec["id"] != fmt.Sprint(id) || len(rec) != 4 {
		t.Fatalf("order %v", rec)
	}
	if _, err := c.Read(ctx, "get-order", "999"); !errors.Is(err, writeguard.ErrNotFound) {
		t.Fatalf("missing order: %v", err)
	}
	// IDs are escaped as one path segment.
	if _, err := c.Read(ctx, "get-order", "../customers/7"); !errors.Is(err, writeguard.ErrNotFound) {
		t.Fatalf("path traversal: %v", err)
	}
	if got, err := c.Read(ctx, "get-customer", "7"); err != nil || !strings.Contains(string(got), "Ada") {
		t.Fatalf("customer %s %v", got, err)
	}
}

func TestCapturedUpdateIsPreviewedConfirmedAndRestored(t *testing.T) {
	shop, c := newShop(t)
	ctx := context.Background()
	id := shop.AddOrder(order(1001))
	payload := json.RawMessage(fmt.Sprintf(`{"orderId": %d, "note": "ERP order 7"}`, id))

	preview, err := c.Simulate(ctx, "update-order", payload)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(preview), `"current":{"note":null}`) || !strings.Contains(string(preview), `"proposed":{"note":"ERP order 7"}`) {
		t.Fatalf("preview %s", preview)
	}
	if shop.Order(id)["note"] != nil {
		t.Fatal("simulation wrote")
	}

	result, err := c.Commit(ctx, "update-order", "key-1", payload)
	if err != nil {
		t.Fatal(err)
	}
	if shop.Order(id)["note"] != "ERP order 7" {
		t.Fatalf("order after update: %v", shop.Order(id))
	}
	if err := c.Confirm(ctx, "update-order", result); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	var r UpdateResult
	dec := json.NewDecoder(strings.NewReader(string(result)))
	dec.UseNumber()
	if err := dec.Decode(&r); err != nil || fmt.Sprint(r.Params["orderId"]) != fmt.Sprint(id) || r.Previous["note"] != nil {
		t.Fatalf("result %s", result)
	}

	// The saga's compensation sends the previous values back.
	if _, err := c.Commit(ctx, "restore-order", "key-1#compensate", result); err != nil {
		t.Fatal(err)
	}
	if shop.Order(id)["note"] != nil {
		t.Fatalf("not restored: %v", shop.Order(id)["note"])
	}
}

func TestErrorsAreClassified(t *testing.T) {
	shop, c := newShop(t)
	ctx := context.Background()
	id := shop.AddOrder(order(1001))
	shop.FailUpdates = true
	_, err := c.Commit(ctx, "update-order", "k", json.RawMessage(fmt.Sprintf(`{"orderId": %d, "note": "x"}`, id)))
	if !errors.Is(err, writeguard.ErrInvalid) || !strings.Contains(err.Error(), "422") || !strings.Contains(err.Error(), "locked") {
		t.Fatalf("422 is not permanent: %v", err)
	}
	if _, err := c.Commit(ctx, "update-order", "k", json.RawMessage(`{"note": "x"}`)); !errors.Is(err, writeguard.ErrInvalid) || !strings.Contains(err.Error(), "orderId") {
		t.Fatalf("missing path variable: %v", err)
	}

	bad, _ := New(shopConfig(shop.URL()), "wrong-token", http.DefaultClient)
	res := bad.Check(ctx)
	if len(res) == 0 || res[0].OK || !strings.Contains(res[0].Fix, "rejected the credential") {
		t.Fatalf("check with a wrong token: %+v", res)
	}
	for _, r := range c.Check(ctx) {
		if !r.OK {
			t.Fatalf("check: %+v", r)
		}
	}

	// Rate limits and server errors are retried, not failed.
	busy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTooManyRequests) }))
	defer busy.Close()
	cfg := shopConfig(busy.URL)
	b, _ := New(cfg, "t", http.DefaultClient)
	if _, err := b.Commit(ctx, "update-order", "k", json.RawMessage(`{"orderId": 1, "note": "x"}`)); err == nil || errors.Is(err, writeguard.ErrInvalid) {
		t.Fatalf("429 must be retryable: %v", err)
	}
}

func TestConfigIsValidated(t *testing.T) {
	cases := map[string]func(*Config){
		"not a URL":         func(c *Config) { c.BaseURL = "ftp://x" },
		"credential header": func(c *Config) { c.Headers = map[string]string{"X-Shopify-Access-Token": "t"} },
		"bad auth":          func(c *Config) { c.Auth.Type = "magic" },
		"read without id":   func(c *Config) { c.Operations["get-order"] = Operation{Method: "GET", Path: "/orders.json"} },
		"relative path":     func(c *Config) { c.Operations["x"] = Operation{Method: "POST", Path: "orders"} },
		"capture sans field": func(c *Config) {
			c.Operations["x"] = Operation{Method: "PUT", Path: "/o", Capture: &Capture{Path: "/o"}}
		},
	}
	for name, mutate := range cases {
		cfg := shopConfig("https://shop.example.com/admin/api/2026-07")
		mutate(&cfg)
		if _, err := New(cfg, "t", http.DefaultClient); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// A create is undone by a request templated from its own result; the
// write's idempotency key is sent where the API accepts one; OAuth2
// tokens are fetched once and refreshed after a 401.
func TestCreateCompensationIdempotencyAndOAuth2(t *testing.T) {
	var mu sync.Mutex
	tokens, invoices := 0, map[string]bool{}
	var keys []string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.URL.Path == "/oauth/token":
			id, secret, _ := r.BasicAuth()
			if id != "turgon" || secret != "s3cret" || r.FormValue("grant_type") != "client_credentials" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			tokens++
			fmt.Fprintf(w, `{"access_token": "tok-%d", "expires_in": 3600}`, tokens)
		case r.Header.Get("Authorization") != fmt.Sprintf("Bearer tok-%d", tokens):
			w.WriteHeader(http.StatusUnauthorized)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/invoices":
			keys = append(keys, r.Header.Get("Idempotency-Key"))
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			id := fmt.Sprintf("in_%d", len(invoices)+1)
			invoices[id] = true
			_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "customer": body["customer"], "status": "draft"})
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/v1/invoices/"):
			delete(invoices, strings.TrimPrefix(r.URL.Path, "/v1/invoices/"))
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer api.Close()
	cfg := Config{
		BaseURL: api.URL,
		Auth:    Auth{Type: "oauth2", TokenURL: api.URL + "/oauth/token"},
		Operations: map[string]Operation{
			"create-invoice": {Method: "POST", Path: "/v1/invoices", IdempotencyHeader: "Idempotency-Key", Fields: map[string]string{"customer": "customerId"}},
			"void-invoice":   {Method: "DELETE", Path: "/v1/invoices/{{id}}"},
		},
	}
	c, err := New(cfg, `{"clientId": "turgon", "clientSecret": "s3cret"}`, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	result, err := c.Commit(ctx, "create-invoice", "SHOP-1001", json.RawMessage(`{"customerId": "cus_1", "internal": "not sent"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(result), `"id":"in_1"`) || len(keys) != 1 || keys[0] != "SHOP-1001" || !invoices["in_1"] {
		t.Fatalf("create: %s, keys %v", result, keys)
	}
	mu.Lock()
	tokens++ // the provider revokes the token: the next call re-authenticates
	mu.Unlock()
	if _, err := c.Commit(ctx, "void-invoice", "SHOP-1001#compensate", result); err != nil {
		t.Fatal(err)
	}
	if invoices["in_1"] {
		t.Fatal("invoice not voided")
	}
	if tokens != 3 {
		t.Fatalf("token requests: %d", tokens-1)
	}
}

func TestTimestampCursorNeverSplitsASecond(t *testing.T) {
	items := `[{"id":"a","updated":"2026-09-24T10:00:00Z"},{"id":"b","updated":"2026-09-24T10:00:01Z"},{"id":"c","updated":"2026-09-24T10:00:01Z"}]`
	var gotSince string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSince = r.URL.Query().Get("updated_after")
		fmt.Fprint(w, items)
	}))
	defer api.Close()
	c, err := New(Config{BaseURL: api.URL, Auth: Auth{Type: "none"}, Events: map[string]Event{"Changed": {
		Path: "/things", Params: map[string]string{"updated_after": "{{position}}", "limit": "{{limit}}"}, Position: "updated", PositionFormat: "rfc3339",
	}}}, "", http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	evs, err := c.Poll(context.Background(), "Changed", 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	// The page is full and b and c share a timestamp: only a is taken now.
	if len(evs) != 1 || evs[0].ID != "a" || gotSince != "1970-01-01T00:00:00.000Z" {
		t.Fatalf("events %+v, since %q", evs, gotSince)
	}
}

// Values from records and agents fill paths; a dot segment would move the
// request to another resource once the server resolves it.
func TestPathValuesCannotClimb(t *testing.T) {
	for _, v := range []string{"..", ".", ""} {
		if _, _, err := render("/v1/invoices/{{id}}", map[string]any{"id": v}); !errors.Is(err, writeguard.ErrInvalid) {
			t.Errorf("%q: %v", v, err)
		}
	}
	got, _, err := render("/v1/invoices/{{id}}", map[string]any{"id": "../admin?x=1#y"})
	if err != nil || got != "/v1/invoices/..%2Fadmin%3Fx=1%23y" {
		t.Fatalf("%q %v", got, err)
	}
	s := stripetest.New("sk")
	defer s.Close()
	c, _ := New(stripeConfig(s.URL()), "sk", http.DefaultClient)
	if _, err := c.Read(context.Background(), "get-invoice", ".."); !errors.Is(err, writeguard.ErrNotFound) {
		t.Fatalf("read ..: %v", err)
	}
	if s.Requests["GET /v1"] != 0 || s.Requests["GET /v1/"] != 0 {
		t.Fatalf("requested the parent: %v", s.Requests)
	}
}
