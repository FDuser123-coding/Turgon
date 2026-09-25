package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/fduser123-coding/turgon/internal/pgtest"
	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/catalog"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/postgres"
	"github.com/fduser123-coding/turgon/pkg/connector/rest"
	"github.com/fduser123-coding/turgon/pkg/connector/rest/hubspottest"
	"github.com/fduser123-coding/turgon/pkg/connector/rest/shoptest"
	"github.com/fduser123-coding/turgon/pkg/connector/rest/stripetest"
	"github.com/fduser123-coding/turgon/pkg/connector/salesforce"
	"github.com/fduser123-coding/turgon/pkg/connector/salesforce/sftest"
	"github.com/fduser123-coding/turgon/pkg/identity"
	"github.com/fduser123-coding/turgon/pkg/policy"
	"github.com/fduser123-coding/turgon/pkg/store/pgstore"
	"github.com/fduser123-coding/turgon/pkg/verifier"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// fixture runs the example shop-orders-to-erp recipe against Postgres.
type fixture struct {
	t        *testing.T
	pool     *pgxpool.Pool
	schema   string
	rt       *Runtime
	store    *pgstore.Store
	auditLog *bytes.Buffer
}

// localize rewrites the example's schema names so parallel tests do not collide.
func localize(s, schema string) string {
	s = strings.ReplaceAll(s, "shop.", schema+"_shop.")
	s = strings.ReplaceAll(s, "erp.", schema+"_erp.")
	s = strings.ReplaceAll(s, "SCHEMA IF NOT EXISTS shop", "SCHEMA IF NOT EXISTS "+schema+"_shop")
	return strings.ReplaceAll(s, "SCHEMA IF NOT EXISTS erp", "SCHEMA IF NOT EXISTS "+schema+"_erp")
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	return newFixtureFor(t, "shop-orders-to-erp", nil)
}

// newFixtureFor compiles the named example recipe and connects it: Postgres
// endpoints to the test database, others through extra secrets.
func newFixtureFor(t *testing.T, recipe string, extra connector.StaticSecrets, rewrites ...func(string) string) *fixture {
	t.Helper()
	return newFixtureWith(t, recipe, extra, false, rewrites...)
}

// newFixtureWith also receives webhooks when webhooks is set.
func newFixtureWith(t *testing.T, recipe string, extra connector.StaticSecrets, webhooks bool, rewrites ...func(string) string) *fixture {
	t.Helper()
	pool, schema := pgtest.Pool(t)
	ctx := context.Background()
	sql, err := os.ReadFile("../../examples/sql/demo.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, localize(string(sql), schema)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+"_shop CASCADE; DROP SCHEMA IF EXISTS "+schema+"_erp CASCADE")
	})
	if err := pgstore.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := pgstore.New(pool)
	if err := store.PutXref(ctx, "Customer", "shop-db", "ada@example.com", "C-100"); err != nil {
		t.Fatal(err)
	}

	cat, err := catalog.Load("../../examples")
	if err != nil {
		t.Fatal(err)
	}
	obj, _ := cat.Find(recipe)
	spec, rep, err := compiler.Compile(cat, obj, verifier.Options{})
	if err != nil {
		t.Fatalf("%v: %+v", err, rep.Errors())
	}
	for i := range spec.Spec.Connectors {
		c := &spec.Spec.Connectors[i]
		cfg := localize(string(c.Config), schema)
		for _, rw := range rewrites {
			cfg = rw(cfg)
		}
		c.Config = json.RawMessage(cfg)
	}

	url := pgtest.URL(t, schema)
	var buf bytes.Buffer
	secrets := connector.StaticSecrets{"openbao://shop-db/dsn": url, "openbao://erp-db/dsn": url}
	for k, v := range extra {
		secrets[k] = v
	}
	rt, err := New(ctx, spec, Options{
		Registry: connector.Registry{postgres.Name: postgres.Factory, salesforce.Name: salesforce.Factory, rest.Name: rest.Factory},
		Secrets:  secrets,
		Store:    store,
		Resolver: store,
		Audit:    audit.New(&syncWriter{w: &buf}),
		Webhooks: webhooks,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)
	return &fixture{t: t, pool: pool, schema: schema, rt: rt, store: store, auditLog: &buf}
}

type syncWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func (f *fixture) publish(orderNumber int, email string, total string) {
	f.t.Helper()
	payload := map[string]any{
		"order_number": orderNumber, "created_at": "2026-09-24T09:30:00Z", "total": total, "currency": "eur",
		"customer": map[string]any{"email": email},
		"items":    []any{map[string]any{"sku": "M-1", "qty": 2}},
	}
	b, _ := json.Marshal(payload)
	_, err := f.pool.Exec(context.Background(), "INSERT INTO "+f.schema+"_shop.outbox (event, payload) VALUES ('Order.Created', $1)", b)
	if err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) orders(where string) int {
	f.t.Helper()
	var n int
	if err := f.pool.QueryRow(context.Background(), "SELECT count(*) FROM "+f.schema+"_erp.sales_orders WHERE "+where).Scan(&n); err != nil {
		f.t.Fatal(err)
	}
	return n
}

// recorder is a Starter that remembers runs instead of starting them.
type recorder struct {
	runs  map[string]RunInput
	ids   []string
	calls int // including starts of IDs already started
}

func (r *recorder) Start(_ context.Context, id string, in RunInput) error {
	r.calls++
	if r.runs == nil {
		r.runs = map[string]RunInput{}
	}
	if _, dup := r.runs[id]; !dup {
		r.runs[id] = in
		r.ids = append(r.ids, id)
	}
	return nil
}

func (f *fixture) dispatch() []RunInput {
	f.t.Helper()
	rec := &recorder{}
	d := &Dispatcher{Runtime: f.rt, Cursors: f.store, Starter: rec}
	if _, err := d.Poll(context.Background()); err != nil {
		f.t.Fatal(err)
	}
	var out []RunInput
	for _, id := range rec.ids {
		out = append(out, rec.runs[id])
	}
	return out
}

// run executes one workflow in Temporal's test environment, sending the
// given approval signals one simulated hour apart.
func (f *fixture) run(in RunInput, signals ...ApprovalSignal) (RunResult, error, []PendingApproval) {
	f.t.Helper()
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	Register(env, f.rt.Activities)
	var seen []PendingApproval
	for i, sig := range signals {
		sig := sig
		env.RegisterDelayedCallback(func() {
			if v, err := env.QueryWorkflow(QueryPending); err == nil {
				var p *PendingApproval
				if v.Get(&p) == nil && p != nil {
					seen = append(seen, *p)
					if sig.Digest == "" {
						sig.Digest = p.Digest // the approver saw this request
					}
				}
			}
			env.SignalWorkflow(SignalApproval, sig)
		}, time.Duration(i+1)*time.Hour)
	}
	env.ExecuteWorkflow(IntegrationWorkflow, in)
	if !env.IsWorkflowCompleted() {
		f.t.Fatal("workflow did not complete")
	}
	var res RunResult
	err := env.GetWorkflowError()
	if err == nil {
		_ = env.GetWorkflowResult(&res)
	}
	return res, err, seen
}

func approve(step string) ApprovalSignal {
	return ApprovalSignal{Step: step, Status: "approved", By: "controller@customer", Note: "matches the PO"}
}

func errType(err error) string {
	var app *temporal.ApplicationError
	if errors.As(err, &app) {
		return app.Type()
	}
	return ""
}

func TestEndToEndShopOrderToERP(t *testing.T) {
	f := newFixture(t)
	f.publish(1001, "Ada@Example.com", "1200.50")

	runs := f.dispatch()
	if len(runs) != 1 || RunID("shop-orders-to-erp", runs[0].Event) != "shop-orders-to-erp/1" {
		t.Fatalf("runs = %+v", runs)
	}
	if again := f.dispatch(); len(again) != 0 {
		t.Fatalf("cursor did not advance: %d runs", len(again))
	}

	res, err, pending := f.run(runs[0], approve("03-write"))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Writes) != 1 || res.Writes[0].Status != writeguard.StatusCommitted {
		t.Fatalf("result = %+v", res)
	}
	// The approver saw a real dry-run of the ERP insert before approving.
	if len(pending) != 1 || !strings.Contains(string(pending[0].Preview), `"mode":"rollback"`) ||
		!strings.Contains(string(pending[0].Preview), `"customer_id":"C-100"`) {
		t.Fatalf("pending approval = %+v", pending)
	}
	// Only mapped and resolved fields are written; source fields are not.
	var payload map[string]any
	_ = json.Unmarshal(pending[0].Request.Payload, &payload)
	want := []string{"currency", "customerId", "customerRef", "externalId", "lines", "netValue", "orderDate"}
	if got := sortedKeys(payload); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("write payload fields = %v, want %v", got, want)
	}
	if n := f.orders(`external_id = 'SHOP-1001' AND customer_id = 'C-100' AND net_value = 1200.50
		AND currency = 'EUR' AND lines = '[{"material":"M-1","quantity":2}]' AND status = 'open'`); n != 1 {
		t.Fatalf("expected the mapped order in the ERP, found %d", n)
	}

	// A re-delivered event is recognized; nothing is written twice and no
	// one is asked to approve again.
	res, err, pending = f.run(runs[0])
	if err != nil || res.Writes[0].Status != writeguard.StatusDuplicate || len(pending) != 0 || f.orders("true") != 1 {
		t.Fatalf("replay: %+v %v %d pending, %d orders", res, err, len(pending), f.orders("true"))
	}

	if _, err := audit.Verify(bytes.NewReader(f.auditLog.Bytes())); err != nil {
		t.Fatalf("audit chain: %v", err)
	}
	if !strings.Contains(f.auditLog.String(), `"note":"matches the PO"`) {
		t.Error("the approver's note is not in the audit log")
	}
	for _, action := range []string{"writeback.simulated", "writeback.approval", "writeback.committed", "writeback.duplicate"} {
		if !strings.Contains(f.auditLog.String(), `"action":"`+action+`"`) {
			t.Errorf("audit log has no %s entry", action)
		}
	}
}

func TestRejectedApprovalWritesNothing(t *testing.T) {
	f := newFixture(t)
	f.publish(1002, "ada@example.com", "99.00")
	_, err, _ := f.run(f.dispatch()[0], ApprovalSignal{Step: "03-write", Status: "rejected", By: "controller@customer"})
	if errType(err) != ErrTypeRejected || f.orders("true") != 0 {
		t.Fatalf("err = %v (%s), orders = %d", err, errType(err), f.orders("true"))
	}
}

func TestUnknownCustomerGoesToSteward(t *testing.T) {
	f := newFixture(t)
	f.publish(1003, "grace@example.com", "10.00")
	_, err, _ := f.run(f.dispatch()[0])
	if errType(err) != ErrTypeUnresolved || !strings.Contains(err.Error(), "data steward") || f.orders("true") != 0 {
		t.Fatalf("err = %v", err)
	}
	// The failure names what the steward must link.
	var app *temporal.ApplicationError
	var u Unresolved
	if !errors.As(err, &app) || app.Details(&u) != nil || u.Entity != "Customer" || u.System != "shop-db" || u.Ref != "grace@example.com" {
		t.Fatalf("details = %+v", u)
	}
}

func TestLaterFailureCompensatesEarlierWrite(t *testing.T) {
	f := newFixture(t)
	f.publish(1004, "ada@example.com", "500.00")
	in := f.dispatch()[0]
	// Extend the compiled workflow: after the order is written, a second
	// order with an invalid value fails at commit, so the first is undone.
	write := *in.Workflow.Steps[2].Write
	write.Simulation = ""
	write.IdempotencyKey = "{{ doc.externalId }}"
	in.Workflow.Steps = append(in.Workflow.Steps,
		compiler.WorkflowStep{Name: "04-map", Map: &compiler.MapConfig{Mapping: "test-negative@1", Fields: map[string]string{
			"externalId": "externalId & '-B'", "customerId": "customerId", "orderDate": "orderDate",
			"netValue": "-1", "currency": "currency",
		}}},
		compiler.WorkflowStep{Name: "05-write", Write: &write},
	)
	_, err, _ := f.run(in, approve("03-write"), approve("05-write"))
	if errType(err) != ErrTypeInvalid {
		t.Fatalf("err = %v (%s)", err, errType(err))
	}
	if f.orders("external_id = 'SHOP-1004' AND status = 'cancelled'") != 1 || f.orders("external_id = 'SHOP-1004-B'") != 0 {
		t.Fatalf("expected SHOP-1004 cancelled and no SHOP-1004-B")
	}
	if !strings.Contains(f.auditLog.String(), `"reason":"compensating create-sales-order"`) {
		t.Error("compensation not audited")
	}
}

func TestRender(t *testing.T) {
	src := map[string]any{"order_number": float64(1001), "nested": map[string]any{"id": "x"}}
	doc := map[string]any{"externalId": "SHOP-1001"}
	for tmpl, want := range map[string]string{
		"SHOP-{{ source.order_number }}":            "SHOP-1001",
		"{{source.nested.id}}/{{ doc.externalId }}": "x/SHOP-1001",
	} {
		if got, err := Render(tmpl, src, doc); err != nil || got != want {
			t.Errorf("Render(%q) = %q, %v", tmpl, got, err)
		}
	}
	for _, bad := range []string{"{{ source.missing }}", "{{ source.nested }}", "   "} {
		if _, err := Render(bad, src, doc); err == nil {
			t.Errorf("Render(%q) succeeded", bad)
		}
	}
}

func TestFailedCompensationIsReportedForOperators(t *testing.T) {
	f := newFixture(t)
	f.publish(1005, "ada@example.com", "500.00")
	in := f.dispatch()[0]
	first := *in.Workflow.Steps[2].Write
	first.Compensation = "not-configured" // the undo cannot run
	in.Workflow.Steps[2].Write = &first
	second := first
	second.Simulation, second.IdempotencyKey = "", "{{ doc.externalId }}"
	in.Workflow.Steps = append(in.Workflow.Steps,
		compiler.WorkflowStep{Name: "04-map", Map: &compiler.MapConfig{Mapping: "test-negative@1", Fields: map[string]string{
			"externalId": "externalId & '-B'", "customerId": "customerId", "orderDate": "orderDate", "netValue": "-1", "currency": "currency",
		}}},
		compiler.WorkflowStep{Name: "05-write", Write: &second},
	)
	_, err, _ := f.run(in, approve("03-write"), approve("05-write"))
	if errType(err) != ErrTypeCompensationFailed || !strings.Contains(err.Error(), "check constraint") {
		t.Fatalf("err = %v (%s)", err, errType(err))
	}
	if f.orders("external_id = 'SHOP-1005' AND status = 'open'") != 1 {
		t.Fatal("expected the uncompensated order to remain for an operator")
	}
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

const (
	wonDeal = "006000000000001AAA"
	account = "001000000000001AAA"
)

func newSalesforceFixture(t *testing.T) (*fixture, *sftest.Server) {
	t.Helper()
	sf := sftest.New()
	t.Cleanup(sf.Close)
	f := newFixtureFor(t, "salesforce-won-deals-to-erp", connector.StaticSecrets{"openbao://salesforce/prod-jwt": sf.Credentials()})
	if err := f.store.PutXref(context.Background(), "Customer", "salesforce-prod", account, "C-100"); err != nil {
		t.Fatal(err)
	}
	sf.Put("Opportunity", wonDeal, map[string]any{
		"StageName": "Closed Won", "AccountId": account, "Amount": 1200.5, "CloseDate": "2026-09-24",
		"CurrencyIsoCode": "EUR", "ERP_Order_Number__c": nil,
		"OpportunityLineItems": map[string]any{"totalSize": 1, "done": true, "records": []any{
			map[string]any{"attributes": map[string]any{"type": "OpportunityLineItem"}, "Quantity": 2,
				"Product2": map[string]any{"attributes": map[string]any{"type": "Product2"}, "ProductCode": "M-1"}},
		}},
	}, time.Now().Add(-time.Minute))
	return f, sf
}

func TestSalesforceWonDealBecomesERPOrderAndIsLinkedBack(t *testing.T) {
	f, sf := newSalesforceFixture(t)
	runs := f.dispatch()
	if len(runs) != 1 || runs[0].Event.ID != wonDeal {
		t.Fatalf("runs = %+v", runs)
	}
	res, err, pending := f.run(runs[0], approve("03-write"))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Writes) != 2 || res.Writes[0].Status != writeguard.StatusCommitted || res.Writes[1].Status != writeguard.StatusCommitted {
		t.Fatalf("writes = %+v", res.Writes)
	}
	// Only the high-risk ERP write needed a person; the low-risk write-back did not.
	if len(pending) != 1 || pending[0].Step != "03-write" {
		t.Fatalf("pending = %+v", pending)
	}
	if f.orders(`external_id = '`+wonDeal+`' AND customer_id = 'C-100' AND net_value = 1200.50
		AND lines = '[{"material":"M-1","quantity":2}]'`) != 1 {
		t.Fatal("ERP order missing or mis-mapped")
	}
	var erpID int64
	_ = f.pool.QueryRow(context.Background(), "SELECT id FROM "+f.schema+"_erp.sales_orders").Scan(&erpID)
	if got := sf.Get("Opportunity", wonDeal)["ERP_Order_Number__c"]; got != fmt.Sprint(erpID) {
		t.Fatalf("opportunity links to %v, want %d", got, erpID)
	}
}

func TestFailedWriteBackCancelsERPOrder(t *testing.T) {
	f, sf := newSalesforceFixture(t)
	sf.FailPatch = func(string, string, map[string]any) (int, string, string) {
		return 400, "FIELD_CUSTOM_VALIDATION_EXCEPTION", "Opportunity is locked for finance review"
	}
	_, err, _ := f.run(f.dispatch()[0], approve("03-write"))
	if errType(err) != ErrTypeInvalid || !strings.Contains(err.Error(), "locked for finance review") {
		t.Fatalf("err = %v (%s)", err, errType(err))
	}
	if f.orders(`external_id = '`+wonDeal+`' AND status = 'cancelled'`) != 1 {
		t.Fatal("ERP order was not cancelled after the write-back failed")
	}
	if v := sf.Get("Opportunity", wonDeal)["ERP_Order_Number__c"]; v != nil {
		t.Fatalf("opportunity changed: %v", v)
	}
}

func TestApprovalMustMatchThePendingRequest(t *testing.T) {
	f := newFixture(t)
	f.publish(1006, "ada@example.com", "10.00")
	in := f.dispatch()[0]
	in.ApprovalTimeout = 4 * time.Hour
	// A stale decision (wrong digest) and one for another step are both
	// ignored; with no valid decision the approval times out as a rejection.
	_, err, pending := f.run(in,
		ApprovalSignal{Step: "03-write", Digest: "0000", Status: "approved", By: "mallory"},
		ApprovalSignal{Step: "05-write", Digest: "x", Status: "approved", By: "mallory"},
	)
	if len(pending) != 2 || errType(err) != ErrTypeRejected || f.orders("true") != 0 {
		t.Fatalf("err = %v (%s), %d pending seen, %d orders", err, errType(err), len(pending), f.orders("true"))
	}
	if !strings.Contains(f.auditLog.String(), `"by":"turgon/approval-timeout"`) && !strings.Contains(f.auditLog.String(), `"actor":"turgon/approval-timeout"`) {
		t.Error("timeout rejection not audited")
	}
}

func TestPolicyDenialStopsBeforeAnyoneIsAsked(t *testing.T) {
	f := newFixture(t)
	// Rebuild the runtime with a freeze pack added to the recipe's policies.
	spec := *f.rt.Spec
	spec.Spec.Policies = append(append([]compiler.PolicyRef{}, spec.Spec.Policies...), compiler.PolicyRef{
		Name: "erp-freeze", Rego: "package turgon.writeback\ndeny contains \"ERP writes are frozen for the year-end close\" if input.recipe == \"shop-orders-to-erp\"",
	})
	spec.Spec.Workflows = append([]compiler.Workflow{}, spec.Spec.Workflows...)
	spec.Spec.Workflows[0].Policies = append(append([]string{}, spec.Spec.Workflows[0].Policies...), "erp-freeze")
	d, err := recipeDeciders(context.Background(), &spec, policy.WritebackDefault{})
	if err != nil {
		t.Fatal(err)
	}
	f.rt.Activities.Guard = mustGuard(t, f, d)

	f.publish(1007, "ada@example.com", "10.00")
	_, err, pending := f.run(f.dispatch()[0], approve("03-write"))
	if errType(err) != ErrTypeDenied || !strings.Contains(err.Error(), "denied") || len(pending) != 0 || f.orders("true") != 0 {
		t.Fatalf("err = %v (%s), pending = %d, orders = %d", err, errType(err), len(pending), f.orders("true"))
	}
	if !strings.Contains(f.auditLog.String(), "frozen for the year-end close") {
		t.Error("the denial reason is not in the audit log")
	}
}

func mustGuard(t *testing.T, f *fixture, d policy.Decider) *writeguard.Guard {
	t.Helper()
	targets := map[string]writeguard.TargetConfig{}
	for _, c := range f.rt.Spec.Spec.Connectors {
		if inst, ok := f.rt.instances[c.Endpoint]; ok {
			targets[c.Endpoint] = writeguard.TargetConfig{Target: inst, Limits: c.Limits, Metered: c.Metered}
		}
	}
	g, err := writeguard.New(writeguard.Config{Targets: targets, Policy: d, Audit: audit.New(&syncWriter{w: f.auditLog}), Store: f.store})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// newShopifyFixture runs the shopify-store-orders-to-erp recipe against the
// fake Shopify store and Postgres.
func newShopifyFixture(t *testing.T) (*fixture, *shoptest.Shop, int64) {
	t.Helper()
	shop := shoptest.New("shpat_test")
	t.Cleanup(shop.Close)
	f := newFixtureFor(t, "shopify-store-orders-to-erp", connector.StaticSecrets{"openbao://shopify-store/token": "shpat_test"},
		func(cfg string) string {
			return strings.ReplaceAll(cfg, "https://turgon-demo.myshopify.com/admin/api/"+shoptest.Version, shop.URL())
		})
	if err := f.store.PutXref(context.Background(), "Customer", "shopify-store", "ada@example.com", "C-100"); err != nil {
		t.Fatal(err)
	}
	id := shop.AddOrder(map[string]any{
		"name": "#1001", "email": "Ada@Example.com", "created_at": "2026-09-24T09:30:00-04:00",
		"subtotal_price": "310.00", "currency": "eur", "line_items": []any{map[string]any{"sku": "M-1", "quantity": 3}},
	})
	return f, shop, id
}

func TestShopifyOrderBecomesERPOrderAndIsNotedBack(t *testing.T) {
	f, shop, id := newShopifyFixture(t)
	runs := f.dispatch()
	if len(runs) != 1 || runs[0].Event.ID != fmt.Sprint(id) {
		t.Fatalf("runs = %+v", runs)
	}
	res, err, pending := f.run(runs[0], approve("03-write"))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Writes) != 2 || res.Writes[1].Status != writeguard.StatusCommitted || len(pending) != 1 {
		t.Fatalf("writes = %+v, pending = %d", res.Writes, len(pending))
	}
	ext := fmt.Sprintf("SHOPIFY-%d", id)
	if f.orders(`external_id = '`+ext+`' AND customer_id = 'C-100' AND net_value = 310.00 AND currency = 'EUR'
		AND lines = '[{"material":"M-1","quantity":3}]'`) != 1 {
		t.Fatal("ERP order missing or mis-mapped")
	}
	var erpID int64
	_ = f.pool.QueryRow(context.Background(), "SELECT id FROM "+f.schema+"_erp.sales_orders").Scan(&erpID)
	if got := shop.Order(id)["note"]; got != fmt.Sprintf("ERP order %d", erpID) {
		t.Fatalf("Shopify note = %v", got)
	}
	// The write-back was previewed from the order's current note, read back
	// after it was written, and audited with its previous value.
	log := f.auditLog.String()
	if !strings.Contains(log, `"proposed":{"note":"ERP order`) || !strings.Contains(log, `"previous":{"note":null}`) {
		t.Fatalf("write-back not previewed or audited:\n%s", log)
	}
	if shop.Requests["GET /orders/"+fmt.Sprint(id)+".json"] < 3 {
		t.Errorf("expected capture, preview and confirmation reads: %v", shop.Requests)
	}

	// A second poll finds nothing new: the cursor moved past the order.
	if again := f.dispatch(); len(again) != 0 {
		t.Fatalf("re-dispatched %+v", again)
	}
}

func TestShopifyRejectionCancelsTheERPOrder(t *testing.T) {
	f, shop, id := newShopifyFixture(t)
	shop.FailUpdates = true
	_, err, _ := f.run(f.dispatch()[0], approve("03-write"))
	if errType(err) != ErrTypeInvalid || !strings.Contains(err.Error(), "locked") {
		t.Fatalf("err = %v (%s)", err, errType(err))
	}
	if f.orders(fmt.Sprintf(`external_id = 'SHOPIFY-%d' AND status = 'cancelled'`, id)) != 1 {
		t.Fatal("ERP order was not cancelled after Shopify rejected the note")
	}
	if shop.Order(id)["note"] != nil {
		t.Fatal("Shopify order changed")
	}
}

// newStripeFixture runs the stripe-payments-to-erp recipe against the fake
// Stripe account and Postgres.
func newStripeFixture(t *testing.T) (*fixture, *stripetest.Stripe) {
	t.Helper()
	st := stripetest.New("rk_test_turgon")
	t.Cleanup(st.Close)
	f := newFixtureFor(t, "stripe-payments-to-erp", connector.StaticSecrets{"openbao://stripe-billing/restricted-key": "rk_test_turgon"},
		func(cfg string) string { return strings.ReplaceAll(cfg, "https://api.stripe.com", st.URL()) })
	if err := f.store.PutXref(context.Background(), "Customer", "stripe-billing", "cus_ada", "C-100"); err != nil {
		t.Fatal(err)
	}
	return f, st
}

func (f *fixture) payments(where string) int {
	f.t.Helper()
	var n int
	if err := f.pool.QueryRow(context.Background(), "SELECT count(*) FROM "+f.schema+"_erp.payments WHERE "+where).Scan(&n); err != nil {
		f.t.Fatal(err)
	}
	return n
}

func TestStripePaymentsAreRecordedWithoutAPerson(t *testing.T) {
	f, st := newStripeFixture(t)
	a := st.PayInvoice("cus_ada", 123450, "eur")
	b := st.PayInvoice("cus_ada", 9900, "eur")
	runs := f.dispatch()
	if len(runs) != 2 {
		t.Fatalf("runs = %d", len(runs))
	}
	for _, in := range runs {
		res, err, pending := f.run(in)
		if err != nil {
			t.Fatal(err)
		}
		// Low-risk writes under the threshold need no approval.
		if len(pending) != 0 || len(res.Writes) != 2 || res.Writes[1].Status != writeguard.StatusCommitted {
			t.Fatalf("writes = %+v, pending = %d", res.Writes, len(pending))
		}
	}
	if f.payments(`external_id = '`+a+`' AND customer_id = 'C-100' AND amount = 1234.50 AND currency = 'EUR'
		AND paid_on = '2026-09-25' AND invoice_ref = 'TURGON-0001'`) != 1 || f.payments("true") != 2 {
		t.Fatal("ERP payments missing or mis-mapped")
	}
	var id int64
	_ = f.pool.QueryRow(context.Background(), "SELECT id FROM "+f.schema+"_erp.payments WHERE external_id = $1", b).Scan(&id)
	if md := st.Invoice(b)["metadata"].(map[string]any); md["erp_payment_id"] != fmt.Sprint(id) {
		t.Fatalf("invoice metadata %v, want ERP payment %d", md, id)
	}
	if again := f.dispatch(); len(again) != 0 {
		t.Fatalf("re-dispatched %d", len(again))
	}
}

func TestLargePaymentsWaitForApproval(t *testing.T) {
	f, st := newStripeFixture(t)
	st.PayInvoice("cus_ada", 6_000_000, "eur") // 60,000.00: above the approval threshold
	_, err, pending := f.run(f.dispatch()[0], approve("03-write"))
	if err != nil || len(pending) != 1 || !strings.Contains(strings.Join(pending[0].Reasons, ","), "amount above approval threshold") {
		t.Fatalf("err %v, pending %+v", err, pending)
	}
}

func TestStripeRejectionVoidsTheERPPayment(t *testing.T) {
	f, st := newStripeFixture(t)
	id := st.PayInvoice("cus_ada", 5000, "eur")
	st.FailUpdates = true
	_, err, _ := f.run(f.dispatch()[0])
	if errType(err) != ErrTypeInvalid || !strings.Contains(err.Error(), "locked for accounting review") {
		t.Fatalf("err = %v (%s)", err, errType(err))
	}
	if f.payments(`external_id = '`+id+`' AND status = 'voided'`) != 1 {
		t.Fatal("ERP payment was not voided after Stripe rejected the link")
	}
}

func (f *fixture) runFor(shop *shoptest.Shop, email string) (RunResult, error, []PendingApproval) {
	f.t.Helper()
	id := shop.AddOrder(map[string]any{
		"name": "#2001", "email": email, "created_at": "2026-09-25T09:00:00Z",
		"subtotal_price": "80.00", "currency": "eur", "line_items": []any{map[string]any{"sku": "M-2", "quantity": 1}},
	})
	for _, in := range f.dispatch() {
		if in.Event.ID == fmt.Sprint(id) {
			return f.run(in, approve("03-write"))
		}
	}
	f.t.Fatalf("no run for order %d", id)
	return RunResult{}, nil, nil
}

// An address already linked in another system is the same customer: the
// probabilistic strategy links it without a person, and audits the match.
func TestKnownAddressIsMatchedAutomatically(t *testing.T) {
	f, shop, _ := newShopifyFixture(t)
	ctx := context.Background()
	if err := f.store.Link(ctx, "Customer", "shop-db", "grace@lovelace-gmbh.example", "C-100",
		identity.Attributes{"email": "grace@lovelace-gmbh.example", "domain": "lovelace-gmbh.example"}); err != nil {
		t.Fatal(err)
	}
	res, err, _ := f.runFor(shop, "Grace@Lovelace-GmbH.example")
	if err != nil || res.Writes[0].Status != writeguard.StatusCommitted {
		t.Fatalf("err %v, writes %+v", err, res.Writes)
	}
	if f.orders(`customer_id = 'C-100' AND external_id LIKE 'SHOPIFY-%' AND net_value = 80`) != 1 {
		t.Fatal("order not booked to the matched customer")
	}
	log := f.auditLog.String()
	if !strings.Contains(log, `"actor":"turgon/resolver","action":"xref.matched"`) || !strings.Contains(log, "same email address") {
		t.Fatalf("match not audited:\n%s", log)
	}
	if id, ok, _ := f.store.Xref(ctx, "Customer", "shopify-store", "grace@lovelace-gmbh.example"); !ok || id != "C-100" {
		t.Fatal("the match was not kept")
	}
}

// A new buyer at a known company is not certain enough: a steward decides,
// with the company suggested, and the link is learned.
func TestNewBuyerAtKnownCompanyIsSuggestedThenLearned(t *testing.T) {
	f, shop, _ := newShopifyFixture(t)
	ctx := context.Background()
	if err := f.store.Link(ctx, "Customer", "shop-db", "ada@lovelace-gmbh.example", "C-100",
		identity.Attributes{"email": "ada@lovelace-gmbh.example", "domain": "lovelace-gmbh.example"}); err != nil {
		t.Fatal(err)
	}
	_, err, _ := f.runFor(shop, "edsger@lovelace-gmbh.example")
	var app *temporal.ApplicationError
	var u Unresolved
	if errType(err) != ErrTypeUnresolved || !errors.As(err, &app) || app.Details(&u) != nil {
		t.Fatalf("err %v", err)
	}
	if len(u.Suggestions) != 1 || u.Suggestions[0].Master != "C-100" || u.Suggestions[0].Score < 0.5 || u.Suggestions[0].Score >= 0.95 ||
		u.Suggestions[0].Reasons[0] != "same company email domain" || u.Attributes["domain"] != "lovelace-gmbh.example" {
		t.Fatalf("details %+v", u)
	}
	// The steward confirms the suggestion (as the console does, with the
	// run's attributes); the buyer's next order goes straight through.
	if err := f.store.Link(ctx, u.Entity, u.System, u.Ref, "C-100", u.Attributes); err != nil {
		t.Fatal(err)
	}
	if _, err, _ := f.runFor(shop, "edsger@lovelace-gmbh.example"); err != nil {
		t.Fatalf("after the steward's link: %v", err)
	}
	if f.orders(`customer_id = 'C-100' AND external_id LIKE 'SHOPIFY-%' AND net_value = 80`) != 1 {
		t.Fatal("order not booked")
	}
}

// Two master records share a domain: no automatic match, both suggested.
func TestAmbiguousMatchesGoToAPerson(t *testing.T) {
	f, shop, _ := newShopifyFixture(t)
	ctx := context.Background()
	if _, err := f.pool.Exec(ctx, "INSERT INTO "+f.schema+"_erp.customers (id, name) VALUES ('C-101', 'Lovelace Holding')"); err != nil {
		t.Fatal(err)
	}
	for master, email := range map[string]string{"C-100": "ada@lovelace.example", "C-101": "cfo@lovelace.example"} {
		if err := f.store.Link(ctx, "Customer", "shop-db", email, master, identity.Attributes{"email": email, "domain": "lovelace.example"}); err != nil {
			t.Fatal(err)
		}
	}
	_, err, _ := f.runFor(shop, "new@lovelace.example")
	var app *temporal.ApplicationError
	var u Unresolved
	if !errors.As(err, &app) || app.Details(&u) != nil || len(u.Suggestions) != 2 {
		t.Fatalf("err %v, details %+v", err, u)
	}
}

// newHubSpotFixture runs the hubspot-won-deals-to-erp recipe against the
// fake HubSpot account and Postgres. The buyer's address is known from the
// web shop, so the probabilistic match links it.
func newHubSpotFixture(t *testing.T) (*fixture, *hubspottest.HubSpot) {
	t.Helper()
	hs := hubspottest.New("pat-eu1-turgon")
	t.Cleanup(hs.Close)
	f := newFixtureFor(t, "hubspot-won-deals-to-erp", connector.StaticSecrets{"openbao://hubspot-crm/private-app-token": "pat-eu1-turgon"},
		func(cfg string) string { return strings.ReplaceAll(cfg, "https://api.hubapi.com", hs.URL()) })
	if err := f.store.Link(context.Background(), "Customer", "shop-db", "ada@lovelace-gmbh.example", "C-100",
		identity.Attributes{"email": "ada@lovelace-gmbh.example", "domain": "lovelace-gmbh.example"}); err != nil {
		t.Fatal(err)
	}
	return f, hs
}

func hubspotDeal(email, amount string) map[string]string {
	return map[string]string{"dealname": "Lovelace rollout", "customer_email": email, "amount": amount,
		"deal_currency_code": "eur", "dealstage": "closedwon", "closedate": "2026-09-24T15:00:00.000Z"}
}

func TestHubSpotWonDealBecomesERPOrderAndIsLinkedBack(t *testing.T) {
	f, hs := newHubSpotFixture(t)
	id := hs.AddDeal(hubspotDeal("Ada@Lovelace-GmbH.example", "1500.50"))
	open := hs.AddDeal(map[string]string{"dealname": "Turing pilot", "dealstage": "qualifiedtobuy", "customer_email": "alan@turing.example"})
	runs := f.dispatch()
	if len(runs) != 1 || runs[0].Event.ID != id {
		t.Fatalf("runs = %+v", runs)
	}
	res, err, pending := f.run(runs[0], approve("03-write"))
	if err != nil {
		t.Fatal(err)
	}
	// The sales order waited for approval; the deal update did not.
	if len(res.Writes) != 2 || res.Writes[1].Status != writeguard.StatusCommitted || len(pending) != 1 || pending[0].Step != "03-write" {
		t.Fatalf("writes = %+v, pending = %+v", res.Writes, pending)
	}
	if f.orders(`external_id = 'HUBSPOT-`+id+`' AND customer_id = 'C-100' AND net_value = 1500.50 AND currency = 'EUR'
		AND order_date = '2026-09-24'`) != 1 {
		t.Fatal("ERP order missing or mis-mapped")
	}
	var erpID int64
	_ = f.pool.QueryRow(context.Background(), "SELECT id FROM "+f.schema+"_erp.sales_orders").Scan(&erpID)
	if got := hs.Deal(id)["erp_order_number"]; got != fmt.Sprint(erpID) {
		t.Fatalf("deal erp_order_number = %q, want %d", got, erpID)
	}
	log := f.auditLog.String()
	if !strings.Contains(log, `"action":"xref.matched"`) || !strings.Contains(log, `"previous":{"properties.erp_order_number":null}`) {
		t.Fatalf("match or write-back not audited:\n%s", log)
	}
	// The update changed the deal, but a linked deal is no longer searched,
	// so it does not start a second run. A deal won later does.
	if again := f.dispatch(); len(again) != 0 {
		t.Fatalf("re-dispatched %+v", again)
	}
	hs.SetStage(open, "closedwon")
	if later := f.dispatch(); len(later) != 1 || later[0].Event.ID != open {
		t.Fatalf("later = %+v", later)
	}
}

func TestHubSpotRejectionCancelsTheERPOrder(t *testing.T) {
	f, hs := newHubSpotFixture(t)
	id := hs.AddDeal(hubspotDeal("ada@lovelace-gmbh.example", "99"))
	hs.FailUpdates = true
	_, err, _ := f.run(f.dispatch()[0], approve("03-write"))
	if errType(err) != ErrTypeInvalid || !strings.Contains(err.Error(), "read-only in this portal") {
		t.Fatalf("err = %v (%s)", err, errType(err))
	}
	if f.orders(`external_id = 'HUBSPOT-`+id+`' AND status = 'cancelled'`) != 1 {
		t.Fatal("ERP order was not cancelled after HubSpot rejected the update")
	}
	if _, ok := hs.Deal(id)["erp_order_number"]; ok {
		t.Fatal("deal changed")
	}
}

// A buyer nobody knows stops the run for a steward before anything is
// written, and the deal stays in the search for won deals.
func TestHubSpotUnknownBuyerWaitsForASteward(t *testing.T) {
	f, hs := newHubSpotFixture(t)
	hs.AddDeal(hubspotDeal("someone@elsewhere.example", "10"))
	_, err, _ := f.run(f.dispatch()[0])
	if errType(err) != ErrTypeUnresolved || f.orders("true") != 0 {
		t.Fatalf("err = %v (%s)", err, errType(err))
	}
}
