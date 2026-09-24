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
	"github.com/fduser123-coding/turgon/pkg/connector/salesforce"
	"github.com/fduser123-coding/turgon/pkg/connector/salesforce/sftest"
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
func newFixtureFor(t *testing.T, recipe string, extra connector.StaticSecrets) *fixture {
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
		c.Config = json.RawMessage(localize(string(c.Config), schema))
	}

	url := pgtest.URL(t, schema)
	var buf bytes.Buffer
	secrets := connector.StaticSecrets{"openbao://shop-db/dsn": url, "openbao://erp-db/dsn": url}
	for k, v := range extra {
		secrets[k] = v
	}
	rt, err := New(ctx, spec, Options{
		Registry: connector.Registry{postgres.Name: postgres.Factory, salesforce.Name: salesforce.Factory},
		Secrets:  secrets,
		Store:    store,
		Resolver: store,
		Audit:    audit.New(&syncWriter{w: &buf}),
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
	runs map[string]RunInput
	ids  []string
}

func (r *recorder) Start(_ context.Context, id string, in RunInput) error {
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
