package console

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/verifier"
)

// reviewCatalog copies the examples with shop-orders-to-erp's AI-mapped
// lines field lowered to confidence 0.80, below the 0.95 write threshold.
func reviewCatalog(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	err := filepath.Walk("../../examples", func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, _ := filepath.Rel("../../examples", p)
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if rel == filepath.Join("mappings", "shop-order-to-sales-order.yaml") {
			data = []byte(strings.Replace(string(data), "confidence: 0.97", "confidence: 0.80", 1))
		}
		if err := os.MkdirAll(filepath.Join(dir, filepath.Dir(rel)), 0o755); err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dir, rel), data, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

type memReviews struct{ rv verifier.Reviews }

func (m *memReviews) Put(_ context.Context, rv verifier.Review) error {
	if m.rv == nil {
		m.rv = verifier.Reviews{}
	}
	m.rv[verifier.ReviewKey{Mapping: rv.Mapping, Target: rv.Target, ExpressionSHA256: verifier.ExpressionDigest(rv.Expression)}] = rv
	return nil
}

func (m *memReviews) All(context.Context) (verifier.Reviews, error) { return m.rv, nil }

func shopReport(t *testing.T, s *Server) *verifier.Report {
	t.Helper()
	rec := do(t, s, "GET", "/api/catalog", "", as("vera@example.com", "sales"))
	var c CatalogReport
	if err := json.Unmarshal(rec.Body.Bytes(), &c); err != nil {
		t.Fatal(err)
	}
	for _, r := range c.Reports {
		if strings.HasPrefix(r.Subject, "Recipe/shop-orders-to-erp@") {
			return r
		}
	}
	t.Fatalf("no shop-orders-to-erp report in %s", rec.Body)
	return nil
}

const linesExpr = "items.{ 'material': sku, 'quantity': qty }[]"

func TestStewardApprovesAQueuedMapping(t *testing.T) {
	var log bytes.Buffer
	reviews := &memReviews{}
	s := New(Config{Runs: newRuns(), Auth: stewardAuth(), Catalogs: []string{reviewCatalog(t)}, Reviews: reviews, Recorder: audit.New(&log)})

	r := shopReport(t, s)
	if r.Deployable || len(r.ReviewQueue) != 1 || r.ReviewQueue[0].Target != "lines" {
		t.Fatalf("before review: deployable %v, queue %+v", r.Deployable, r.ReviewQueue)
	}
	item := r.ReviewQueue[0]
	body := func(decision, note, expr string) string {
		b, _ := json.Marshal(ReviewRequest{Mapping: item.Mapping, Target: item.Target, Expression: expr, Decision: decision, Note: note})
		return string(b)
	}

	// Only stewards review; a rejection needs a reason; the expression
	// must be the one queued.
	if rec := do(t, s, "POST", "/api/reviews", body("approved", "", linesExpr), as("carl@example.com", "turgon-approvers")); rec.Code != 403 {
		t.Fatalf("non-steward: %d", rec.Code)
	}
	if rec := do(t, s, "POST", "/api/reviews", body("rejected", "", linesExpr), as("sam@example.com", "turgon-stewards")); rec.Code != 400 {
		t.Fatalf("rejection without a note: %d", rec.Code)
	}
	if rec := do(t, s, "POST", "/api/reviews", body("approved", "", "items.sku"), as("sam@example.com", "turgon-stewards")); rec.Code != 409 {
		t.Fatalf("another expression: %d %s", rec.Code, rec.Body)
	}

	// A rejection blocks the recipe with the steward's reason.
	if rec := do(t, s, "POST", "/api/reviews", body("rejected", "qty is in boxes, the ERP counts pieces", linesExpr), as("sam@example.com", "turgon-stewards")); rec.Code != 200 {
		t.Fatalf("reject: %d %s", rec.Code, rec.Body)
	}
	r = shopReport(t, s)
	if r.Deployable || !strings.Contains(findingsText(r), "rejected by sam@example.com: qty is in boxes") {
		t.Fatalf("after rejection: %+v", r)
	}

	// The mapping author fixes it... here, the steward reconsiders: the
	// field is still queued without the stored reviews, so it can be approved.
	if rec := do(t, s, "POST", "/api/reviews", body("approved", "checked against the ERP unit of measure", linesExpr), as("sam@example.com", "turgon-stewards")); rec.Code != 200 {
		t.Fatalf("approve: %d %s", rec.Code, rec.Body)
	}
	r = shopReport(t, s)
	if !r.Deployable || len(r.ReviewQueue) != 0 || !strings.Contains(findingsText(r), "approved by sam@example.com") {
		t.Fatalf("after approval: deployable %v, queue %+v, findings %s", r.Deployable, r.ReviewQueue, findingsText(r))
	}
	entries := log.String()
	if strings.Count(entries, `"action":"mapping.reviewed"`) != 2 || !strings.Contains(entries, `"decision":"approved"`) {
		t.Fatalf("audit log:\n%s", entries)
	}
}

// Changing the expression needs a new review.
func TestReviewHoldsForOneExpression(t *testing.T) {
	reviews := verifier.Reviews{}
	(&memReviews{rv: reviews}).Put(context.Background(), verifier.Review{Mapping: "shop-order-to-sales-order@1.0.0", Target: "lines",
		Expression: "items.sku", Decision: "approved", Reviewer: "sam"})
	s := New(Config{Runs: newRuns(), Auth: stewardAuth(), Catalogs: []string{reviewCatalog(t)}, Reviews: &memReviews{rv: reviews}})
	if r := shopReport(t, s); r.Deployable || len(r.ReviewQueue) != 1 {
		t.Fatalf("an approval of another expression was applied: %+v", r)
	}
}

func findingsText(r *verifier.Report) string {
	var b strings.Builder
	for _, f := range r.Findings {
		b.WriteString(f.Message + "\n")
	}
	return b.String()
}
