package console

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

type wakeRec struct{ woken []string }

func (w *wakeRec) Wake(_ context.Context, key string, waiters []string) int {
	w.woken = append(w.woken, waiters...)
	return len(waiters)
}

// Operators see the open requests for approval, with whether the run
// holding each still runs, and release one whose run ended: the release is
// audited first, and the runs that waited on it are woken.
func TestOperatorsReleaseOrphanedRequests(t *testing.T) {
	runs := &failedRuns{fakeRuns: newRuns(), status: map[string]string{
		"deals/006A@1": "terminated", "deals/006A@2": "running", "deals/006B@1": "running",
	}}
	ledger := writeguard.NewMemoryStore()
	soon := time.Now().Add(time.Hour)
	_, _ = ledger.OpenApproval("erp/create/006A", "deals/006A@1", soon)
	_, _ = ledger.OpenApproval("erp/create/006A", "deals/006A@2", soon) // waits on the terminated one
	_, _ = ledger.OpenApproval("erp/create/006B", "deals/006B@1", soon)
	waker := &wakeRec{}
	var log bytes.Buffer
	s := New(Config{Runs: runs, Auth: operatorAuth(), Ledger: ledger, Waker: waker, Recorder: audit.New(&log)})

	rec := do(t, s, "GET", "/api/ledger", "", as("vera@example.com", ""))
	var held []HeldRequest
	if rec.Code != 200 || json.Unmarshal(rec.Body.Bytes(), &held) != nil || len(held) != 2 {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	if !held[0].Orphaned || held[0].HolderStatus != "terminated" || len(held[0].Waiters) != 1 || held[1].Orphaned {
		t.Fatalf("%+v", held)
	}

	body := `{"key":"erp/create/006A","holder":"deals/006A@1","note":"its run was terminated"}`
	for name, c := range map[string]struct {
		body, groups string
		code         int
	}{
		"approvers are not operators": {body, "turgon-approvers", http.StatusForbidden},
		"a reason is required":        {strings.Replace(body, "its run was terminated", "", 1), "turgon-operators", http.StatusBadRequest},
		"a running holder decides":    {`{"key":"erp/create/006B","holder":"deals/006B@1","note":"x"}`, "turgon-operators", http.StatusConflict},
		"another run holds it":        {strings.Replace(body, "006A@1", "006A@9", 1), "turgon-operators", http.StatusConflict},
		"unknown fields are refused":  {strings.Replace(body, `"note"`, `"force":true,"note"`, 1), "turgon-operators", http.StatusBadRequest},
	} {
		if rec := do(t, s, "POST", "/api/ledger/release", c.body, as("olga@example.com", c.groups)); rec.Code != c.code {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body)
		}
	}
	if len(waker.woken) != 0 {
		t.Fatalf("refused releases woke %v", waker.woken)
	}

	rec = do(t, s, "POST", "/api/ledger/release", body, as("olga@example.com", "turgon-operators"))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"woken":1`) || len(waker.woken) != 1 || waker.woken[0] != "deals/006A@2" {
		t.Fatalf("%d %s, woken %v", rec.Code, rec.Body, waker.woken)
	}
	if !strings.Contains(log.String(), `"actor":"olga@example.com","action":"approval.released"`) || !strings.Contains(log.String(), `"holderStatus":"terminated"`) {
		t.Fatalf("audit:\n%s", log.String())
	}
	if left, _ := ledger.OpenApprovals(); len(left) != 1 || left[0].Key != "erp/create/006B" {
		t.Fatalf("left %+v", left)
	}
}
