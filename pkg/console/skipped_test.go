package console

import (
	"testing"

	commonpb "go.temporal.io/api/common/v1"

	"github.com/fduser123-coding/turgon/pkg/engine"
)

// A skipped run's reason is read from its memo, as the engine records it,
// so the list shows it without reading each run's result.
func TestSkippedReasonFromMemo(t *testing.T) {
	p, err := engine.DataConverter().ToPayload("02-resolve: a SalesOrder from exact-online without a cross-reference")
	if err != nil {
		t.Fatal(err)
	}
	got := skippedMemo(map[string]*commonpb.Payload{engine.MemoSkipped: p})
	if got != "02-resolve: a SalesOrder from exact-online without a cross-reference" {
		t.Fatalf("skipped = %q", got)
	}
	if skippedMemo(nil) != "" || skippedMemo(map[string]*commonpb.Payload{"other": p}) != "" {
		t.Fatal("a run without the memo field is not skipped")
	}
}
