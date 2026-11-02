package console

import (
	"errors"
	"fmt"
	"testing"

	"go.temporal.io/sdk/temporal"

	"github.com/fduser123-coding/turgon/pkg/engine"
)

// A refused write fails its run with Turgon's type around the connector's
// wrapped Go error, which Temporal records as an application error of type
// wrapError: the run shows TurgonInvalid, with the innermost message.
func TestFailuresShowTurgonsTypeAndTheSystemsMessage(t *testing.T) {
	refusal := fmt.Errorf("write payload invalid: %w", errors.New(`PUT /orders/1.json: HTTP 422: {"errors":{"tags":["can't be blank"]}}`))
	err := temporal.NewNonRetryableApplicationError("writeguard: commit shopify-store.update-order: "+refusal.Error(), engine.ErrTypeInvalid, refusal)
	fc := engine.FailureConverter()
	typ, msg := describeFailure(fc.FailureToError(fc.ErrorToFailure(err)))
	if typ != engine.ErrTypeInvalid || msg != refusal.Error() {
		t.Fatalf("%q: %q", typ, msg)
	}
	if typ, msg := describeFailure(errors.New("workflow timed out")); typ != "" || msg != "workflow timed out" {
		t.Fatalf("%q: %q", typ, msg)
	}
}
