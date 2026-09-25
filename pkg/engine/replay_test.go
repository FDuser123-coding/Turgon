package engine

import (
	"path/filepath"
	"testing"

	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Histories recorded from real runs on a Temporal server replay against
// the current workflow code, so upgrading workers never breaks runs in
// flight:
//   - old-completed, old-waiting: runs started before notifications
//     existed, one finished and one waiting for approval;
//   - new-completed, new-steward: runs that sent notifications.
//
// Record a new one with `temporal workflow show -w <id> -o json`.
func TestRecordedRunsReplay(t *testing.T) {
	files, _ := filepath.Glob("testdata/histories/*.json")
	if len(files) < 4 {
		t.Fatalf("histories: %v", files)
	}
	for _, f := range files {
		r := worker.NewWorkflowReplayer()
		r.RegisterWorkflowWithOptions(IntegrationWorkflow, workflow.RegisterOptions{Name: WorkflowName})
		if err := r.ReplayWorkflowHistoryFromJSONFile(nil, f); err != nil {
			t.Errorf("%s: %v", filepath.Base(f), err)
		}
	}
}
