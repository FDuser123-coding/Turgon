// Package engine runs compiled integrations on Temporal (architecture §7.6,
// AD-04, AD-06). One generic workflow interprets any compiled workflow
// definition; activities do all I/O. Writes go through the write guard in
// two phases so a workflow can wait durably for a human approval, and a
// failure after a committed write runs the declared compensations.
package engine

import (
	"encoding/json"
	"time"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/identity"
	"github.com/fduser123-coding/turgon/pkg/policy"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// Workflow, signal and query names.
const (
	WorkflowName   = "turgon.integration"
	SignalApproval = "turgon.approval"
	QueryPending   = "turgon.pending"
	// SignalApprovalWaiting tells a run asking for a write's approval
	// that another run (a later change of the same record) waits on its
	// decision; the payload is that run's ID.
	SignalApprovalWaiting = "turgon.approval-waiting"
	// SignalApprovalClosed wakes a waiting run once the request it waited
	// on is decided; the payload is the write's ledger key.
	SignalApprovalClosed = "turgon.approval-closed"
	// SignalApprovalNewer replaces SignalApprovalWaiting: it carries the
	// waiting run's request (a NewerChange), so the run holding the
	// request can tell the approvers what changed.
	SignalApprovalNewer = "turgon.approval-newer"
)

// NewerChange is a later change's run waiting on an open request, with the
// request it would make.
type NewerChange struct {
	Run     string             `json:"run"`
	Request writeguard.Request `json:"request"`
}

// Error types reported by activities. Temporal does not retry them.
const (
	ErrTypeDenied     = "TurgonDenied"
	ErrTypeRejected   = "TurgonRejected"
	ErrTypeInvalid    = "TurgonInvalid"
	ErrTypeUnresolved = "TurgonUnresolved"
	ErrTypeMapping    = "TurgonMapping"
	// ErrTypeCompensationFailed means a run failed and left writes that
	// could not be undone; its cause is the original failure.
	ErrTypeCompensationFailed = "TurgonCompensationFailed"
)

// Unresolved is the detail of an ErrTypeUnresolved failure: the source
// record a data steward must link to a master record before the run is
// retried.
type Unresolved struct {
	Entity string `json:"entity"`
	System string `json:"system"`
	Ref    string `json:"ref"`
	// Attributes are the record's identifying values; a steward's link
	// keeps them for matching later records.
	Attributes identity.Attributes `json:"attributes,omitempty"`
	// Suggestions are the master records it most likely is.
	Suggestions []identity.Suggestion `json:"suggestions,omitempty"`
}

// RunInput starts one workflow run for one source event. It carries the
// workflow definition itself, so a run keeps executing the spec it started
// with even if a newer spec is deployed meanwhile.
type RunInput struct {
	SpecDigest string            `json:"specDigest"`
	Workflow   compiler.Workflow `json:"workflow"`
	Event      connector.Event   `json:"event"`
	// ApprovalTimeout rejects a pending approval after this long. Default 72h.
	ApprovalTimeout time.Duration `json:"approvalTimeout,omitempty"`
	// TaskQueue is the queue of the workers that run this spec.
	TaskQueue string `json:"taskQueue,omitempty"`
}

// RunResult reports what a run wrote.
type RunResult struct {
	Writes []WriteRecord `json:"writes"`
	// Skipped says why the run ended before its last step without a
	// failure: a resolve step with ifUnknown: skip found no
	// cross-reference.
	Skipped string `json:"skipped,omitempty"`
}

type WriteRecord struct {
	Step      string            `json:"step"`
	Endpoint  string            `json:"endpoint"`
	Operation string            `json:"operation"`
	Status    writeguard.Status `json:"status"`
	Result    json.RawMessage   `json:"result,omitempty"`
}

// ApprovalSignal is sent by the console, CLI or a chat integration.
// It must name the waiting step and echo the pending approval's Digest, so
// a decision always refers to the exact request the approver was shown.
type ApprovalSignal struct {
	Step   string `json:"step"`
	Digest string `json:"digest"`
	Status string `json:"status"` // approved or rejected
	By     string `json:"by"`
	Note   string `json:"note,omitempty"`
}

// PendingApproval is returned by the pending query while a run waits.
type PendingApproval struct {
	Step string `json:"step"`
	// Digest is the SHA-256 of the request; approvals must echo it.
	Digest  string             `json:"digest"`
	Request writeguard.Request `json:"request"`
	Preview json.RawMessage    `json:"preview,omitempty"`
	Reasons []string           `json:"reasons,omitempty"`
	Since   time.Time          `json:"since"`
	// Waiting lists runs for later changes of the same record that wait on
	// this decision: approving makes their write a duplicate, rejecting
	// lets the newest of them ask with its own data.
	Waiting []string `json:"waiting,omitempty"`
}

// LedgerInput names a write's request for approval in the ledger.
type LedgerInput struct {
	Key   string    `json:"key"`
	Run   string    `json:"run"`
	Until time.Time `json:"until,omitempty"`
}

// Activity inputs and outputs.

type MapInput struct {
	Config compiler.MapConfig `json:"config"`
	Doc    map[string]any     `json:"doc"`
}

type ResolveInput struct {
	Config compiler.ResolveConfig `json:"config"`
	System string                 `json:"system"`
	Doc    map[string]any         `json:"doc"`
}

type PrepareInput struct {
	Workflow string               `json:"workflow"`
	Step     string               `json:"step"`
	Config   compiler.WriteConfig `json:"config"`
	Source   map[string]any       `json:"source"`
	Doc      map[string]any       `json:"doc"`
}

type PrepareOutput struct {
	Request  writeguard.Request  `json:"request"`
	Prepared writeguard.Prepared `json:"prepared"`
}

type CommitInput struct {
	Request  writeguard.Request `json:"request"`
	Approval *policy.Approval   `json:"approval,omitempty"`
}

// LinkInput records a written record as a cross-reference: (Entity,
// System, the ID at Link.ID in Result) -> the document's Link.Master.
type LinkInput struct {
	Link   v1alpha1.WriteLink `json:"link"`
	System string             `json:"system"`
	Result json.RawMessage    `json:"result"`
	Doc    map[string]any     `json:"doc"`
}

type CompensateInput struct {
	Request writeguard.Request `json:"request"`
	Result  json.RawMessage    `json:"result"`
}
