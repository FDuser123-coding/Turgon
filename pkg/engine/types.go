// Package engine runs compiled integrations on Temporal (architecture §7.6,
// AD-04, AD-06). One generic workflow interprets any compiled workflow
// definition; activities do all I/O. Writes go through the write guard in
// two phases so a workflow can wait durably for a human approval, and a
// failure after a committed write runs the declared compensations.
package engine

import (
	"encoding/json"
	"time"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/policy"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// Workflow, signal and query names.
const (
	WorkflowName   = "turgon.integration"
	SignalApproval = "turgon.approval"
	QueryPending   = "turgon.pending"
)

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

type CompensateInput struct {
	Request writeguard.Request `json:"request"`
	Result  json.RawMessage    `json:"result"`
}
