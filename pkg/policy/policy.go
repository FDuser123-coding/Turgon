// Package policy defines the authorization decision the write guard asks
// for on every action. Production deployments evaluate signed OPA bundles
// (architecture §9); WritebackDefault is the built-in fallback and mirrors
// the Rego in examples/policies/writeback-default.yaml (Appendix C).
package policy

import (
	"context"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
)

// Approval statuses.
const (
	ApprovalNone     = ""
	ApprovalApproved = "approved"
	ApprovalRejected = "rejected"
)

// Input is the document a policy evaluates; field names match the Rego input.
type Input struct {
	// Recipe names the recipe making the write, if any; it selects the
	// recipe's own policy packs.
	Recipe   string   `json:"recipe,omitempty"`
	Tool     Tool     `json:"tool"`
	Subject  Subject  `json:"subject"`
	Action   Action   `json:"action"`
	Approval Approval `json:"approval"`
}

type Tool struct {
	Name string `json:"name"`
	Risk string `json:"risk"`
}

// Subject is who is acting. For agents, OnBehalfOf names the user.
type Subject struct {
	ID         string   `json:"id"`
	Roles      []string `json:"roles"`
	Agent      bool     `json:"agent,omitempty"`
	OnBehalfOf string   `json:"onBehalfOf,omitempty"`
}

func (s Subject) HasRole(role string) bool {
	for _, r := range s.Roles {
		if r == role {
			return true
		}
	}
	return false
}

type Action struct {
	Entity string  `json:"entity,omitempty"`
	Amount float64 `json:"amount,omitempty"`
}

type Approval struct {
	Status string `json:"status"`
	By     string `json:"by,omitempty"`
	// Note is the approver's reason, kept in the audit log.
	Note string `json:"note,omitempty"`
}

// Decision is a policy result.
type Decision struct {
	Allow           bool `json:"allow"`
	RequireApproval bool `json:"requireApproval"`
	// Denied means no approval can make the write proceed.
	Denied  bool     `json:"denied,omitempty"`
	Reasons []string `json:"reasons,omitempty"`
}

// Decider evaluates policy.
type Decider interface {
	Decide(ctx context.Context, in Input) (Decision, error)
}

// DeciderFunc adapts a function to Decider.
type DeciderFunc func(ctx context.Context, in Input) (Decision, error)

func (f DeciderFunc) Decide(ctx context.Context, in Input) (Decision, error) { return f(ctx, in) }

// Role names used by the default policy.
const (
	RoleOperator = "integration-operator"
	RoleReader   = "integration-reader"
)

// WritebackDefault is deny-by-default:
//   - reads are allowed for operators and readers;
//   - low-risk writes are allowed for operators;
//   - high-risk writes are allowed only once approved;
//   - approval is required for high-risk writes and for amounts above the threshold.
type WritebackDefault struct {
	AmountThreshold float64 // default 50000
}

func (p WritebackDefault) Decide(_ context.Context, in Input) (Decision, error) {
	threshold := p.AmountThreshold
	if threshold == 0 {
		threshold = 50000
	}
	var d Decision
	switch in.Tool.Risk {
	case v1alpha1.RiskRead:
		d.Allow = in.Subject.HasRole(RoleOperator) || in.Subject.HasRole(RoleReader)
	case v1alpha1.RiskLow:
		d.Allow = in.Subject.HasRole(RoleOperator)
	case v1alpha1.RiskHigh:
		d.Allow = in.Approval.Status == ApprovalApproved
		d.RequireApproval = true
		d.Reasons = append(d.Reasons, "high-risk tool")
	default:
		d.Reasons = append(d.Reasons, "unknown risk tier "+in.Tool.Risk)
	}
	if in.Action.Amount > threshold {
		d.RequireApproval = true
		d.Reasons = append(d.Reasons, "amount above approval threshold")
	}
	if in.Approval.Status == ApprovalRejected {
		d.Allow, d.Denied = false, true
		d.Reasons = append(d.Reasons, "approval rejected")
	}
	return d, nil
}
