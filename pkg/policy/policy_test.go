package policy

import (
	"context"
	"testing"
)

func TestWritebackDefault(t *testing.T) {
	op := Subject{ID: "u", Roles: []string{RoleOperator}}
	reader := Subject{ID: "r", Roles: []string{RoleReader}}
	nobody := Subject{ID: "n"}
	cases := []struct {
		name            string
		in              Input
		allow, approval bool
	}{
		{"read by reader", Input{Tool: Tool{Risk: "read"}, Subject: reader}, true, false},
		{"read by nobody", Input{Tool: Tool{Risk: "read"}, Subject: nobody}, false, false},
		{"low by operator", Input{Tool: Tool{Risk: "low"}, Subject: op}, true, false},
		{"low by reader", Input{Tool: Tool{Risk: "low"}, Subject: reader}, false, false},
		{"low above amount", Input{Tool: Tool{Risk: "low"}, Subject: op, Action: Action{Amount: 60000}}, true, true},
		{"high unapproved", Input{Tool: Tool{Risk: "high"}, Subject: op}, false, true},
		{"high approved", Input{Tool: Tool{Risk: "high"}, Subject: nobody, Approval: Approval{Status: ApprovalApproved}}, true, true},
		{"high rejected", Input{Tool: Tool{Risk: "high"}, Subject: op, Approval: Approval{Status: ApprovalRejected}}, false, true},
		{"unknown risk", Input{Tool: Tool{Risk: "yolo"}, Subject: op}, false, false},
	}
	for _, c := range cases {
		d, err := WritebackDefault{}.Decide(context.Background(), c.in)
		if err != nil {
			t.Fatal(err)
		}
		if d.Allow != c.allow || d.RequireApproval != c.approval {
			t.Errorf("%s: allow=%v approval=%v, want %v %v", c.name, d.Allow, d.RequireApproval, c.allow, c.approval)
		}
	}
}
