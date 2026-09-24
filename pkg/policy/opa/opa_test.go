package opa

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/catalog"
	"github.com/fduser123-coding/turgon/pkg/policy"
)

func examplePack(t *testing.T, name string) Module {
	t.Helper()
	cat, err := catalog.Load("../../../examples")
	if err != nil {
		t.Fatal(err)
	}
	p, err := cat.Policy(name)
	if err != nil {
		t.Fatal(err)
	}
	return Module{Name: p.Metadata.Name, Source: p.Spec.Rego}
}

// The Rego pack in examples and the built-in Go default must never drift.
func TestRegoPackMatchesBuiltInDefault(t *testing.T) {
	ctx := context.Background()
	d, err := New(ctx, []Module{examplePack(t, "writeback-default")})
	if err != nil {
		t.Fatal(err)
	}
	subjects := []policy.Subject{
		{ID: "op", Roles: []string{policy.RoleOperator}},
		{ID: "reader", Roles: []string{policy.RoleReader}},
		{ID: "nobody"},
		{ID: "agent", Agent: true, OnBehalfOf: "alice", Roles: []string{policy.RoleOperator, "sales"}},
		{ID: "reading-agent", Agent: true, OnBehalfOf: "alice", Roles: []string{policy.RoleReader}},
	}
	cases := 0
	for _, risk := range []string{v1alpha1.RiskRead, v1alpha1.RiskLow, v1alpha1.RiskHigh, "bogus"} {
		for _, s := range subjects {
			for _, amount := range []float64{0, 49999.99, 50000, 50000.01, 1e6} {
				for _, status := range []string{"", policy.ApprovalApproved, policy.ApprovalRejected} {
					in := policy.Input{Tool: policy.Tool{Risk: risk}, Subject: s, Action: policy.Action{Amount: amount}, Approval: policy.Approval{Status: status}}
					want, _ := policy.WritebackDefault{}.Decide(ctx, in)
					got, err := d.Decide(ctx, in)
					if err != nil {
						t.Fatal(err)
					}
					sort.Strings(want.Reasons)
					if fmt.Sprint(got) != fmt.Sprint(want) {
						t.Errorf("%+v:\n rego %+v\n go   %+v", in, got, want)
					}
					cases++
				}
			}
		}
	}
	if cases != 300 {
		t.Fatalf("ran %d cases", cases)
	}
}

func TestPackTestsRun(t *testing.T) {
	fails, n, err := Test(context.Background(), []Module{examplePack(t, "writeback-default")})
	if err != nil || len(fails) != 0 || n != 4 {
		t.Fatalf("n=%d fails=%+v err=%v", n, fails, err)
	}
	broken := Module{Name: "broken", Source: `package porter.writeback
test_wrong if { 1 == 2 }`}
	fails, _, err = Test(context.Background(), []Module{broken})
	if err != nil || len(fails) != 1 || !strings.Contains(fails[0].Name, "test_wrong") {
		t.Fatalf("fails=%+v err=%v", fails, err)
	}
}

func TestPacksCombine(t *testing.T) {
	ctx := context.Background()
	// A stricter pack adds a denial on top of the default.
	stricter := Module{Name: "no-weekend-writes", Source: `package porter.writeback
deny contains "writes to erp-db are frozen for the audit" if input.recipe == "shop-orders-to-erp"`}
	d, err := New(ctx, []Module{examplePack(t, "writeback-default"), stricter})
	if err != nil {
		t.Fatal(err)
	}
	in := policy.Input{Recipe: "shop-orders-to-erp", Tool: policy.Tool{Risk: "low"}, Subject: policy.Subject{Roles: []string{policy.RoleOperator}}}
	got, _ := d.Decide(ctx, in)
	if got.Allow || !got.Denied || len(got.Reasons) != 1 || !strings.Contains(got.Reasons[0], "frozen") {
		t.Fatalf("decision = %+v", got)
	}
	in.Recipe = "other"
	if got, _ := d.Decide(ctx, in); !got.Allow {
		t.Fatalf("other recipe denied: %+v", got)
	}
}

func TestCompileErrors(t *testing.T) {
	if err := Compile([]Module{{Name: "bad", Source: "package porter.writeback\nallow if {"}}); err == nil {
		t.Fatal("syntax error accepted")
	}
	if _, err := New(context.Background(), []Module{{Name: "masking", Source: "package porter.data.masking\nx := 1"}}); err == nil {
		t.Fatal("decider without a writeback package accepted")
	}
}
