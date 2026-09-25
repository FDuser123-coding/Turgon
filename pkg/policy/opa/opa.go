// Package opa evaluates Rego policy packs with Open Policy Agent
// (architecture §9, AD-11). Packs contribute rules to the package
// turgon.writeback:
//
//	allow             write may proceed (after approval if also required)
//	require_approval  a person must approve first
//	reasons           strings shown to the approver
//	deny              strings; any entry blocks the write, even if allowed
//
// Rules from several packs in that package combine as OPA combines rules:
// any allow or require_approval rule that holds applies, and reasons and
// denials are unioned.
package opa

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/open-policy-agent/opa/v1/ast"
	"github.com/open-policy-agent/opa/v1/rego"
	"github.com/open-policy-agent/opa/v1/tester"

	"github.com/fduser123-coding/turgon/pkg/policy"
)

// WritebackPackage is the Rego package write decisions come from.
const WritebackPackage = "turgon.writeback"

// Module is one policy pack's Rego source.
type Module struct {
	Name   string
	Source string
}

// decision normalizes whatever the packs define into one document.
const decisionModule = `package turgon.decision

import rego.v1

default allow := false

allow if data.turgon.writeback.allow

default require_approval := false

require_approval if data.turgon.writeback.require_approval

reasons contains r if some r in data.turgon.writeback.reasons

deny contains d if some d in data.turgon.writeback.deny
`

func parse(mods []Module) (map[string]*ast.Module, error) {
	parsed := map[string]*ast.Module{}
	var errs []error
	for _, m := range mods {
		file := m.Name + ".rego"
		mod, err := ast.ParseModuleWithOpts(file, m.Source, ast.ParserOptions{RegoVersion: ast.RegoV1})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		parsed[file] = mod
	}
	return parsed, errors.Join(errs...)
}

// Compile checks that the modules parse and compile together.
func Compile(mods []Module) error {
	parsed, err := parse(mods)
	if err != nil {
		return err
	}
	c := ast.NewCompiler()
	if c.Compile(parsed); c.Failed() {
		return c.Errors
	}
	return nil
}

// DefinesWriteback reports whether any module is in the writeback package.
func DefinesWriteback(mods []Module) bool {
	parsed, _ := parse(mods)
	for _, m := range parsed {
		if strings.TrimPrefix(m.Package.Path.String(), "data.") == WritebackPackage {
			return true
		}
	}
	return false
}

// TestFailure is a failing or erroring test_ rule in a pack.
type TestFailure struct {
	Name    string
	Message string
}

// Test runs every test_ rule in the modules, as `opa test` would. It
// returns the failures and the number of tests run.
func Test(ctx context.Context, mods []Module) ([]TestFailure, int, error) {
	parsed, err := parse(mods)
	if err != nil {
		return nil, 0, err
	}
	ch, err := tester.NewRunner().SetModules(parsed).RunTests(ctx, nil)
	if err != nil {
		return nil, 0, err
	}
	var fails []TestFailure
	n := 0
	for r := range ch {
		n++
		switch {
		case r.Error != nil:
			fails = append(fails, TestFailure{Name: r.Package + "." + r.Name, Message: r.Error.Error()})
		case r.Fail:
			fails = append(fails, TestFailure{Name: r.Package + "." + r.Name, Message: "test failed"})
		}
	}
	sort.Slice(fails, func(i, j int) bool { return fails[i].Name < fails[j].Name })
	return fails, n, nil
}

// Decider evaluates write decisions. It is safe for concurrent use.
type Decider struct {
	query rego.PreparedEvalQuery
}

var _ policy.Decider = (*Decider)(nil)

// New prepares a decider. At least one module must define turgon.writeback.
func New(ctx context.Context, mods []Module) (*Decider, error) {
	if !DefinesWriteback(mods) {
		return nil, fmt.Errorf("no policy pack defines package %s", WritebackPackage)
	}
	opts := []func(*rego.Rego){
		rego.Query("x = data.turgon.decision"),
		rego.Module("turgon-decision.rego", decisionModule),
		rego.SetRegoVersion(ast.RegoV1),
	}
	for _, m := range mods {
		opts = append(opts, rego.Module(m.Name+".rego", m.Source))
	}
	q, err := rego.New(opts...).PrepareForEval(ctx)
	if err != nil {
		return nil, err
	}
	return &Decider{query: q}, nil
}

// Decide evaluates the policy for one request.
func (d *Decider) Decide(ctx context.Context, in policy.Input) (policy.Decision, error) {
	b, err := json.Marshal(in)
	if err != nil {
		return policy.Decision{}, err
	}
	var input map[string]any
	if err := json.Unmarshal(b, &input); err != nil {
		return policy.Decision{}, err
	}
	rs, err := d.query.Eval(ctx, rego.EvalInput(input))
	if err != nil {
		return policy.Decision{}, fmt.Errorf("opa: %w", err)
	}
	if len(rs) != 1 {
		return policy.Decision{}, errors.New("opa: policy produced no decision")
	}
	raw, err := json.Marshal(rs[0].Bindings["x"])
	if err != nil {
		return policy.Decision{}, err
	}
	var out struct {
		Allow           bool     `json:"allow"`
		RequireApproval bool     `json:"require_approval"`
		Reasons         []string `json:"reasons"`
		Deny            []string `json:"deny"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return policy.Decision{}, fmt.Errorf("opa: reasons and deny must be sets of strings: %w", err)
	}
	dec := policy.Decision{
		Allow:           out.Allow && len(out.Deny) == 0,
		RequireApproval: out.RequireApproval,
		Denied:          len(out.Deny) > 0,
		Reasons:         append(append([]string{}, out.Reasons...), out.Deny...),
	}
	sort.Strings(dec.Reasons)
	return dec, nil
}
