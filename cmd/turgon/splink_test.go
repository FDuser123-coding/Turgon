package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/compiler"
)

func TestCheckSplink(t *testing.T) {
	spec := &compiler.RuntimeSpec{}
	if r := checkSplink(context.Background(), spec); r != nil {
		t.Fatalf("no splink steps: %v", r)
	}
	spec.Spec.Workflows = []compiler.Workflow{{Steps: []compiler.WorkflowStep{
		{Resolve: &compiler.ResolveConfig{Entity: "model.Customer", Strategy: v1alpha1.StrategySplink}},
		{Resolve: &compiler.ResolveConfig{Entity: "model.Supplier", Strategy: v1alpha1.StrategySplink}},
		{Resolve: &compiler.ResolveConfig{Entity: "model.Product", Strategy: v1alpha1.StrategyExact}},
	}}}
	if got := splinkEntities(spec); strings.Join(got, ",") != "Customer,Supplier" {
		t.Fatalf("entities %v", got)
	}

	t.Setenv("TURGON_SPLINK_URL", "")
	if r := checkSplink(context.Background(), spec); len(r) != 1 || r[0].OK || !strings.Contains(r[0].Detail, "TURGON_SPLINK_URL") {
		t.Fatalf("unset: %+v", r)
	}
	t.Setenv("TURGON_SPLINK_URL", "ftp://x")
	if _, err := splinkFromEnv(); err == nil {
		t.Fatal("ftp accepted")
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ok","entities":{"Customer":{"records":10,"masters":6,"trained":["m","u"],"trainedAt":"2026-10-01T10:00:00Z"}}}`))
	}))
	t.Setenv("TURGON_SPLINK_URL", srv.URL)
	r := checkSplink(context.Background(), spec)
	if len(r) != 2 || !r[0].OK || !strings.Contains(r[0].Detail, "10 records of 6 masters") || !r[1].OK || !strings.Contains(r[1].Detail, "no model yet") {
		t.Fatalf("up: %+v", r)
	}
	srv.Close()
	if r := checkSplink(context.Background(), spec); len(r) != 1 || r[0].OK {
		t.Fatalf("down: %+v", r)
	}
}
