package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/plugin"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

type fakePlugins struct {
	mu   sync.Mutex
	runs []PluginInput
	err  map[string]error
}

func (f *fakePlugins) Run(_ context.Context, in PluginInput) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runs = append(f.runs, in)
	return f.err[in.Plugin]
}

func TestPluginsRunAfterTheWriteCommits(t *testing.T) {
	f := newFixture(t)
	plugins := &fakePlugins{err: map[string]error{"flaky": fmt.Errorf("%w: trap", plugin.ErrPlugin)}}
	f.rt.Activities.Plugins = plugins
	f.publish(1101, "ada@example.com", "75.00")
	runs := f.dispatch()
	in := runs[0]
	w := *in.Workflow.Steps[2].Write
	w.Event, w.Plugins = "model.SalesOrder.created", []string{"credit-check", "flaky"}
	steps := append([]compiler.WorkflowStep(nil), in.Workflow.Steps...)
	steps[2].Write = &w
	in.Workflow.Steps = steps

	// A plugin failing does not fail the run: the write stands.
	res, err, _ := f.run(in, approve("03-write"))
	if err != nil || res.Writes[0].Status != writeguard.StatusCommitted {
		t.Fatalf("run: %+v %v", res, err)
	}
	if len(plugins.runs) != 2 {
		t.Fatalf("plugin runs %+v", plugins.runs)
	}
	for _, r := range plugins.runs {
		if r.Event != "model.SalesOrder.created" || r.Entity != "SalesOrder" || r.RunID == "" || r.Step != "03-write" ||
			!strings.Contains(string(r.Record), `"external_id":"SHOP-1101"`) {
			t.Fatalf("plugin input %+v", r)
		}
	}
	// A redelivered event finds the write done: it caused nothing new, so
	// no plugin runs again.
	if _, err, _ := f.run(in); err != nil || len(plugins.runs) != 2 {
		t.Fatalf("redelivery: %v, %d runs", err, len(plugins.runs))
	}
}

func TestRunPluginClassifiesFailures(t *testing.T) {
	x := &Activities{}
	if err := x.RunPlugin(context.Background(), PluginInput{Plugin: "p"}); errType(err) != ErrTypePlugin {
		t.Fatalf("no runner: %v", err)
	}
	x.Plugins = &fakePlugins{err: map[string]error{"p": fmt.Errorf("%w: ran longer than 2s", plugin.ErrPlugin), "q": errors.New("erp-db is down")}}
	if err := x.RunPlugin(context.Background(), PluginInput{Plugin: "p"}); errType(err) != ErrTypePlugin {
		t.Fatalf("plugin failure: %v", err)
	}
	if err := x.RunPlugin(context.Background(), PluginInput{Plugin: "q"}); err == nil || errType(err) == ErrTypePlugin {
		t.Fatalf("host failure: %v (want retryable)", err)
	}
}
