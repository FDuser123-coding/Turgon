package rest

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/meta"
)

var _ connector.Discoverer = (*Conn)(nil)

// sampleSize is how many items of each event's list describe its records.
const sampleSize = 50

// Discover describes each event's records. A REST API declares no schema
// Turgon can read, so an event's fields are those of the items its list
// request returns first (one page, from the start of the list, or the
// newest for a list in descending order). objects is not used.
func (c *Conn) Discover(ctx context.Context, _ []string) (meta.Catalog, error) {
	cat := meta.Catalog{DiscoveredAt: time.Now().UTC(), Events: map[string]string{}}
	names := make([]string, 0, len(c.cfg.Events))
	for n := range c.cfg.Events {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		e := c.cfg.Events[name]
		method, q, body := e.listRequest(0, sampleSize)
		var resp any
		if err := c.do(ctx, method, e.Path, q, headerOf(e.Headers), body, &resp); err != nil {
			return cat, fmt.Errorf("rest: discover %s: %w", name, err)
		}
		list, ok := lookup(resp, e.Items).([]any)
		if !ok {
			return cat, fmt.Errorf("rest: discover %s: %s has no item array at %q", name, e.Path, e.Items)
		}
		var items []map[string]any
		for _, it := range list {
			if m, ok := it.(map[string]any); ok && len(items) < sampleSize {
				items = append(items, m)
			}
		}
		by := "event " + name
		idField := or(e.ID, "id")
		cat.Events[name] = name
		cat.Uses = append(cat.Uses, meta.Use{Object: name, Field: idField, By: by})
		if pos := or(e.Position, idField); pos != idField {
			cat.Uses = append(cat.Uses, meta.Use{Object: name, Field: pos, By: by + " (position)"})
		}
		if len(items) == 0 {
			continue // nothing to describe yet; the next discovery will
		}
		fields := meta.SampleFields(items)
		for i := range fields {
			fields[i].Key = fields[i].Name == idField
		}
		cat.Objects = append(cat.Objects, meta.Object{Name: name, Kind: "list items", Sampled: true, Fields: fields,
			Label: fmt.Sprintf("%s %s, %d item(s) sampled", or(e.Method, "GET"), e.Path, len(items))})
	}
	// An event whose list was empty has no object yet: its uses would
	// read as missing.
	kept := cat.Uses[:0]
	for _, u := range cat.Uses {
		if _, ok := cat.Object(u.Object); ok {
			kept = append(kept, u)
		}
	}
	cat.Uses = kept
	cat.Normalize()
	return cat, nil
}
