package rest

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/meta"
)

var _ connector.Discoverer = (*Conn)(nil)

// sampleSize is how many items of each event's list describe its records.
const sampleSize = 50

// Discover describes what the connection reads and writes. With an
// OpenAPI description (openapi), each event's list items, each read's
// record and each write's request body come from its schemas: types,
// formats, maximum lengths, required and read-only properties. Without
// one, or for an event the description does not cover, an event's fields
// are those of the items its list returns first (one page). objects is not
// used.
func (c *Conn) Discover(ctx context.Context, _ []string) (meta.Catalog, error) {
	cat := meta.Catalog{DiscoveredAt: time.Now().UTC(), Events: map[string]string{}}
	var sp *spec
	if c.cfg.OpenAPI != "" {
		var err error
		if sp, err = fetchSpec(ctx, c.cfg.OpenAPI); err != nil {
			return cat, fmt.Errorf("rest: discover: %w", err)
		}
	}
	base := ""
	if u, err := url.Parse(c.cfg.BaseURL); err == nil {
		base = strings.TrimRight(u.Path, "/")
	}
	objects := map[string]meta.Object{}
	add := func(o meta.Object) {
		if _, ok := objects[o.Name]; !ok {
			objects[o.Name] = o
		}
	}

	names := make([]string, 0, len(c.cfg.Events))
	for n := range c.cfg.Events {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		e := c.cfg.Events[name]
		by := "event " + name
		idField := or(e.ID, "id")
		object := ""
		if sp != nil {
			if where, op := sp.operation(or(e.Method, http.MethodGet), base, e.Path); op != nil {
				list := sp.property(sp.content(op, false), e.Items)
				if m, _ := sp.resolve(list, ""); m != nil {
					if o, ok := sp.describe(m["items"], where+" items", "list items", false); ok {
						add(o)
						object = o.Name
					}
				}
			}
		}
		if object == "" {
			o, ok, err := c.sample(ctx, name, e)
			if err != nil {
				return cat, err
			}
			if ok {
				add(o)
				object = o.Name
			}
		}
		cat.Events[name] = name
		if object == "" {
			continue // nothing to describe yet; the next discovery will
		}
		cat.Events[name] = object
		cat.Uses = append(cat.Uses, meta.Use{Object: object, Field: idField, By: by})
		if pos := or(e.Position, idField); pos != idField {
			cat.Uses = append(cat.Uses, meta.Use{Object: object, Field: pos, By: by + " (position)"})
		}
	}

	if sp != nil {
		ops := make([]string, 0, len(c.cfg.Operations))
		for n := range c.cfg.Operations {
			ops = append(ops, n)
		}
		sort.Strings(ops)
		for _, name := range ops {
			op := c.cfg.Operations[name]
			if op.Restore {
				continue // it sends back what its update captured
			}
			where, sop := sp.operation(op.Method, base, op.Path)
			if sop == nil {
				continue
			}
			var o meta.Object
			var ok bool
			if op.Method == http.MethodGet {
				o, ok = sp.describe(sp.property(sp.content(sop, false), op.Result), where+" response", "response", false)
			} else {
				o, ok = sp.describe(sp.property(sp.content(sop, true), op.Wrap), where+" request", "request body", true)
			}
			if !ok {
				continue
			}
			add(o)
			by := "operation " + name
			// A write sends its request body: a property that becomes
			// required breaks it, whether it creates or updates.
			creates := op.Method != http.MethodGet
			if len(op.Fields) == 0 {
				cat.Uses = append(cat.Uses, meta.Use{Object: o.Name, By: by, Creates: creates})
				continue
			}
			for api := range op.Fields {
				cat.Uses = append(cat.Uses, meta.Use{Object: o.Name, Field: topField(api), By: by, Creates: creates})
			}
		}
	}
	for _, o := range objects {
		cat.Objects = append(cat.Objects, o)
	}
	cat.Normalize()
	return cat, nil
}

// sample describes an event's records from the items its list returns
// first: one page, from the start of the list, or the newest for a list
// in descending order.
func (c *Conn) sample(ctx context.Context, name string, e Event) (meta.Object, bool, error) {
	method, q, body := e.listRequest(0, sampleSize)
	var resp any
	if err := c.do(ctx, method, e.Path, q, headerOf(e.Headers), body, &resp); err != nil {
		return meta.Object{}, false, fmt.Errorf("rest: discover %s: %w", name, err)
	}
	list, ok := lookup(resp, e.Items).([]any)
	if !ok {
		return meta.Object{}, false, fmt.Errorf("rest: discover %s: %s has no item array at %q", name, e.Path, e.Items)
	}
	var items []map[string]any
	for _, it := range list {
		if m, ok := it.(map[string]any); ok && len(items) < sampleSize {
			items = append(items, m)
		}
	}
	if len(items) == 0 {
		return meta.Object{}, false, nil
	}
	idField := or(e.ID, "id")
	fields := meta.SampleFields(items)
	for i := range fields {
		fields[i].Key = fields[i].Name == idField
	}
	return meta.Object{Name: name, Kind: "list items", Sampled: true, Fields: fields,
		Label: fmt.Sprintf("%s %s, %d item(s) sampled", or(e.Method, "GET"), e.Path, len(items))}, true, nil
}
