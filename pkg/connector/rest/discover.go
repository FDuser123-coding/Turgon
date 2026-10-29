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
// one, or for what the description does not cover, records are sampled:
// an event's are the items its list returns first (one page), and a read
// or a write targets the resource its path names (/orders/{{orderId}}.json
// is /orders), sampled from an event listing it or from a GET of the
// collection, with one whole record read by the ID of a sampled item. A
// write then uses the fields it sends, so one its records stop having is
// reported. objects is not used.
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
	samples := map[string]*resourceSample{} // by resource

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
			o, items, err := c.sample(ctx, name, e)
			if err != nil {
				return cat, err
			}
			if len(items) > 0 {
				object = o.Name
				if _, seen := samples[resource(e.Path)]; !seen {
					samples[resource(e.Path)] = &resourceSample{object: o, items: items, id: idField}
				}
				add(o)
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

	ops := make([]string, 0, len(c.cfg.Operations))
	for n := range c.cfg.Operations {
		ops = append(ops, n)
	}
	sort.Strings(ops)
	var sampled []string
	for _, name := range ops {
		op := c.cfg.Operations[name]
		if op.Restore {
			continue // it sends back what its update captured
		}
		where, sop := "", map[string]any(nil)
		if sp != nil {
			where, sop = sp.operation(op.Method, base, op.Path)
		}
		if sop == nil {
			sampled = append(sampled, name)
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
	for _, name := range sampled {
		if err := c.sampleOperation(ctx, name, samples, objects, &cat); err != nil {
			return cat, err
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
func (c *Conn) sample(ctx context.Context, name string, e Event) (meta.Object, []map[string]any, error) {
	method, q, body := e.listRequest(0, sampleSize)
	var resp any
	if err := c.do(ctx, method, e.Path, q, headerOf(e.Headers), body, &resp); err != nil {
		return meta.Object{}, nil, fmt.Errorf("rest: discover %s: %w", name, err)
	}
	list, ok := lookup(resp, e.Items).([]any)
	if !ok {
		return meta.Object{}, nil, fmt.Errorf("rest: discover %s: %s has no item array at %q", name, e.Path, e.Items)
	}
	items := mapsOf(list)
	if len(items) == 0 {
		return meta.Object{}, nil, nil
	}
	o := sampledObject(name, "list items", items, or(e.ID, "id"))
	o.Label = fmt.Sprintf("%s %s, %d item(s) sampled", or(e.Method, "GET"), e.Path, len(items))
	return o, items, nil
}

func mapsOf(list []any) []map[string]any {
	var items []map[string]any
	for _, it := range list {
		if m, ok := it.(map[string]any); ok && len(items) < sampleSize {
			items = append(items, m)
		}
	}
	return items
}

func sampledObject(name, kind string, items []map[string]any, idField string) meta.Object {
	fields := meta.SampleFields(items)
	for i := range fields {
		fields[i].Key = fields[i].Name == idField
	}
	return meta.Object{Name: name, Kind: kind, Sampled: true, Fields: fields}
}

// resourceSample is what was sampled of one resource: the object that
// describes its records, and the records.
type resourceSample struct {
	object  meta.Object
	items   []map[string]any
	id      string // the items' ID field
	records int    // whole records read
	reads   map[string]bool
}

// resource names the collection a path addresses: its segments without
// templated ones, and without a format extension (/orders/{{id}}.json and
// /orders.json are /orders).
func resource(path string) string {
	path, _, _ = strings.Cut(path, "?")
	var out []string
	for _, seg := range strings.Split(strings.Trim(path, "/"), "/") {
		if seg == "" || strings.Contains(seg, "{{") {
			continue
		}
		out = append(out, seg)
	}
	if n := len(out); n > 0 {
		if i := strings.LastIndex(out[n-1], "."); i > 0 && !strings.ContainsAny(out[n-1][i:], "({") {
			out[n-1] = out[n-1][:i]
		}
	}
	return "/" + strings.Join(out, "/")
}

// sampleOperation describes the records an operation the OpenAPI
// description does not cover reads or writes: its resource's, sampled.
func (c *Conn) sampleOperation(ctx context.Context, name string, samples map[string]*resourceSample, objects map[string]meta.Object,
	cat *meta.Catalog) error {
	op := c.cfg.Operations[name]
	target := op.Path
	if op.Capture != nil {
		target = op.Capture.Path // an update changes the record it captures
	}
	key := resource(target)
	rs := samples[key]
	if rs == nil {
		// No event lists the resource: GET its collection, as REST APIs
		// list one. Not every API does; then there is nothing to sample.
		ext := ""
		if last := target[strings.LastIndex(target, "/")+1:]; strings.Contains(last, ".") && !strings.Contains(last[strings.LastIndex(last, "."):], "}") {
			ext = last[strings.LastIndex(last, "."):]
		}
		var resp any
		if err := c.do(ctx, http.MethodGet, key+ext, nil, headerOf(op.Headers), nil, &resp); err != nil {
			return nil // not listable (404, 405), or not by this user
		}
		items := mapsOf(firstArray(resp))
		if len(items) == 0 {
			return nil
		}
		o := sampledObject(key, "records", items, "id")
		o.Label = fmt.Sprintf("GET %s, %d item(s) sampled", key+ext, len(items))
		rs = &resourceSample{object: o, items: items, id: "id"}
		samples[key] = rs
	}
	// A read, or an update's capture, gives the whole record, which a list
	// may abbreviate: read one, by a sampled item's ID.
	read, result := "", ""
	switch {
	case op.Capture != nil:
		read, result = op.Capture.Path, op.Capture.Result
	case op.Method == http.MethodGet:
		read, result = op.Path, op.Result
	}
	if vars := templateVars(read); len(vars) == 1 && !rs.reads[read] {
		if rs.reads == nil {
			rs.reads = map[string]bool{}
		}
		rs.reads[read] = true
		if id, ok := rs.items[0][rs.id]; ok && id != nil {
			if path, _, err := render(read, map[string]any{vars[0]: fmt.Sprint(id)}); err == nil {
				var resp any
				if c.do(ctx, http.MethodGet, path, nil, headerOf(op.Headers), nil, &resp) == nil {
					if rec, ok := lookup(resp, result).(map[string]any); ok {
						rs.items = append(rs.items, rec)
						rs.records++
						label := rs.object.Label
						rs.object = sampledObject(rs.object.Name, rs.object.Kind, rs.items, rs.id)
						rs.object.Label = label + fmt.Sprintf(", and the record GET %s", read)
					}
				}
			}
		}
	}
	objects[rs.object.Name] = rs.object
	by := "operation " + name
	creates := op.Method != http.MethodGet
	if creates {
		by += " (writes)"
	}
	if len(op.Fields) == 0 {
		cat.Uses = append(cat.Uses, meta.Use{Object: rs.object.Name, By: by, Creates: creates})
		return nil
	}
	for api := range op.Fields {
		cat.Uses = append(cat.Uses, meta.Use{Object: rs.object.Name, Field: topField(api), By: by, Creates: creates})
	}
	return nil
}

// firstArray is a response's records: the response itself if it is an
// array, or its first array property ({"customers": [...]}, {"data": [...]}).
func firstArray(v any) []any {
	switch x := v.(type) {
	case []any:
		return x
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if a, ok := x[k].([]any); ok {
				return a
			}
		}
	}
	return nil
}

func templateVars(path string) []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range varRE.FindAllStringSubmatch(path, -1) {
		if !seen[m[1]] {
			seen[m[1]] = true
			out = append(out, m[1])
		}
	}
	return out
}
