// Package meta is Turgon's metadata graph (architecture §7.2): what each
// connected system holds, as its connector discovers it (tables, sObjects,
// entity sets, BAPIs, IDoc segments, their fields and relations), kept as
// snapshots so a change in a system is seen before it breaks a run. Each
// field links to what uses it: the connector's own operations, and the
// mappings that read it.
package meta

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/fduser123-coding/turgon/pkg/compiler"
)

// Catalog is what one endpoint holds.
type Catalog struct {
	Endpoint  string `json:"endpoint"`
	Connector string `json:"connector"`
	// Version is the connector's version that discovered it.
	Version      string    `json:"version,omitempty"`
	DiscoveredAt time.Time `json:"discoveredAt"`
	Objects      []Object  `json:"objects"`
	// Uses are the fields the connection's configuration relies on.
	Uses []Use `json:"uses,omitempty"`
	// Events names, by event, the object whose records the event carries
	// (a change capture's table, a polled sObject or entity set), so a
	// mapping reading the event reads that object's fields.
	Events map[string]string `json:"events,omitempty"`
}

// Snapshot describes one stored catalog of an endpoint.
type Snapshot struct {
	ID        int64  `json:"id"`
	Endpoint  string `json:"endpoint"`
	Connector string `json:"connector"`
	Digest    string `json:"digest"`
	// DiscoveredAt is when the endpoint first held this catalog;
	// CheckedAt, when a discovery last found it unchanged.
	DiscoveredAt time.Time `json:"discoveredAt"`
	CheckedAt    time.Time `json:"checkedAt"`
	Objects      int       `json:"objects"`
}

// Object is a table, sObject, entity set, BAPI or IDoc segment.
type Object struct {
	Name   string  `json:"name"`
	Kind   string  `json:"kind"`
	Label  string  `json:"label,omitempty"`
	Fields []Field `json:"fields"`
	Links  []Link  `json:"links,omitempty"`
}

// Field is one field of an object.
type Field struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Label string `json:"label,omitempty"`
	// Length is the maximum length of text (0: unknown or unbounded).
	Length int `json:"length,omitempty"`
	// Required: the system refuses a record without it.
	Required bool `json:"required,omitempty"`
	Key      bool `json:"key,omitempty"`
	ReadOnly bool `json:"readOnly,omitempty"`
}

// Link is a relation from a field (or navigation) to another object.
type Link struct {
	Name    string `json:"name"`
	To      string `json:"to"`
	ToField string `json:"toField,omitempty"`
}

// Use says what relies on a field.
type Use struct {
	Object string `json:"object"`
	Field  string `json:"field"`
	// By names the user, such as "operation create-sales-order".
	By string `json:"by"`
	// Creates: the user creates records of the object, so a field that
	// becomes required breaks it even if it does not use that field.
	Creates bool `json:"creates,omitempty"`
}

// Normalize sorts objects, fields, links and uses, so equal catalogs are
// equal byte for byte.
func (c *Catalog) Normalize() {
	sort.Slice(c.Objects, func(i, j int) bool { return c.Objects[i].Name < c.Objects[j].Name })
	for i := range c.Objects {
		o := &c.Objects[i]
		sort.Slice(o.Fields, func(a, b int) bool { return o.Fields[a].Name < o.Fields[b].Name })
		sort.Slice(o.Links, func(a, b int) bool { return o.Links[a].Name < o.Links[b].Name })
	}
	sort.Slice(c.Uses, func(i, j int) bool {
		a, b := c.Uses[i], c.Uses[j]
		if a.Object != b.Object {
			return a.Object < b.Object
		}
		if a.Field != b.Field {
			return a.Field < b.Field
		}
		return a.By < b.By
	})
}

// Digest identifies what the system holds: objects and fields, not when
// they were discovered or who uses them.
func (c Catalog) Digest() string {
	cc := c
	cc.Objects = append([]Object(nil), c.Objects...)
	for i := range cc.Objects {
		cc.Objects[i].Fields = append([]Field(nil), c.Objects[i].Fields...)
		cc.Objects[i].Links = append([]Link(nil), c.Objects[i].Links...)
	}
	cc.Normalize()
	b, _ := json.Marshal(cc.Objects)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Object returns the named object.
func (c Catalog) Object(name string) (Object, bool) {
	for _, o := range c.Objects {
		if o.Name == name {
			return o, true
		}
	}
	return Object{}, false
}

// Field returns the named field.
func (o Object) Field(name string) (Field, bool) {
	for _, f := range o.Fields {
		if f.Name == name {
			return f, true
		}
	}
	return Field{}, false
}

// Change kinds. Breaking ones can fail runs that use the field.
const (
	ObjectAdded     = "object-added"
	ObjectRemoved   = "object-removed"
	FieldAdded      = "field-added"
	FieldRemoved    = "field-removed"
	TypeChanged     = "type-changed"
	BecameRequired  = "became-required"
	LengthShrunk    = "length-shrunk"
	BecameReadOnly  = "became-read-only"
	RequiredDropped = "no-longer-required"
)

// Change is one difference between two snapshots of an endpoint.
type Change struct {
	Kind   string `json:"kind"`
	Object string `json:"object"`
	Field  string `json:"field,omitempty"`
	Old    string `json:"old,omitempty"`
	New    string `json:"new,omitempty"`
	// Breaking: a run that uses the field or object may fail.
	Breaking bool `json:"breaking"`
	// UsedBy lists what relies on it (see Usage).
	UsedBy []string `json:"usedBy,omitempty"`
}

func (c Change) String() string {
	where := c.Object
	if c.Field != "" {
		where += "." + c.Field
	}
	s := fmt.Sprintf("%s %s", c.Kind, where)
	switch {
	case c.Old != "" && c.New != "":
		s += fmt.Sprintf(" (%s -> %s)", c.Old, c.New)
	case c.Old != "":
		s += fmt.Sprintf(" (was %s)", c.Old)
	case c.New != "":
		s += fmt.Sprintf(" (%s)", c.New)
	}
	return s
}

// Diff lists what changed from old to new.
func Diff(old, new Catalog) []Change {
	var out []Change
	oldObjs := map[string]Object{}
	for _, o := range old.Objects {
		oldObjs[o.Name] = o
	}
	newObjs := map[string]Object{}
	for _, o := range new.Objects {
		newObjs[o.Name] = o
	}
	for name, o := range oldObjs {
		n, ok := newObjs[name]
		if !ok {
			out = append(out, Change{Kind: ObjectRemoved, Object: name, Breaking: true})
			continue
		}
		oldFields := map[string]Field{}
		for _, f := range o.Fields {
			oldFields[f.Name] = f
		}
		for _, f := range n.Fields {
			was, ok := oldFields[f.Name]
			delete(oldFields, f.Name)
			if !ok {
				out = append(out, Change{Kind: FieldAdded, Object: name, Field: f.Name, New: f.Type, Breaking: f.Required})
				continue
			}
			if was.Type != f.Type {
				out = append(out, Change{Kind: TypeChanged, Object: name, Field: f.Name, Old: was.Type, New: f.Type, Breaking: true})
			}
			if !was.Required && f.Required {
				out = append(out, Change{Kind: BecameRequired, Object: name, Field: f.Name, Breaking: true})
			}
			if was.Required && !f.Required {
				out = append(out, Change{Kind: RequiredDropped, Object: name, Field: f.Name})
			}
			if f.Length > 0 && (was.Length == 0 || f.Length < was.Length) {
				out = append(out, Change{Kind: LengthShrunk, Object: name, Field: f.Name,
					Old: fmt.Sprint(was.Length), New: fmt.Sprint(f.Length), Breaking: true})
			}
			if !was.ReadOnly && f.ReadOnly {
				out = append(out, Change{Kind: BecameReadOnly, Object: name, Field: f.Name, Breaking: true})
			}
		}
		for _, f := range oldFields {
			out = append(out, Change{Kind: FieldRemoved, Object: name, Field: f.Name, Old: f.Type, Breaking: true})
		}
	}
	for name := range newObjs {
		if _, ok := oldObjs[name]; !ok {
			out = append(out, Change{Kind: ObjectAdded, Object: name})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Object != out[j].Object {
			return out[i].Object < out[j].Object
		}
		if out[i].Field != out[j].Field {
			return out[i].Field < out[j].Field
		}
		return out[i].Kind < out[j].Kind
	})
	return out
}

var identRE = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*`)

// Usage lists, by "object.field", what relies on each field of an
// endpoint: the uses its connector reports, and the mappings in the specs
// that read the endpoint's objects. A mapping reads an object when it maps
// from "<endpoint or connector>.<object>" ("salesforce.Opportunity"), or
// from the endpoint's event that triggers the workflow, whose records come
// from the object the catalog names for that event. Its expressions name
// the fields it reads.
func Usage(c Catalog, specs ...*compiler.RuntimeSpec) map[string][]string {
	out := map[string][]string{}
	add := func(key, by string) {
		if !slices.Contains(out[key], by) {
			out[key] = append(out[key], by)
		}
	}
	for _, u := range c.Uses {
		add(u.Object+"."+u.Field, u.By)
		if u.Field == "" {
			add(u.Object, u.By)
		}
	}
	for _, spec := range specs {
		if spec == nil {
			continue
		}
		for _, wf := range spec.Spec.Workflows {
			for _, st := range wf.Steps {
				m := st.Map
				if m == nil {
					continue
				}
				system, entity, ok := strings.Cut(m.From, ".")
				if !ok || (system != c.Endpoint && system != c.Connector) {
					continue
				}
				o, ok := c.Object(entity)
				if !ok && wf.Trigger.Endpoint == c.Endpoint {
					o, ok = c.Object(c.Events[wf.Trigger.Event])
				}
				if !ok {
					continue
				}
				targets := make([]string, 0, len(m.Fields))
				for t := range m.Fields {
					targets = append(targets, t)
				}
				sort.Strings(targets)
				for _, target := range targets {
					for _, id := range identRE.FindAllString(m.Fields[target], -1) {
						if _, ok := o.Field(id); ok {
							add(o.Name+"."+id, fmt.Sprintf("mapping %s (%s)", m.Mapping, target))
						}
					}
				}
			}
		}
	}
	for k := range out {
		sort.Strings(out[k])
	}
	return out
}

// Missing is an object or field the configuration uses that the system
// does not hold.
type Missing struct {
	Object string   `json:"object"`
	Field  string   `json:"field,omitempty"`
	UsedBy []string `json:"usedBy"`
}

func (m Missing) String() string {
	where := m.Object
	if m.Field != "" {
		where += "." + m.Field
	}
	return where + "; used by " + strings.Join(m.UsedBy, ", ")
}

// MissingUses lists what the configuration uses that the catalog lacks.
func MissingUses(c Catalog) []Missing {
	var out []Missing
	at := map[string]int{}
	for _, u := range c.Uses {
		m := Missing{Object: u.Object}
		if o, ok := c.Object(u.Object); ok {
			if u.Field == "" {
				continue
			}
			if _, ok := o.Field(u.Field); ok {
				continue
			}
			m.Field = u.Field
		}
		key := m.Object + "." + m.Field
		i, seen := at[key]
		if !seen {
			i = len(out)
			at[key] = i
			out = append(out, m)
		}
		if !slices.Contains(out[i].UsedBy, u.By) {
			out[i].UsedBy = append(out[i].UsedBy, u.By)
		}
	}
	for i := range out {
		sort.Strings(out[i].UsedBy)
	}
	return out
}

// Compare lists the changes from old to new, with what uses each: the
// uses of both catalogs, so a field the new one lacks is still known to be
// read by the mappings that read it before. Changes that can break a run
// come first.
func Compare(old, new Catalog, specs ...*compiler.RuntimeSpec) []Change {
	usage := Usage(new, specs...)
	for k, by := range Usage(old, specs...) {
		for _, b := range by {
			if !slices.Contains(usage[k], b) {
				usage[k] = append(usage[k], b)
			}
		}
	}
	return SortChanges(Annotate(Diff(old, new), usage, Creators(new)))
}

// Breaks reports whether a change can fail this deployment's runs: it
// breaks, and something uses what changed.
func (c Change) Breaks() bool { return c.Breaking && len(c.UsedBy) > 0 }

// SortChanges puts the changes that can break a run first.
func SortChanges(cs []Change) []Change {
	sort.SliceStable(cs, func(i, j int) bool { return cs[i].Breaks() && !cs[j].Breaks() })
	return cs
}

// Creators lists, by object, what creates its records (see Use.Creates).
func Creators(c Catalog) map[string][]string {
	out := map[string][]string{}
	for _, u := range c.Uses {
		if u.Creates && !slices.Contains(out[u.Object], u.By) {
			out[u.Object] = append(out[u.Object], u.By)
		}
	}
	for k := range out {
		sort.Strings(out[k])
	}
	return out
}

// Annotate sets each change's UsedBy from usage, and from creators for a
// field that became required. Breaking stays as Diff set it: whether a
// breaking change matters here is whether anything uses it.
func Annotate(changes []Change, usage, creators map[string][]string) []Change {
	for i, ch := range changes {
		key := ch.Object
		if ch.Field != "" {
			key += "." + ch.Field
		}
		ch.UsedBy = append([]string(nil), usage[key]...)
		if ch.Field == "" { // an object: anything using one of its fields
			for k, by := range usage {
				if strings.HasPrefix(k, ch.Object+".") {
					ch.UsedBy = append(ch.UsedBy, by...)
				}
			}
		}
		if ch.Breaking && (ch.Kind == FieldAdded || ch.Kind == BecameRequired) {
			for _, by := range creators[ch.Object] {
				if !slices.Contains(ch.UsedBy, by) {
					ch.UsedBy = append(ch.UsedBy, by+" (creates)")
				}
			}
		}
		sort.Strings(ch.UsedBy)
		ch.UsedBy = compactStrings(ch.UsedBy)
		if len(ch.UsedBy) == 0 {
			ch.UsedBy = nil
		}
		changes[i] = ch
	}
	return changes
}

func compactStrings(s []string) []string {
	out := s[:0]
	for i, v := range s {
		if i == 0 || v != s[i-1] {
			out = append(out, v)
		}
	}
	return out
}
