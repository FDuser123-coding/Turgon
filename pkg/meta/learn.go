package meta

import (
	"fmt"
	"time"
)

// Refusal is a write the system refused, as the audit log recorded it.
type Refusal struct {
	Operation string    `json:"operation"`
	Error     string    `json:"error"`
	At        time.Time `json:"at"`
}

// Learn marks a sampled object's field required, adding it if no sampled
// record had it, and says why. It reports whether the field was not
// already known to be required. Objects with a declared schema are left
// as they are: their schema says what is required.
func Learn(c *Catalog, object, field string, why Refusal) bool {
	for i := range c.Objects {
		o := &c.Objects[i]
		if o.Name != object || !o.Sampled {
			continue
		}
		by := fmt.Sprintf("operation %s was refused without it on %s", why.Operation, why.At.UTC().Format(time.DateOnly))
		for j := range o.Fields {
			f := &o.Fields[j]
			if f.Name == field {
				if f.Required {
					return false
				}
				f.Required, f.RequiredBy = true, by
				return true
			}
		}
		o.Fields = append(o.Fields, Field{Name: field, Required: true, RequiredBy: by})
		return true
	}
	return false
}

// KeepRequired carries into next the fields of sampled objects that prev
// knew to be required. A sample shows which fields records have, not which
// a write must send, so it can never show such a field stop being
// required; a person who knows it did clears it by deleting the snapshot.
func KeepRequired(prev Catalog, next *Catalog) {
	for _, o := range prev.Objects {
		if !o.Sampled {
			continue
		}
		for _, f := range o.Fields {
			if !f.Required {
				continue
			}
			for i := range next.Objects {
				n := &next.Objects[i]
				if n.Name != o.Name || !n.Sampled {
					continue
				}
				found := false
				for j := range n.Fields {
					if g := &n.Fields[j]; g.Name == f.Name {
						found = true
						if !g.Required {
							g.Required, g.RequiredBy = true, f.RequiredBy
						}
					}
				}
				if !found {
					n.Fields = append(n.Fields, f) // records need not carry it to be required
				}
			}
		}
	}
}
