package salesforce

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/meta"
)

var _ connector.Discoverer = (*Conn)(nil)

var (
	soqlIdentRE = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*`)
	literalRE   = regexp.MustCompile(`'(?:[^'\\]|\\.)*'`)
)

// describe is the part of an sObject's describe discovery reads.
type describe struct {
	Name   string `json:"name"`
	Label  string `json:"label"`
	Fields []struct {
		Name              string   `json:"name"`
		Label             string   `json:"label"`
		Type              string   `json:"type"`
		Length            int      `json:"length"`
		Nillable          bool     `json:"nillable"`
		Createable        bool     `json:"createable"`
		Updateable        bool     `json:"updateable"`
		DefaultedOnCreate bool     `json:"defaultedOnCreate"`
		ReferenceTo       []string `json:"referenceTo"`
		RelationshipName  string   `json:"relationshipName"`
	} `json:"fields"`
}

// Discover describes the sObjects the configuration uses, and those named
// in objects: their fields as the integration user sees them, and their
// lookups to other sObjects.
func (c *Conn) Discover(ctx context.Context, objects []string) (meta.Catalog, error) {
	cat := meta.Catalog{DiscoveredAt: time.Now().UTC()}
	sobjects := map[string]bool{}
	use := func(sobject, field, by string) {
		sobjects[sobject] = true
		if field = strings.TrimSpace(field); identRE.MatchString(field) {
			cat.Uses = append(cat.Uses, meta.Use{Object: sobject, Field: field, By: by})
		}
	}
	where := map[string][]string{} // sObject -> "condition\x00by"
	for name, q := range c.cfg.Events {
		use(q.SObject, "", "event "+name)
		if q.Where != "" {
			where[q.SObject] = append(where[q.SObject], q.Where+"\x00event "+name)
		}
		for _, f := range q.Fields {
			use(q.SObject, f, "event "+name)
		}
	}
	for name, e := range c.cfg.Exports {
		use(e.SObject, "", "export "+name)
		for _, f := range e.Fields {
			use(e.SObject, f, "export "+name)
		}
	}
	for name, op := range c.cfg.Operations {
		use(op.SObject, "", "operation "+name)
		for f := range op.Fields {
			use(op.SObject, f, "operation "+name)
		}
	}
	for _, o := range objects {
		sobjects[o] = true
	}
	names := make([]string, 0, len(sobjects))
	for s := range sobjects {
		names = append(names, s)
	}
	sort.Strings(names)
	for _, s := range names {
		var d describe
		if err := c.do(ctx, http.MethodGet, fmt.Sprintf("%s/sobjects/%s/describe", c.base(), s), nil, &d); err != nil {
			return cat, fmt.Errorf("salesforce: describe %s: %w", s, err)
		}
		o := meta.Object{Name: s, Kind: "sobject", Label: d.Label}
		for _, f := range d.Fields {
			o.Fields = append(o.Fields, meta.Field{
				Name: f.Name, Type: f.Type, Label: f.Label, Length: f.Length, Key: f.Name == "Id",
				Required: !f.Nillable && f.Createable && !f.DefaultedOnCreate,
				ReadOnly: !f.Createable && !f.Updateable && f.Name != "Id",
			})
			if len(f.ReferenceTo) > 0 {
				name := f.RelationshipName
				if name == "" {
					name = f.Name
				}
				for _, to := range f.ReferenceTo {
					o.Links = append(o.Links, meta.Link{Name: name, To: to, ToField: "Id"})
				}
			}
		}
		// Fields a condition names are used too: SOQL identifiers that are
		// fields of the sObject (string literals are left out).
		for _, w := range where[s] {
			cond, by, _ := strings.Cut(w, "\x00")
			for _, id := range soqlIdentRE.FindAllString(literalRE.ReplaceAllString(cond, "''"), -1) {
				if _, ok := o.Field(id); ok {
					cat.Uses = append(cat.Uses, meta.Use{Object: s, Field: id, By: by + " (where)"})
				}
			}
		}
		cat.Objects = append(cat.Objects, o)
	}
	cat.Normalize()
	return cat, nil
}
