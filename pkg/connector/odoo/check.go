package odoo

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/meta"
)

// uses lists, by model, the fields the configuration relies on and who uses
// them; creates marks the fields of records a create writes whole.
type use struct {
	model, field, by string
	creates          bool
}

func (c *Conn) uses(ctx context.Context) ([]use, error) {
	var out []use
	for name, e := range c.cfg.Events {
		for _, f := range append([]string{"id", "write_date"}, e.Fields...) {
			out = append(out, use{e.Model, f, "event " + name, false})
		}
	}
	for name, op := range c.cfg.Operations {
		by := "operation " + name
		create := op.Action == "create"
		fields := []string{}
		for f := range op.Fields {
			fields = append(fields, f)
		}
		for f := range op.Constants {
			fields = append(fields, f)
		}
		if op.Reference != "" {
			fields = append(fields, op.Reference)
		}
		fields = append(fields, op.Preview...)
		if op.Action == "cancel" && op.Method != "unlink" {
			fields = append(fields, "state")
		}
		if op.Lines != nil {
			fields = append(fields, op.Lines.Field)
		}
		for _, f := range fields {
			out = append(out, use{op.Model, f, by, create})
		}
		if l := op.Lines; l != nil {
			all, err := c.fieldsOf(ctx, op.Model)
			if err != nil {
				return nil, err
			}
			rel := all[l.Field].Relation
			if rel == "" {
				return nil, fmt.Errorf("%s: %s.%s is not a one2many field", by, op.Model, l.Field)
			}
			for f := range l.Fields {
				out = append(out, use{rel, f, by, true})
			}
			for _, f := range l.Preview {
				out = append(out, use{rel, f, by, false})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].model != out[j].model {
			return out[i].model < out[j].model
		}
		return out[i].field < out[j].field
	})
	return out, nil
}

// Check makes sure the API key signs in, then reads each model's fields:
// every configured field must exist, so a typo, a missing app (Sales) or
// a key whose user may not read the model fails here, not on the first order.
func (c *Conn) Check(ctx context.Context) []connector.CheckResult {
	c.Refresh() // check the fields as they are now
	var out []connector.CheckResult
	var who map[string]any
	if err := c.call(ctx, "res.users", "context_get", nil, &who); err != nil {
		return append(out, connector.Fail("sign-in", err.Error(), c.fix(err)))
	}
	out = append(out, connector.Pass("sign-in", fmt.Sprintf("%s: the API key signs in (uid %v)", c.cfg.BaseURL, who["uid"])))
	uses, err := c.uses(ctx)
	if err != nil {
		return append(out, connector.Fail("configuration", err.Error(), c.fix(err)))
	}
	byModel := map[string][]use{}
	for _, u := range uses {
		byModel[u.model] = append(byModel[u.model], u)
	}
	models := make([]string, 0, len(byModel))
	for m := range byModel {
		models = append(models, m)
	}
	sort.Strings(models)
	for _, m := range models {
		fields, err := c.fieldsOf(ctx, m)
		if err != nil {
			out = append(out, connector.Fail("model "+m, err.Error(), c.fix(err)))
			continue
		}
		var missing []string
		for _, u := range byModel[m] {
			if _, ok := fields[u.field]; !ok {
				missing = append(missing, fmt.Sprintf("%s: no field %s", u.by, u.field))
			}
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			out = append(out, connector.Fail("model "+m, strings.Join(missing, "; "),
				"Fix the names in the connection's config; Settings > Technical > Models lists the fields (developer mode). A custom field is named x_studio_… or x_…."))
			continue
		}
		var count int64
		if err := c.call(ctx, m, "search_count", map[string]any{"domain": []any{}}, &count); err != nil {
			out = append(out, connector.Fail("model "+m, "the fields exist, but reading records failed: "+err.Error(), c.fix(err)))
			continue
		}
		out = append(out, connector.Pass("model "+m, fmt.Sprintf("%d field(s) found; %d record(s) readable", len(byModel[m]), count)))
	}
	return out
}

func (c *Conn) fix(err error) string {
	var api *APIError
	if errors.As(err, &api) {
		switch api.Status {
		case http.StatusUnauthorized:
			return "The API key is not valid or has expired (keys expire, 90 days by default): create one in Preferences > Account Security and put it in the secret."
		case http.StatusForbidden:
			return "The key's user lacks access rights to this model: give it the Sales / User: All Documents group, or the rights the flow needs."
		case http.StatusNotFound:
			if c.cfg.Database == "" {
				return "The model or method does not exist, or the server hosts several databases: install the app (Sales) or set the connection's database."
			}
			return "The model or method does not exist in this database: install the app (Sales) or fix the model's name."
		}
	}
	return "Check baseURL (the instance's address) and that the External API is available: Odoo 19 or later, on a plan that includes it."
}

// Discover reads the fields of every model the configuration uses (and of
// the models named in objects) with fields_get.
func (c *Conn) Discover(ctx context.Context, objects []string) (meta.Catalog, error) {
	// Discovery reads the fields as they are now, not as a long-running
	// worker first read them.
	c.Refresh()
	cat := meta.Catalog{DiscoveredAt: time.Now().UTC(), Events: map[string]string{}}
	uses, err := c.uses(ctx)
	if err != nil {
		return cat, err
	}
	models := map[string]bool{}
	for _, u := range uses {
		models[u.model] = true
		cat.Uses = append(cat.Uses, meta.Use{Object: u.model, Field: u.field, By: u.by, Creates: u.creates})
	}
	for name, e := range c.cfg.Events {
		cat.Events[name] = e.Model
	}
	for _, o := range objects {
		if !modelRE.MatchString(o) {
			return cat, fmt.Errorf("odoo: %q is not a model name", o)
		}
		models[o] = true
	}
	names := make([]string, 0, len(models))
	for m := range models {
		names = append(names, m)
	}
	sort.Strings(names)
	for _, m := range names {
		fields, err := c.fieldsOf(ctx, m)
		if err != nil {
			return cat, fmt.Errorf("odoo: discover %s: %w", m, err)
		}
		obj := meta.Object{Name: m, Kind: "model"}
		for name, f := range fields {
			obj.Fields = append(obj.Fields, meta.Field{Name: name, Type: f.Type, Label: f.String, Length: f.Size,
				Required: f.Required, ReadOnly: f.Readonly, Key: name == "id"})
			if f.Relation != "" && (f.Type == "many2one" || f.Type == "one2many" || f.Type == "many2many") {
				obj.Links = append(obj.Links, meta.Link{Name: name, To: f.Relation})
			}
		}
		cat.Objects = append(cat.Objects, obj)
	}
	cat.Normalize()
	return cat, nil
}

// Refresh forgets the cached field definitions, so the next call reads them again.
func (c *Conn) Refresh() {
	c.mu.Lock()
	c.fields = map[string]map[string]FieldInfo{}
	c.mu.Unlock()
}
