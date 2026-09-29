package sap

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/fduser123-coding/turgon/pkg/connector"
)

var _ connector.Checker = (*Conn)(nil)

// edmx is the part of a service's $metadata the check reads.
type edmx struct {
	Schemas []struct {
		Namespace   string `xml:"Namespace,attr"`
		EntityTypes []struct {
			Name       string `xml:"Name,attr"`
			Properties []struct {
				Name string `xml:"Name,attr"`
			} `xml:"Property"`
			Navigation []struct {
				Name string `xml:"Name,attr"`
			} `xml:"NavigationProperty"`
		} `xml:"EntityType"`
		Containers []struct {
			Sets []struct {
				Name string `xml:"Name,attr"`
				Type string `xml:"EntityType,attr"`
			} `xml:"EntitySet"`
		} `xml:"EntityContainer"`
	} `xml:"DataServices>Schema"`
}

// sets returns each entity set's properties and navigation properties.
func (m edmx) sets() map[string]map[string]bool {
	types := map[string]map[string]bool{}
	for _, s := range m.Schemas {
		for _, t := range s.EntityTypes {
			props := map[string]bool{}
			for _, p := range t.Properties {
				props[p.Name] = true
			}
			for _, p := range t.Navigation {
				props[p.Name] = true
			}
			types[s.Namespace+"."+t.Name] = props
		}
	}
	out := map[string]map[string]bool{}
	for _, s := range m.Schemas {
		for _, c := range s.Containers {
			for _, set := range c.Sets {
				out[set.Name] = types[set.Type]
			}
		}
	}
	return out
}

// Check signs in, then reads each service's metadata: every configured
// entity set and property must exist, so a typo or a service that is not
// activated for the communication user fails here, not on the first order.
func (c *Conn) Check(ctx context.Context) []connector.CheckResult {
	type use struct {
		set   string
		props []string
		what  string
	}
	byService := map[string][]use{}
	for name, e := range c.cfg.Events {
		props := append([]string{e.Key, e.Changed}, e.Select...)
		byService[e.Service] = append(byService[e.Service], use{e.EntitySet, props, "event " + name})
	}
	for name, op := range c.cfg.Operations {
		props := []string{op.Key}
		for p := range op.Fields {
			props = append(props, p)
		}
		for p := range op.Constants {
			props = append(props, p)
		}
		if op.Reference != "" {
			props = append(props, op.Reference)
		}
		if op.Items != nil {
			props = append(props, op.Items.Navigation)
		}
		byService[op.Service] = append(byService[op.Service], use{op.EntitySet, props, "operation " + name})
		if s := op.Simulate; s != nil {
			byService[s.Service] = append(byService[s.Service], use{s.EntitySet, nil, "operation " + name + " (simulation)"})
		}
	}
	for name, sub := range c.cfg.Subscriptions {
		if r := sub.Read; r != nil {
			props := append([]string{r.Key}, r.Select...)
			for _, e := range r.Expand {
				props = append(props, strings.SplitN(e, "/", 2)[0])
			}
			byService[r.Service] = append(byService[r.Service], use{r.EntitySet, props, "subscription " + name})
		}
	}
	services := make([]string, 0, len(byService))
	for s := range byService {
		services = append(services, s)
	}
	sort.Strings(services)

	var out []connector.CheckResult
	if c.cfg.EventMesh != nil {
		out = append(out, c.checkEventMesh(ctx))
	}
	for _, svc := range services {
		name := "service " + svc
		r, err := c.do(ctx, http.MethodGet, c.metadataURL(svc), nil, http.Header{"Accept": {"application/xml"}})
		if err != nil {
			out = append(out, connector.Fail(name, err.Error(), c.fix(err, svc)))
			continue
		}
		var m edmx
		if err := xml.Unmarshal(r.body, &m); err != nil {
			out = append(out, connector.Fail(name, "the $metadata document is not EDMX: "+err.Error(),
				"Check baseURL and servicePath: they must point at the OData services, e.g. https://host/sap/opu/odata/sap."))
			continue
		}
		sets := m.sets()
		var problems []string
		for _, u := range byService[svc] {
			props, ok := sets[u.set]
			if !ok {
				problems = append(problems, fmt.Sprintf("%s: no entity set %s", u.what, u.set))
				continue
			}
			for _, p := range u.props {
				if !props[p] {
					problems = append(problems, fmt.Sprintf("%s: %s has no property %s", u.what, u.set, p))
				}
			}
		}
		if len(problems) > 0 {
			sort.Strings(problems)
			out = append(out, connector.Fail(name, strings.Join(problems, "; "),
				"Fix the names in the connection's config; the service's $metadata lists what exists. Custom fields must be enabled for this API in the Custom Fields app."))
			continue
		}
		out = append(out, connector.Pass(name, fmt.Sprintf("%d entity set(s) and their properties found", len(byService[svc]))))
	}
	return out
}

func (c *Conn) metadataURL(service string) string {
	u := c.cfg.BaseURL + c.cfg.ServicePath + "/" + service + "/$metadata"
	if c.cfg.Client != "" {
		u += "?" + url.Values{"sap-client": {c.cfg.Client}}.Encode()
	}
	return u
}

func (c *Conn) fix(err error, service string) string {
	var api *APIError
	if errors.As(err, &api) {
		switch api.Status {
		case http.StatusUnauthorized:
			return "SAP rejected the credentials. Check the communication user (or OAuth client) in the secret."
		case http.StatusForbidden:
			return fmt.Sprintf("The user may not use %s. In S/4HANA Cloud, add the service's communication scenario to the communication arrangement; on premise, assign the role for the service (transaction PFCG).", service)
		case http.StatusNotFound:
			return fmt.Sprintf("%s is not there. Activate it (transaction /IWFND/MAINT_SERVICE on premise) or check the name and servicePath.", service)
		}
		return ""
	}
	return connector.NetworkFix(err, c.cfg.BaseURL)
}
