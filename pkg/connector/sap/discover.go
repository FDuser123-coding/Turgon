package sap

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/meta"
)

var _ connector.Discoverer = (*Conn)(nil)

const sapNS = "http://www.sap.com/Protocols/SAPData"

// edmxFull is a service's $metadata as discovery reads it: types, keys,
// SAP's labels and creatable/updatable flags, and navigation targets.
type edmxFull struct {
	Schemas []struct {
		Namespace   string `xml:"Namespace,attr"`
		EntityTypes []struct {
			Name string `xml:"Name,attr"`
			Keys []struct {
				Name string `xml:"Name,attr"`
			} `xml:"Key>PropertyRef"`
			Properties []struct {
				Name      string     `xml:"Name,attr"`
				Type      string     `xml:"Type,attr"`
				Nullable  string     `xml:"Nullable,attr"`
				MaxLength string     `xml:"MaxLength,attr"`
				Attrs     []xml.Attr `xml:",any,attr"`
			} `xml:"Property"`
			Navigation []struct {
				Name         string `xml:"Name,attr"`
				Relationship string `xml:"Relationship,attr"`
				ToRole       string `xml:"ToRole,attr"`
			} `xml:"NavigationProperty"`
		} `xml:"EntityType"`
		Associations []struct {
			Name string `xml:"Name,attr"`
			Ends []struct {
				Role string `xml:"Role,attr"`
				Type string `xml:"Type,attr"`
			} `xml:"End"`
		} `xml:"Association"`
		Containers []struct {
			Sets []struct {
				Name string `xml:"Name,attr"`
				Type string `xml:"EntityType,attr"`
			} `xml:"EntitySet"`
		} `xml:"EntityContainer"`
	} `xml:"DataServices>Schema"`
}

func sapAttr(attrs []xml.Attr, name string) string {
	for _, a := range attrs {
		if a.Name.Space == sapNS && a.Name.Local == name {
			return a.Value
		}
	}
	return ""
}

// objects turns a service's entity sets into objects.
func (m edmxFull) objects() []meta.Object {
	setOf := map[string]string{} // qualified entity type -> entity set
	for _, s := range m.Schemas {
		for _, c := range s.Containers {
			for _, set := range c.Sets {
				setOf[set.Type] = set.Name
			}
		}
	}
	roleType := map[string]string{} // qualified association/role -> qualified type
	for _, s := range m.Schemas {
		for _, a := range s.Associations {
			for _, e := range a.Ends {
				roleType[s.Namespace+"."+a.Name+"/"+e.Role] = e.Type
			}
		}
	}
	var out []meta.Object
	for _, s := range m.Schemas {
		for _, t := range s.EntityTypes {
			set, ok := setOf[s.Namespace+"."+t.Name]
			if !ok {
				continue
			}
			keys := map[string]bool{}
			for _, k := range t.Keys {
				keys[k.Name] = true
			}
			o := meta.Object{Name: set, Kind: "entity-set"}
			for _, p := range t.Properties {
				length, _ := strconv.Atoi(p.MaxLength)
				creatable, updatable := sapAttr(p.Attrs, "creatable"), sapAttr(p.Attrs, "updatable")
				o.Fields = append(o.Fields, meta.Field{
					Name: p.Name, Type: p.Type, Label: sapAttr(p.Attrs, "label"), Length: length, Key: keys[p.Name],
					Required: p.Nullable == "false" && !keys[p.Name],
					ReadOnly: creatable == "false" && updatable == "false",
				})
			}
			for _, n := range t.Navigation {
				l := meta.Link{Name: n.Name}
				if typ := roleType[n.Relationship+"/"+n.ToRole]; typ != "" {
					l.To = setOf[typ]
				}
				o.Links = append(o.Links, l)
			}
			out = append(out, o)
		}
	}
	return out
}

// Discover reads the $metadata of every service the configuration uses
// (and of the services named in objects, as "SERVICE" or "SERVICE/EntitySet"),
// and returns their entity sets with their properties.
func (c *Conn) Discover(ctx context.Context, objects []string) (meta.Catalog, error) {
	cat := meta.Catalog{DiscoveredAt: time.Now().UTC()}
	services := map[string]bool{}
	type use struct {
		service, set, prop, by string
		creates                bool
	}
	var uses []use
	add := func(svc, set, by string, props ...string) {
		services[svc] = true
		for _, p := range props {
			if p != "" {
				uses = append(uses, use{service: svc, set: set, prop: p, by: by})
			}
		}
	}
	for name, e := range c.cfg.Events {
		add(e.Service, e.EntitySet, "event "+name, append([]string{e.Key, e.Changed}, e.Select...)...)
	}
	itemUses := map[string][]string{} // "service/set/navigation" -> item properties, resolved below
	for name, op := range c.cfg.Operations {
		by := "operation " + name
		props := []string{op.Key, op.Reference}
		for p := range op.Fields {
			props = append(props, p)
		}
		for p := range op.Constants {
			props = append(props, p)
		}
		add(op.Service, op.EntitySet, by, props...)
		if op.Action == "create" {
			uses[len(uses)-1].creates = true
		}
		if it := op.Items; it != nil {
			add(op.Service, op.EntitySet, by, it.Navigation)
			key := op.Service + "/" + op.EntitySet + "/" + it.Navigation
			for p := range it.Fields {
				itemUses[key] = append(itemUses[key], p+"\x00"+by)
			}
			for p := range it.Constants {
				itemUses[key] = append(itemUses[key], p+"\x00"+by)
			}
		}
		if s := op.Simulate; s != nil {
			add(s.Service, s.EntitySet, by+" (simulation)", "")
		}
	}
	for name, sub := range c.cfg.Subscriptions {
		if r := sub.Read; r != nil {
			add(r.Service, r.EntitySet, "subscription "+name, append([]string{r.Key}, r.Select...)...)
		}
	}
	for _, o := range objects {
		svc, _, _ := strings.Cut(o, "/")
		services[svc] = true
	}
	names := make([]string, 0, len(services))
	for s := range services {
		names = append(names, s)
	}
	sort.Strings(names)
	links := map[string]string{} // "service/set/navigation" -> target set
	for _, svc := range names {
		r, err := c.do(ctx, http.MethodGet, c.metadataURL(svc), nil, http.Header{"Accept": {"application/xml"}})
		if err != nil {
			return cat, fmt.Errorf("sap: discover %s: %w", svc, err)
		}
		var m edmxFull
		if err := xml.Unmarshal(r.body, &m); err != nil {
			return cat, fmt.Errorf("sap: discover %s: the $metadata document is not EDMX: %w", svc, err)
		}
		for _, o := range m.objects() {
			for _, l := range o.Links {
				links[svc+"/"+o.Name+"/"+l.Name] = l.To
			}
			cat.Objects = append(cat.Objects, o)
		}
	}
	for _, u := range uses {
		cat.Uses = append(cat.Uses, meta.Use{Object: u.set, Field: u.prop, By: u.by, Creates: u.creates})
	}
	for key, props := range itemUses {
		to := links[key]
		if to == "" {
			continue // the navigation's target is not declared
		}
		for _, p := range props {
			prop, by, _ := strings.Cut(p, "\x00")
			cat.Uses = append(cat.Uses, meta.Use{Object: to, Field: prop, By: by + " (items)", Creates: true}) // items are only created
		}
	}
	cat.Normalize()
	return cat, nil
}
