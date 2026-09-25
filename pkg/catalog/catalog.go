// Package catalog indexes Turgon objects by kind, name and version and
// resolves references such as "sf-opportunity-to-order@3" to the highest
// version that satisfies the constraint.
package catalog

import (
	"errors"
	"fmt"
	"sort"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/semver"
	"github.com/fduser123-coding/turgon/pkg/spec"
)

// ErrNotFound is returned when no object satisfies a reference.
var ErrNotFound = errors.New("not found")

type entry struct {
	version semver.Version
	obj     v1alpha1.Object
	source  string
}

// Catalog is an in-memory index. It is not safe for concurrent mutation.
type Catalog struct {
	objects map[string]map[string][]entry // kind -> name -> versions, highest first
}

// New returns an empty catalog.
func New() *Catalog {
	return &Catalog{objects: map[string]map[string][]entry{}}
}

// FromDocuments builds a catalog, rejecting duplicate kind/name/version triples.
func FromDocuments(docs []spec.Document) (*Catalog, error) {
	c := New()
	for _, d := range docs {
		if err := c.add(d.Object, d.Source); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// Load reads every spec file under the given paths into a new catalog.
func Load(paths ...string) (*Catalog, error) {
	docs, err := spec.LoadPaths(paths...)
	if err != nil {
		return nil, err
	}
	return FromDocuments(docs)
}

// Add indexes an object.
func (c *Catalog) Add(obj v1alpha1.Object) error { return c.add(obj, "") }

func (c *Catalog) add(obj v1alpha1.Object, source string) error {
	kind, meta := obj.GetTypeMeta().Kind, obj.GetMeta()
	raw := meta.Version
	if raw == "" {
		raw = "0.0.0"
	}
	v, err := semver.Parse(raw)
	if err != nil {
		return fmt.Errorf("%s/%s: %w", kind, meta.Name, err)
	}
	byName := c.objects[kind]
	if byName == nil {
		byName = map[string][]entry{}
		c.objects[kind] = byName
	}
	for _, e := range byName[meta.Name] {
		if e.version.Compare(v) == 0 {
			return fmt.Errorf("duplicate %s %s@%s (%s and %s)", kind, meta.Name, v, e.source, source)
		}
	}
	list := append(byName[meta.Name], entry{version: v, obj: obj, source: source})
	sort.Slice(list, func(i, j int) bool { return list[i].version.Compare(list[j].version) > 0 })
	byName[meta.Name] = list
	return nil
}

// Resolve returns the highest version of kind/name satisfying constraint.
func (c *Catalog) Resolve(kind, name, constraint string) (v1alpha1.Object, error) {
	con, err := semver.ParseConstraint(constraint)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", kind, name, err)
	}
	for _, e := range c.objects[kind][name] {
		if con.Check(e.version) {
			return e.obj, nil
		}
	}
	if len(c.objects[kind][name]) == 0 {
		return nil, fmt.Errorf("%s %q: %w", kind, name, ErrNotFound)
	}
	return nil, fmt.Errorf("%s %q: no version satisfies %s: %w", kind, name, con, ErrNotFound)
}

// ResolveRef resolves "name@constraint".
func (c *Catalog) ResolveRef(kind, ref string) (v1alpha1.Object, error) {
	name, con := v1alpha1.ParseRef(ref)
	return c.Resolve(kind, name, con)
}

// Connector resolves a connector manifest.
func (c *Catalog) Connector(name, constraint string) (*v1alpha1.ConnectorManifest, error) {
	o, err := c.Resolve(v1alpha1.KindConnectorManifest, name, constraint)
	if err != nil {
		return nil, err
	}
	return o.(*v1alpha1.ConnectorManifest), nil
}

// Connection resolves a connection by name.
func (c *Catalog) Connection(name string) (*v1alpha1.Connection, error) {
	o, err := c.Resolve(v1alpha1.KindConnection, name, "")
	if err != nil {
		return nil, err
	}
	return o.(*v1alpha1.Connection), nil
}

// Mapping resolves a mapping reference.
func (c *Catalog) Mapping(ref string) (*v1alpha1.Mapping, error) {
	o, err := c.ResolveRef(v1alpha1.KindMapping, ref)
	if err != nil {
		return nil, err
	}
	return o.(*v1alpha1.Mapping), nil
}

// Contract resolves a slot contract reference.
func (c *Catalog) Contract(ref string) (*v1alpha1.SlotContract, error) {
	o, err := c.ResolveRef(v1alpha1.KindSlotContract, ref)
	if err != nil {
		return nil, err
	}
	return o.(*v1alpha1.SlotContract), nil
}

// Plugin resolves a plugin.
func (c *Catalog) Plugin(name, constraint string) (*v1alpha1.Plugin, error) {
	o, err := c.Resolve(v1alpha1.KindPlugin, name, constraint)
	if err != nil {
		return nil, err
	}
	return o.(*v1alpha1.Plugin), nil
}

// Recipe resolves a recipe reference.
func (c *Catalog) Recipe(ref string) (*v1alpha1.Recipe, error) {
	o, err := c.ResolveRef(v1alpha1.KindRecipe, ref)
	if err != nil {
		return nil, err
	}
	return o.(*v1alpha1.Recipe), nil
}

// Policy resolves a policy pack reference.
func (c *Catalog) Policy(ref string) (*v1alpha1.PolicyPack, error) {
	o, err := c.ResolveRef(v1alpha1.KindPolicyPack, ref)
	if err != nil {
		return nil, err
	}
	return o.(*v1alpha1.PolicyPack), nil
}

// Find returns the latest object with the given name in any kind, preferring
// kinds in v1alpha1.Kinds order from the end (blueprints, recipes, plugins
// first) since those are what users ask to verify or compile.
func (c *Catalog) Find(name string) (v1alpha1.Object, error) {
	for i := len(v1alpha1.Kinds) - 1; i >= 0; i-- {
		if list := c.objects[v1alpha1.Kinds[i]][name]; len(list) > 0 {
			return list[0].obj, nil
		}
	}
	return nil, fmt.Errorf("%q: %w", name, ErrNotFound)
}

// All returns every indexed object in kind order, then by name and version.
func (c *Catalog) All() []v1alpha1.Object {
	var out []v1alpha1.Object
	for _, kind := range v1alpha1.Kinds {
		names := make([]string, 0, len(c.objects[kind]))
		for n := range c.objects[kind] {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			for _, e := range c.objects[kind][n] {
				out = append(out, e.obj)
			}
		}
	}
	return out
}
