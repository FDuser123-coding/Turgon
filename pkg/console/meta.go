package console

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strconv"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/catalog"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/meta"
)

// MetaStore reads the metadata graph's snapshots (`turgon discover`
// stores them).
type MetaStore interface {
	// Snapshots lists an endpoint's snapshots (all endpoints' when empty),
	// newest first.
	Snapshots(ctx context.Context, endpoint string) ([]meta.Snapshot, error)
	Catalog(ctx context.Context, id int64) (meta.Catalog, bool, error)
}

// MetaOverview lists the discovered systems and what changed in each.
type MetaOverview struct {
	Error   string       `json:"error,omitempty"`
	Systems []MetaSystem `json:"systems"`
}

// MetaSystem is a discovered endpoint's latest snapshot, and how it
// differs from the one before.
type MetaSystem struct {
	Endpoint  string        `json:"endpoint"`
	Connector string        `json:"connector"`
	Version   string        `json:"version,omitempty"`
	Latest    meta.Snapshot `json:"latest"`
	Snapshots int           `json:"snapshots"`
	Fields    int           `json:"fields"`
	// Changes since the previous snapshot; Breaking counts those that can
	// fail a run; Missing, what the configuration uses that the system lacks.
	Changes  int `json:"changes"`
	Breaking int `json:"breaking"`
	Missing  int `json:"missing"`
}

// MetaDetail is one endpoint's catalog with what uses each field, and the
// changes between two of its snapshots (the latest and the one before, by
// default).
type MetaDetail struct {
	Error     string          `json:"error,omitempty"`
	Endpoint  string          `json:"endpoint"`
	Connector string          `json:"connector"`
	Version   string          `json:"version,omitempty"`
	Snapshots []meta.Snapshot `json:"snapshots"`
	// To is the snapshot shown; From, the one the changes are measured from.
	To      meta.Snapshot  `json:"to"`
	From    *meta.Snapshot `json:"from,omitempty"`
	Changes []meta.Change  `json:"changes"`
	Missing []meta.Missing `json:"missing"`
	Objects []MetaObject   `json:"objects"`
	// Events names the object each event's records come from.
	Events map[string]string `json:"events,omitempty"`
}

// MetaObject is an object with what uses each field.
type MetaObject struct {
	meta.Object
	Fields []MetaField `json:"fields"`
	// UsedBy is what uses the object as a whole (a change event's table).
	UsedBy []string `json:"usedBy,omitempty"`
}

// MetaField is a field and what uses it.
type MetaField struct {
	meta.Field
	UsedBy []string `json:"usedBy,omitempty"`
}

// specs compiles the catalog's recipes, for the mappings that read each
// system. A recipe that does not compile (one waiting for mapping review)
// is sketched: its mappings read fields all the same.
func (s *Server) specs(ctx context.Context) ([]*compiler.RuntimeSpec, error) {
	if len(s.cfg.Catalogs) == 0 {
		return nil, nil
	}
	cat, err := catalog.Load(s.cfg.Catalogs...)
	if err != nil {
		return nil, err
	}
	opts, err := s.verifierOptions(ctx)
	if err != nil {
		return nil, err
	}
	var out []*compiler.RuntimeSpec
	for _, obj := range cat.All() {
		if rec, ok := obj.(*v1alpha1.Recipe); ok {
			if spec, _, err := compiler.Compile(cat, rec, opts); err == nil {
				out = append(out, spec)
			} else {
				out = append(out, compiler.Sketch(cat, rec))
			}
		}
	}
	return out, nil
}

func (s *Server) metaOverview(w http.ResponseWriter, r *http.Request) {
	out := MetaOverview{Systems: []MetaSystem{}}
	if s.cfg.Meta == nil {
		out.Error = "Start the console with --database-url to see what `turgon discover` found."
		writeJSON(w, http.StatusOK, out)
		return
	}
	all, err := s.cfg.Meta.Snapshots(r.Context(), "")
	if err != nil {
		writeError(w, http.StatusBadGateway, "state database: "+err.Error())
		return
	}
	specs, err := s.specs(r.Context())
	if err != nil {
		out.Error = "catalog: " + err.Error() + " (mappings are left out of what uses each field)"
	}
	byEndpoint := map[string][]meta.Snapshot{}
	var endpoints []string
	for _, sn := range all {
		if byEndpoint[sn.Endpoint] == nil {
			endpoints = append(endpoints, sn.Endpoint)
		}
		byEndpoint[sn.Endpoint] = append(byEndpoint[sn.Endpoint], sn)
	}
	sort.Strings(endpoints)
	for _, ep := range endpoints {
		list := byEndpoint[ep]
		latest, ok, err := s.cfg.Meta.Catalog(r.Context(), list[0].ID)
		if err != nil || !ok {
			writeError(w, http.StatusBadGateway, "state database: snapshot "+strconv.FormatInt(list[0].ID, 10)+" could not be read")
			return
		}
		sys := MetaSystem{Endpoint: ep, Connector: latest.Connector, Version: latest.Version, Latest: list[0], Snapshots: len(list),
			Missing: len(meta.MissingUses(latest))}
		for _, o := range latest.Objects {
			sys.Fields += len(o.Fields)
		}
		if len(list) > 1 {
			if prev, ok, err := s.cfg.Meta.Catalog(r.Context(), list[1].ID); err == nil && ok {
				changes := meta.Compare(prev, latest, specs...)
				sys.Changes = len(changes)
				for _, c := range changes {
					if c.Breaks() {
						sys.Breaking++
					}
				}
			}
		}
		out.Systems = append(out.Systems, sys)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) metaDetail(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Meta == nil {
		writeError(w, http.StatusNotFound, "start the console with --database-url to see what `turgon discover` found")
		return
	}
	endpoint := r.PathValue("endpoint")
	list, err := s.cfg.Meta.Snapshots(r.Context(), endpoint)
	if err != nil {
		writeError(w, http.StatusBadGateway, "state database: "+err.Error())
		return
	}
	if len(list) == 0 {
		writeError(w, http.StatusNotFound, "no snapshot of "+endpoint+"; run turgon discover")
		return
	}
	find := func(param string, def int) (*meta.Snapshot, error) {
		v := r.URL.Query().Get(param)
		if v == "" {
			if def >= 0 && def < len(list) {
				return &list[def], nil
			}
			return nil, nil
		}
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, errors.New(param + " must be a snapshot id")
		}
		for i := range list {
			if list[i].ID == id {
				return &list[i], nil
			}
		}
		return nil, errors.New("no snapshot " + v + " of " + endpoint)
	}
	to, err := find("to", 0)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	from, err := find("from", -1)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if r.URL.Query().Get("from") == "" { // the snapshot before to
		for i := range list {
			if list[i].ID == to.ID && i+1 < len(list) {
				from = &list[i+1]
			}
		}
	}
	cat, ok, err := s.cfg.Meta.Catalog(r.Context(), to.ID)
	if err != nil || !ok {
		writeError(w, http.StatusBadGateway, "state database: snapshot could not be read")
		return
	}
	out := MetaDetail{Endpoint: endpoint, Connector: cat.Connector, Version: cat.Version, Snapshots: list, To: *to, From: from,
		Changes: []meta.Change{}, Missing: meta.MissingUses(cat), Objects: []MetaObject{}, Events: cat.Events}
	if out.Missing == nil {
		out.Missing = []meta.Missing{}
	}
	specs, err := s.specs(r.Context())
	if err != nil {
		out.Error = "catalog: " + err.Error() + " (mappings are left out of what uses each field)"
	}
	if from != nil {
		prev, ok, err := s.cfg.Meta.Catalog(r.Context(), from.ID)
		if err != nil || !ok {
			writeError(w, http.StatusBadGateway, "state database: snapshot could not be read")
			return
		}
		if changes := meta.Compare(prev, cat, specs...); changes != nil {
			out.Changes = changes
		}
	}
	usage := meta.Usage(cat, specs...)
	for _, o := range cat.Objects {
		mo := MetaObject{Object: o, Fields: make([]MetaField, 0, len(o.Fields)), UsedBy: usage[o.Name]}
		for _, f := range o.Fields {
			mo.Fields = append(mo.Fields, MetaField{Field: f, UsedBy: usage[o.Name+"."+f.Name]})
		}
		out.Objects = append(out.Objects, mo)
	}
	writeJSON(w, http.StatusOK, out)
}
