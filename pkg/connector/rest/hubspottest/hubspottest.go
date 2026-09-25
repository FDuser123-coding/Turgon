// Package hubspottest is a fake HubSpot CRM API for tests and demos: deals,
// searched with POST /crm/v3/objects/deals/search, read by ID and updated
// with PATCH. Like HubSpot, requests carry a private app's access token as
// a bearer token, property values are strings, a property is cleared with
// "", every change sets hs_lastmodifieddate, and a PATCH of a property the
// account does not define is rejected.
package hubspottest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Properties is the deal schema: HubSpot's own properties used here and
// the custom ones the example connection needs.
var Properties = []string{
	"dealname", "amount", "deal_currency_code", "dealstage", "pipeline", "closedate",
	"createdate", "hs_lastmodifieddate", "hs_object_id",
	"customer_email", "erp_order_number", // custom
}

// HubSpot is the fake account.
type HubSpot struct {
	Token string

	mu     sync.Mutex
	deals  map[string]map[string]string
	nextID int64
	clock  time.Time
	// FailUpdates makes deal updates fail with 400, as when HubSpot
	// rejects a property value.
	FailUpdates bool
	// Requests counts requests by "METHOD path".
	Requests map[string]int

	srv *httptest.Server
}

// New starts a fake account on a local port.
func New(token string) *HubSpot {
	h := NewHubSpot(token)
	h.srv = httptest.NewServer(h)
	return h
}

// NewHubSpot returns an account without starting a server.
func NewHubSpot(token string) *HubSpot {
	return &HubSpot{Token: token, deals: map[string]map[string]string{}, nextID: 20000000000, Requests: map[string]int{},
		clock: time.Date(2026, 9, 25, 8, 0, 0, 0, time.UTC)}
}

// StartAt makes the next deal's ID first+1 and sets the account's clock;
// demos use it so deals from different runs of the fake never share an ID
// and are always newer than a cursor from an earlier run.
func (h *HubSpot) StartAt(first int64, now time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nextID, h.clock = first, now.UTC().Truncate(time.Millisecond)
}

// URL is the API base URL for the connection's config.
func (h *HubSpot) URL() string { return h.srv.URL }

func (h *HubSpot) Close() { h.srv.Close() }

// tick returns the next modification time: each change is at least a
// millisecond after the previous one, unless sameTime is set.
func (h *HubSpot) tick(sameTime bool) string {
	if !sameTime {
		h.clock = h.clock.Add(time.Millisecond)
	}
	return h.clock.Format("2006-01-02T15:04:05.000Z")
}

// AddDeal creates a deal with the given properties and returns its ID.
func (h *HubSpot) AddDeal(props map[string]string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.add(props, false)
}

// AddDealsAtOnce creates deals that share one modification time, as a
// bulk import does.
func (h *HubSpot) AddDealsAtOnce(props ...map[string]string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var ids []string
	for i, p := range props {
		ids = append(ids, h.add(p, i > 0))
	}
	return ids
}

func (h *HubSpot) add(props map[string]string, sameTime bool) string {
	h.nextID++
	id := strconv.FormatInt(h.nextID, 10)
	now := h.tick(sameTime)
	d := map[string]string{"hs_object_id": id, "createdate": now, "hs_lastmodifieddate": now, "pipeline": "default"}
	for k, v := range props {
		if v != "" {
			d[k] = v
		}
	}
	h.deals[id] = d
	return id
}

// SetStage moves a deal to a stage, as a sales rep does.
func (h *HubSpot) SetStage(id, stage string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if d, ok := h.deals[id]; ok {
		d["dealstage"] = stage
		d["hs_lastmodifieddate"] = h.tick(false)
	}
}

// Deal returns a copy of a deal's properties, or nil.
func (h *HubSpot) Deal(id string) map[string]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	d, ok := h.deals[id]
	if !ok {
		return nil
	}
	out := map[string]string{}
	for k, v := range d {
		out[k] = v
	}
	return out
}

func defined(p string) bool {
	for _, q := range Properties {
		if p == q {
			return true
		}
	}
	return false
}

// object renders a deal as the API does: the requested properties (null
// when unset) plus the ones HubSpot always returns.
func object(d map[string]string, props []string) map[string]any {
	out := map[string]any{}
	for _, p := range append([]string{"createdate", "hs_lastmodifieddate", "hs_object_id"}, props...) {
		if v, ok := d[p]; ok {
			out[p] = v
		} else {
			out[p] = nil
		}
	}
	return map[string]any{
		"id": d["hs_object_id"], "properties": out,
		"createdAt": d["createdate"], "updatedAt": d["hs_lastmodifieddate"], "archived": false,
	}
}

func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, status int, category, msg string) {
	write(w, status, map[string]any{"status": "error", "message": msg, "category": category,
		"correlationId": "00000000-0000-4000-8000-000000000000"})
}

// ServeHTTP implements the API.
func (h *HubSpot) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.Requests[r.Method+" "+r.URL.Path]++
	if h.Token == "" || r.Header.Get("Authorization") != "Bearer "+h.Token {
		fail(w, http.StatusUnauthorized, "INVALID_AUTHENTICATION",
			"Authentication credentials not found. This API supports OAuth 2.0 authentication.")
		return
	}
	const deals = "/crm/v3/objects/deals"
	switch {
	case r.Method == http.MethodGet && r.URL.Path == deals:
		// The connection check lists one deal.
		write(w, http.StatusOK, map[string]any{"results": []any{}})
	case r.Method == http.MethodPost && r.URL.Path == deals+"/search":
		h.search(w, r)
	case strings.HasPrefix(r.URL.Path, deals+"/"):
		id := strings.TrimPrefix(r.URL.Path, deals+"/")
		d, ok := h.deals[id]
		if !ok {
			fail(w, http.StatusNotFound, "OBJECT_NOT_FOUND", "Object not found.  objectId are usually numeric.")
			return
		}
		switch r.Method {
		case http.MethodGet:
			var props []string
			if p := r.URL.Query().Get("properties"); p != "" {
				props = strings.Split(p, ",")
			}
			write(w, http.StatusOK, object(d, props))
		case http.MethodPatch:
			h.update(w, r, d)
		default:
			fail(w, http.StatusMethodNotAllowed, "VALIDATION_ERROR", "method not allowed")
		}
	default:
		fail(w, http.StatusNotFound, "OBJECT_NOT_FOUND", "resource not found")
	}
}

func (h *HubSpot) update(w http.ResponseWriter, r *http.Request, d map[string]string) {
	var body struct {
		Properties map[string]any `json:"properties"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Properties == nil {
		fail(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid input JSON: properties are required")
		return
	}
	if h.FailUpdates {
		fail(w, http.StatusBadRequest, "VALIDATION_ERROR", "Property values were not valid: erp_order_number is read-only in this portal")
		return
	}
	set := map[string]string{}
	for k, v := range body.Properties {
		if !defined(k) || strings.HasPrefix(k, "hs_") || k == "createdate" {
			fail(w, http.StatusBadRequest, "VALIDATION_ERROR",
				fmt.Sprintf(`Property values were not valid: [{"isValid":false,"message":"Property \"%s\" does not exist","error":"PROPERTY_DOESNT_EXIST","name":"%s"}]`, k, k))
			return
		}
		switch x := v.(type) {
		case string:
			set[k] = x
		case float64:
			set[k] = strconv.FormatFloat(x, 'f', -1, 64)
		case bool:
			set[k] = strconv.FormatBool(x)
		default:
			// HubSpot does not clear a property with null: "" does that.
			fail(w, http.StatusBadRequest, "VALIDATION_ERROR",
				fmt.Sprintf(`Property values were not valid: [{"isValid":false,"message":"%v was not a valid string.","error":"INVALID_STRING","name":"%s"}]`, v, k))
			return
		}
	}
	for k, v := range set {
		if v == "" {
			delete(d, k)
		} else {
			d[k] = v
		}
	}
	d["hs_lastmodifieddate"] = h.tick(false)
	props := make([]string, 0, len(set))
	for k := range set {
		props = append(props, k)
	}
	write(w, http.StatusOK, object(d, props))
}

type filter struct {
	PropertyName string `json:"propertyName"`
	Operator     string `json:"operator"`
	Value        any    `json:"value"`
}

type searchRequest struct {
	FilterGroups []struct {
		Filters []filter `json:"filters"`
	} `json:"filterGroups"`
	Sorts []struct {
		PropertyName string `json:"propertyName"`
		Direction    string `json:"direction"`
	} `json:"sorts"`
	Properties []string `json:"properties"`
	Limit      int      `json:"limit"`
	After      string   `json:"after"`
}

func (h *HubSpot) search(w http.ResponseWriter, r *http.Request) {
	var req searchRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid input JSON: "+err.Error())
		return
	}
	if req.Limit == 0 {
		req.Limit = 10
	}
	if req.Limit < 0 || req.Limit > 200 {
		fail(w, http.StatusBadRequest, "VALIDATION_ERROR", "limit must be between 1 and 200")
		return
	}
	if len(req.FilterGroups) > 5 || len(req.Sorts) > 1 {
		fail(w, http.StatusBadRequest, "VALIDATION_ERROR", "at most 5 filterGroups and one sort")
		return
	}
	var ids []string
	for id, d := range h.deals {
		ok := len(req.FilterGroups) == 0
		for _, g := range req.FilterGroups {
			all := true
			for _, f := range g.Filters {
				m, err := matches(d, f)
				if err != nil {
					fail(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
					return
				}
				all = all && m
			}
			ok = ok || all
		}
		if ok {
			ids = append(ids, id)
		}
	}
	sortBy, desc := "hs_object_id", false
	if len(req.Sorts) == 1 {
		sortBy, desc = req.Sorts[0].PropertyName, req.Sorts[0].Direction == "DESCENDING"
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := h.deals[ids[i]][sortBy], h.deals[ids[j]][sortBy]
		if na, nb, ok := numbers(sortBy, a, b); ok {
			if na != nb {
				return (na < nb) != desc
			}
		} else if a != b {
			return (a < b) != desc
		}
		return ids[i] < ids[j]
	})
	total := len(ids)
	start, _ := strconv.Atoi(req.After)
	if start > len(ids) {
		start = len(ids)
	}
	ids = ids[start:]
	resp := map[string]any{"total": total}
	if len(ids) > req.Limit {
		ids = ids[:req.Limit]
		resp["paging"] = map[string]any{"next": map[string]any{"after": strconv.Itoa(start + req.Limit)}}
	}
	results := []any{}
	for _, id := range ids {
		results = append(results, object(h.deals[id], req.Properties))
	}
	resp["results"] = results
	write(w, http.StatusOK, resp)
}

var dateProps = map[string]bool{"createdate": true, "hs_lastmodifieddate": true, "closedate": true}

// value converts a property for comparison: dates to Unix milliseconds,
// numbers to numbers.
func value(prop, v string) (float64, bool) {
	if dateProps[prop] {
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return float64(t.UnixMilli()), true
		}
	}
	f, err := strconv.ParseFloat(v, 64)
	return f, err == nil
}

func numbers(prop, a, b string) (float64, float64, bool) {
	na, ok1 := value(prop, a)
	nb, ok2 := value(prop, b)
	return na, nb, ok1 && ok2
}

func matches(d map[string]string, f filter) (bool, error) {
	if !defined(f.PropertyName) {
		return false, fmt.Errorf("There was a problem with the request. Property %q does not exist", f.PropertyName)
	}
	v, has := d[f.PropertyName]
	want := fmt.Sprint(f.Value)
	if n, ok := f.Value.(float64); ok {
		want = strconv.FormatFloat(n, 'f', -1, 64)
	}
	switch f.Operator {
	case "HAS_PROPERTY":
		return has, nil
	case "NOT_HAS_PROPERTY":
		return !has, nil
	case "EQ":
		return has && v == want, nil
	case "NEQ":
		return !has || v != want, nil
	case "GT", "GTE", "LT", "LTE":
		if !has {
			return false, nil
		}
		x, ok1 := value(f.PropertyName, v)
		y, ok2 := strconv.ParseFloat(want, 64)
		if !ok1 || ok2 != nil {
			return false, fmt.Errorf("There was a problem with the request. %q is not a number or Unix millisecond time", want)
		}
		switch f.Operator {
		case "GT":
			return x > y, nil
		case "GTE":
			return x >= y, nil
		case "LT":
			return x < y, nil
		}
		return x <= y, nil
	}
	return false, fmt.Errorf("There was a problem with the request. Unsupported operator %q", f.Operator)
}
