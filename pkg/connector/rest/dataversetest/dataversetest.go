// Package dataversetest is a fake Microsoft Dataverse (Dynamics 365 Sales)
// Web API for tests and demos: sales orders with write-in order lines,
// upserted by an alternate key, and accounts. Like Dataverse it takes an
// OAuth 2.0 client credentials token from its own token endpoint, answers
// OData v4 JSON ({"value": [...]}), filters and orders lists by
// modifiedon, answers an upsert with 204 and OData-EntityId, and reports
// errors as {"error": {"code", "message"}}.
package dataversetest

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// API is the Web API path prefix.
const API = "/api/data/v9.2"

// Dataverse is the fake environment.
type Dataverse struct {
	ClientID, ClientSecret string

	mu       sync.Mutex
	tokens   map[string]time.Time
	orders   map[string]map[string]any // by salesorderid
	accounts map[string]map[string]any
	now      func() time.Time
	srv      *httptest.Server
	// FailUpserts makes order upserts fail with 400, as a plug-in that
	// rejects the order would.
	FailUpserts bool
	// Requests counts requests by "METHOD path".
	Requests map[string]int
}

// Accounts the environment starts with.
const (
	LovelaceGmbH = "6a1c0e2f-8b3d-4f5a-9c7e-1d2b3a4c5e6f"
	HopperInc    = "0b9d8c7e-6f5a-4b3c-8d2e-1f0a9b8c7d6e"
)

// New returns an environment served by an httptest server.
func New(clientID, clientSecret string) *Dataverse {
	d := NewDataverse(clientID, clientSecret)
	d.srv = httptest.NewServer(d)
	return d
}

// NewDataverse returns an environment to serve yourself.
func NewDataverse(clientID, clientSecret string) *Dataverse {
	d := &Dataverse{ClientID: clientID, ClientSecret: clientSecret, tokens: map[string]time.Time{},
		orders: map[string]map[string]any{}, accounts: map[string]map[string]any{}, now: time.Now, Requests: map[string]int{}}
	d.accounts[LovelaceGmbH] = map[string]any{"accountid": LovelaceGmbH, "name": "Lovelace GmbH", "accountnumber": "C-100", "statecode": 0}
	d.accounts[HopperInc] = map[string]any{"accountid": HopperInc, "name": "Hopper Industries", "accountnumber": "C-200", "statecode": 0}
	return d
}

// SetClock sets the time modifiedon is stamped with.
func (d *Dataverse) SetClock(now func() time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.now = now
}

func (d *Dataverse) URL() string { return d.srv.URL }
func (d *Dataverse) Close()      { d.srv.Close() }

// TokenURL is where clients get tokens.
func (d *Dataverse) TokenURL(base string) string { return base + "/oauth2/v2.0/token" }

// Order returns a copy of a sales order, or nil.
func (d *Dataverse) Order(id string) map[string]any {
	d.mu.Lock()
	defer d.mu.Unlock()
	return clone(d.orders[id])
}

// OrderByKey returns the order carrying an external reference, or nil.
func (d *Dataverse) OrderByKey(ref string) map[string]any {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, o := range d.orders {
		if o["turgon_externalref"] == ref {
			return clone(o)
		}
	}
	return nil
}

// Orders counts sales orders.
func (d *Dataverse) Orders() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.orders)
}

func clone(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	b, _ := json.Marshal(m)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}

func fail(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": code, "message": msg}})
}

func guid() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

var (
	byKey  = regexp.MustCompile(`^salesorders\(turgon_externalref='((?:[^']|'')+)'\)$`)
	byID   = regexp.MustCompile(`^(salesorders|accounts)\(([0-9a-f-]{36})\)$`)
	bindRE = regexp.MustCompile(`^/accounts\(([0-9a-f-]{36})\)$`)
)

func (d *Dataverse) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/oauth2/v2.0/token" {
		d.token(w, r)
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	auth := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if exp, ok := d.tokens[auth]; !ok || d.now().After(exp) {
		w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if !strings.HasPrefix(r.URL.Path, API+"/") {
		fail(w, http.StatusNotFound, "0x80060888", "Resource not found for the segment")
		return
	}
	res := strings.TrimPrefix(r.URL.Path, API+"/")
	d.Requests[r.Method+" "+res]++
	switch {
	case res == "WhoAmI" && r.Method == http.MethodGet:
		reply(w, http.StatusOK, map[string]any{"UserId": "11111111-2222-3333-4444-555555555555", "BusinessUnitId": "b", "OrganizationId": "o"})
	case res == "salesorders" && r.Method == http.MethodGet:
		d.list(w, r)
	case byKey.MatchString(res) && r.Method == http.MethodPatch:
		d.upsert(w, r, strings.ReplaceAll(byKey.FindStringSubmatch(res)[1], "''", "'"))
	case byID.MatchString(res):
		m := byID.FindStringSubmatch(res)
		table := d.orders
		if m[1] == "accounts" {
			table = d.accounts
		}
		rec := table[m[2]]
		if rec == nil {
			fail(w, http.StatusNotFound, "0x80040217", fmt.Sprintf("%s With Id = %s Does Not Exist", strings.TrimSuffix(m[1], "s"), m[2]))
			return
		}
		switch r.Method {
		case http.MethodGet:
			reply(w, http.StatusOK, selected(rec, r.URL.Query().Get("$select")))
		case http.MethodDelete:
			if m[1] != "salesorders" {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			delete(d.orders, m[2])
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	default:
		fail(w, http.StatusNotFound, "0x80060888", "Resource not found for the segment '"+res+"'")
	}
}

func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; odata.metadata=minimal")
	w.Header().Set("OData-Version", "4.0")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func selected(rec map[string]any, sel string) map[string]any {
	if sel == "" {
		return rec
	}
	out := map[string]any{}
	for _, f := range strings.Split(sel, ",") {
		if v, ok := rec[strings.TrimSpace(f)]; ok {
			out[strings.TrimSpace(f)] = v
		}
	}
	return out
}

func (d *Dataverse) token(w http.ResponseWriter, r *http.Request) {
	id, secret, ok := r.BasicAuth()
	if err := r.ParseForm(); err != nil || r.PostForm.Get("grant_type") != "client_credentials" {
		fail(w, http.StatusBadRequest, "unsupported_grant_type", "grant_type must be client_credentials")
		return
	}
	if !ok {
		id, secret = r.PostForm.Get("client_id"), r.PostForm.Get("client_secret")
	}
	if id != d.ClientID || secret != d.ClientSecret {
		fail(w, http.StatusUnauthorized, "invalid_client", "AADSTS7000215: Invalid client secret provided.")
		return
	}
	if !strings.HasSuffix(r.PostForm.Get("scope"), "/.default") {
		fail(w, http.StatusBadRequest, "invalid_scope", "AADSTS1002012: The provided value for scope is not valid; use <environment URL>/.default")
		return
	}
	d.mu.Lock()
	tok := guid()
	d.tokens[tok] = d.now().Add(time.Hour)
	d.mu.Unlock()
	reply(w, http.StatusOK, map[string]any{"token_type": "Bearer", "expires_in": 3599, "access_token": tok})
}

// upsert creates or updates the order with an alternate key.
func (d *Dataverse) upsert(w http.ResponseWriter, r *http.Request, ref string) {
	if d.FailUpserts {
		fail(w, http.StatusBadRequest, "0x80040265", "The order could not be saved: a plug-in rejected it")
		return
	}
	var in map[string]any
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(&in); err != nil {
		fail(w, http.StatusBadRequest, "0x80048d19", "Error identified in Payload provided by the user for Entity :'salesorders'")
		return
	}
	name, _ := in["name"].(string)
	if name == "" {
		fail(w, http.StatusBadRequest, "0x80040200", "Attribute 'name' of entity 'salesorder' is required")
		return
	}
	bind, _ := in["customerid_account@odata.bind"].(string)
	m := bindRE.FindStringSubmatch(bind)
	if m == nil || d.accounts[m[1]] == nil {
		fail(w, http.StatusBadRequest, "0x80040217", fmt.Sprintf("account With Id = %s Does Not Exist", strings.TrimSuffix(strings.TrimPrefix(bind, "/accounts("), ")")))
		return
	}
	lines, _ := in["order_details"].([]any)
	total := 0.0
	var details []any
	for i, x := range lines {
		l, _ := x.(map[string]any)
		desc, _ := l["productdescription"].(string)
		q, qErr := num(l["quantity"])
		p, pErr := num(l["priceperunit"])
		if l["isproductoverridden"] != true || desc == "" || qErr != nil || pErr != nil || q <= 0 {
			fail(w, http.StatusBadRequest, "0x80040203", fmt.Sprintf("order_details[%d]: a write-in product needs isproductoverridden, productdescription, quantity > 0 and priceperunit", i))
			return
		}
		total += q * p
		details = append(details, map[string]any{"salesorderdetailid": guid(), "productdescription": desc, "quantity": q, "priceperunit": p, "extendedamount": q * p})
	}
	var id string
	for oid, o := range d.orders {
		if o["turgon_externalref"] == ref {
			id = oid
		}
	}
	status := "created"
	if id == "" {
		id = guid()
	} else {
		status = "updated"
	}
	d.orders[id] = map[string]any{
		"salesorderid": id, "turgon_externalref": ref, "name": name, "description": in["description"],
		"_customerid_value": m[1], "totalamount": total, "order_details": details,
		"statecode": 0, "modifiedon": d.now().UTC().Format("2006-01-02T15:04:05Z"),
	}
	w.Header().Set("OData-EntityId", fmt.Sprintf("https://%s%s/salesorders(%s)", r.Host, API, id))
	w.Header().Set("X-Upsert", status)
	w.WriteHeader(http.StatusNoContent)
}

func num(v any) (float64, error) {
	switch x := v.(type) {
	case json.Number:
		return x.Float64()
	case float64:
		return x, nil
	}
	return 0, fmt.Errorf("not a number")
}

var gtRE = regexp.MustCompile(`^modifiedon gt (\S+)$`)

// list answers $filter=modifiedon gt <time>, $orderby=modifiedon asc and $top.
func (d *Dataverse) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var after time.Time
	if f := q.Get("$filter"); f != "" {
		m := gtRE.FindStringSubmatch(f)
		if m == nil {
			fail(w, http.StatusBadRequest, "0x80060888", "Unsupported filter: "+f)
			return
		}
		t, err := time.Parse(time.RFC3339, m[1])
		if err != nil {
			fail(w, http.StatusBadRequest, "0x80060888", "Invalid DateTimeOffset: "+m[1])
			return
		}
		after = t
	}
	var list []map[string]any
	for _, o := range d.orders {
		t, _ := time.Parse(time.RFC3339, o["modifiedon"].(string))
		if t.After(after) {
			list = append(list, o)
		}
	}
	sort.Slice(list, func(i, j int) bool {
		a, b := list[i]["modifiedon"].(string), list[j]["modifiedon"].(string)
		if a != b {
			return a < b
		}
		return list[i]["salesorderid"].(string) < list[j]["salesorderid"].(string)
	})
	if n, err := strconv.Atoi(q.Get("$top")); err == nil && n < len(list) {
		list = list[:n]
	}
	out := make([]any, len(list))
	for i, o := range list {
		out[i] = selected(o, q.Get("$select"))
	}
	reply(w, http.StatusOK, map[string]any{"@odata.context": "$metadata#salesorders", "value": out})
}

// OrderIDs lists the sales orders' IDs.
func (d *Dataverse) OrderIDs() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	ids := make([]string, 0, len(d.orders))
	for id := range d.orders {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
