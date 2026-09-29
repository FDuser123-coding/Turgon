// Package netsuitetest is a fake NetSuite REST web services account for
// tests and demos: sales orders upserted by external ID (eid:), customers
// and items, and SuiteQL queries over changed sales orders. Like NetSuite
// it authenticates every request with token-based authentication (OAuth
// 1.0a signed with HMAC-SHA256, the account ID as realm), refuses reused
// nonces and stale timestamps, answers a write with 204 and a Location
// header, requires "Prefer: transient" on SuiteQL, and reports errors as
// {"title", "status", "o:errorDetails": [...]}.
package netsuitetest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector/rest"
)

// Record is the REST record service's path prefix.
const Record = "/services/rest/record/v1"

// Account is the fake account.
type Account struct {
	ID    string // realm, e.g. 1234567_SB1
	Creds rest.OAuth1Credentials

	mu        sync.Mutex
	orders    map[string]map[string]any // by internal ID
	customers map[string]map[string]any
	items     map[string]float64 // internal ID -> price
	nonces    map[string]bool
	next      int
	now       func() time.Time
	srv       *httptest.Server
	// FailWrites makes sales order writes fail, as a user event script
	// that rejects them would.
	FailWrites bool
	// Requests counts requests by "METHOD path".
	Requests map[string]int
}

// New returns an account served by an httptest server.
func New(id string, creds rest.OAuth1Credentials) *Account {
	a := NewAccount(id, creds)
	a.srv = httptest.NewServer(a)
	return a
}

// NewAccount returns an account to serve yourself.
func NewAccount(id string, creds rest.OAuth1Credentials) *Account {
	a := &Account{ID: id, Creds: creds, orders: map[string]map[string]any{}, customers: map[string]map[string]any{},
		items: map[string]float64{"101": 10, "102": 235}, nonces: map[string]bool{}, next: 5000, now: time.Now, Requests: map[string]int{}}
	a.customers["1001"] = map[string]any{"id": "1001", "entityId": "C-100", "companyName": "Lovelace GmbH", "isInactive": false}
	a.customers["1002"] = map[string]any{"id": "1002", "entityId": "C-200", "companyName": "Hopper Industries", "isInactive": false}
	return a
}

// StartAt numbers records from n and sets the clock.
func (a *Account) StartAt(n int, now func() time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.next, a.now = n, now
}

func (a *Account) URL() string { return a.srv.URL }
func (a *Account) Close()      { a.srv.Close() }

// Order returns a copy of a sales order, or nil.
func (a *Account) Order(id string) map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	return clone(a.orders[id])
}

// OrderByExternalID returns the sales order with an external ID, or nil.
func (a *Account) OrderByExternalID(eid string) map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, o := range a.orders {
		if o["externalId"] == eid {
			return clone(o)
		}
	}
	return nil
}

// Orders counts sales orders.
func (a *Account) Orders() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.orders)
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

func fail(w http.ResponseWriter, status int, code, detail string) {
	w.Header().Set("Content-Type", "application/vnd.oracle.resource+json; type=error")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type": "https://www.rfc-editor.org/rfc/rfc9110.html#section-15.5.1", "title": http.StatusText(status), "status": status,
		"o:errorDetails": []any{map[string]any{"detail": detail, "o:errorCode": code}},
	})
}

var (
	authParam = regexp.MustCompile(`(\w+)="([^"]*)"`)
	sigParam  = regexp.MustCompile(`oauth_signature="[^"]*"`)
)

// authenticate checks the request's OAuth 1.0a signature.
func (a *Account) authenticate(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "OAuth ") {
		return "no OAuth Authorization header"
	}
	p := map[string]string{}
	for _, m := range authParam.FindAllStringSubmatch(h, -1) {
		v, _ := url.PathUnescape(m[2])
		p[m[1]] = v
	}
	if p["realm"] != a.ID {
		return "wrong realm (account ID)"
	}
	if p["oauth_consumer_key"] != a.Creds.ConsumerKey || p["oauth_token"] != a.Creds.TokenID {
		return "unknown consumer key or token"
	}
	if p["oauth_signature_method"] != "HMAC-SHA256" {
		return "signature method must be HMAC-SHA256"
	}
	ts, err := strconv.ParseInt(p["oauth_timestamp"], 10, 64)
	if err != nil || time.Since(time.Unix(ts, 0)).Abs() > 5*time.Minute {
		return "timestamp missing or outside five minutes"
	}
	if a.nonces[p["oauth_nonce"]] || p["oauth_nonce"] == "" {
		return "nonce reused"
	}
	u := *r.URL
	u.Scheme, u.Host = "http", r.Host
	want := rest.SignOAuth1(r.Method, &u, a.ID, a.Creds, time.Unix(ts, 0), p["oauth_nonce"])
	if sig := sigParam.FindString(h); sig == "" || sig != sigParam.FindString(want) {
		return "signature does not match"
	}
	a.nonces[p["oauth_nonce"]] = true
	return ""
}

var (
	eidPath   = regexp.MustCompile(`^/salesOrder/eid:([^/]+)$`)
	idPath    = regexp.MustCompile(`^/(salesOrder|customer)/(\d+)$`)
	sinceExpr = regexp.MustCompile(`TO_TIMESTAMP\('([^']+)'`)
)

func (a *Account) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if msg := a.authenticate(r); msg != "" {
		w.Header().Set("WWW-Authenticate", `OAuth realm="`+a.ID+`"`)
		fail(w, http.StatusUnauthorized, "INVALID_LOGIN", "Invalid login attempt: "+msg)
		return
	}
	a.Requests[r.Method+" "+r.URL.Path]++
	if r.URL.Path == "/services/rest/query/v1/suiteql" && r.Method == http.MethodPost {
		a.suiteql(w, r)
		return
	}
	res := strings.TrimPrefix(r.URL.Path, Record)
	if res == r.URL.Path {
		fail(w, http.StatusNotFound, "NONEXISTENT_ID", "Resource not found")
		return
	}
	if m := eidPath.FindStringSubmatch(res); m != nil && r.Method == http.MethodPut {
		eid, _ := url.PathUnescape(m[1])
		a.upsert(w, r, eid)
		return
	}
	m := idPath.FindStringSubmatch(res)
	if m == nil {
		fail(w, http.StatusNotFound, "NONEXISTENT_ID", "Record type or ID not found: "+res)
		return
	}
	table := a.orders
	if m[1] == "customer" {
		table = a.customers
	}
	rec := table[m[2]]
	if rec == nil {
		fail(w, http.StatusNotFound, "NONEXISTENT_ID", fmt.Sprintf("The record instance does not exist. Provide a valid record instance ID (%s %s).", m[1], m[2]))
		return
	}
	switch r.Method {
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/vnd.oracle.resource+json; type=singular-resource")
		_ = json.NewEncoder(w).Encode(rec)
	case http.MethodDelete:
		if m[1] != "salesOrder" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		delete(a.orders, m[2])
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// upsert creates or replaces the sales order with an external ID.
func (a *Account) upsert(w http.ResponseWriter, r *http.Request, eid string) {
	if a.FailWrites {
		fail(w, http.StatusBadRequest, "USER_ERROR", "Error while accessing a resource. A user event script rejected the sales order.")
		return
	}
	var in map[string]any
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(&in); err != nil {
		fail(w, http.StatusBadRequest, "INVALID_CONTENT", "Invalid JSON: "+err.Error())
		return
	}
	entity, _ := in["entity"].(map[string]any)
	cid := fmt.Sprint(entity["id"])
	if a.customers[cid] == nil {
		fail(w, http.StatusBadRequest, "USER_ERROR", fmt.Sprintf("Error while accessing a resource. Invalid entity reference key %s.", cid))
		return
	}
	tranDate, _ := in["tranDate"].(string)
	if _, err := time.Parse("2006-01-02", tranDate); err != nil {
		fail(w, http.StatusBadRequest, "INVALID_FIELD_VALUE", "Invalid value for the field tranDate: "+tranDate)
		return
	}
	sub, _ := in["item"].(map[string]any)
	lines, _ := sub["items"].([]any)
	if len(lines) == 0 {
		fail(w, http.StatusBadRequest, "USER_ERROR", "You must enter at least one line item for this transaction.")
		return
	}
	total := 0.0
	var items []any
	for i, x := range lines {
		l, _ := x.(map[string]any)
		ref, _ := l["item"].(map[string]any)
		iid := fmt.Sprint(ref["id"])
		price, ok := a.items[iid]
		q, err := strconv.ParseFloat(fmt.Sprint(l["quantity"]), 64)
		if !ok || err != nil || q <= 0 {
			fail(w, http.StatusBadRequest, "USER_ERROR", fmt.Sprintf("Error while accessing a resource. Line %d: invalid item reference key %s or quantity.", i+1, iid))
			return
		}
		total += price * q
		items = append(items, map[string]any{"item": map[string]any{"id": iid}, "quantity": q, "amount": price * q})
	}
	id := ""
	for oid, o := range a.orders {
		if o["externalId"] == eid {
			id = oid
		}
	}
	if id == "" {
		id = strconv.Itoa(a.next)
		a.next++
	}
	a.orders[id] = map[string]any{
		"id": id, "externalId": eid, "tranId": "SO" + id, "entity": map[string]any{"id": cid}, "tranDate": tranDate,
		"memo": in["memo"], "item": map[string]any{"items": items}, "total": total, "status": "Pending Approval",
		"lastModifiedDate": a.now().UTC().Format("2006-01-02T15:04:05Z"),
	}
	w.Header().Set("Location", fmt.Sprintf("http://%s%s/salesOrder/%s", r.Host, Record, id))
	w.WriteHeader(http.StatusNoContent)
}

// suiteql answers the changed-sales-orders query: rows modified after the
// TO_TIMESTAMP literal, oldest first, limited by ?limit.
func (a *Account) suiteql(w http.ResponseWriter, r *http.Request) {
	if !strings.EqualFold(r.Header.Get("Prefer"), "transient") {
		fail(w, http.StatusBadRequest, "INVALID_HEADER", `SuiteQL requests need the header "Prefer: transient".`)
		return
	}
	var in struct {
		Q string `json:"q"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || !strings.Contains(strings.ToLower(in.Q), "from transaction") {
		fail(w, http.StatusBadRequest, "INVALID_PARAMETER", "Invalid search query.")
		return
	}
	var after time.Time
	if m := sinceExpr.FindStringSubmatch(in.Q); m != nil {
		t, err := time.Parse(time.RFC3339Nano, m[1])
		if err != nil {
			fail(w, http.StatusBadRequest, "INVALID_PARAMETER", "Invalid timestamp: "+m[1])
			return
		}
		after = t
	}
	rows := []map[string]any{}
	for _, o := range a.orders {
		t, _ := time.Parse(time.RFC3339, o["lastModifiedDate"].(string))
		if t.After(after) {
			rows = append(rows, map[string]any{"id": o["id"], "tranid": o["tranId"], "externalid": o["externalId"],
				"entity": o["entity"].(map[string]any)["id"], "total": o["total"], "modified": o["lastModifiedDate"]})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i]["modified"] != rows[j]["modified"] {
			return rows[i]["modified"].(string) < rows[j]["modified"].(string)
		}
		x, _ := strconv.Atoi(rows[i]["id"].(string))
		y, _ := strconv.Atoi(rows[j]["id"].(string))
		return x < y
	})
	if n, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && n < len(rows) {
		rows = rows[:n]
	}
	w.Header().Set("Content-Type", "application/vnd.oracle.resource+json; type=collection")
	_ = json.NewEncoder(w).Encode(map[string]any{"count": len(rows), "hasMore": false, "items": rows})
}

// OrderIDs lists the sales orders' internal IDs.
func (a *Account) OrderIDs() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	ids := make([]string, 0, len(a.orders))
	for id := range a.orders {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
