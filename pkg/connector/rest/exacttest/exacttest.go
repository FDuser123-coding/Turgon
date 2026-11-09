// Package exacttest is a fake Exact Online REST API (v1) and its OAuth 2.0
// token endpoint, for tests and demos: one division with accounts, items
// and sales orders.
//
// Like Exact Online it:
//   - takes the client's credentials in the token request's form, rotates
//     the refresh token on every refresh (the old one is refused with
//     invalid_grant), and gives access tokens of ten minutes, their
//     lifetime as a string ("expires_in": "600");
//   - wants Accept: application/json (Exact answers XML otherwise);
//   - wraps one entity as {"d": {...}} and lists as {"d": {"results": [...]}},
//     at most 60 a page, and dates records as Microsoft JSON
//     ("/Date(1730803200000)/");
//   - filters lists with OData v2: Modified gt datetime'…', YourRef eq '…',
//     ID eq guid'…', ordered by $orderby (Modified asc), cut to $top, with
//     the fields $select names;
//   - creates a sales order and its lines in one POST (deep insert), each
//     line's item a GUID, priced from the item without a UnitPrice;
//   - deletes a sales order unless it has been delivered;
//   - reports errors as {"error": {"code": "", "message": {"lang": "", "value": "…"}}}.
package exacttest

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Division is the company's division; TokenPath and API are Exact's paths.
const (
	Division  = 3191512
	TokenPath = "/api/oauth2/token"
)

// API is the division's base path.
var API = fmt.Sprintf("/api/v1/%d", Division)

// Accounts and items the division starts with.
const (
	Lovelace = "4b2fd0b1-3c8a-4b8e-9a3e-1d2c3b4a5f60" // customer 1001, ada@example.com
	Hopper   = "7c9e1a2b-5d4f-4e3a-8b2c-6f1e0d9c8b7a" // customer 1002
	Desk     = "a1b2c3d4-0001-4e5f-9a8b-0c1d2e3f4a5b" // item M-1, 100.00
	Chair    = "a1b2c3d4-0002-4e5f-9a8b-0c1d2e3f4a5b" // item M-2, 235.00
)

// PageSize is the most rows Exact Online answers at once.
const PageSize = 60

// Exact is the fake division.
type Exact struct {
	ClientID, ClientSecret string

	mu       sync.Mutex
	refresh  map[string]bool
	access   map[string]time.Time
	accounts map[string]map[string]any
	items    map[string]map[string]any
	orders   map[string]map[string]any
	next     int
	now      func() time.Time
	srv      *httptest.Server
	// Refreshes counts refresh grants that succeeded; Spent counts refused ones.
	Refreshes, Spent int
	// Requests counts API calls by "METHOD resource".
	Requests map[string]int
}

// New returns a division served by an httptest server, and the refresh
// token a person's authorization would have given.
func New(clientID, clientSecret string) (*Exact, string) {
	x, tok := NewExact(clientID, clientSecret)
	x.srv = httptest.NewServer(x)
	return x, tok
}

// NewExact returns a division to serve yourself, and its first refresh token.
func NewExact(clientID, clientSecret string) (*Exact, string) {
	x := &Exact{ClientID: clientID, ClientSecret: clientSecret, refresh: map[string]bool{}, access: map[string]time.Time{},
		accounts: map[string]map[string]any{}, items: map[string]map[string]any{}, orders: map[string]map[string]any{},
		next: 1, now: time.Now, Requests: map[string]int{}}
	x.accounts[Lovelace] = map[string]any{"ID": Lovelace, "Code": "                1001", "Name": "Lovelace GmbH", "Email": "ada@example.com", "Status": "C", "Blocked": false}
	x.accounts[Hopper] = map[string]any{"ID": Hopper, "Code": "                1002", "Name": "Hopper Industries", "Email": "grace@example.com", "Status": "C", "Blocked": true}
	x.items[Desk] = map[string]any{"ID": Desk, "Code": "M-1", "Description": "Desk", "SalesPrice": 100.0}
	x.items[Chair] = map[string]any{"ID": Chair, "Code": "M-2", "Description": "Chair", "SalesPrice": 235.0}
	return x, x.issueRefresh()
}

func (x *Exact) issueRefresh() string {
	t := token()
	x.refresh[t] = true
	return t
}

// Authorize returns a new refresh token, as authorizing the app again would.
func (x *Exact) Authorize() string {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.issueRefresh()
}

// StartAt sets the next order number and the clock.
func (x *Exact) StartAt(number int, now func() time.Time) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.next, x.now = number, now
}

// ExpireAccessTokens ends every access token.
func (x *Exact) ExpireAccessTokens() {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.access = map[string]time.Time{}
}

// Deliver marks an order delivered, after which Exact refuses to delete it.
func (x *Exact) Deliver(id string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if o := x.orders[id]; o != nil {
		o["DeliveryStatus"], o["DeliveryStatusDescription"], o["Modified"] = 21, "Complete", msdate(x.now())
	}
}

// Touch changes an order, as editing it in Exact would.
func (x *Exact) Touch(id, description string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if o := x.orders[id]; o != nil {
		o["Description"], o["Modified"] = description, msdate(x.now())
	}
}

func (x *Exact) URL() string { return x.srv.URL }
func (x *Exact) Close()      { x.srv.Close() }

// Order returns a copy of an order with its lines, or nil.
func (x *Exact) Order(id string) map[string]any {
	x.mu.Lock()
	defer x.mu.Unlock()
	return clone(x.orders[id])
}

// OrderByRef returns the order with a YourRef, or nil.
func (x *Exact) OrderByRef(ref string) map[string]any {
	x.mu.Lock()
	defer x.mu.Unlock()
	for _, o := range x.orders {
		if o["YourRef"] == ref {
			return clone(o)
		}
	}
	return nil
}

// Orders counts sales orders.
func (x *Exact) Orders() int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return len(x.orders)
}

// OrderIDs lists the orders' IDs, oldest first.
func (x *Exact) OrderIDs() []string {
	x.mu.Lock()
	defer x.mu.Unlock()
	var ids []string
	for id := range x.orders {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return x.orders[ids[i]]["OrderNumber"].(int) < x.orders[ids[j]]["OrderNumber"].(int)
	})
	return ids
}

func clone(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	raw, _ := json.Marshal(m)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return out
}

func token() string {
	r := make([]byte, 16)
	_, _ = rand.Read(r)
	return hex.EncodeToString(r)
}

func guid() string {
	h := token()
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

func msdate(t time.Time) string { return fmt.Sprintf("/Date(%d)/", t.UnixMilli()) }

func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

func fail(w http.ResponseWriter, status int, msg string) {
	reply(w, status, map[string]any{"error": map[string]any{"code": "", "message": map[string]any{"lang": "", "value": msg}}})
}

var keyRE = regexp.MustCompile(`^(\w+)/(\w+)(?:\(guid'([0-9a-f-]{36})'\))?$`)

func (x *Exact) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == TokenPath {
		x.token(w, r)
		return
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	auth := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if exp, ok := x.access[auth]; !ok || x.now().After(exp) {
		w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
		fail(w, http.StatusUnauthorized, "Access token is invalid or expired")
		return
	}
	if !strings.Contains(r.Header.Get("Accept"), "application/json") {
		fail(w, http.StatusNotAcceptable, "the fake answers JSON only; Exact Online answers XML without Accept: application/json")
		return
	}
	if r.URL.Path == "/api/v1/current/Me" {
		reply(w, http.StatusOK, map[string]any{"d": map[string]any{"results": []any{map[string]any{"CurrentDivision": Division, "FullName": "Turgon Integration"}}}})
		return
	}
	if !strings.HasPrefix(r.URL.Path, API+"/") {
		fail(w, http.StatusUnauthorized, "Forbidden: the division is not in this user's divisions")
		return
	}
	m := keyRE.FindStringSubmatch(strings.TrimPrefix(r.URL.Path, API+"/"))
	if m == nil {
		fail(w, http.StatusNotFound, "Resource not found")
		return
	}
	res, id := m[1]+"/"+m[2], m[3]
	x.Requests[r.Method+" "+res]++
	switch {
	case res == "crm/Accounts" && r.Method == http.MethodGet && id == "":
		x.list(w, r, x.accounts, "Name")
	case res == "salesorder/SalesOrders" && r.Method == http.MethodGet && id == "":
		x.list(w, r, x.orders, "OrderNumber")
	case res == "salesorder/SalesOrders" && r.Method == http.MethodGet:
		if o := x.orders[id]; o != nil {
			reply(w, http.StatusOK, map[string]any{"d": x.view(o)})
			return
		}
		fail(w, http.StatusNotFound, "Sales order not found")
	case res == "salesorder/SalesOrders" && r.Method == http.MethodPost && id == "":
		x.create(w, r)
	case res == "salesorder/SalesOrders" && r.Method == http.MethodDelete && id != "":
		x.delete(w, id)
	default:
		fail(w, http.StatusNotFound, "Resource not found: "+r.Method+" "+res)
	}
}

func (x *Exact) token(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil || r.PostForm.Get("client_id") != x.ClientID || r.PostForm.Get("client_secret") != x.ClientSecret {
		reply(w, http.StatusBadRequest, map[string]any{"error": "invalid_client"})
		return
	}
	if r.PostForm.Get("grant_type") != "refresh_token" {
		reply(w, http.StatusBadRequest, map[string]any{"error": "unsupported_grant_type"})
		return
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	old := r.PostForm.Get("refresh_token")
	if !x.refresh[old] {
		x.Spent++
		reply(w, http.StatusBadRequest, map[string]any{"error": "invalid_grant", "error_description": "Old or invalid refresh token"})
		return
	}
	delete(x.refresh, old)
	access := "gAAAA" + token()
	x.access[access] = x.now().Add(10 * time.Minute)
	x.Refreshes++
	reply(w, http.StatusOK, map[string]any{"access_token": access, "token_type": "bearer", "expires_in": "600", "refresh_token": x.issueRefresh()})
}

// view is an order as Exact answers it: its lines are a deferred link.
func (x *Exact) view(o map[string]any) map[string]any {
	v := clone(o)
	delete(v, "Lines")
	v["SalesOrderLines"] = map[string]any{"__deferred": map[string]any{"uri": fmt.Sprintf("%s/salesorder/SalesOrders(guid'%s')/SalesOrderLines", API, o["OrderID"])}}
	return v
}

var (
	modifiedRE = regexp.MustCompile(`^Modified gt datetime'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?)'$`)
	refRE      = regexp.MustCompile(`^YourRef eq '((?:[^']|'')*)'$`)
	idRE       = regexp.MustCompile(`^ID eq guid'([0-9a-f-]{36})'$`)
)

func millis(v any) int64 {
	s := strings.TrimSuffix(strings.TrimPrefix(fmt.Sprint(v), "/Date("), ")/")
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

// list answers rows matching $filter, ordered, cut to $top (at most 60),
// with the fields $select names.
func (x *Exact) list(w http.ResponseWriter, r *http.Request, rows map[string]map[string]any, order string) {
	q := r.URL.Query()
	keep := func(map[string]any) bool { return true }
	if f := q.Get("$filter"); f != "" {
		switch {
		case modifiedRE.MatchString(f):
			t, err := time.Parse("2006-01-02T15:04:05", strings.SplitN(modifiedRE.FindStringSubmatch(f)[1], ".", 2)[0])
			if err != nil {
				fail(w, http.StatusBadRequest, "Invalid datetime literal")
				return
			}
			keep = func(row map[string]any) bool { return millis(row["Modified"]) > t.UnixMilli() }
		case refRE.MatchString(f):
			ref := strings.ReplaceAll(refRE.FindStringSubmatch(f)[1], "''", "'")
			keep = func(row map[string]any) bool { return row["YourRef"] == ref }
		case idRE.MatchString(f):
			id := idRE.FindStringSubmatch(f)[1]
			keep = func(row map[string]any) bool { return row["ID"] == id || row["OrderID"] == id }
		default:
			fail(w, http.StatusBadRequest, "The fake does not take the filter "+f)
			return
		}
	}
	var out []map[string]any
	for _, row := range rows {
		if keep(row) {
			out = append(out, row)
		}
	}
	byModified := q.Get("$orderby") == "Modified asc"
	if ob := q.Get("$orderby"); ob != "" && !byModified {
		fail(w, http.StatusBadRequest, "The fake orders by Modified asc only")
		return
	}
	sort.Slice(out, func(i, j int) bool {
		if byModified {
			if a, b := millis(out[i]["Modified"]), millis(out[j]["Modified"]); a != b {
				return a < b
			}
		}
		return fmt.Sprint(out[i][order]) < fmt.Sprint(out[j][order])
	})
	top := PageSize
	if t := q.Get("$top"); t != "" {
		n, err := strconv.Atoi(t)
		if err != nil || n < 0 {
			fail(w, http.StatusBadRequest, "Invalid $top")
			return
		}
		top = min(n, PageSize)
	}
	if len(out) > top {
		out = out[:top]
	}
	var sel []string
	if s := q.Get("$select"); s != "" {
		sel = strings.Split(s, ",")
	}
	results := make([]any, len(out))
	for i, row := range out {
		v := clone(row)
		if _, isOrder := row["OrderID"]; isOrder {
			v = x.view(row)
		}
		if sel != nil {
			p := map[string]any{}
			for _, f := range sel {
				if val, ok := v[strings.TrimSpace(f)]; ok {
					p[strings.TrimSpace(f)] = val
				}
			}
			v = p
		}
		results[i] = v
	}
	reply(w, http.StatusOK, map[string]any{"d": map[string]any{"results": results}})
}

func num(v any) (float64, bool) {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case float64:
		return n, true
	}
	return 0, false
}

func (x *Exact) create(w http.ResponseWriter, r *http.Request) {
	var in map[string]any
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(&in); err != nil {
		fail(w, http.StatusBadRequest, "Error reading JSON: "+err.Error())
		return
	}
	cust, _ := in["OrderedBy"].(string)
	acct := x.accounts[cust]
	if acct == nil {
		fail(w, http.StatusBadRequest, "Ordered by: Mandatory or not valid ("+cust+")")
		return
	}
	if acct["Blocked"] == true {
		fail(w, http.StatusBadRequest, "Account "+strings.TrimSpace(acct["Code"].(string))+" is blocked")
		return
	}
	lines, _ := in["SalesOrderLines"].([]any)
	if len(lines) == 0 {
		fail(w, http.StatusBadRequest, "Sales order lines: Mandatory")
		return
	}
	id := guid()
	var out []any
	total := 0.0
	for i, v := range lines {
		l, _ := v.(map[string]any)
		itemID, _ := l["Item"].(string)
		item := x.items[itemID]
		if item == nil {
			fail(w, http.StatusBadRequest, fmt.Sprintf("Item: Mandatory or not valid (line %d)", i+1))
			return
		}
		qty, ok := num(l["Quantity"])
		if !ok || qty <= 0 {
			fail(w, http.StatusBadRequest, fmt.Sprintf("Quantity: Mandatory (line %d)", i+1))
			return
		}
		price, ok := num(l["UnitPrice"])
		if !ok {
			price = item["SalesPrice"].(float64)
		}
		amt := math.Round(qty*price*100) / 100
		total += amt
		out = append(out, map[string]any{"ID": guid(), "OrderID": id, "LineNumber": i + 1, "Item": itemID, "ItemCode": item["Code"],
			"Description": item["Description"], "Quantity": qty, "UnitPrice": price, "NetPrice": price, "AmountDC": amt})
	}
	date := x.now()
	if d, ok := in["OrderDate"].(string); ok && d != "" {
		t, err := time.Parse("2006-01-02", strings.SplitN(d, "T", 2)[0])
		if err != nil {
			fail(w, http.StatusBadRequest, "Order date: invalid date "+d)
			return
		}
		date = t
	}
	o := map[string]any{"OrderID": id, "OrderNumber": x.next, "OrderedBy": cust, "OrderedByName": acct["Name"], "OrderDate": msdate(date),
		"YourRef": in["YourRef"], "Description": in["Description"], "Status": 12, "StatusDescription": "Open", "DeliveryStatus": 12,
		"DeliveryStatusDescription": "Open", "AmountDC": math.Round(total*100) / 100, "Currency": "EUR",
		"Created": msdate(x.now()), "Modified": msdate(x.now()), "Lines": out}
	x.next++
	x.orders[id] = o
	reply(w, http.StatusCreated, map[string]any{"d": x.view(o)})
}

func (x *Exact) delete(w http.ResponseWriter, id string) {
	o := x.orders[id]
	if o == nil {
		fail(w, http.StatusNotFound, "Sales order not found")
		return
	}
	if o["DeliveryStatus"] == 21 {
		fail(w, http.StatusBadRequest, fmt.Sprintf("Sales order %v cannot be deleted: it has been delivered", o["OrderNumber"]))
		return
	}
	delete(x.orders, id)
	reply(w, http.StatusNoContent, nil)
}
