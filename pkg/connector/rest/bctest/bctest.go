// Package bctest is a fake Microsoft Dynamics 365 Business Central API
// (v2.0) for tests and demos: one company with customers, items and sales
// orders. Like Business Central it takes an OAuth 2.0 client credentials
// token from Entra ID's token endpoint (served here too) for the scope
// https://api.businesscentral.dynamics.com/.default, answers OData v4 JSON
// ({"value": [...]}), creates a sales order and its lines in one POST
// (deep insert) with 201 and the order, prices lines without a unitPrice
// from the item card, filters orders by externalDocumentNumber or
// lastModifiedDateTime, wants If-Match on a delete, and reports errors as
// {"error": {"code", "message"}}.
//
// It has no upsert by an external key and takes no idempotency key, as
// Business Central does not: a retried create finds its order by
// externalDocumentNumber (the rest connector's existing lookup).
package bctest

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

// Tenant, environment and company of the fake.
const (
	Tenant      = "contoso.onmicrosoft.com"
	Environment = "production"
	Company     = "c0a1b2c3-d4e5-4f60-8a7b-9c0d1e2f3a4b"
	// API is the path of the company's resources.
	API = "/v2.0/" + Tenant + "/" + Environment + "/api/v2.0/companies(" + Company + ")"
	// TokenPath is Entra ID's token endpoint for the tenant.
	TokenPath = "/" + Tenant + "/oauth2/v2.0/token"
	// Scope is the one Business Central's API takes.
	Scope = "https://api.businesscentral.dynamics.com/.default"
)

// Customers and items the company starts with (Business Central's demo
// company has the same numbers).
const (
	Adatum       = "10000" // Adatum Corporation
	TreyResearch = "20000"
	AlpineSki    = "40000" // blocked
)

// BC is the fake company.
type BC struct {
	ClientID, ClientSecret string

	mu        sync.Mutex
	tokens    map[string]time.Time
	customers map[string]map[string]any // by id
	items     map[string]map[string]any // by number
	orders    map[string]map[string]any // by id
	next      int
	now       func() time.Time
	srv       *httptest.Server
	// FailCreates makes order creates fail with 400, as an event
	// subscriber that rejects the order would.
	FailCreates bool
	// Requests counts requests by "METHOD resource".
	Requests map[string]int
}

// New returns a company served by an httptest server.
func New(clientID, clientSecret string) *BC {
	b := NewBC(clientID, clientSecret)
	b.srv = httptest.NewServer(b)
	return b
}

// NewBC returns a company to serve yourself.
func NewBC(clientID, clientSecret string) *BC {
	b := &BC{ClientID: clientID, ClientSecret: clientSecret, tokens: map[string]time.Time{},
		customers: map[string]map[string]any{}, items: map[string]map[string]any{}, orders: map[string]map[string]any{},
		next: 101001, now: time.Now, Requests: map[string]int{}}
	for _, c := range []struct{ number, name, email, blocked string }{
		{Adatum, "Adatum Corporation", "ada@example.com", " "},
		{TreyResearch, "Trey Research", "grace@example.com", " "},
		{AlpineSki, "Alpine Ski House", "ski@example.com", "All"},
	} {
		id := guid()
		b.customers[id] = map[string]any{"id": id, "number": c.number, "displayName": c.name, "email": c.email,
			"blocked": c.blocked, "currencyCode": "", "lastModifiedDateTime": "2026-01-01T00:00:00Z"}
	}
	b.items["1896-S"] = map[string]any{"id": guid(), "number": "1896-S", "displayName": "ATHENS Desk", "unitPrice": 1000.8}
	b.items["1900-S"] = map[string]any{"id": guid(), "number": "1900-S", "displayName": "PARIS Guest Chair, black", "unitPrice": 192.8}
	return b
}

// StartAt sets the next order number and the clock, so numbers keep
// increasing across restarts of a demo.
func (b *BC) StartAt(number int, now func() time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.next, b.now = number, now
}

func (b *BC) URL() string { return b.srv.URL }
func (b *BC) Close()      { b.srv.Close() }

// CustomerID returns the ID of the customer with a number.
func (b *BC) CustomerID(number string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	for id, c := range b.customers {
		if c["number"] == number {
			return id
		}
	}
	return ""
}

// Order returns a copy of a sales order, with its lines, or nil.
func (b *BC) Order(id string) map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	return clone(b.orders[id])
}

// OrderByExternal returns the order with an external document number, or nil.
func (b *BC) OrderByExternal(ext string) map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, o := range b.orders {
		if o["externalDocumentNumber"] == ext {
			return clone(o)
		}
	}
	return nil
}

// Orders counts sales orders.
func (b *BC) Orders() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.orders)
}

// OrderIDs lists the sales orders' IDs.
func (b *BC) OrderIDs() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	ids := make([]string, 0, len(b.orders))
	for id := range b.orders {
		ids = append(ids, id)
	}
	sort.Strings(ids)
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

func fail(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": code, "message": msg}})
}

func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; odata.metadata=minimal")
	w.Header().Set("OData-Version", "4.0")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func guid() string {
	r := make([]byte, 16)
	_, _ = rand.Read(r)
	h := hex.EncodeToString(r)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

var byID = regexp.MustCompile(`^(salesOrders|customers)\(([0-9a-f-]{36})\)$`)

func (b *BC) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == TokenPath {
		b.token(w, r)
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	auth := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if exp, ok := b.tokens[auth]; !ok || b.now().After(exp) {
		w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
		fail(w, http.StatusUnauthorized, "Authentication_InvalidCredentials", "The credentials provided are incorrect")
		return
	}
	if !strings.HasPrefix(r.URL.Path, API+"/") {
		fail(w, http.StatusNotFound, "BadRequest_NotFound", "The request URL is not valid.")
		return
	}
	res := strings.TrimPrefix(r.URL.Path, API+"/")
	b.Requests[r.Method+" "+res]++
	switch {
	case res == "salesOrders" && r.Method == http.MethodGet:
		b.list(w, r)
	case res == "salesOrders" && r.Method == http.MethodPost:
		b.create(w, r)
	case res == "customers" && r.Method == http.MethodGet:
		reply(w, http.StatusOK, map[string]any{"value": b.sorted(b.customers, "number")})
	case byID.MatchString(res):
		m := byID.FindStringSubmatch(res)
		table := b.orders
		if m[1] == "customers" {
			table = b.customers
		}
		rec := table[m[2]]
		if rec == nil {
			fail(w, http.StatusNotFound, "Internal_RecordNotFound",
				fmt.Sprintf("The %s does not exist. Identification fields and values: Id='%s'", strings.TrimSuffix(m[1], "s"), m[2]))
			return
		}
		switch r.Method {
		case http.MethodGet:
			reply(w, http.StatusOK, header(rec))
		case http.MethodDelete:
			if m[1] != "salesOrders" {
				fail(w, http.StatusMethodNotAllowed, "BadRequest_MethodNotAllowed", "'DELETE' requests for 'customers' of EdmType 'Entity' are not allowed within Dynamics 365 Business Central OData web services.")
				return
			}
			if r.Header.Get("If-Match") == "" {
				fail(w, http.StatusBadRequest, "Request_IfMatchMissing", "Could not process the request: the If-Match header is missing.")
				return
			}
			delete(b.orders, m[2])
			w.WriteHeader(http.StatusNoContent)
		default:
			fail(w, http.StatusMethodNotAllowed, "BadRequest_MethodNotAllowed", "The method is not allowed.")
		}
	default:
		fail(w, http.StatusNotFound, "BadRequest_NotFound", "The request URL is not valid: "+res)
	}
}

func (b *BC) sorted(table map[string]map[string]any, by string) []any {
	list := make([]map[string]any, 0, len(table))
	for _, rec := range table {
		list = append(list, rec)
	}
	sort.Slice(list, func(i, j int) bool { return fmt.Sprint(list[i][by]) < fmt.Sprint(list[j][by]) })
	out := make([]any, len(list))
	for i, rec := range list {
		out[i] = header(rec)
	}
	return out
}

// header is a record without its lines, as Business Central answers
// without $expand.
func header(rec map[string]any) map[string]any {
	out := map[string]any{"@odata.etag": fmt.Sprintf(`W/"JzQ0O%s"`, rec["id"])}
	for k, v := range rec {
		if k != "salesOrderLines" {
			out[k] = v
		}
	}
	return out
}

func (b *BC) token(w http.ResponseWriter, r *http.Request) {
	id, secret, ok := r.BasicAuth()
	if err := r.ParseForm(); err != nil || r.PostForm.Get("grant_type") != "client_credentials" {
		fail(w, http.StatusBadRequest, "unsupported_grant_type", "grant_type must be client_credentials")
		return
	}
	if !ok {
		id, secret = r.PostForm.Get("client_id"), r.PostForm.Get("client_secret")
	}
	if id != b.ClientID || secret != b.ClientSecret {
		fail(w, http.StatusUnauthorized, "invalid_client", "AADSTS7000215: Invalid client secret provided.")
		return
	}
	if r.PostForm.Get("scope") != Scope {
		fail(w, http.StatusBadRequest, "invalid_scope", "AADSTS1002012: The provided value for scope is not valid; Business Central takes "+Scope)
		return
	}
	b.mu.Lock()
	tok := guid()
	b.tokens[tok] = b.now().Add(time.Hour)
	b.mu.Unlock()
	reply(w, http.StatusOK, map[string]any{"token_type": "Bearer", "expires_in": 3599, "access_token": tok})
}

func num(v any) (float64, bool) {
	switch x := v.(type) {
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case float64:
		return x, true
	}
	return 0, false
}

// create makes a sales order and its lines (deep insert).
func (b *BC) create(w http.ResponseWriter, r *http.Request) {
	if b.FailCreates {
		fail(w, http.StatusBadRequest, "Application_DialogException", "The sales order could not be saved: an extension rejected it.")
		return
	}
	var in map[string]any
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(&in); err != nil {
		fail(w, http.StatusBadRequest, "BadRequest", "Invalid Request Body")
		return
	}
	known := map[string]bool{"customerNumber": true, "customerId": true, "externalDocumentNumber": true, "orderDate": true,
		"currencyCode": true, "salesOrderLines": true}
	for k := range in {
		if !known[k] {
			fail(w, http.StatusBadRequest, "BadRequest_PropertyNotFound", fmt.Sprintf("The property '%s' does not exist on type 'Microsoft.NAV.salesOrder'.", k))
			return
		}
	}
	var cust map[string]any
	number, _ := in["customerNumber"].(string)
	id, _ := in["customerId"].(string)
	for _, c := range b.customers {
		if (number != "" && c["number"] == number) || (id != "" && c["id"] == id) {
			cust = c
		}
	}
	if cust == nil {
		fail(w, http.StatusBadRequest, "Internal_RecordNotFound",
			fmt.Sprintf("The Customer does not exist. Identification fields and values: No.='%s'", number+id))
		return
	}
	if cust["blocked"] != " " {
		fail(w, http.StatusBadRequest, "Application_DialogException",
			fmt.Sprintf("You cannot create this type of document when Customer %s is blocked with type %s", cust["number"], cust["blocked"]))
		return
	}
	ext, _ := in["externalDocumentNumber"].(string)
	if len(ext) > 35 {
		fail(w, http.StatusBadRequest, "BadRequest_InvalidValue", "The length of the string is 36, but it must be less than or equal to 35 characters. Value: "+ext)
		return
	}
	date := b.now().UTC().Format("2006-01-02")
	if d, ok := in["orderDate"].(string); ok && d != "" {
		if _, err := time.Parse("2006-01-02", d); err != nil {
			fail(w, http.StatusBadRequest, "BadRequest_InvalidValue", "Cannot convert '"+d+"' to Edm.Date.")
			return
		}
		date = d
	}
	raw, _ := in["salesOrderLines"].([]any)
	var lines []any
	total := 0.0
	for i, x := range raw {
		l, _ := x.(map[string]any)
		if t, _ := l["lineType"].(string); t != "Item" {
			fail(w, http.StatusBadRequest, "BadRequest_InvalidValue", fmt.Sprintf("salesOrderLines[%d]: lineType must be Item", i))
			return
		}
		no, _ := l["lineObjectNumber"].(string)
		item := b.items[no]
		if item == nil {
			fail(w, http.StatusBadRequest, "Internal_RecordNotFound", fmt.Sprintf("The Item does not exist. Identification fields and values: No.='%s'", no))
			return
		}
		qty, ok := num(l["quantity"])
		if !ok || qty <= 0 {
			fail(w, http.StatusBadRequest, "Application_DialogException", fmt.Sprintf("Quantity must be positive in Sales Line Document Type=Order, Line No.=%d", (i+1)*10000))
			return
		}
		price := item["unitPrice"].(float64)
		if p, ok := num(l["unitPrice"]); ok {
			price = p
		} else if l["unitPrice"] != nil {
			fail(w, http.StatusBadRequest, "BadRequest_InvalidValue", fmt.Sprintf("salesOrderLines[%d]: Cannot convert unitPrice to Edm.Decimal.", i))
			return
		}
		amount := math.Round(qty*price*100) / 100
		total += amount
		lines = append(lines, map[string]any{"id": guid(), "sequence": (i + 1) * 10000, "lineType": "Item", "lineObjectNumber": no,
			"description": item["displayName"], "quantity": qty, "unitPrice": price, "amountExcludingTax": amount})
	}
	if len(lines) == 0 {
		fail(w, http.StatusBadRequest, "BadRequest", "A sales order sent to Turgon's fake needs at least one line")
		return
	}
	oid := guid()
	o := map[string]any{
		"id": oid, "number": strconv.Itoa(b.next), "externalDocumentNumber": ext, "orderDate": date,
		"customerId": cust["id"], "customerNumber": cust["number"], "customerName": cust["displayName"],
		"currencyCode": in["currencyCode"], "status": "Draft",
		"totalAmountExcludingTax": math.Round(total*100) / 100, "totalAmountIncludingTax": math.Round(total*100) / 100,
		"lastModifiedDateTime": b.now().UTC().Format("2006-01-02T15:04:05.000Z"), "salesOrderLines": lines,
	}
	if o["currencyCode"] == nil {
		o["currencyCode"] = ""
	}
	b.next++
	b.orders[oid] = o
	w.Header().Set("Location", fmt.Sprintf("http://%s%s/salesOrders(%s)", r.Host, API, oid))
	reply(w, http.StatusCreated, header(o))
}

var (
	extRE = regexp.MustCompile(`^externalDocumentNumber eq '((?:[^']|'')*)'$`)
	gtRE  = regexp.MustCompile(`^lastModifiedDateTime gt (\S+)$`)
)

// list answers $filter=externalDocumentNumber eq '<x>' or
// lastModifiedDateTime gt <time>, ordered by lastModifiedDateTime, with $top.
func (b *BC) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	keep := func(map[string]any) bool { return true }
	if f := q.Get("$filter"); f != "" {
		switch {
		case extRE.MatchString(f):
			ext := strings.ReplaceAll(extRE.FindStringSubmatch(f)[1], "''", "'")
			keep = func(o map[string]any) bool { return o["externalDocumentNumber"] == ext }
		case gtRE.MatchString(f):
			after, err := time.Parse(time.RFC3339, gtRE.FindStringSubmatch(f)[1])
			if err != nil {
				fail(w, http.StatusBadRequest, "BadRequest_InvalidToken", "Invalid DateTimeOffset: "+gtRE.FindStringSubmatch(f)[1])
				return
			}
			keep = func(o map[string]any) bool {
				t, _ := time.Parse(time.RFC3339, o["lastModifiedDateTime"].(string))
				return t.After(after)
			}
		default:
			fail(w, http.StatusBadRequest, "BadRequest_InvalidToken", "The fake supports no filter "+f)
			return
		}
	}
	var list []map[string]any
	for _, o := range b.orders {
		if keep(o) {
			list = append(list, o)
		}
	}
	sort.Slice(list, func(i, j int) bool {
		a, c := list[i]["lastModifiedDateTime"].(string), list[j]["lastModifiedDateTime"].(string)
		if a != c {
			return a < c
		}
		return list[i]["number"].(string) < list[j]["number"].(string)
	})
	if n, err := strconv.Atoi(q.Get("$top")); err == nil && n < len(list) {
		list = list[:n]
	}
	out := make([]any, len(list))
	for i, o := range list {
		out[i] = header(o)
	}
	reply(w, http.StatusOK, map[string]any{"@odata.context": "$metadata#companies(" + Company + ")/salesOrders", "value": out})
}
