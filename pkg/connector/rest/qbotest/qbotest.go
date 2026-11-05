// Package qbotest is a fake QuickBooks Online Accounting API (v3) and
// Intuit's OAuth 2.0 token endpoint, for tests and demos: one company with
// customers, items and sales receipts.
//
// Like Intuit it rotates the refresh token: each refresh answers a new one
// and the old stops working (invalid_grant), so a client that loses the new
// token cannot sign in again. Access tokens last an hour. A create carrying
// a requestid already used answers what the first create answered, which
// is how QuickBooks makes a retried request idempotent. A void sets a
// receipt's amounts to zero and needs its current SyncToken. Queries take
// "select * from SalesReceipt where MetaData.LastUpdatedTime > '<time>'
// orderby MetaData.LastUpdatedTime maxresults <n>". Errors are
// {"Fault": {"Error": [{"Message", "Detail", "code"}], "type"}}.
package qbotest

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Realm is the fake company's ID; TokenPath is Intuit's token endpoint path.
const (
	Realm     = "9130357766543210"
	TokenPath = "/oauth2/v1/tokens/bearer"
	API       = "/v3/company/" + Realm
)

// Customers and items the company starts with.
const (
	Amy    = "1" // Amy's Bird Sanctuary, ada@example.com
	Bill   = "2" // Bill's Windsurf Shop
	Garden = "11"
	Pump   = "12"
)

// QBO is the fake company.
type QBO struct {
	ClientID, ClientSecret string

	mu        sync.Mutex
	refresh   map[string]bool      // refresh tokens that still work
	access    map[string]time.Time // access tokens and their expiry
	customers map[string]map[string]any
	items     map[string]map[string]any
	receipts  map[string]map[string]any
	requests  map[string]map[string]any // requestid -> the first answer
	next      int
	now       func() time.Time
	srv       *httptest.Server
	// Refreshes counts refresh grants that succeeded; Spent counts refused
	// ones (a rotated-away token used again).
	Refreshes, Spent int
	// Requests counts API calls by "METHOD resource".
	Requests map[string]int
}

// New returns a company served by an httptest server, and the refresh token
// a person's authorization would have given.
func New(clientID, clientSecret string) (*QBO, string) {
	q, tok := NewQBO(clientID, clientSecret)
	q.srv = httptest.NewServer(q)
	return q, tok
}

// NewQBO returns a company to serve yourself, and its first refresh token.
func NewQBO(clientID, clientSecret string) (*QBO, string) {
	q := &QBO{ClientID: clientID, ClientSecret: clientSecret, refresh: map[string]bool{}, access: map[string]time.Time{},
		customers: map[string]map[string]any{}, items: map[string]map[string]any{}, receipts: map[string]map[string]any{},
		requests: map[string]map[string]any{}, next: 1001, now: time.Now, Requests: map[string]int{}}
	q.customers[Amy] = map[string]any{"Id": Amy, "DisplayName": "Amy's Bird Sanctuary", "PrimaryEmailAddr": map[string]any{"Address": "ada@example.com"}, "Active": true, "SyncToken": "0"}
	q.customers[Bill] = map[string]any{"Id": Bill, "DisplayName": "Bill's Windsurf Shop", "PrimaryEmailAddr": map[string]any{"Address": "grace@example.com"}, "Active": true, "SyncToken": "0"}
	q.items[Garden] = map[string]any{"Id": Garden, "Name": "Garden Supplies", "Sku": "M-1", "UnitPrice": 100.0}
	q.items[Pump] = map[string]any{"Id": Pump, "Name": "Pump", "Sku": "M-2", "UnitPrice": 235.0}
	tok := q.issueRefresh()
	return q, tok
}

func (q *QBO) issueRefresh() string {
	t := "AB1" + token()
	q.refresh[t] = true
	return t
}

// Authorize returns a new refresh token, as a person authorizing the app
// again would get. Tokens already issued keep working until spent.
func (q *QBO) Authorize() string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.issueRefresh()
}

// StartAt sets the next document number and the clock.
func (q *QBO) StartAt(number int, now func() time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.next, q.now = number, now
}

// ExpireAccessTokens ends every access token, as an hour passing would.
func (q *QBO) ExpireAccessTokens() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.access = map[string]time.Time{}
}

func (q *QBO) URL() string { return q.srv.URL }
func (q *QBO) Close()      { q.srv.Close() }

// Receipt returns a copy of a sales receipt, or nil.
func (q *QBO) Receipt(id string) map[string]any {
	q.mu.Lock()
	defer q.mu.Unlock()
	return clone(q.receipts[id])
}

// ReceiptByDocNumber returns the receipt with a document number, or nil.
func (q *QBO) ReceiptByDocNumber(doc string) map[string]any {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, r := range q.receipts {
		if r["DocNumber"] == doc {
			return clone(r)
		}
	}
	return nil
}

// Receipts counts sales receipts.
func (q *QBO) Receipts() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.receipts)
}

// ReceiptIDs lists the receipts' IDs.
func (q *QBO) ReceiptIDs() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	var ids []string
	for id := range q.receipts {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { a, _ := strconv.Atoi(ids[i]); b, _ := strconv.Atoi(ids[j]); return a < b })
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

func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json;charset=UTF-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (q *QBO) fault(w http.ResponseWriter, status int, kind, code, msg, detail string) {
	reply(w, status, map[string]any{"Fault": map[string]any{"Error": []any{map[string]any{"Message": msg, "Detail": detail, "code": code}}, "type": kind},
		"time": q.now().Format("2006-01-02T15:04:05.000-07:00")})
}

func (q *QBO) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == TokenPath {
		q.token(w, r)
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	auth := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if exp, ok := q.access[auth]; !ok || q.now().After(exp) {
		reply(w, http.StatusUnauthorized, map[string]any{"fault": map[string]any{"error": []any{map[string]any{
			"message": "message=AuthenticationFailed; errorCode=003200; statusCode=401", "detail": "Token expired", "code": "3200"}}, "type": "AUTHENTICATION"}})
		return
	}
	if !strings.HasPrefix(r.URL.Path, API+"/") {
		q.fault(w, http.StatusForbidden, "AUTHORIZATION", "3100", "ApplicationAuthorizationFailed", "The token is not authorized for this company")
		return
	}
	res := strings.TrimPrefix(r.URL.Path, API+"/")
	q.Requests[r.Method+" "+res]++
	switch {
	case res == "companyinfo/"+Realm && r.Method == http.MethodGet:
		reply(w, http.StatusOK, map[string]any{"CompanyInfo": map[string]any{"Id": "1", "CompanyName": "Sandbox Company_US_1", "Country": "US"}})
	case strings.HasPrefix(res, "customer/") && r.Method == http.MethodGet:
		c := q.customers[strings.TrimPrefix(res, "customer/")]
		if c == nil {
			q.fault(w, http.StatusBadRequest, "ValidationFault", "610", "Object Not Found", "Object Not Found : Something you're trying to use has been made inactive. Check the fields with accounts, customers, items, vendors or employees.")
			return
		}
		reply(w, http.StatusOK, map[string]any{"Customer": c})
	case res == "query" && r.Method == http.MethodGet:
		q.query(w, r.URL.Query().Get("query"))
	case res == "salesreceipt" && r.Method == http.MethodPost:
		switch r.URL.Query().Get("operation") {
		case "":
			q.create(w, r)
		case "void":
			q.void(w, r)
		default:
			q.fault(w, http.StatusBadRequest, "ValidationFault", "2010", "Request has invalid or unsupported property", "operation "+r.URL.Query().Get("operation"))
		}
	default:
		q.fault(w, http.StatusBadRequest, "ValidationFault", "4000", "Unsupported Operation", "Operation "+r.Method+" "+res+" is not supported.")
	}
}

func (q *QBO) token(w http.ResponseWriter, r *http.Request) {
	id, secret, ok := r.BasicAuth()
	if err := r.ParseForm(); err != nil || !ok || id != q.ClientID || secret != q.ClientSecret {
		reply(w, http.StatusUnauthorized, map[string]any{"error": "invalid_client"})
		return
	}
	if r.PostForm.Get("grant_type") != "refresh_token" {
		reply(w, http.StatusBadRequest, map[string]any{"error": "unsupported_grant_type"})
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	old := r.PostForm.Get("refresh_token")
	if !q.refresh[old] {
		q.Spent++
		reply(w, http.StatusBadRequest, map[string]any{"error": "invalid_grant"})
		return
	}
	delete(q.refresh, old) // rotated: the old token stops working
	next := q.issueRefresh()
	access := "eyJ" + token()
	q.access[access] = q.now().Add(time.Hour)
	q.Refreshes++
	reply(w, http.StatusOK, map[string]any{"token_type": "bearer", "access_token": access, "expires_in": 3600,
		"refresh_token": next, "x_refresh_token_expires_in": 8726400})
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

func ref(v any) string {
	m, _ := v.(map[string]any)
	s, _ := m["value"].(string)
	return s
}

func (q *QBO) stamp() string { return q.now().UTC().Format("2006-01-02T15:04:05-07:00") }

func (q *QBO) create(w http.ResponseWriter, r *http.Request) {
	rid := r.URL.Query().Get("requestid")
	if prior, ok := q.requests[rid]; ok && rid != "" {
		reply(w, http.StatusOK, prior) // a retried request: the first answer
		return
	}
	var in map[string]any
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(&in); err != nil {
		q.fault(w, http.StatusBadRequest, "ValidationFault", "2500", "Invalid Reference Id", "Request body is not valid JSON")
		return
	}
	cust := q.customers[ref(in["CustomerRef"])]
	if cust == nil {
		q.fault(w, http.StatusBadRequest, "ValidationFault", "2500", "Invalid Reference Id", "Invalid Reference Id : Customer assigned to this transaction has been deleted. Before you can modify this transaction, you must restore "+ref(in["CustomerRef"])+" (deleted).")
		return
	}
	lines, _ := in["Line"].([]any)
	if len(lines) == 0 {
		q.fault(w, http.StatusBadRequest, "ValidationFault", "2020", "Required param missing, need to supply the required value for the API", "Required parameter Line is missing in the request")
		return
	}
	var out []any
	total := 0.0
	for i, x := range lines {
		l, _ := x.(map[string]any)
		d, _ := l["SalesItemLineDetail"].(map[string]any)
		if l["DetailType"] != "SalesItemLineDetail" || d == nil {
			q.fault(w, http.StatusBadRequest, "ValidationFault", "2020", "Required param missing, need to supply the required value for the API", fmt.Sprintf("Required parameter Line.DetailType is missing in the request (line %d)", i+1))
			return
		}
		item := q.items[ref(d["ItemRef"])]
		if item == nil {
			q.fault(w, http.StatusBadRequest, "ValidationFault", "2500", "Invalid Reference Id", "Invalid Reference Id : Item assigned to this transaction has been deleted: "+ref(d["ItemRef"]))
			return
		}
		amount, ok := num(l["Amount"])
		if !ok {
			q.fault(w, http.StatusBadRequest, "ValidationFault", "2020", "Required param missing, need to supply the required value for the API", "Required parameter Line.Amount is missing in the request")
			return
		}
		qty, hasQty := num(d["Qty"])
		price, hasPrice := num(d["UnitPrice"])
		if hasQty && hasPrice && math.Abs(qty*price-amount) > 0.005 {
			q.fault(w, http.StatusBadRequest, "ValidationFault", "6070", "Amount is not equal to UnitPrice * Qty", fmt.Sprintf("Amount is not equal to UnitPrice * Qty. Supplied value:%v", amount))
			return
		}
		total += amount
		out = append(out, map[string]any{"Id": strconv.Itoa(i + 1), "LineNum": i + 1, "Amount": amount, "DetailType": "SalesItemLineDetail",
			"SalesItemLineDetail": map[string]any{"ItemRef": map[string]any{"value": item["Id"], "name": item["Name"]}, "Qty": qty, "UnitPrice": price}})
	}
	id := strconv.Itoa(q.next)
	doc, _ := in["DocNumber"].(string)
	if doc == "" {
		doc = id
	}
	if len(doc) > 21 {
		q.fault(w, http.StatusBadRequest, "ValidationFault", "6000", "A business validation error has occurred while processing your request", "String length is either shorter or longer than supported by specification. Min:0 Max:21 supplied:"+strconv.Itoa(len(doc)))
		return
	}
	q.next++
	rec := map[string]any{"Id": id, "SyncToken": "0", "DocNumber": doc, "TxnDate": in["TxnDate"], "PrivateNote": in["PrivateNote"],
		"CustomerRef": map[string]any{"value": cust["Id"], "name": cust["DisplayName"]}, "Line": out, "TotalAmt": math.Round(total*100) / 100,
		"MetaData": map[string]any{"CreateTime": q.stamp(), "LastUpdatedTime": q.stamp()}}
	if rec["TxnDate"] == nil {
		rec["TxnDate"] = q.now().Format("2006-01-02")
	}
	q.receipts[id] = rec
	answer := map[string]any{"SalesReceipt": clone(rec), "time": q.now().Format("2006-01-02T15:04:05.000-07:00")}
	if rid != "" {
		q.requests[rid] = answer
	}
	reply(w, http.StatusOK, answer)
}

func (q *QBO) void(w http.ResponseWriter, r *http.Request) {
	var in struct{ Id, SyncToken string }
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		q.fault(w, http.StatusBadRequest, "ValidationFault", "2500", "Invalid Reference Id", "Request body is not valid JSON")
		return
	}
	rec := q.receipts[in.Id]
	if rec == nil {
		q.fault(w, http.StatusBadRequest, "ValidationFault", "610", "Object Not Found", "Object Not Found : Something you're trying to use has been made inactive.")
		return
	}
	if rec["SyncToken"] != in.SyncToken {
		q.fault(w, http.StatusBadRequest, "ValidationFault", "5010", "Stale Object Error", "Stale Object Error : You and someone else are both editing the same transaction. Refresh and try again.")
		return
	}
	n, _ := strconv.Atoi(in.SyncToken)
	rec["SyncToken"], rec["TotalAmt"], rec["PrivateNote"] = strconv.Itoa(n+1), 0.0, "Voided"
	rec["MetaData"].(map[string]any)["LastUpdatedTime"] = q.stamp()
	for _, l := range rec["Line"].([]any) {
		l.(map[string]any)["Amount"] = 0.0
	}
	reply(w, http.StatusOK, map[string]any{"SalesReceipt": clone(rec), "time": q.now().Format("2006-01-02T15:04:05.000-07:00")})
}

var queryRE = regexp.MustCompile(`(?i)^select \* from SalesReceipt where MetaData\.LastUpdatedTime > '([^']+)' orderby MetaData\.LastUpdatedTime(?: maxresults (\d+))?$`)

func (q *QBO) query(w http.ResponseWriter, query string) {
	m := queryRE.FindStringSubmatch(strings.TrimSpace(query))
	if m == nil {
		q.fault(w, http.StatusBadRequest, "ValidationFault", "4000", "Error parsing query", "QueryParserError: the fake supports no query "+url.QueryEscape(query))
		return
	}
	after, err := time.Parse(time.RFC3339, m[1])
	if err != nil {
		q.fault(w, http.StatusBadRequest, "ValidationFault", "4000", "Error parsing query", "QueryValidationError: invalid date "+m[1])
		return
	}
	var list []map[string]any
	for _, rec := range q.receipts {
		t, _ := time.Parse(time.RFC3339, rec["MetaData"].(map[string]any)["LastUpdatedTime"].(string))
		if t.After(after) {
			list = append(list, rec)
		}
	}
	sort.Slice(list, func(i, j int) bool {
		a := list[i]["MetaData"].(map[string]any)["LastUpdatedTime"].(string)
		b := list[j]["MetaData"].(map[string]any)["LastUpdatedTime"].(string)
		if a != b {
			return a < b
		}
		x, _ := strconv.Atoi(list[i]["Id"].(string))
		y, _ := strconv.Atoi(list[j]["Id"].(string))
		return x < y
	})
	if n, err := strconv.Atoi(m[2]); err == nil && n < len(list) {
		list = list[:n]
	}
	out := make([]any, len(list))
	for i, rec := range list {
		out[i] = rec
	}
	resp := map[string]any{"startPosition": 1, "maxResults": len(out)}
	if len(out) > 0 {
		resp["SalesReceipt"] = out
	}
	reply(w, http.StatusOK, map[string]any{"QueryResponse": resp, "time": q.now().Format("2006-01-02T15:04:05.000-07:00")})
}
