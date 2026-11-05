// Package b1test is a fake SAP Business One Service Layer (OData v4, /b1s/v2)
// for tests and demos: business partners, items and sales orders. Like the
// Service Layer it takes a session login (POST /Login with CompanyDB,
// UserName and Password) and authenticates later requests by the B1SESSION
// and ROUTEID cookies it sets, ends a session after SessionTimeout minutes
// without use, creates an order with its DocumentLines in one POST (201 and
// the order), prices a line without a UnitPrice from the item's price list,
// cancels an order with POST /Orders(<DocEntry>)/Cancel, filters orders by
// NumAtCard or DocEntry, and reports errors as {"error": {"code", "message"}}.
//
// Like the Service Layer it has no upsert by an external key: a retried
// create finds its order by NumAtCard (the rest connector's existing lookup).
package b1test

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

// Base is the Service Layer's path.
const Base = "/b1s/v2"

// Business partners and items the company starts with (the demo database's).
const (
	Maxi    = "C20000" // Maxi-Teq
	Norm    = "C23900" // Norm Thompson
	Blocked = "C99999" // frozen
)

// B1 is the fake company database.
type B1 struct {
	CompanyDB, UserName, Password string

	mu       sync.Mutex
	sessions map[string]time.Time // B1SESSION -> last use
	partners map[string]map[string]any
	items    map[string]map[string]any
	orders   map[int]map[string]any
	next     int
	now      func() time.Time
	srv      *httptest.Server
	// Timeout is a session's idle timeout (default 30 minutes).
	Timeout time.Duration
	// FailCreates makes order creates fail, as a transaction notification would.
	FailCreates bool
	// Requests counts requests by "METHOD resource"; Logins counts logins.
	Requests map[string]int
	Logins   int
}

// New returns a company served by an httptest server.
func New(companyDB, user, password string) *B1 {
	b := NewB1(companyDB, user, password)
	b.srv = httptest.NewServer(b)
	return b
}

// NewB1 returns a company to serve yourself.
func NewB1(companyDB, user, password string) *B1 {
	b := &B1{CompanyDB: companyDB, UserName: user, Password: password, sessions: map[string]time.Time{},
		partners: map[string]map[string]any{}, items: map[string]map[string]any{}, orders: map[int]map[string]any{},
		next: 1, now: time.Now, Timeout: 30 * time.Minute, Requests: map[string]int{}}
	b.partners[Maxi] = map[string]any{"CardCode": Maxi, "CardName": "Maxi-Teq", "CardType": "cCustomer", "EmailAddress": "ada@example.com", "Frozen": "tNO", "Currency": "EUR"}
	b.partners[Norm] = map[string]any{"CardCode": Norm, "CardName": "Norm Thompson", "CardType": "cCustomer", "EmailAddress": "grace@example.com", "Frozen": "tNO", "Currency": "EUR"}
	b.partners[Blocked] = map[string]any{"CardCode": Blocked, "CardName": "Frozen Ltd", "CardType": "cCustomer", "EmailAddress": "", "Frozen": "tYES", "Currency": "EUR"}
	b.items["A00001"] = map[string]any{"ItemCode": "A00001", "ItemName": "J.B. Officeprint 1420", "Price": 400.0}
	b.items["A00002"] = map[string]any{"ItemCode": "A00002", "ItemName": "J.B. Officeprint 1111", "Price": 182.0}
	return b
}

// StartAt sets the next DocEntry and DocNum and the clock.
func (b *B1) StartAt(entry int, now func() time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.next, b.now = entry, now
}

// ExpireSessions ends every session, as the server does at its timeout.
func (b *B1) ExpireSessions() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sessions = map[string]time.Time{}
}

func (b *B1) URL() string { return b.srv.URL }
func (b *B1) Close()      { b.srv.Close() }

// Order returns a copy of an order, or nil.
func (b *B1) Order(entry int) map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	return clone(b.orders[entry])
}

// OrderByRef returns the order with a customer reference (NumAtCard), or nil.
func (b *B1) OrderByRef(ref string) map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, o := range b.orders {
		if o["NumAtCard"] == ref {
			return clone(o)
		}
	}
	return nil
}

// Orders counts orders.
func (b *B1) Orders() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.orders)
}

// OrderEntries lists the orders' DocEntry.
func (b *B1) OrderEntries() []int {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []int
	for e := range b.orders {
		out = append(out, e)
	}
	sort.Ints(out)
	return out
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
	w.Header().Set("Content-Type", "application/json;odata.metadata=minimal;charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": code, "message": msg}})
}

func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json;odata.metadata=minimal;charset=utf-8")
	w.Header().Set("OData-Version", "4.0")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func token() string {
	r := make([]byte, 16)
	_, _ = rand.Read(r)
	h := hex.EncodeToString(r)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

var (
	orderRE  = regexp.MustCompile(`^Orders\((\d+)\)$`)
	cancelRE = regexp.MustCompile(`^Orders\((\d+)\)/Cancel$`)
	bpRE     = regexp.MustCompile(`^BusinessPartners\('((?:[^']|'')+)'\)$`)
)

func (b *B1) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, Base+"/") {
		fail(w, http.StatusNotFound, "-1", "Invalid request URL")
		return
	}
	res := strings.TrimPrefix(r.URL.Path, Base+"/")
	if res == "Login" && r.Method == http.MethodPost {
		b.login(w, r)
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	sess, _ := r.Cookie("B1SESSION")
	route, _ := r.Cookie("ROUTEID")
	if sess == nil || route == nil {
		fail(w, http.StatusUnauthorized, "301", "Invalid session or session already timeout.")
		return
	}
	last, ok := b.sessions[sess.Value]
	if !ok || b.now().Sub(last) > b.Timeout {
		delete(b.sessions, sess.Value)
		fail(w, http.StatusUnauthorized, "301", "Invalid session or session already timeout.")
		return
	}
	b.sessions[sess.Value] = b.now()
	b.Requests[r.Method+" "+res]++
	switch {
	case res == "Logout" && r.Method == http.MethodPost:
		delete(b.sessions, sess.Value)
		w.WriteHeader(http.StatusNoContent)
	case res == "Orders" && r.Method == http.MethodGet:
		b.list(w, r)
	case res == "Orders" && r.Method == http.MethodPost:
		b.create(w, r)
	case orderRE.MatchString(res) && r.Method == http.MethodGet:
		n, _ := strconv.Atoi(orderRE.FindStringSubmatch(res)[1])
		if o := b.orders[n]; o != nil {
			reply(w, http.StatusOK, o)
			return
		}
		fail(w, http.StatusNotFound, "-2028", "No matching records found (ODBC -2028)")
	case cancelRE.MatchString(res) && r.Method == http.MethodPost:
		n, _ := strconv.Atoi(cancelRE.FindStringSubmatch(res)[1])
		o := b.orders[n]
		switch {
		case o == nil:
			fail(w, http.StatusNotFound, "-2028", "No matching records found (ODBC -2028)")
		case r.ContentLength > 0:
			fail(w, http.StatusBadRequest, "-1000", "The Cancel action takes no body")
		case o["Cancelled"] == "tYES":
			fail(w, http.StatusBadRequest, "-5006", "Document is already closed")
		default:
			o["Cancelled"], o["DocumentStatus"], o["UpdateDate"] = "tYES", "bost_Close", b.now().UTC().Format("2006-01-02")
			w.WriteHeader(http.StatusNoContent)
		}
	case bpRE.MatchString(res) && r.Method == http.MethodGet:
		code := strings.ReplaceAll(bpRE.FindStringSubmatch(res)[1], "''", "'")
		if p := b.partners[code]; p != nil {
			reply(w, http.StatusOK, p)
			return
		}
		fail(w, http.StatusNotFound, "-2028", "No matching records found (ODBC -2028)")
	default:
		fail(w, http.StatusNotFound, "-1", "Invalid request URL: "+res)
	}
}

func (b *B1) login(w http.ResponseWriter, r *http.Request) {
	var in struct{ CompanyDB, UserName, Password string }
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		fail(w, http.StatusBadRequest, "-1000", "Invalid login request")
		return
	}
	if in.CompanyDB != b.CompanyDB {
		fail(w, http.StatusUnauthorized, "-304", "Fail to get DB Credentials from SLD")
		return
	}
	if in.UserName != b.UserName || in.Password != b.Password {
		fail(w, http.StatusUnauthorized, "100000027", "Login failed")
		return
	}
	b.mu.Lock()
	id := token()
	b.sessions[id] = b.now()
	b.Logins++
	minutes := int(b.Timeout / time.Minute)
	b.mu.Unlock()
	http.SetCookie(w, &http.Cookie{Name: "B1SESSION", Value: id, Path: Base, HttpOnly: true})
	http.SetCookie(w, &http.Cookie{Name: "ROUTEID", Value: ".node1", Path: "/"})
	reply(w, http.StatusOK, map[string]any{"odata.metadata": "$metadata#B1Sessions/@Element", "SessionId": id, "Version": "1000220", "SessionTimeout": minutes})
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

func (b *B1) create(w http.ResponseWriter, r *http.Request) {
	if b.FailCreates {
		fail(w, http.StatusBadRequest, "-1116", "(SBO_SP_TransactionNotification) The order was rejected by a validation rule")
		return
	}
	var in map[string]any
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(&in); err != nil {
		fail(w, http.StatusBadRequest, "-1000", "Invalid request body")
		return
	}
	known := map[string]bool{"CardCode": true, "DocDate": true, "DocDueDate": true, "NumAtCard": true, "Comments": true, "DocumentLines": true}
	for k := range in {
		if !known[k] {
			fail(w, http.StatusBadRequest, "-1000", fmt.Sprintf("Property '%s' of 'Document' is invalid", k))
			return
		}
	}
	code, _ := in["CardCode"].(string)
	p := b.partners[code]
	if p == nil {
		fail(w, http.StatusBadRequest, "-10", fmt.Sprintf("Invalid BP code '%s'", code))
		return
	}
	if p["Frozen"] == "tYES" {
		fail(w, http.StatusBadRequest, "-10", fmt.Sprintf("BP %s is inactive", code))
		return
	}
	due, _ := in["DocDueDate"].(string)
	if due == "" {
		fail(w, http.StatusBadRequest, "-5002", "Enter a delivery date [ORDR.DocDueDate]")
		return
	}
	date, _ := in["DocDate"].(string)
	if date == "" {
		date = b.now().UTC().Format("2006-01-02")
	}
	for _, d := range []string{date, due} {
		if _, err := time.Parse("2006-01-02", d); err != nil {
			fail(w, http.StatusBadRequest, "-1000", "Invalid date value '"+d+"'")
			return
		}
	}
	ref, _ := in["NumAtCard"].(string)
	if len(ref) > 100 {
		fail(w, http.StatusBadRequest, "-5002", "Value too long in property 'NumAtCard' of 'Document'")
		return
	}
	raw, _ := in["DocumentLines"].([]any)
	if len(raw) == 0 {
		fail(w, http.StatusBadRequest, "-5002", "To generate this document, first define the numbering series in the Administration module (no lines)")
		return
	}
	var lines []any
	total := 0.0
	for i, x := range raw {
		l, _ := x.(map[string]any)
		ic, _ := l["ItemCode"].(string)
		item := b.items[ic]
		if item == nil {
			fail(w, http.StatusBadRequest, "-5002", fmt.Sprintf("Invalid item code '%s' [RDR1.ItemCode][line: %d]", ic, i+1))
			return
		}
		qty, ok := num(l["Quantity"])
		if !ok || qty <= 0 {
			fail(w, http.StatusBadRequest, "-5002", fmt.Sprintf("Enter a quantity [RDR1.Quantity][line: %d]", i+1))
			return
		}
		price := item["Price"].(float64)
		if v, ok := num(l["UnitPrice"]); ok {
			price = v
		}
		lt := math.Round(qty*price*100) / 100
		total += lt
		lines = append(lines, map[string]any{"LineNum": i, "ItemCode": ic, "ItemDescription": item["ItemName"], "Quantity": qty, "UnitPrice": price, "LineTotal": lt})
	}
	entry := b.next
	b.next++
	o := map[string]any{
		"DocEntry": entry, "DocNum": entry, "CardCode": code, "CardName": p["CardName"], "DocDate": date, "DocDueDate": due,
		"NumAtCard": ref, "Comments": in["Comments"], "DocTotal": math.Round(total*100) / 100, "DocCurrency": p["Currency"],
		"DocumentStatus": "bost_Open", "Cancelled": "tNO", "UpdateDate": b.now().UTC().Format("2006-01-02"), "DocumentLines": lines,
	}
	b.orders[entry] = o
	w.Header().Set("Location", fmt.Sprintf("http://%s%s/Orders(%d)", r.Host, Base, entry))
	reply(w, http.StatusCreated, o)
}

var (
	refRE   = regexp.MustCompile(`^NumAtCard eq '((?:[^']|'')*)'$`)
	entryRE = regexp.MustCompile(`^DocEntry gt (\d+)$`)
)

// list answers $filter=NumAtCard eq '<x>' or DocEntry gt <n>, by DocEntry, with $top.
func (b *B1) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	keep := func(map[string]any) bool { return true }
	if f := q.Get("$filter"); f != "" {
		switch {
		case refRE.MatchString(f):
			ref := strings.ReplaceAll(refRE.FindStringSubmatch(f)[1], "''", "'")
			keep = func(o map[string]any) bool { return o["NumAtCard"] == ref }
		case entryRE.MatchString(f):
			after, _ := strconv.Atoi(entryRE.FindStringSubmatch(f)[1])
			keep = func(o map[string]any) bool { return o["DocEntry"].(int) > after }
		default:
			fail(w, http.StatusBadRequest, "-1000", "The fake supports no filter "+f)
			return
		}
	}
	var entries []int
	for e, o := range b.orders {
		if keep(o) {
			entries = append(entries, e)
		}
	}
	sort.Ints(entries)
	if n, err := strconv.Atoi(q.Get("$top")); err == nil && n < len(entries) {
		entries = entries[:n]
	}
	out := make([]any, len(entries))
	for i, e := range entries {
		out[i] = b.orders[e]
	}
	reply(w, http.StatusOK, map[string]any{"odata.metadata": "$metadata#Orders", "value": out})
}
