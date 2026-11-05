// Package xerotest is a fake Xero Accounting API (2.0) and Xero's identity
// token endpoint, for tests and demos: one organisation (tenant) with
// contacts and sales invoices (ACCREC).
//
// Like Xero it rotates the refresh token on every refresh (the old one is
// refused with invalid_grant), lets access tokens live 30 minutes, wants
// the xero-tenant-id header on every call, answers every write and read
// with a list ({"Invoices": [...]}), dates records as Microsoft JSON
// ("/Date(1730803200000+0000)/"), answers a write carrying an
// Idempotency-Key it has seen with the first answer, voids an AUTHORISED
// invoice when it is posted Status VOIDED, filters invoices by
// If-Modified-Since, and reports errors as {"ErrorNumber", "Type",
// "Message", "Elements": [{"ValidationErrors": [{"Message"}]}]}.
package xerotest

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
	"strings"
	"sync"
	"time"
)

// Tenant is the organisation's ID; TokenPath and API are Xero's paths.
const (
	Tenant    = "70784a63-d24b-46a9-a4db-0e70a274b056"
	TokenPath = "/connect/token"
	API       = "/api.xro/2.0"
)

// Contacts the organisation starts with.
const (
	Lovelace = "bd2270c3-8706-4c11-9cfb-000b551c3f51" // ada@example.com
	Hopper   = "9b9ba9e5-e907-4b4e-8210-54d82b0aa479"
	Archived = "565acaa9-e7f3-4fbf-80c3-16b081ddae10"
)

// Xero is the fake organisation.
type Xero struct {
	ClientID, ClientSecret string

	mu       sync.Mutex
	refresh  map[string]bool
	access   map[string]time.Time
	contacts map[string]map[string]any
	items    map[string]map[string]any
	invoices map[string]map[string]any
	seen     map[string]map[string]any // Idempotency-Key -> the first answer
	next     int
	now      func() time.Time
	srv      *httptest.Server
	// Refreshes counts refresh grants that succeeded; Spent counts refused ones.
	Refreshes, Spent int
	// Requests counts API calls by "METHOD resource".
	Requests map[string]int
}

// New returns an organisation served by an httptest server, and the
// refresh token a person's authorization would have given.
func New(clientID, clientSecret string) (*Xero, string) {
	x, tok := NewXero(clientID, clientSecret)
	x.srv = httptest.NewServer(x)
	return x, tok
}

// NewXero returns an organisation to serve yourself, and its first refresh token.
func NewXero(clientID, clientSecret string) (*Xero, string) {
	x := &Xero{ClientID: clientID, ClientSecret: clientSecret, refresh: map[string]bool{}, access: map[string]time.Time{},
		contacts: map[string]map[string]any{}, items: map[string]map[string]any{}, invoices: map[string]map[string]any{},
		seen: map[string]map[string]any{}, next: 1, now: time.Now, Requests: map[string]int{}}
	x.contacts[Lovelace] = map[string]any{"ContactID": Lovelace, "Name": "Lovelace GmbH", "EmailAddress": "ada@example.com", "ContactStatus": "ACTIVE"}
	x.contacts[Hopper] = map[string]any{"ContactID": Hopper, "Name": "Hopper Industries", "EmailAddress": "grace@example.com", "ContactStatus": "ACTIVE"}
	x.contacts[Archived] = map[string]any{"ContactID": Archived, "Name": "Gone Ltd", "EmailAddress": "", "ContactStatus": "ARCHIVED"}
	x.items["M-1"] = map[string]any{"Code": "M-1", "Name": "Desk", "SalesDetails": map[string]any{"UnitPrice": 100.0, "AccountCode": "200"}}
	x.items["M-2"] = map[string]any{"Code": "M-2", "Name": "Chair", "SalesDetails": map[string]any{"UnitPrice": 235.0, "AccountCode": "200"}}
	return x, x.issueRefresh()
}

func (x *Xero) issueRefresh() string {
	t := token()
	x.refresh[t] = true
	return t
}

// Authorize returns a new refresh token, as authorizing the app again would.
func (x *Xero) Authorize() string {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.issueRefresh()
}

// StartAt sets the next invoice number and the clock.
func (x *Xero) StartAt(number int, now func() time.Time) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.next, x.now = number, now
}

// ExpireAccessTokens ends every access token.
func (x *Xero) ExpireAccessTokens() {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.access = map[string]time.Time{}
}

func (x *Xero) URL() string { return x.srv.URL }
func (x *Xero) Close()      { x.srv.Close() }

// Invoice returns a copy of an invoice, or nil.
func (x *Xero) Invoice(id string) map[string]any {
	x.mu.Lock()
	defer x.mu.Unlock()
	return clone(x.invoices[id])
}

// InvoiceByReference returns the invoice with a reference, or nil.
func (x *Xero) InvoiceByReference(ref string) map[string]any {
	x.mu.Lock()
	defer x.mu.Unlock()
	for _, inv := range x.invoices {
		if inv["Reference"] == ref {
			return clone(inv)
		}
	}
	return nil
}

// Invoices counts invoices.
func (x *Xero) Invoices() int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return len(x.invoices)
}

// InvoiceIDs lists the invoices' IDs, oldest first.
func (x *Xero) InvoiceIDs() []string {
	x.mu.Lock()
	defer x.mu.Unlock()
	var ids []string
	for id := range x.invoices {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return x.invoices[ids[i]]["InvoiceNumber"].(string) < x.invoices[ids[j]]["InvoiceNumber"].(string)
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

func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func validation(w http.ResponseWriter, msgs ...string) {
	var errs []any
	for _, m := range msgs {
		errs = append(errs, map[string]any{"Message": m})
	}
	reply(w, http.StatusBadRequest, map[string]any{"ErrorNumber": 10, "Type": "ValidationException",
		"Message": "A validation exception occurred", "Elements": []any{map[string]any{"ValidationErrors": errs}}})
}

func msdate(t time.Time) string { return fmt.Sprintf("/Date(%d+0000)/", t.UnixMilli()) }

func (x *Xero) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == TokenPath {
		x.token(w, r)
		return
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	auth := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if exp, ok := x.access[auth]; !ok || x.now().After(exp) {
		w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
		reply(w, http.StatusUnauthorized, map[string]any{"Type": "", "Title": "Unauthorized", "Status": 401, "Detail": "AuthenticationUnsuccessful"})
		return
	}
	if r.Header.Get("xero-tenant-id") != Tenant {
		reply(w, http.StatusForbidden, map[string]any{"Type": "", "Title": "Forbidden", "Status": 403, "Detail": "AuthenticationUnsuccessful: xero-tenant-id is missing or not connected"})
		return
	}
	if !strings.HasPrefix(r.URL.Path, API+"/") {
		reply(w, http.StatusNotFound, map[string]any{"Title": "Not Found", "Status": 404})
		return
	}
	res := strings.TrimPrefix(r.URL.Path, API+"/")
	x.Requests[r.Method+" "+res]++
	key := r.Header.Get("Idempotency-Key")
	if prior, ok := x.seen[key]; ok && key != "" && r.Method != http.MethodGet {
		reply(w, http.StatusOK, prior)
		return
	}
	switch {
	case res == "Organisation" && r.Method == http.MethodGet:
		reply(w, http.StatusOK, map[string]any{"Organisations": []any{map[string]any{"Name": "Demo Company (Global)", "OrganisationID": Tenant}}})
	case strings.HasPrefix(res, "Contacts/") && r.Method == http.MethodGet:
		c := x.contacts[strings.TrimPrefix(res, "Contacts/")]
		if c == nil {
			reply(w, http.StatusNotFound, map[string]any{"Title": "Not Found", "Status": 404, "Detail": "The resource you're looking for cannot be found"})
			return
		}
		reply(w, http.StatusOK, map[string]any{"Contacts": []any{c}})
	case res == "Invoices" && r.Method == http.MethodGet:
		x.list(w, r)
	case res == "Invoices" && (r.Method == http.MethodPost || r.Method == http.MethodPut):
		x.create(w, r, key)
	case strings.HasPrefix(res, "Invoices/") && r.Method == http.MethodPost:
		x.update(w, r, strings.TrimPrefix(res, "Invoices/"), key)
	default:
		reply(w, http.StatusNotFound, map[string]any{"Title": "Not Found", "Status": 404, "Detail": res})
	}
}

func (x *Xero) token(w http.ResponseWriter, r *http.Request) {
	id, secret, ok := r.BasicAuth()
	if err := r.ParseForm(); err != nil || !ok || id != x.ClientID || secret != x.ClientSecret {
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
		reply(w, http.StatusBadRequest, map[string]any{"error": "invalid_grant"})
		return
	}
	delete(x.refresh, old)
	access := "eyJ" + token()
	x.access[access] = x.now().Add(30 * time.Minute)
	x.Refreshes++
	reply(w, http.StatusOK, map[string]any{"id_token": "", "access_token": access, "expires_in": 1800, "token_type": "Bearer",
		"refresh_token": x.issueRefresh(), "scope": "accounting.transactions accounting.contacts offline_access"})
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

// decode takes one invoice, sent as {"Invoices": [{...}]} or on its own.
func decode(r *http.Request) (map[string]any, error) {
	var in map[string]any
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(&in); err != nil {
		return nil, err
	}
	if list, ok := in["Invoices"].([]any); ok {
		if len(list) != 1 {
			return nil, fmt.Errorf("the fake takes one invoice per request")
		}
		in, _ = list[0].(map[string]any)
	}
	return in, nil
}

func (x *Xero) answer(inv map[string]any, key string) map[string]any {
	out := map[string]any{"Id": guid(), "Status": "OK", "ProviderName": "Turgon", "DateTimeUTC": msdate(x.now()), "Invoices": []any{clone(inv)}}
	if key != "" {
		x.seen[key] = out
	}
	return out
}

func (x *Xero) create(w http.ResponseWriter, r *http.Request, key string) {
	in, err := decode(r)
	if err != nil {
		validation(w, err.Error())
		return
	}
	if in["Type"] != "ACCREC" {
		validation(w, "Invoice Type must be specified (the fake takes ACCREC)")
		return
	}
	contact, _ := in["Contact"].(map[string]any)
	cid, _ := contact["ContactID"].(string)
	c := x.contacts[cid]
	if c == nil {
		validation(w, "The Contact ID "+cid+" does not exist or is not valid for this organisation.")
		return
	}
	if c["ContactStatus"] == "ARCHIVED" {
		validation(w, "The contact "+c["Name"].(string)+" is archived. Restore it before invoicing.")
		return
	}
	lines, _ := in["LineItems"].([]any)
	if len(lines) == 0 {
		validation(w, "At least one line item is required.")
		return
	}
	var out []any
	total := 0.0
	var errs []string
	for i, v := range lines {
		l, _ := v.(map[string]any)
		code, _ := l["ItemCode"].(string)
		item := x.items[code]
		if code != "" && item == nil {
			errs = append(errs, fmt.Sprintf("Item code '%s' is not valid (line %d)", code, i+1))
			continue
		}
		qty, ok := num(l["Quantity"])
		if !ok {
			qty = 1
		}
		price, ok := num(l["UnitAmount"])
		if !ok && item != nil {
			price = item["SalesDetails"].(map[string]any)["UnitPrice"].(float64)
		}
		desc, _ := l["Description"].(string)
		if desc == "" && item != nil {
			desc = item["Name"].(string)
		}
		if desc == "" {
			errs = append(errs, fmt.Sprintf("Description cannot be empty (line %d)", i+1))
			continue
		}
		amt := math.Round(qty*price*100) / 100
		total += amt
		out = append(out, map[string]any{"LineItemID": guid(), "Description": desc, "Quantity": qty, "UnitAmount": price,
			"ItemCode": code, "AccountCode": "200", "LineAmount": amt})
	}
	if len(errs) > 0 {
		validation(w, errs...)
		return
	}
	date, _ := in["Date"].(string)
	if date == "" {
		date = x.now().Format("2006-01-02")
	}
	if _, err := time.Parse("2006-01-02", date); err != nil {
		validation(w, "Date '"+date+"' is not a valid date (YYYY-MM-DD)")
		return
	}
	status, _ := in["Status"].(string)
	if status == "" {
		status = "DRAFT"
	}
	if status != "DRAFT" && status != "AUTHORISED" {
		validation(w, "Status "+status+" is not valid for a new invoice")
		return
	}
	id := guid()
	inv := map[string]any{"Type": "ACCREC", "InvoiceID": id, "InvoiceNumber": fmt.Sprintf("INV-%04d", x.next), "Reference": in["Reference"],
		"Contact": map[string]any{"ContactID": cid, "Name": c["Name"]}, "Date": date, "DueDate": in["DueDate"], "Status": status,
		"LineAmountTypes": "Exclusive", "LineItems": out, "SubTotal": math.Round(total*100) / 100, "TotalTax": 0.0, "Total": math.Round(total*100) / 100,
		"AmountDue": math.Round(total*100) / 100, "CurrencyCode": "USD", "UpdatedDateUTC": msdate(x.now())}
	x.next++
	x.invoices[id] = inv
	reply(w, http.StatusOK, x.answer(inv, key))
}

func (x *Xero) update(w http.ResponseWriter, r *http.Request, id, key string) {
	inv := x.invoices[id]
	if inv == nil {
		reply(w, http.StatusNotFound, map[string]any{"Title": "Not Found", "Status": 404, "Detail": "The resource you're looking for cannot be found"})
		return
	}
	in, err := decode(r)
	if err != nil {
		validation(w, err.Error())
		return
	}
	if in["Status"] != "VOIDED" {
		validation(w, "The fake updates an invoice's Status to VOIDED only")
		return
	}
	switch inv["Status"] {
	case "AUTHORISED", "SUBMITTED":
	case "VOIDED":
		validation(w, "This document cannot be edited as it has a status of VOIDED")
		return
	default:
		validation(w, fmt.Sprintf("Invoice not of valid status for modification: %v; a DRAFT invoice is deleted, not voided", inv["Status"]))
		return
	}
	inv["Status"], inv["AmountDue"], inv["UpdatedDateUTC"] = "VOIDED", 0.0, msdate(x.now())
	reply(w, http.StatusOK, x.answer(inv, key))
}

var msRE = regexp.MustCompile(`^/Date\((\d+)`)

func millis(v any) int64 {
	m := msRE.FindStringSubmatch(fmt.Sprint(v))
	if m == nil {
		return 0
	}
	var n int64
	fmt.Sscan(m[1], &n)
	return n
}

// list answers invoices modified at or after If-Modified-Since, oldest first.
func (x *Xero) list(w http.ResponseWriter, r *http.Request) {
	var since time.Time
	if h := r.Header.Get("If-Modified-Since"); h != "" {
		t, err := time.Parse("2006-01-02T15:04:05", strings.TrimSuffix(strings.TrimSuffix(h, "Z"), ".000"))
		if err != nil {
			validation(w, "If-Modified-Since must be a UTC time like 2009-11-12T00:00:00")
			return
		}
		since = t
	}
	var list []map[string]any
	for _, inv := range x.invoices {
		if millis(inv["UpdatedDateUTC"]) >= since.UnixMilli() {
			list = append(list, inv)
		}
	}
	sort.Slice(list, func(i, j int) bool {
		a, b := millis(list[i]["UpdatedDateUTC"]), millis(list[j]["UpdatedDateUTC"])
		if a != b {
			return a < b
		}
		return list[i]["InvoiceNumber"].(string) < list[j]["InvoiceNumber"].(string)
	})
	out := make([]any, len(list))
	for i, inv := range list {
		out[i] = inv
	}
	reply(w, http.StatusOK, map[string]any{"Id": guid(), "Status": "OK", "DateTimeUTC": msdate(x.now()), "Invoices": out})
}
