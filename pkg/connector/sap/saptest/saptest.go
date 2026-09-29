// Package saptest is a fake SAP S/4HANA system for tests and demos. It
// serves the parts of three released OData v2 APIs Turgon uses:
// API_SALES_ORDER_SRV (A_SalesOrder with items, deep insert, delete),
// API_SALES_ORDER_SIMULATION_SRV (A_SalesOrderSimulation) and
// API_BUSINESS_PARTNER (A_Customer). Like S/4HANA it requires basic
// authentication and the sap-client, a CSRF token bound to the session
// cookie for every modifying request, and the entity's ETag to delete it;
// it prices orders from a price list, answers in OData v2 JSON ({"d": ...},
// /Date(ms)/ dates, decimals as strings), and serves $metadata.
package saptest

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Prefix is where the services live.
const Prefix = "/sap/opu/odata/sap/"

// S4 is the fake system.
type S4 struct {
	User, Password, Client string

	mu        sync.Mutex
	orders    map[string]map[string]any
	customers map[string]map[string]any
	prices    map[string]float64
	next      int
	sessions  map[string]string // session cookie -> CSRF token
	now       func() time.Time
	// LoseNextCreate saves the next order but answers 504, as when a
	// gateway times out after the backend committed.
	LoseNextCreate bool
	// Requests counts requests by "METHOD service/resource".
	Requests map[string]int
	// Events, if set, receives the business events S/4HANA publishes when
	// a sales order is created, changed or deleted (to SAP Event Mesh,
	// through an event channel): the CloudEvent type and its data, which
	// carries the order's key and a few header fields. Mesh.Publisher
	// makes one.
	Events func(eventType string, data map[string]any)
}

// Sales order event types, as S/4HANA names them.
const (
	SalesOrderCreated = "sap.s4.beh.salesorder.v1.SalesOrder.Created.v1"
	SalesOrderChanged = "sap.s4.beh.salesorder.v1.SalesOrder.Changed.v1"
	SalesOrderDeleted = "sap.s4.beh.salesorder.v1.SalesOrder.Deleted.v1"
)

// emit publishes an order's event. The caller holds s.mu.
func (s *S4) emit(eventType string, o map[string]any) {
	if s.Events == nil {
		return
	}
	data := map[string]any{}
	for _, k := range []string{"SalesOrder", "SalesOrderType", "SalesOrganization", "DistributionChannel", "OrganizationDivision", "SoldToParty"} {
		if v, ok := o[k]; ok {
			data[k] = v
		}
	}
	s.Events(eventType, data)
}

// CreateOrder enters an order as a person would in the SAP GUI (VA01), with
// one item, and returns its number.
func (s *S4) CreateOrder(soldTo, material, quantity string) (string, error) {
	body, _ := json.Marshal(map[string]any{
		"SalesOrderType": "OR", "SalesOrganization": "1710", "DistributionChannel": "10", "OrganizationDivision": "00",
		"SoldToParty": soldTo, "SalesOrderDate": fmt.Sprintf("/Date(%d)/", s.now().UTC().Truncate(24*time.Hour).UnixMilli()),
		"to_Item": []map[string]any{{"Material": material, "RequestedQuantity": quantity}},
	})
	r, _ := http.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	s.mu.Lock()
	defer s.mu.Unlock()
	o, e := s.priced(r)
	if e != nil {
		return "", fmt.Errorf("%s", e.msg)
	}
	return s.store(o), nil
}

// store numbers a new order and keeps it. The caller holds s.mu.
func (s *S4) store(o map[string]any) string {
	id := strconv.Itoa(s.next)
	s.next++
	o["SalesOrder"] = id
	for i, it := range o["to_Item"].([]map[string]any) {
		it["SalesOrder"], it["SalesOrderItem"] = id, strconv.Itoa((i+1)*10)
	}
	s.orders[id] = o
	s.emit(SalesOrderCreated, o)
	return id
}

// New returns a system with a few customers and a price list.
func New(user, password, client string) *S4 {
	s := &S4{
		User: user, Password: password, Client: client,
		orders: map[string]map[string]any{}, customers: map[string]map[string]any{},
		prices: map[string]float64{"TG11": 125.0, "TG12": 49.5, "M-1": 10, "M-2": 235},
		next:   1, sessions: map[string]string{}, now: time.Now, Requests: map[string]int{},
	}
	for id, name := range map[string]string{"C-100": "Lovelace GmbH", "C-200": "Hopper Industries", "17100001": "Domestic Customer DE 1"} {
		s.customers[id] = map[string]any{"Customer": id, "CustomerName": name, "CustomerAccountGroup": "CUST", "DeletionIndicator": false}
	}
	return s
}

// StartAt numbers orders from n and sets the clock, so a demo restarted
// never reuses order numbers.
func (s *S4) StartAt(n int, now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next, s.now = n, now
}

// Order returns a copy of an order, with its items, or nil.
func (s *S4) Order(id string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	o := s.orders[id]
	if o == nil {
		return nil
	}
	b, _ := json.Marshal(o)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}

// Orders returns the order numbers, in creation order.
func (s *S4) Orders() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.orders))
	for id := range s.orders {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return len(ids[i]) < len(ids[j]) || (len(ids[i]) == len(ids[j]) && ids[i] < ids[j])
	})
	return ids
}

// Change touches an order as a person would in the SAP GUI: it sets a
// header property and bumps LastChangeDateTime.
func (s *S4) Change(id, prop string, v any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if o := s.orders[id]; o != nil {
		o[prop] = v
		s.touch(o)
		s.emit(SalesOrderChanged, o)
	}
}

func (s *S4) touch(o map[string]any) {
	t := s.now().UTC()
	o["LastChangeDateTime"] = t
	o["ETag"] = fmt.Sprintf("W/\"datetimeoffset'%s'\"", t.Format(time.RFC3339Nano))
}

type odataError struct {
	status    int
	code, msg string
}

func fail(w http.ResponseWriter, e odataError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code": e.code, "message": map[string]any{"lang": "en", "value": e.msg},
	}})
}

func reply(w http.ResponseWriter, status int, d any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"d": d})
}

func (s *S4) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if u, p, ok := r.BasicAuth(); !ok || u != s.User || p != s.Password {
		w.Header().Set("WWW-Authenticate", `Basic realm="SAP NetWeaver Application Server"`)
		http.Error(w, "Logon failed", http.StatusUnauthorized)
		return
	}
	if !strings.HasPrefix(r.URL.Path, Prefix) {
		http.NotFound(w, r)
		return
	}
	if s.Client != "" && r.URL.Query().Get("sap-client") != s.Client {
		fail(w, odataError{http.StatusUnauthorized, "/IWFND/CM_CONSUMER/101", "sap-client missing or wrong"})
		return
	}
	service, resource, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, Prefix), "/")
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Requests[r.Method+" "+service+"/"+resource]++

	// Sessions: a cookie on first contact, and a CSRF token on request.
	sid := ""
	if c, err := r.Cookie("SAP_SESSIONID"); err == nil && s.sessions[c.Value] != "" {
		sid = c.Value
	} else {
		sid = random()
		s.sessions[sid] = random()
		http.SetCookie(w, &http.Cookie{Name: "SAP_SESSIONID", Value: sid, Path: "/", HttpOnly: true})
	}
	if r.Method == http.MethodGet && strings.EqualFold(r.Header.Get("X-CSRF-Token"), "Fetch") {
		w.Header().Set("X-CSRF-Token", s.sessions[sid])
	}
	if r.Method != http.MethodGet && r.Header.Get("X-CSRF-Token") != s.sessions[sid] {
		w.Header().Set("X-CSRF-Token", "Required")
		http.Error(w, "CSRF token validation failed", http.StatusForbidden)
		return
	}

	switch {
	case resource == "":
		reply(w, http.StatusOK, map[string]any{"EntitySets": setsOf(service)})
	case resource == "$metadata":
		md, ok := metadata[service]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(md))
	case service == "API_SALES_ORDER_SRV":
		s.salesOrders(w, r, resource)
	case service == "API_SALES_ORDER_SIMULATION_SRV" && resource == "A_SalesOrderSimulation" && r.Method == http.MethodPost:
		o, e := s.priced(r)
		if e != nil {
			fail(w, *e)
			return
		}
		reply(w, http.StatusCreated, s.render(o, true))
	case service == "API_BUSINESS_PARTNER":
		m := keyPath.FindStringSubmatch(resource)
		if m == nil || m[1] != "A_Customer" || r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		c := s.customers[m[2]]
		if c == nil {
			fail(w, odataError{http.StatusNotFound, "CX_SADL_ENTITY_NOT_FOUND", "Resource not found for segment 'A_CustomerType'"})
			return
		}
		reply(w, http.StatusOK, c)
	default:
		fail(w, odataError{http.StatusNotFound, "/IWFND/MED/170", fmt.Sprintf("No service found for namespace '', name '%s'", service)})
	}
}

func setsOf(service string) []string {
	return map[string][]string{
		"API_SALES_ORDER_SRV":            {"A_SalesOrder", "A_SalesOrderItem"},
		"API_SALES_ORDER_SIMULATION_SRV": {"A_SalesOrderSimulation"},
		"API_BUSINESS_PARTNER":           {"A_Customer"},
	}[service]
}

var keyPath = regexp.MustCompile(`^(\w+)\('([^']*)'\)$`)

func (s *S4) salesOrders(w http.ResponseWriter, r *http.Request, resource string) {
	expand := strings.Contains(r.URL.Query().Get("$expand"), "to_Item")
	if resource == "A_SalesOrder" {
		switch r.Method {
		case http.MethodGet:
			list, e := s.query(r)
			if e != nil {
				fail(w, *e)
				return
			}
			out := make([]any, len(list))
			for i, o := range list {
				out[i] = s.render(o, expand)
			}
			reply(w, http.StatusOK, map[string]any{"results": out})
		case http.MethodPost:
			o, e := s.priced(r)
			if e != nil {
				fail(w, *e)
				return
			}
			s.store(o)
			if s.LoseNextCreate {
				s.LoseNextCreate = false
				fail(w, odataError{http.StatusGatewayTimeout, "", "Gateway Timeout"})
				return
			}
			w.Header().Set("ETag", o["ETag"].(string))
			reply(w, http.StatusCreated, s.render(o, true))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
		return
	}
	m := keyPath.FindStringSubmatch(resource)
	if m == nil || m[1] != "A_SalesOrder" {
		http.NotFound(w, r)
		return
	}
	o := s.orders[m[2]]
	if o == nil {
		fail(w, odataError{http.StatusNotFound, "CX_SADL_ENTITY_NOT_FOUND", "Resource not found for segment 'A_SalesOrderType'"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		w.Header().Set("ETag", o["ETag"].(string))
		reply(w, http.StatusOK, s.render(o, expand))
	case http.MethodDelete:
		if im := r.Header.Get("If-Match"); im != "*" && im != o["ETag"] {
			fail(w, odataError{http.StatusPreconditionFailed, "/IWBEP/CM_MGW_RT/022", "The Data Services Request could not be understood due to malformed syntax"})
			return
		}
		delete(s.orders, m[2])
		s.emit(SalesOrderDeleted, o)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// priced validates an order request and prices it.
func (s *S4) priced(r *http.Request) (map[string]any, *odataError) {
	var in map[string]any
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(&in); err != nil {
		return nil, &odataError{http.StatusBadRequest, "/IWBEP/CM_MGW_RT/004", "Malformed JSON: " + err.Error()}
	}
	str := func(m map[string]any, k string) string { v, _ := m[k].(string); return v }
	for _, k := range []string{"SalesOrderType", "SalesOrganization", "DistributionChannel", "OrganizationDivision", "SoldToParty"} {
		if str(in, k) == "" {
			return nil, &odataError{http.StatusBadRequest, "V1/320", fmt.Sprintf("Property %s is required", k)}
		}
	}
	if s.customers[str(in, "SoldToParty")] == nil {
		return nil, &odataError{http.StatusBadRequest, "V1/320", fmt.Sprintf("Sold-to party %s not maintained for sales area %s %s %s",
			str(in, "SoldToParty"), str(in, "SalesOrganization"), str(in, "DistributionChannel"), str(in, "OrganizationDivision"))}
	}
	o := map[string]any{"TransactionCurrency": "EUR"}
	for k, v := range in {
		if k == "to_Item" {
			continue
		}
		if _, isNum := v.(json.Number); isNum {
			return nil, &odataError{http.StatusBadRequest, "/IWBEP/CM_MGW_RT/004", fmt.Sprintf("Invalid value for property %s: Edm values are sent as strings", k)}
		}
		o[k] = v
	}
	if d, ok := o["SalesOrderDate"].(string); ok && !regexp.MustCompile(`^/Date\(\d+\)/$`).MatchString(d) {
		return nil, &odataError{http.StatusBadRequest, "/IWBEP/CM_MGW_RT/004", "Invalid value for property SalesOrderDate: " + d}
	}
	rawItems, _ := in["to_Item"].([]any)
	if len(rawItems) == 0 {
		return nil, &odataError{http.StatusBadRequest, "V1/320", "A sales order needs at least one item"}
	}
	total := 0.0
	items := make([]map[string]any, 0, len(rawItems))
	for i, x := range rawItems {
		it, _ := x.(map[string]any)
		mat := str(it, "Material")
		price, known := s.prices[mat]
		if !known {
			return nil, &odataError{http.StatusBadRequest, "V1/320", fmt.Sprintf("Item %d: material %q does not exist", (i+1)*10, mat)}
		}
		q, err := strconv.ParseFloat(str(it, "RequestedQuantity"), 64)
		if err != nil || q <= 0 {
			return nil, &odataError{http.StatusBadRequest, "/IWBEP/CM_MGW_RT/004", fmt.Sprintf("Item %d: RequestedQuantity must be a positive decimal string", (i+1)*10)}
		}
		net := price * q
		total += net
		items = append(items, map[string]any{"Material": mat, "RequestedQuantity": str(it, "RequestedQuantity"),
			"NetAmount": strconv.FormatFloat(net, 'f', 2, 64), "TransactionCurrency": o["TransactionCurrency"]})
	}
	o["to_Item"] = items
	o["TotalNetAmount"] = strconv.FormatFloat(total, 'f', 2, 64)
	o["OverallSDProcessStatus"] = "A"
	o["CreationDate"] = s.now().UTC().Truncate(24 * time.Hour)
	s.touch(o)
	return o, nil
}

// render writes an order in OData v2 JSON.
func (s *S4) render(o map[string]any, expand bool) map[string]any {
	out := map[string]any{}
	for k, v := range o {
		switch x := v.(type) {
		case time.Time:
			if k == "CreationDate" {
				out[k] = fmt.Sprintf("/Date(%d)/", x.UnixMilli())
			} else {
				out[k] = fmt.Sprintf("/Date(%d+0000)/", x.UnixMilli())
			}
		case []map[string]any:
			if expand {
				out[k] = map[string]any{"results": x}
			} else {
				out[k] = map[string]any{"__deferred": map[string]any{"uri": "to_Item"}}
			}
		default:
			if k != "ETag" {
				out[k] = v
			}
		}
	}
	out["__metadata"] = map[string]any{"type": "API_SALES_ORDER_SRV.A_SalesOrderType", "etag": o["ETag"]}
	return out
}

var clause = regexp.MustCompile(`^(\w+) (eq|gt) (?:datetimeoffset)?'((?:[^']|'')*)'$`)

// query answers $filter (clauses joined by "and", each "Prop eq|gt
// 'value'" or a datetimeoffset literal, parentheses allowed), $orderby
// (ascending), $top and $skip.
func (s *S4) query(r *http.Request) ([]map[string]any, *odataError) {
	q := r.URL.Query()
	var list []map[string]any
	for _, o := range s.orders {
		list = append(list, o)
	}
	if f := strings.TrimSpace(q.Get("$filter")); f != "" {
		f = strings.NewReplacer("(", "", ")", "").Replace(f)
		for _, part := range strings.Split(f, " and ") {
			m := clause.FindStringSubmatch(strings.TrimSpace(part))
			if m == nil {
				return nil, &odataError{http.StatusBadRequest, "/IWBEP/CM_MGW_RT/004", "Invalid filter expression: " + part}
			}
			prop, op, val := m[1], m[2], strings.ReplaceAll(m[3], "''", "'")
			var keep []map[string]any
			for _, o := range list {
				if match(o[prop], op, val) {
					keep = append(keep, o)
				}
			}
			list = keep
		}
	}
	order := strings.Split(q.Get("$orderby"), ",")
	sort.SliceStable(list, func(i, j int) bool {
		for _, p := range order {
			p = strings.TrimSuffix(strings.TrimSpace(p), " asc")
			a, b := list[i][p], list[j][p]
			if ta, ok := a.(time.Time); ok {
				tb, _ := b.(time.Time)
				if !ta.Equal(tb) {
					return ta.Before(tb)
				}
				continue
			}
			as, bs := fmt.Sprint(a), fmt.Sprint(b)
			if p == "SalesOrder" && len(as) != len(bs) {
				return len(as) < len(bs)
			}
			if as != bs {
				return as < bs
			}
		}
		return false
	})
	if n, err := strconv.Atoi(q.Get("$skip")); err == nil && n > 0 {
		if n > len(list) {
			n = len(list)
		}
		list = list[n:]
	}
	if n, err := strconv.Atoi(q.Get("$top")); err == nil && n < len(list) {
		list = list[:n]
	}
	return list, nil
}

func match(v any, op, val string) bool {
	if t, ok := v.(time.Time); ok {
		w, err := time.Parse(time.RFC3339Nano, val)
		if err != nil {
			return false
		}
		if op == "gt" {
			return t.After(w)
		}
		return t.Equal(w)
	}
	if op == "gt" {
		return fmt.Sprint(v) > val
	}
	return fmt.Sprint(v) == val
}

func random() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

var metadata = map[string]string{
	"API_SALES_ORDER_SRV": edmx("API_SALES_ORDER_SRV", `
<EntityType Name="A_SalesOrderType"><Key><PropertyRef Name="SalesOrder"/></Key>
 <Property Name="SalesOrder" Type="Edm.String"/><Property Name="SalesOrderType" Type="Edm.String"/>
 <Property Name="SalesOrganization" Type="Edm.String"/><Property Name="DistributionChannel" Type="Edm.String"/>
 <Property Name="OrganizationDivision" Type="Edm.String"/><Property Name="SoldToParty" Type="Edm.String"/>
 <Property Name="PurchaseOrderByCustomer" Type="Edm.String"/><Property Name="SalesOrderDate" Type="Edm.DateTime"/>
 <Property Name="TransactionCurrency" Type="Edm.String"/><Property Name="TotalNetAmount" Type="Edm.Decimal"/>
 <Property Name="OverallSDProcessStatus" Type="Edm.String"/><Property Name="CreationDate" Type="Edm.DateTime"/>
 <Property Name="LastChangeDateTime" Type="Edm.DateTimeOffset"/>
 <NavigationProperty Name="to_Item"/></EntityType>
<EntityType Name="A_SalesOrderItemType"><Key><PropertyRef Name="SalesOrder"/><PropertyRef Name="SalesOrderItem"/></Key>
 <Property Name="SalesOrder" Type="Edm.String"/><Property Name="SalesOrderItem" Type="Edm.String"/>
 <Property Name="Material" Type="Edm.String"/><Property Name="RequestedQuantity" Type="Edm.Decimal"/>
 <Property Name="NetAmount" Type="Edm.Decimal"/></EntityType>`,
		`<EntitySet Name="A_SalesOrder" EntityType="API_SALES_ORDER_SRV.A_SalesOrderType"/>
<EntitySet Name="A_SalesOrderItem" EntityType="API_SALES_ORDER_SRV.A_SalesOrderItemType"/>`),
	"API_SALES_ORDER_SIMULATION_SRV": edmx("API_SALES_ORDER_SIMULATION_SRV", `
<EntityType Name="A_SalesOrderSimulationType"><Key><PropertyRef Name="SalesOrder"/></Key>
 <Property Name="SalesOrder" Type="Edm.String"/><Property Name="TotalNetAmount" Type="Edm.Decimal"/></EntityType>`,
		`<EntitySet Name="A_SalesOrderSimulation" EntityType="API_SALES_ORDER_SIMULATION_SRV.A_SalesOrderSimulationType"/>`),
	"API_BUSINESS_PARTNER": edmx("API_BUSINESS_PARTNER", `
<EntityType Name="A_CustomerType"><Key><PropertyRef Name="Customer"/></Key>
 <Property Name="Customer" Type="Edm.String"/><Property Name="CustomerName" Type="Edm.String"/>
 <Property Name="CustomerAccountGroup" Type="Edm.String"/><Property Name="DeletionIndicator" Type="Edm.Boolean"/></EntityType>`,
		`<EntitySet Name="A_Customer" EntityType="API_BUSINESS_PARTNER.A_CustomerType"/>`),
}

func edmx(ns, types, sets string) string {
	return `<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="1.0" xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
<edmx:DataServices m:DataServiceVersion="2.0">
<Schema Namespace="` + ns + `" xmlns="http://schemas.microsoft.com/ado/2008/09/edm">` + types + `
<EntityContainer Name="` + ns + `_Entities" m:IsDefaultEntityContainer="true">` + sets + `</EntityContainer>
</Schema></edmx:DataServices></edmx:Edmx>`
}

// ExpireSessions ends every session, as a server restart or timeout
// does: their CSRF tokens stop working.
func (s *S4) ExpireSessions() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions = map[string]string{}
}
