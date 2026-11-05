// Package odootest is a fake Odoo (19) external JSON-2 API for tests and
// demos: customers (res.partner), products (product.product) and quotations
// (sale.order with sale.order.line). Like Odoo it takes POST
// /json/2/<model>/<method> with named arguments and an API key as a bearer
// token, describes fields with fields_get, evaluates domains in
// search_read, search and search_count, creates records with one2many
// commands ([0, 0, {...}]) and returns their IDs, prices a line without a
// price_unit at the product's list price, computes a form without saving
// it in onchange, stamps write_date to the second, and reports errors as
// {"name": "odoo.exceptions.…", "message": ...} with 401, 404 or 422.
package odootest

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"time"
)

// Records the database starts with.
const (
	LovelaceGmbH = 7 // ada@example.com
	HopperInc    = 8 // grace@example.com
	Desk         = 11
	Chair        = 12
)

type field struct {
	Type     string `json:"type"`
	String   string `json:"string"`
	Required bool   `json:"required"`
	Readonly bool   `json:"readonly"`
	Relation string `json:"relation,omitempty"`
	Size     int    `json:"size,omitempty"`
}

// Odoo is the fake database.
type Odoo struct {
	// APIKey is the key requests must carry; Database, if set, the
	// X-Odoo-Database they must name.
	APIKey, Database string

	mu      sync.Mutex
	models  map[string]map[string]field
	records map[string]map[int64]map[string]any
	nextID  map[string]int64
	next    int // quotation number
	now     func() time.Time
	srv     *httptest.Server
	// FailCreates makes sale.order creates fail with a ValidationError, as
	// a custom constraint would.
	FailCreates bool
	// Calls counts calls by "model/method".
	Calls map[string]int
}

// New returns a database served by an httptest server.
func New(apiKey string) *Odoo {
	o := NewOdoo(apiKey)
	o.srv = httptest.NewServer(o)
	return o
}

// NewOdoo returns a database to serve yourself.
func NewOdoo(apiKey string) *Odoo {
	o := &Odoo{APIKey: apiKey, records: map[string]map[int64]map[string]any{}, nextID: map[string]int64{},
		next: 1, now: time.Now, Calls: map[string]int{}}
	common := func(m map[string]field) map[string]field {
		m["id"] = field{Type: "integer", String: "ID", Readonly: true}
		m["display_name"] = field{Type: "char", String: "Display Name", Readonly: true}
		m["write_date"] = field{Type: "datetime", String: "Last Updated on", Readonly: true}
		return m
	}
	o.models = map[string]map[string]field{
		"res.users": common(map[string]field{"name": {Type: "char", String: "Name", Required: true}}),
		"res.partner": common(map[string]field{
			"name":       {Type: "char", String: "Name"},
			"email":      {Type: "char", String: "Email"},
			"ref":        {Type: "char", String: "Reference"},
			"vat":        {Type: "char", String: "Tax ID"},
			"active":     {Type: "boolean", String: "Active"},
			"is_company": {Type: "boolean", String: "Is a Company"},
		}),
		"product.product": common(map[string]field{
			"name":         {Type: "char", String: "Name", Required: true},
			"default_code": {Type: "char", String: "Internal Reference"},
			"list_price":   {Type: "float", String: "Sales Price"},
			"active":       {Type: "boolean", String: "Active"},
		}),
		"sale.order": common(map[string]field{
			"name":             {Type: "char", String: "Order Reference", Required: true, Readonly: true},
			"partner_id":       {Type: "many2one", String: "Customer", Required: true, Relation: "res.partner"},
			"client_order_ref": {Type: "char", String: "Customer Reference"},
			"date_order":       {Type: "datetime", String: "Order Date", Required: true},
			"origin":           {Type: "char", String: "Source Document"},
			"note":             {Type: "html", String: "Terms and conditions"},
			"order_line":       {Type: "one2many", String: "Order Lines", Relation: "sale.order.line"},
			"amount_untaxed":   {Type: "monetary", String: "Untaxed Amount", Readonly: true},
			"amount_total":     {Type: "monetary", String: "Total", Readonly: true},
			"state":            {Type: "selection", String: "Status", Readonly: true},
		}),
		"sale.order.line": common(map[string]field{
			"order_id":        {Type: "many2one", String: "Order Reference", Required: true, Relation: "sale.order"},
			"product_id":      {Type: "many2one", String: "Product", Relation: "product.product"},
			"name":            {Type: "text", String: "Description", Required: true},
			"product_uom_qty": {Type: "float", String: "Quantity", Required: true},
			"price_unit":      {Type: "float", String: "Unit Price", Required: true},
			"price_subtotal":  {Type: "monetary", String: "Subtotal", Readonly: true},
		}),
	}
	for m := range o.models {
		o.records[m] = map[int64]map[string]any{}
		o.nextID[m] = 1
	}
	stamp := "2026-01-01 00:00:00"
	o.records["res.users"][2] = map[string]any{"id": int64(2), "name": "Turgon Integration", "display_name": "Turgon Integration", "write_date": stamp}
	o.records["res.partner"][LovelaceGmbH] = map[string]any{"id": int64(LovelaceGmbH), "name": "Lovelace GmbH", "display_name": "Lovelace GmbH",
		"email": "ada@example.com", "ref": "C-100", "vat": "DE123456789", "active": true, "is_company": true, "write_date": stamp}
	o.records["res.partner"][HopperInc] = map[string]any{"id": int64(HopperInc), "name": "Hopper Industries", "display_name": "Hopper Industries",
		"email": "grace@example.com", "ref": "C-200", "vat": false, "active": true, "is_company": true, "write_date": stamp}
	o.records["product.product"][Desk] = map[string]any{"id": int64(Desk), "name": "Desk", "display_name": "[M-1] Desk",
		"default_code": "M-1", "list_price": 100.0, "active": true, "write_date": stamp}
	o.records["product.product"][Chair] = map[string]any{"id": int64(Chair), "name": "Chair", "display_name": "[M-2] Chair",
		"default_code": "M-2", "list_price": 235.0, "active": true, "write_date": stamp}
	o.nextID["res.partner"], o.nextID["product.product"] = 100, 100
	return o
}

// StartAt sets the next quotation number and the clock.
func (o *Odoo) StartAt(number int, now func() time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.next, o.now = number, now
}

// SetRequired makes a field required, as a module or Studio would.
func (o *Odoo) SetRequired(model, name string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	f := o.models[model][name]
	if f.Type == "" {
		f = field{Type: "char", String: name}
	}
	f.Required = true
	o.models[model][name] = f
}

func (o *Odoo) URL() string { return o.srv.URL }
func (o *Odoo) Close()      { o.srv.Close() }

// Order returns a copy of a sales order, with its lines, or nil.
func (o *Odoo) Order(id int64) map[string]any {
	o.mu.Lock()
	defer o.mu.Unlock()
	rec := o.records["sale.order"][id]
	if rec == nil {
		return nil
	}
	out := clone(rec)
	var lines []any
	for _, lid := range rec["order_line"].([]int64) {
		lines = append(lines, clone(o.records["sale.order.line"][lid]))
	}
	out["order_line"] = lines
	return out
}

// OrderByRef returns the order with a customer reference, or nil.
func (o *Odoo) OrderByRef(ref string) map[string]any {
	o.mu.Lock()
	var id int64
	for rid, r := range o.records["sale.order"] {
		if r["client_order_ref"] == ref {
			id = rid
		}
	}
	o.mu.Unlock()
	if id == 0 {
		return nil
	}
	return o.Order(id)
}

// Orders counts sales orders.
func (o *Odoo) Orders() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.records["sale.order"])
}

// OrderIDs lists the sales orders' IDs.
func (o *Odoo) OrderIDs() []int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	var ids []int64
	for id := range o.records["sale.order"] {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func clone(m map[string]any) map[string]any {
	raw, _ := json.Marshal(m)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return out
}

type odooError struct {
	status        int
	name, message string
}

func (e *odooError) Error() string { return e.message }

func validation(format string, a ...any) error {
	return &odooError{http.StatusUnprocessableEntity, "odoo.exceptions.ValidationError", fmt.Sprintf(format, a...)}
}
func userError(format string, a ...any) error {
	return &odooError{http.StatusUnprocessableEntity, "odoo.exceptions.UserError", fmt.Sprintf(format, a...)}
}
func missing(format string, a ...any) error {
	return &odooError{http.StatusNotFound, "odoo.exceptions.MissingError", fmt.Sprintf(format, a...)}
}

func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (o *Odoo) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fail := func(e *odooError) {
		reply(w, e.status, map[string]any{"name": e.name, "message": e.message, "arguments": []any{e.message}, "context": map[string]any{}, "debug": "Traceback (most recent call last): …"})
	}
	if !strings.HasPrefix(r.URL.Path, "/json/2/") || r.Method != http.MethodPost {
		fail(&odooError{http.StatusNotFound, "werkzeug.exceptions.NotFound", "404 Not Found: The requested URL was not found on the server."})
		return
	}
	if !strings.EqualFold(r.Header.Get("Authorization"), "bearer "+o.APIKey) {
		fail(&odooError{http.StatusUnauthorized, "werkzeug.exceptions.Unauthorized", "401 Unauthorized: Invalid apikey"})
		return
	}
	if o.Database != "" && r.Header.Get("X-Odoo-Database") != o.Database {
		fail(&odooError{http.StatusNotFound, "werkzeug.exceptions.NotFound", "404 Not Found: database not found (set X-Odoo-Database)"})
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/json/2/"), "/")
	if len(parts) != 2 {
		fail(&odooError{http.StatusNotFound, "werkzeug.exceptions.NotFound", "404 Not Found"})
		return
	}
	model, method := parts[0], parts[1]
	var args map[string]any
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(&args); err != nil {
		fail(&odooError{http.StatusBadRequest, "werkzeug.exceptions.BadRequest", "400 Bad Request: the body must be a JSON object"})
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.Calls[model+"/"+method]++
	if _, ok := o.models[model]; !ok {
		fail(&odooError{http.StatusNotFound, "werkzeug.exceptions.NotFound", fmt.Sprintf("404 Not Found: the model %q does not exist", model)})
		return
	}
	out, err := o.dispatch(model, method, args)
	if err != nil {
		var e *odooError
		if ok := asOdoo(err, &e); ok {
			fail(e)
			return
		}
		fail(&odooError{http.StatusInternalServerError, "builtins.Exception", err.Error()})
		return
	}
	reply(w, http.StatusOK, out)
}

func asOdoo(err error, target **odooError) bool {
	e, ok := err.(*odooError)
	if ok {
		*target = e
	}
	return ok
}

func (o *Odoo) dispatch(model, method string, args map[string]any) (any, error) {
	switch method {
	case "context_get":
		return map[string]any{"lang": "en_US", "tz": "Europe/Berlin", "uid": 2}, nil
	case "fields_get":
		return o.models[model], nil
	case "search_read", "search", "search_count":
		ids, err := o.search(model, args["domain"])
		if err != nil {
			return nil, err
		}
		if method == "search_count" {
			return len(ids), nil
		}
		o.order(model, ids, fmt.Sprint(args["order"]))
		ids = page(ids, args["offset"], args["limit"])
		if method == "search" {
			return ids, nil
		}
		return o.read(model, ids, args["fields"])
	case "read":
		ids, err := idList(args["ids"])
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			if o.records[model][id] == nil {
				return nil, missing("Record does not exist or has been deleted.\n(Record: %s(%d,), User: 2)", model, id)
			}
		}
		return o.read(model, ids, args["fields"])
	case "create":
		if model != "sale.order" {
			return nil, &odooError{http.StatusForbidden, "odoo.exceptions.AccessError", "You are not allowed to create '" + model + "' records."}
		}
		list, _ := args["vals_list"].([]any)
		if len(list) == 0 {
			return nil, &odooError{http.StatusUnprocessableEntity, "builtins.TypeError", "create() missing 1 required argument: 'vals_list'"}
		}
		var ids []int64
		for _, x := range list {
			vals, _ := x.(map[string]any)
			id, err := o.createOrder(vals)
			if err != nil {
				return nil, err
			}
			ids = append(ids, id)
		}
		return ids, nil
	case "onchange":
		if model != "sale.order" {
			return nil, userError("the fake computes sale.order forms only")
		}
		vals, _ := args["values"].(map[string]any)
		order, lines, err := o.compute(vals)
		if err != nil {
			return nil, err
		}
		var cmds []any
		for _, l := range lines {
			cmds = append(cmds, []any{0, 0, map[string]any{"product_id": l["product_id"], "product_uom_qty": l["product_uom_qty"],
				"price_unit": l["price_unit"], "price_subtotal": l["price_subtotal"]}})
		}
		return map[string]any{"value": map[string]any{"amount_untaxed": order["amount_untaxed"], "amount_total": order["amount_total"], "order_line": cmds}}, nil
	case "action_cancel", "unlink":
		if model != "sale.order" {
			return nil, &odooError{http.StatusForbidden, "odoo.exceptions.AccessError", "You are not allowed to modify '" + model + "' records."}
		}
		ids, err := idList(args["ids"])
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			rec := o.records[model][id]
			if rec == nil {
				return nil, missing("Record does not exist or has been deleted.\n(Record: %s(%d,), User: 2)", model, id)
			}
			if method == "unlink" {
				if rec["state"] != "draft" && rec["state"] != "cancel" {
					return nil, userError("You can not delete a sent quotation or a confirmed sales order. You must first cancel it.")
				}
				for _, lid := range rec["order_line"].([]int64) {
					delete(o.records["sale.order.line"], lid)
				}
				delete(o.records[model], id)
				continue
			}
			rec["state"] = "cancel"
			rec["write_date"] = o.stamp()
		}
		return true, nil
	}
	return nil, &odooError{http.StatusNotFound, "werkzeug.exceptions.NotFound", fmt.Sprintf("404 Not Found: the method %s.%s does not exist or is private", model, method)}
}

func (o *Odoo) stamp() string { return o.now().UTC().Format("2006-01-02 15:04:05") }

func num(v any) (float64, bool) {
	switch x := v.(type) {
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case float64:
		return x, true
	case int64:
		return float64(x), true
	}
	return 0, false
}

func idList(v any) ([]int64, error) {
	list, ok := v.([]any)
	if !ok {
		return nil, &odooError{http.StatusUnprocessableEntity, "builtins.TypeError", "ids must be a list of integers"}
	}
	var ids []int64
	for _, x := range list {
		f, ok := num(x)
		if !ok || f != math.Trunc(f) {
			return nil, &odooError{http.StatusUnprocessableEntity, "builtins.TypeError", "ids must be a list of integers"}
		}
		ids = append(ids, int64(f))
	}
	return ids, nil
}

// compute validates an order's values and prices it, without storing it.
func (o *Odoo) compute(vals map[string]any) (map[string]any, []map[string]any, error) {
	for k := range vals {
		f, ok := o.models["sale.order"][k]
		if !ok {
			return nil, nil, validation("Invalid field %q on model 'sale.order'", k)
		}
		if f.Readonly && k != "name" {
			return nil, nil, validation("Field %q on model 'sale.order' is read-only", k)
		}
	}
	for name, f := range o.models["sale.order"] {
		if f.Required && !f.Readonly && name != "date_order" && (vals[name] == nil || vals[name] == false || vals[name] == "") {
			return nil, nil, validation("The operation cannot be completed: Missing required fields on Sales Order: %s", f.String)
		}
	}
	pid, ok := num(vals["partner_id"])
	if !ok || o.records["res.partner"][int64(pid)] == nil {
		return nil, nil, missing("Record does not exist or has been deleted.\n(Record: res.partner(%v,), User: 2)", vals["partner_id"])
	}
	order := map[string]any{"partner_id": int64(pid)}
	for k, v := range vals {
		if k != "order_line" && k != "partner_id" {
			order[k] = v
		}
	}
	if d, ok := vals["date_order"].(string); ok && d != "" {
		t, err := time.Parse("2006-01-02 15:04:05", d)
		if err != nil {
			t2, err2 := time.Parse("2006-01-02", d)
			if err2 != nil {
				return nil, nil, validation("'%s' is not a valid datetime (use YYYY-MM-DD HH:MM:SS)", d)
			}
			t = t2
		}
		order["date_order"] = t.Format("2006-01-02 15:04:05")
	} else {
		order["date_order"] = o.stamp()
	}
	cmds, _ := vals["order_line"].([]any)
	var lines []map[string]any
	total := 0.0
	for i, x := range cmds {
		cmd, _ := x.([]any)
		if len(cmd) != 3 {
			return nil, nil, validation("order_line[%d]: a one2many command is [0, 0, {values}]", i)
		}
		if c, _ := num(cmd[0]); c != 0 {
			return nil, nil, validation("order_line[%d]: the fake takes create commands (0) only", i)
		}
		lv, _ := cmd[2].(map[string]any)
		for k := range lv {
			if _, ok := o.models["sale.order.line"][k]; !ok {
				return nil, nil, validation("Invalid field %q on model 'sale.order.line'", k)
			}
		}
		prod, ok := num(lv["product_id"])
		p := o.records["product.product"][int64(prod)]
		if !ok || p == nil {
			return nil, nil, missing("Record does not exist or has been deleted.\n(Record: product.product(%v,), User: 2)", lv["product_id"])
		}
		qty, ok := num(lv["product_uom_qty"])
		if !ok {
			qty = 1
		}
		if qty <= 0 {
			return nil, nil, validation("order_line[%d]: the quantity must be positive", i)
		}
		price, ok := num(lv["price_unit"])
		if !ok {
			price = p["list_price"].(float64) // the pricelist's price
		}
		sub := math.Round(qty*price*100) / 100
		total += sub
		name, _ := lv["name"].(string)
		if name == "" {
			name = p["display_name"].(string)
		}
		lines = append(lines, map[string]any{"product_id": int64(prod), "product_uom_qty": qty, "price_unit": price, "price_subtotal": sub, "name": name})
	}
	order["amount_untaxed"] = math.Round(total*100) / 100
	order["amount_total"] = order["amount_untaxed"]
	return order, lines, nil
}

func (o *Odoo) createOrder(vals map[string]any) (int64, error) {
	if o.FailCreates {
		return 0, validation("The order could not be saved: a custom constraint rejected it.")
	}
	if ref, ok := vals["client_order_ref"].(string); ok && len(ref) > 0 && strings.ContainsAny(ref, "\x00") {
		return 0, validation("Invalid customer reference")
	}
	order, lines, err := o.compute(vals)
	if err != nil {
		return 0, err
	}
	id := o.nextID["sale.order"]
	o.nextID["sale.order"]++
	name := fmt.Sprintf("S%05d", o.next)
	o.next++
	order["id"], order["name"], order["display_name"], order["state"], order["write_date"] = id, name, name, "draft", o.stamp()
	var lineIDs []int64
	for _, l := range lines {
		lid := o.nextID["sale.order.line"]
		o.nextID["sale.order.line"]++
		l["id"], l["order_id"], l["display_name"], l["write_date"] = lid, id, name+" - "+l["name"].(string), o.stamp()
		o.records["sale.order.line"][lid] = l
		lineIDs = append(lineIDs, lid)
	}
	order["order_line"] = lineIDs
	o.records["sale.order"][id] = order
	return id, nil
}

// search evaluates a domain: terms [field, op, value] (=, !=, >, <, >=, <=,
// in, ilike), ANDed; "&" is accepted, "|" and "!" are not supported.
func (o *Odoo) search(model string, domain any) ([]int64, error) {
	terms, _ := domain.([]any)
	var conds [][]any
	for _, t := range terms {
		switch x := t.(type) {
		case string:
			if x != "&" {
				return nil, validation("the fake supports no domain operator %q", x)
			}
		case []any:
			if len(x) != 3 {
				return nil, validation("Invalid domain term %v", x)
			}
			f, _ := x[0].(string)
			if _, ok := o.models[model][f]; !ok {
				return nil, validation("Invalid field %s.%s in leaf %v", model, f, x)
			}
			conds = append(conds, x)
		default:
			return nil, validation("Invalid domain term %v", t)
		}
	}
	var ids []int64
	for id, rec := range o.records[model] {
		ok := true
		for _, c := range conds {
			if !match(rec[c[0].(string)], fmt.Sprint(c[1]), c[2]) {
				ok = false
				break
			}
		}
		if ok {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

func match(got any, op string, want any) bool {
	if g, ok := num(got); ok {
		w, ok := num(want)
		if !ok && op != "in" {
			return false
		}
		switch op {
		case "=":
			return g == w
		case "!=":
			return g != w
		case ">":
			return g > w
		case "<":
			return g < w
		case ">=":
			return g >= w
		case "<=":
			return g <= w
		case "in":
			list, _ := want.([]any)
			for _, x := range list {
				if n, ok := num(x); ok && n == g {
					return true
				}
			}
			return false
		}
		return false
	}
	gs, ws := fmt.Sprint(got), fmt.Sprint(want)
	switch op {
	case "=":
		return got != nil && gs == ws
	case "!=":
		return gs != ws
	case ">":
		return gs > ws
	case "<":
		return gs < ws
	case ">=":
		return gs >= ws
	case "<=":
		return gs <= ws
	case "ilike":
		return strings.Contains(strings.ToLower(gs), strings.ToLower(ws))
	}
	return false
}

func (o *Odoo) order(model string, ids []int64, spec string) {
	if !strings.HasPrefix(spec, "write_date") {
		return
	}
	recs := o.records[model]
	sort.SliceStable(ids, func(i, j int) bool {
		a, b := recs[ids[i]]["write_date"].(string), recs[ids[j]]["write_date"].(string)
		if a != b {
			return a < b
		}
		return ids[i] < ids[j]
	})
}

func page(ids []int64, offset, limit any) []int64 {
	if n, ok := num(offset); ok && int(n) > 0 {
		if int(n) >= len(ids) {
			return nil
		}
		ids = ids[int(n):]
	}
	if n, ok := num(limit); ok && int(n) > 0 && int(n) < len(ids) {
		ids = ids[:int(n)]
	}
	return ids
}

func (o *Odoo) read(model string, ids []int64, fields any) ([]map[string]any, error) {
	names, _ := fields.([]any)
	out := []map[string]any{}
	for _, id := range ids {
		rec := o.records[model][id]
		row := map[string]any{"id": id}
		for _, x := range names {
			f, _ := x.(string)
			info, ok := o.models[model][f]
			if !ok {
				return nil, validation("Invalid field %q on model %q", f, model)
			}
			v, present := rec[f]
			switch {
			case info.Type == "many2one" && present && v != nil:
				ref, _ := num(v)
				target := o.records[info.Relation][int64(ref)]
				dn := ""
				if target != nil {
					dn, _ = target["display_name"].(string)
				}
				row[f] = []any{int64(ref), dn}
			case !present || v == nil:
				row[f] = false
			default:
				row[f] = v
			}
		}
		out = append(out, row)
	}
	return out, nil
}
