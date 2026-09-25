// Package stripetest is a fake Stripe API for tests and demos: paid
// invoices, the invoice.paid events announcing them, and metadata updates.
// Like Stripe, lists are newest first and paged with starting_after and
// has_more, requests carry a secret key as a bearer token, updates are
// form-encoded, and an Idempotency-Key replays the first response.
package stripetest

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

// Stripe is the fake account.
type Stripe struct {
	Key string

	mu       sync.Mutex
	invoices map[string]map[string]any
	events   []map[string]any // oldest first
	n        int
	clock    int64
	replays  map[string][]byte
	// FailUpdates makes invoice updates fail with 400, as when Stripe
	// rejects a parameter.
	FailUpdates bool
	// Requests counts requests by "METHOD path".
	Requests map[string]int

	srv *httptest.Server
}

// New starts a fake account on a local port.
func New(key string) *Stripe {
	s := NewStripe(key)
	s.srv = httptest.NewServer(s)
	return s
}

// NewStripe returns an account without starting a server.
func NewStripe(key string) *Stripe {
	return &Stripe{Key: key, invoices: map[string]map[string]any{}, replays: map[string][]byte{}, Requests: map[string]int{},
		clock: time.Date(2026, 9, 25, 8, 0, 0, 0, time.UTC).Unix()}
}

// URL is the API base URL for the connection's config.
func (s *Stripe) URL() string { return s.srv.URL }

func (s *Stripe) Close() { s.srv.Close() }

// PayInvoice records a paid invoice for a customer and its invoice.paid
// event, and returns the invoice ID. Amounts are in cents. Each call is one
// second after the previous, unless sameSecond is set.
func (s *Stripe) PayInvoice(customer string, cents int64, currency string, sameSecond ...bool) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(sameSecond) == 0 || !sameSecond[0] {
		s.clock++
	}
	s.n++
	id := fmt.Sprintf("in_%04d", s.n)
	inv := map[string]any{
		"id": id, "object": "invoice", "customer": customer, "currency": currency, "status": "paid",
		"amount_due": cents, "amount_paid": cents, "number": fmt.Sprintf("TURGON-%04d", s.n),
		"status_transitions": map[string]any{"paid_at": s.clock}, "metadata": map[string]any{}, "created": s.clock - 60,
	}
	s.invoices[id] = inv
	s.events = append(s.events, map[string]any{
		"id": fmt.Sprintf("evt_%04d", s.n), "object": "event", "type": "invoice.paid", "created": s.clock,
		"data": map[string]any{"object": clone(inv)},
	})
	return id
}

// Invoice returns a copy of an invoice, or nil.
func (s *Stripe) Invoice(id string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if inv, ok := s.invoices[id]; ok {
		return clone(inv)
	}
	return nil
}

func clone(m map[string]any) map[string]any {
	b, _ := json.Marshal(m)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}

func stripeError(w http.ResponseWriter, status int, typ, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"type": typ, "message": msg}})
}

func (s *Stripe) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Requests[r.Method+" "+r.URL.Path]++
	if r.Header.Get("Authorization") != "Bearer "+s.Key || s.Key == "" {
		stripeError(w, http.StatusUnauthorized, "invalid_request_error", "Invalid API Key provided.")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if r.Method == http.MethodPost && key != "" {
		if prev, ok := s.replays[key]; ok {
			w.Header().Set("Idempotent-Replayed", "true")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(prev)
			return
		}
	}
	status, body := s.route(r)
	b, _ := json.Marshal(body)
	if r.Method == http.MethodPost && key != "" && status == http.StatusOK {
		s.replays[key] = b
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

func (s *Stripe) route(r *http.Request) (int, any) {
	notFound := map[string]any{"error": map[string]any{"type": "invalid_request_error", "message": "No such resource"}}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/balance":
		return http.StatusOK, map[string]any{"object": "balance", "livemode": false}
	case r.Method == http.MethodGet && r.URL.Path == "/v1/events":
		q := r.URL.Query()
		limit, err := strconv.Atoi(q.Get("limit"))
		if err != nil || limit < 1 || limit > 100 {
			limit = 10
		}
		gt, _ := strconv.ParseInt(q.Get("created[gt]"), 10, 64)
		var list []map[string]any
		for i := len(s.events) - 1; i >= 0; i-- { // newest first
			e := s.events[i]
			if t := q.Get("type"); t != "" && e["type"] != t {
				continue
			}
			if e["created"].(int64) > gt {
				list = append(list, e)
			}
		}
		sort.SliceStable(list, func(i, j int) bool { return list[i]["created"].(int64) > list[j]["created"].(int64) })
		if after := q.Get("starting_after"); after != "" {
			for i, e := range list {
				if e["id"] == after {
					list = list[i+1:]
					break
				}
			}
		}
		more := len(list) > limit
		if more {
			list = list[:limit]
		}
		data := []any{}
		for _, e := range list {
			data = append(data, e)
		}
		return http.StatusOK, map[string]any{"object": "list", "data": data, "has_more": more, "url": "/v1/events"}
	case strings.HasPrefix(r.URL.Path, "/v1/invoices/"):
		id := strings.TrimPrefix(r.URL.Path, "/v1/invoices/")
		inv, ok := s.invoices[id]
		if !ok {
			return http.StatusNotFound, notFound
		}
		switch r.Method {
		case http.MethodGet:
			return http.StatusOK, inv
		case http.MethodPost:
			if s.FailUpdates {
				return http.StatusBadRequest, map[string]any{"error": map[string]any{"type": "invalid_request_error", "message": "This invoice is locked for accounting review."}}
			}
			if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
				return http.StatusBadRequest, map[string]any{"error": map[string]any{"type": "invalid_request_error", "message": "Invalid request: expected a form-encoded body."}}
			}
			if err := r.ParseForm(); err != nil {
				return http.StatusBadRequest, notFound
			}
			md := inv["metadata"].(map[string]any)
			for k, v := range r.PostForm {
				name, ok := strings.CutPrefix(k, "metadata[")
				if !ok || !strings.HasSuffix(name, "]") {
					return http.StatusBadRequest, map[string]any{"error": map[string]any{"type": "invalid_request_error", "message": "Received unknown parameter: " + k}}
				}
				name = strings.TrimSuffix(name, "]")
				if v[0] == "" {
					delete(md, name) // Stripe unsets a key given an empty value
				} else {
					md[name] = v[0]
				}
			}
			return http.StatusOK, inv
		}
	}
	return http.StatusNotFound, notFound
}
