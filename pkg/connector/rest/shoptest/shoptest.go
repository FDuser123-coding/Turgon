// Package shoptest is a fake Shopify Admin REST API for tests and demos:
// orders and customers, listed with since_id, read by ID and updated with
// PUT, authenticated with X-Shopify-Access-Token. It keeps only what the
// REST connector's example connection uses.
package shoptest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Version is the Admin API version the fake serves under /admin/api/.
const Version = "2026-07"

// Shop is the fake store.
type Shop struct {
	Token string

	mu        sync.Mutex
	orders    map[int64]map[string]any
	customers map[int64]map[string]any
	nextID    int64
	// FailUpdates makes order updates fail with 422, as when Shopify
	// rejects a field.
	FailUpdates bool
	// Requests counts requests by "METHOD path".
	Requests map[string]int

	srv *httptest.Server
}

// New starts a fake store on a local port.
func New(token string) *Shop {
	s := NewShop(token)
	s.srv = httptest.NewServer(s)
	return s
}

// NewShop returns a store without starting a server, to serve with Handler.
func NewShop(token string) *Shop {
	return &Shop{Token: token, orders: map[int64]map[string]any{}, customers: map[int64]map[string]any{}, nextID: 450789469, Requests: map[string]int{}}
}

// URL is the Admin API base URL for the connection's config.
func (s *Shop) URL() string { return s.srv.URL + "/admin/api/" + Version }

func (s *Shop) Close() { s.srv.Close() }

// AddOrder stores an order and returns its ID. Orders get increasing IDs.
func (s *Shop) AddOrder(order map[string]any) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	o := clone(order)
	o["id"] = s.nextID
	o["admin_graphql_api_id"] = fmt.Sprintf("gid://shopify/Order/%d", s.nextID)
	if _, ok := o["note"]; !ok {
		o["note"] = nil
	}
	s.orders[s.nextID] = o
	return s.nextID
}

// AddCustomer stores a customer under id.
func (s *Shop) AddCustomer(id int64, c map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c = clone(c)
	c["id"] = id
	s.customers[id] = c
}

// Order returns a copy of an order, or nil.
func (s *Shop) Order(id int64) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if o, ok := s.orders[id]; ok {
		return clone(o)
	}
	return nil
}

func clone(m map[string]any) map[string]any {
	b, _ := json.Marshal(m)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}

func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// ServeHTTP implements the API.
func (s *Shop) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := strings.TrimPrefix(r.URL.Path, "/admin/api/"+Version)
	s.Requests[r.Method+" "+path]++
	if r.Header.Get("X-Shopify-Access-Token") != s.Token || s.Token == "" {
		write(w, http.StatusUnauthorized, map[string]any{"errors": "[API] Invalid API key or access token (unrecognized login or wrong password)"})
		return
	}
	switch {
	case r.Method == http.MethodGet && path == "/shop.json":
		write(w, http.StatusOK, map[string]any{"shop": map[string]any{"name": "Turgon demo store"}})
	case r.Method == http.MethodGet && path == "/orders.json":
		since, _ := strconv.ParseInt(r.URL.Query().Get("since_id"), 10, 64)
		limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
		if err != nil || limit <= 0 || limit > 250 {
			limit = 50
		}
		var ids []int64
		for id := range s.orders {
			if id > since {
				ids = append(ids, id)
			}
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		if len(ids) > limit {
			ids = ids[:limit]
		}
		list := []any{}
		for _, id := range ids {
			list = append(list, s.orders[id])
		}
		write(w, http.StatusOK, map[string]any{"orders": list})
	case strings.HasPrefix(path, "/orders/") && strings.HasSuffix(path, ".json"):
		id, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(path, "/orders/"), ".json"), 10, 64)
		o, ok := s.orders[id]
		if err != nil || !ok {
			write(w, http.StatusNotFound, map[string]any{"errors": "Not Found"})
			return
		}
		switch r.Method {
		case http.MethodGet:
			write(w, http.StatusOK, map[string]any{"order": o})
		case http.MethodPut:
			if s.FailUpdates {
				write(w, http.StatusUnprocessableEntity, map[string]any{"errors": map[string]any{"note": []string{"is locked for this order"}}})
				return
			}
			var body struct {
				Order map[string]any `json:"order"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Order == nil {
				write(w, http.StatusBadRequest, map[string]any{"errors": map[string]any{"order": "Required parameter missing or invalid"}})
				return
			}
			for k, v := range body.Order {
				if k != "id" && k != "admin_graphql_api_id" {
					o[k] = v
				}
			}
			write(w, http.StatusOK, map[string]any{"order": o})
		default:
			write(w, http.StatusMethodNotAllowed, map[string]any{"errors": "method not allowed"})
		}
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/customers/") && strings.HasSuffix(path, ".json"):
		id, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(path, "/customers/"), ".json"), 10, 64)
		c, ok := s.customers[id]
		if err != nil || !ok {
			write(w, http.StatusNotFound, map[string]any{"errors": "Not Found"})
			return
		}
		write(w, http.StatusOK, map[string]any{"customer": c})
	default:
		write(w, http.StatusNotFound, map[string]any{"errors": "Not Found"})
	}
}
