package rest_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/rest"
	"github.com/fduser123-coding/turgon/pkg/connector/rest/exacttest"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

func newExactConn(t *testing.T, x *exacttest.Exact, refresh string, tokens connector.TokenStore) *rest.Conn {
	t.Helper()
	cfg := exampleConfig(t, "exact-online", func(s string) string {
		return strings.ReplaceAll(s, "https://start.exactonline.nl", x.URL())
	})
	c, err := rest.New(cfg, `{"clientId": "exact-app", "clientSecret": "s3cret", "refreshToken": "`+refresh+`"}`, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	c.UseTokens(tokens)
	return c
}

const exactOrder = `{"accountId": "` + exacttest.Lovelace + `", "orderDate": "2026-11-09", "description": "Shopify order SHOPIFY-1",
	"exactLines": [{"Item": "` + exacttest.Desk + `", "Quantity": 2, "UnitPrice": 210}, {"Item": "` + exacttest.Chair + `", "Quantity": 1}]}`

// An order and its lines go in one POST; its result is d. The key goes in
// YourRef, and a retried create finds the first order by it instead of
// making a second. The undo deletes the order, which Exact refuses once it
// is delivered.
func TestExactOrderRetryAndDelete(t *testing.T) {
	x, seed := exacttest.New("exact-app", "s3cret")
	t.Cleanup(x.Close)
	c := newExactConn(t, x, seed, &connector.MemoryTokens{})
	ctx := context.Background()
	res, err := c.Commit(ctx, "create-sales-order", "SHOPIFY-1", json.RawMessage(exactOrder))
	if err != nil {
		t.Fatal(err)
	}
	var o struct {
		OrderID, YourRef string
		OrderNumber      int
		AmountDC         float64
	}
	_ = json.Unmarshal(res, &o)
	// 2 x 210.00 at the shop's price, and the chair at Exact's 235.00.
	if o.OrderID == "" || o.OrderNumber != 1 || o.YourRef != "SHOPIFY-1" || o.AmountDC != 655 {
		t.Fatalf("result %s", res)
	}
	again, err := c.Commit(ctx, "create-sales-order", "SHOPIFY-1", json.RawMessage(exactOrder))
	if err != nil || x.Orders() != 1 || x.Requests["POST salesorder/SalesOrders"] != 1 || !strings.Contains(string(again), o.OrderID) {
		t.Fatalf("retry: %s, %v, orders %d, posts %d", again, err, x.Orders(), x.Requests["POST salesorder/SalesOrders"])
	}
	if _, err := c.Commit(ctx, "delete-sales-order", "SHOPIFY-1#compensate", res); err != nil || x.Orders() != 0 {
		t.Fatalf("delete: %v, orders %d", err, x.Orders())
	}

	// A delivered order stays: the refusal is final, not retried.
	res, err = c.Commit(ctx, "create-sales-order", "SHOPIFY-2", json.RawMessage(exactOrder))
	if err != nil {
		t.Fatal(err)
	}
	_ = json.Unmarshal(res, &o)
	x.Deliver(o.OrderID)
	if _, err := c.Commit(ctx, "delete-sales-order", "SHOPIFY-2#compensate", res); !errors.Is(err, writeguard.ErrInvalid) || !strings.Contains(err.Error(), "delivered") {
		t.Fatalf("deleting a delivered order: %v", err)
	}
	// A blocked account is Exact's refusal, final too.
	blocked := strings.Replace(exactOrder, exacttest.Lovelace, exacttest.Hopper, 1)
	if _, err := c.Commit(ctx, "create-sales-order", "SHOPIFY-3", json.RawMessage(blocked)); !errors.Is(err, writeguard.ErrInvalid) || !strings.Contains(err.Error(), "blocked") {
		t.Fatalf("a blocked account: %v", err)
	}
}

// Exact gives the token's lifetime as a string and takes the client's
// credentials in the form; the refresh token rotates, kept in the store.
// The customer is read by ID, and check reads one order.
func TestExactTokensReadAndCheck(t *testing.T) {
	x, seed := exacttest.New("exact-app", "s3cret")
	t.Cleanup(x.Close)
	store := &connector.MemoryTokens{}
	c := newExactConn(t, x, seed, store)
	ctx := context.Background()
	got, err := c.Read(ctx, "get-customer", exacttest.Lovelace)
	if err != nil || !strings.Contains(string(got), `"name":"Lovelace GmbH"`) || !strings.Contains(string(got), `"email":"ada@example.com"`) {
		t.Fatalf("read: %s, %v", got, err)
	}
	for _, r := range c.Check(ctx) {
		if !r.OK {
			t.Fatalf("check: %+v", r)
		}
	}
	x.ExpireAccessTokens()
	if _, err := newExactConn(t, x, seed, store).Read(ctx, "get-customer", exacttest.Hopper); err != nil {
		t.Fatalf("after expiry, another worker: %v", err)
	}
	if x.Refreshes != 2 || x.Spent != 0 {
		t.Fatalf("refreshes %d, spent %d", x.Refreshes, x.Spent)
	}
}

// A customer that does not exist is not found, rather than read as an
// empty, unblocked one.
func TestExactMissingCustomerIsNotFound(t *testing.T) {
	x, seed := exacttest.New("exact-app", "s3cret")
	t.Cleanup(x.Close)
	c := newExactConn(t, x, seed, &connector.MemoryTokens{})
	if got, err := c.Read(context.Background(), "get-customer", "00000000-0000-4000-8000-000000000000"); !errors.Is(err, writeguard.ErrNotFound) {
		t.Fatalf("%s, %v", got, err)
	}
}

// Changed orders come from the sync API, by row version: every order is
// read once however many pages it takes, an order changed again is a new
// event of it, and a change made after the clocks went back is read too.
func TestExactChangedOrdersBySyncTimestamp(t *testing.T) {
	x, seed := exacttest.New("exact-app", "s3cret")
	t.Cleanup(x.Close)
	clock := time.Date(2026, 10, 25, 2, 30, 0, 0, time.UTC)
	x.StartAt(1, func() time.Time { return clock })
	c := newExactConn(t, x, seed, &connector.MemoryTokens{})
	ctx := context.Background()
	for i := 0; i < 1200; i++ {
		if _, err := c.Commit(ctx, "create-sales-order", fmt.Sprintf("SHOPIFY-%d", i), json.RawMessage(exactOrder)); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	var after int64
	poll := func() []connector.Event {
		evs, err := c.Poll(ctx, "SalesOrder.Changed", after, 100)
		if err != nil {
			t.Fatal(err)
		}
		for _, ev := range evs {
			if seen[ev.ID] {
				t.Fatalf("event %s read twice", ev.ID)
			}
			seen[ev.ID] = true
			after = ev.Position
		}
		return evs
	}
	for i := 0; i < 20; i++ {
		if len(poll()) == 0 {
			break
		}
	}
	if len(seen) != 1200 {
		t.Fatalf("seen %d of 1200 orders", len(seen))
	}
	// The clocks go back an hour; the order changed then is still read.
	clock = clock.Add(-time.Hour)
	ids := x.OrderIDs()
	x.Touch(ids[0], "changed in the repeated hour")
	again := poll()
	if len(again) != 1 || connector.RecordID(again[0].ID) != ids[0] || !strings.Contains(string(again[0].Payload), `"OrderNumber":1`) {
		t.Fatalf("after the change: %+v", again)
	}
	if x.Requests["GET sync/SalesOrder/SalesOrderHeaders"] == 0 {
		t.Fatal("the sync API was not polled")
	}
}

// Deleted orders come from sync/Deleted, which lists deletions of every
// entity: the filter keeps sales orders, and a poll that finds only other
// deletions still moves the cursor past them with a skip marker, so they
// are not read again.
func TestExactDeletedOrders(t *testing.T) {
	x, seed := exacttest.New("exact-app", "s3cret")
	t.Cleanup(x.Close)
	c := newExactConn(t, x, seed, &connector.MemoryTokens{})
	ctx := context.Background()
	var ids []string
	for i := 0; i < 3; i++ {
		res, err := c.Commit(ctx, "create-sales-order", fmt.Sprintf("SHOPIFY-%d", i), json.RawMessage(exactOrder))
		if err != nil {
			t.Fatal(err)
		}
		var o struct{ OrderID string }
		_ = json.Unmarshal(res, &o)
		ids = append(ids, o.OrderID)
	}
	del := func(i int) {
		if _, err := c.Commit(ctx, "delete-sales-order", fmt.Sprintf("SHOPIFY-%d#compensate", i), json.RawMessage(`{"OrderID":"`+ids[i]+`"}`)); err != nil {
			t.Fatal(err)
		}
	}
	del(1)
	// More deleted accounts than a page holds, then an order.
	for i := 0; i < 1500; i++ {
		x.Deleted(exacttest.EntityAccount, fmt.Sprintf("acct-%d", i))
	}
	del(2)

	var after int64
	var got []string
	polls := 0
	for ; polls < 10; polls++ {
		evs, err := c.Poll(ctx, "SalesOrder.Deleted", after, 5000)
		if err != nil {
			t.Fatal(err)
		}
		if len(evs) == 0 {
			break
		}
		for i, ev := range evs {
			if ev.Skip {
				if i != len(evs)-1 || ev.ID != "" || len(ev.Payload) != 0 {
					t.Fatalf("skip marker %+v at %d of %d", ev, i, len(evs))
				}
			} else {
				if !strings.Contains(string(ev.Payload), `"EntityType":32`) {
					t.Fatalf("event %s: %s", ev.ID, ev.Payload)
				}
				got = append(got, ev.ID)
			}
			if ev.Position <= after {
				t.Fatalf("position %d does not move past %d", ev.Position, after)
			}
			after = ev.Position
		}
	}
	if fmt.Sprint(got) != fmt.Sprint([]string{ids[1], ids[2]}) {
		t.Fatalf("deleted orders %v, want %v", got, ids[1:])
	}
	// One page of 1000, then the rest with the order, then nothing.
	if polls != 2 || x.Requests["GET sync/Deleted"] != 3 {
		t.Fatalf("%d polls with events, %d requests", polls, x.Requests["GET sync/Deleted"])
	}

	// Only other entities' deletions: a lone skip marker, read once.
	x.Deleted(exacttest.EntityAccount, "acct-last")
	evs, err := c.Poll(ctx, "SalesOrder.Deleted", after, 100)
	if err != nil || len(evs) != 1 || !evs[0].Skip {
		t.Fatalf("only accounts deleted: %+v, %v", evs, err)
	}
	if evs, err := c.Poll(ctx, "SalesOrder.Deleted", evs[0].Position, 100); err != nil || len(evs) != 0 {
		t.Fatalf("after the skip: %+v, %v", evs, err)
	}
}
