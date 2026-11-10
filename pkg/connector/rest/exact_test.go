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

// Changed orders are read on Modified (/Date(ms)/), at most 60 at once: a
// poll asks for no more, and leaves the timestamp a full page ends in for
// the next one. An order changed again is a new event of the same order.
func TestExactChangedOrders(t *testing.T) {
	x, seed := exacttest.New("exact-app", "s3cret")
	t.Cleanup(x.Close)
	clock := time.Date(2026, 11, 9, 9, 0, 0, 0, time.UTC)
	x.StartAt(1, func() time.Time { return clock })
	c := newExactConn(t, x, seed, &connector.MemoryTokens{})
	ctx := context.Background()
	// 59 orders in one second, then two a second later: a page of 60 ends
	// inside the second second, which waits whole for the next poll.
	for i := 0; i < 61; i++ {
		if i == 59 {
			clock = clock.Add(time.Second)
		}
		if _, err := c.Commit(ctx, "create-sales-order", "SHOPIFY-"+string(rune('A'+i/26))+string(rune('a'+i%26)), json.RawMessage(exactOrder)); err != nil {
			t.Fatal(err)
		}
	}
	first, err := c.Poll(ctx, "SalesOrder.Changed", 0, 100)
	if err != nil || len(first) != 59 {
		t.Fatalf("first poll: %d events, %v", len(first), err)
	}
	rest, err := c.Poll(ctx, "SalesOrder.Changed", first[58].Position, 100)
	if err != nil || len(rest) != 2 || !strings.Contains(string(rest[1].Payload), `"OrderNumber":61`) {
		t.Fatalf("second poll: %d events, %v", len(rest), err)
	}
	clock = clock.Add(time.Minute)
	order := connector.RecordID(first[0].ID)
	x.Touch(order, "changed")
	again, err := c.Poll(ctx, "SalesOrder.Changed", rest[1].Position, 100)
	if err != nil || len(again) != 1 || connector.RecordID(again[0].ID) != order || again[0].ID == first[0].ID {
		t.Fatalf("after a change: %+v, %v", again, err)
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

// More than 60 orders changed within one second are all read: the cursor
// keeps its milliseconds, so a poll does not re-read the start of its
// second and fill the 60 rows with orders it has passed.
func TestExactReadsABurstWithinOneSecond(t *testing.T) {
	x, seed := exacttest.New("exact-app", "s3cret")
	t.Cleanup(x.Close)
	clock := time.Date(2026, 11, 9, 9, 0, 0, 5_000_000, time.UTC)
	x.StartAt(1, func() time.Time { return clock })
	c := newExactConn(t, x, seed, &connector.MemoryTokens{})
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		clock = clock.Add(5 * time.Millisecond)
		if _, err := c.Commit(ctx, "create-sales-order", fmt.Sprintf("SHOPIFY-%d", i), json.RawMessage(exactOrder)); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	var after int64
	for poll := 0; poll < 5; poll++ {
		evs, err := c.Poll(ctx, "SalesOrder.Changed", after, 100)
		if err != nil {
			t.Fatal(err)
		}
		for _, ev := range evs {
			seen[ev.ID] = true
			after = ev.Position
		}
	}
	if len(seen) != 100 {
		t.Fatalf("seen %d of 100 orders", len(seen))
	}
}

// A full page of orders sharing one timestamp cannot be passed without
// skipping some: the poll says so instead of moving on.
func TestExactFullPageOfOneTimestampIsAnError(t *testing.T) {
	x, seed := exacttest.New("exact-app", "s3cret")
	t.Cleanup(x.Close)
	clock := time.Date(2026, 11, 9, 9, 0, 0, 0, time.UTC)
	x.StartAt(1, func() time.Time { return clock })
	c := newExactConn(t, x, seed, &connector.MemoryTokens{})
	ctx := context.Background()
	for i := 0; i < 61; i++ {
		if _, err := c.Commit(ctx, "create-sales-order", fmt.Sprintf("SHOPIFY-%d", i), json.RawMessage(exactOrder)); err != nil {
			t.Fatal(err)
		}
	}
	if evs, err := c.Poll(ctx, "SalesOrder.Changed", 0, 100); err == nil || !strings.Contains(err.Error(), "share one Modified") {
		t.Fatalf("%d events, %v", len(evs), err)
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
