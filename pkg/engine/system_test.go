package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// asSlot edits a recipe's spec as a blueprint would compile it with the
// endpoint filling a slot: the endpoint is renamed to the slot, and the
// system its records are linked by stays the connection's name
// (TestBlueprintNamesSystemsByConnection checks the compiler does that).
func asSlot(connection, slot string) func(*compiler.RuntimeSpec) {
	return func(spec *compiler.RuntimeSpec) {
		for i := range spec.Spec.Connectors {
			if c := &spec.Spec.Connectors[i]; c.Endpoint == connection {
				c.Endpoint = slot
			}
		}
		for i := range spec.Spec.Workflows {
			wf := &spec.Spec.Workflows[i]
			if wf.Trigger.Endpoint == connection {
				wf.Trigger.Endpoint, wf.Trigger.System = slot, connection
			}
			for _, st := range wf.Steps {
				if w := st.Write; w != nil && w.Endpoint == connection {
					w.Endpoint, w.System = slot, connection
				}
			}
		}
	}
}

// A saga in a stack writes to QuickBooks through the erp slot, and the
// deletion recipe on its own reads QuickBooks by its connection. The link
// is kept by the connection's name, so the deletion resolves back.
func TestSlotLinksResolveByConnection(t *testing.T) {
	_, shop, q, deleted, edited := newQuickBooksFixturesRW(t)
	f := edited("shopify-store-orders-to-quickbooks", asSlot("quickbooks", "erp"))
	clock := time.Now().Add(-time.Hour).Truncate(time.Second)
	q.StartAt(1001, func() time.Time { clock = clock.Add(time.Second); return clock })
	id := shop.AddOrder(map[string]any{
		"name": "#1001", "email": "Ada@Example.com", "created_at": "2026-11-05T09:30:00-04:00",
		"subtotal_price": "420.00", "currency": "usd", "line_items": []any{map[string]any{"sku": "M-1", "quantity": 2, "price": "210.00"}},
	})
	if _, err, _ := f.run(f.dispatch()[0], approve("04-write")); err != nil {
		t.Fatal(err)
	}
	f.linked("quickbooks", id)
	f.unlinked("erp")

	g := deleted()
	g.dispatch()
	q.Delete(q.ReceiptByDocNumber(fmt.Sprintf("SHOP%d", id))["Id"].(string))
	runs := g.dispatch()
	if len(runs) != 1 {
		t.Fatalf("runs = %d", len(runs))
	}
	res, err, _ := g.run(runs[0])
	if err != nil || len(res.Writes) != 1 || res.Writes[0].Status != writeguard.StatusCommitted {
		t.Fatalf("deleted receipt: %+v, %v", res, err)
	}
	if got, _ := shop.Order(id)["note"].(string); !strings.HasPrefix(got, "ERP order deleted in QuickBooks Online") {
		t.Fatalf("Shopify note = %q", got)
	}
}

// Links made before systems were named by connection are kept under the
// slot's name. A resolve that misses under the connection's name reads
// the slot's, and moves the link (with its attributes) to the connection.
func TestEndpointLinksMoveToTheConnection(t *testing.T) {
	f, shop, q, _, edited := newQuickBooksFixturesRW(t)
	clock := time.Now().Add(-time.Hour).Truncate(time.Second)
	q.StartAt(1001, func() time.Time { clock = clock.Add(time.Second); return clock })
	id := shop.AddOrder(map[string]any{
		"name": "#1001", "email": "Ada@Example.com", "created_at": "2026-11-05T09:30:00-04:00",
		"subtotal_price": "420.00", "currency": "usd", "line_items": []any{map[string]any{"sku": "M-1", "quantity": 2, "price": "210.00"}},
	})
	if _, err, _ := f.run(f.dispatch()[0], approve("04-write")); err != nil {
		t.Fatal(err)
	}
	// As a stack linked it before: under the slot's name, with attributes.
	ctx := context.Background()
	if _, err := f.pool.Exec(ctx, `UPDATE turgon_xref SET system = 'erp', attributes = '{"exact:doc":"x"}' WHERE entity = 'SalesOrder' AND system = 'quickbooks'`); err != nil {
		t.Fatal(err)
	}

	g := edited("quickbooks-deleted-receipts-to-shopify-note", asSlot("quickbooks", "erp"))
	g.dispatch()
	q.Delete(q.ReceiptByDocNumber(fmt.Sprintf("SHOP%d", id))["Id"].(string))
	runs := g.dispatch()
	if len(runs) != 1 {
		t.Fatalf("runs = %d", len(runs))
	}
	if res, err, _ := g.run(runs[0]); err != nil || len(res.Writes) != 1 {
		t.Fatalf("deleted receipt: %+v, %v", res, err)
	}
	f.linked("quickbooks", id)
	f.unlinked("erp")
	var attrs string
	if err := f.pool.QueryRow(ctx, `SELECT attributes->>'exact:doc' FROM turgon_xref WHERE entity = 'SalesOrder' AND system = 'quickbooks'`).Scan(&attrs); err != nil || attrs != "x" {
		t.Fatalf("moved link's attributes: %q, %v", attrs, err)
	}
}
