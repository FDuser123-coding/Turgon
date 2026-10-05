package sap_test

import (
	"context"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/meta"
)

func TestDiscover(t *testing.T) {
	_, conn, _ := setup(t)
	cat, err := conn.Discover(context.Background(), []string{"API_BUSINESS_PARTNER"})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, o := range cat.Objects {
		names = append(names, o.Name)
	}
	if strings.Join(names, ",") != "A_Customer,A_SalesOrder,A_SalesOrderItem,A_SalesOrderSimulation" {
		t.Fatalf("objects %v", names)
	}
	so, _ := cat.Object("A_SalesOrder")
	key, _ := so.Field("SalesOrder")
	if !key.Key || key.Required || !key.ReadOnly || key.Length != 10 || key.Label != "Sales Order" || key.Type != "Edm.String" {
		t.Fatalf("SalesOrder %+v", key)
	}
	ref, _ := so.Field("PurchaseOrderByCustomer")
	if ref.Length != 35 || ref.Label != "Customer Reference" {
		t.Fatalf("reference %+v", ref)
	}
	if len(so.Links) != 1 || so.Links[0].Name != "to_Item" || so.Links[0].To != "A_SalesOrderItem" {
		t.Fatalf("links %+v", so.Links)
	}
	u := meta.Usage(cat, nil)
	for key, want := range map[string]string{
		"A_SalesOrder.PurchaseOrderByCustomer": "operation create-sales-order",
		"A_SalesOrder.SalesOrderType":          "operation create-sales-order",
		"A_SalesOrderItem.Material":            "operation create-sales-order (items)",
	} {
		if !strings.Contains(strings.Join(u[key], ";"), want) {
			t.Errorf("%s used by %v, want %s", key, u[key], want)
		}
	}
	if cat.Events["SalesOrder.Changed"] != "A_SalesOrder" {
		t.Fatalf("events %v", cat.Events)
	}
	cr := meta.Creators(cat)
	if strings.Join(cr["A_SalesOrder"], ";") != "operation create-sales-order" || strings.Join(cr["A_SalesOrderItem"], ";") != "operation create-sales-order (items)" {
		t.Fatalf("creators %v", cr)
	}
}
