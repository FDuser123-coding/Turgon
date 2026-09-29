package sap_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/rest"
	"github.com/fduser123-coding/turgon/pkg/connector/sap"
	"github.com/fduser123-coding/turgon/pkg/connector/sap/saptest"
)

// An XSUAA client ID, with the characters that break escaped Basic
// credentials.
const (
	meshClientID = "sb-default-5f1c!b1234|xbem-service-broker-!b2436"
	meshSecret   = `{"clientid": "` + meshClientID + `", "clientsecret": "c2VjcmV0", "tokenendpoint": "ignored", "granttype": "client_credentials"}`
	queue        = "acme/s4/turgon/salesorders"
)

func meshSetup(t *testing.T, subs map[string]sap.Subscription) (*saptest.S4, *saptest.Mesh, *sap.Conn) {
	t.Helper()
	s4 := saptest.New("TURGON_COMM", "s3cret", "100")
	s4.StartAt(5000, time.Now)
	srv := httptest.NewServer(s4)
	t.Cleanup(srv.Close)
	mesh := saptest.NewMesh(meshClientID, "c2VjcmV0")
	mesh.Redeliver = 50 * time.Millisecond
	mesh.CreateQueue(queue)
	s4.Events = mesh.Publisher(queue, "/default/sap.s4.beh/100")
	msrv := httptest.NewServer(mesh)
	t.Cleanup(msrv.Close)

	cfg := sap.Config{BaseURL: srv.URL, Client: "100", Auth: rest.Auth{Type: "basic"},
		EventMesh:     &sap.EventMesh{URL: msrv.URL, TokenURL: msrv.URL + saptest.TokenPath, SecretRef: "openbao://s4/event-mesh"},
		Subscriptions: subs}
	conn, err := sap.New(cfg, "TURGON_COMM:s3cret", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.UseEventMesh(meshSecret, msrv.Client()); err != nil {
		t.Fatal(err)
	}
	return s4, mesh, conn
}

var createdOrders = sap.Subscription{
	Queue: queue, Types: []string{saptest.SalesOrderCreated}, Idle: "100ms",
	Match: map[string]any{"SalesOrderType": "OR"},
	Read:  &sap.Read{Service: "API_SALES_ORDER_SRV", EntitySet: "A_SalesOrder", Key: "SalesOrder", Expand: []string{"to_Item"}},
}

// collect streams until n events arrived (or fails after a few seconds).
func collect(t *testing.T, conn *sap.Conn, event string, n int) []connector.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var got []connector.Event
	enough := errors.New("enough")
	err := conn.Stream(ctx, event, nil, func(evs []connector.Event, resume []byte) error {
		if resume != nil {
			t.Errorf("resume = %q; queues keep their own place", resume)
		}
		got = append(got, evs...)
		if len(got) >= n {
			return enough
		}
		return nil
	})
	if !errors.Is(err, enough) {
		t.Fatalf("stream: %v (got %d events)", err, len(got))
	}
	return got
}

func TestEventMeshDeliversCreatedOrdersWithTheirCurrentState(t *testing.T) {
	s4, mesh, conn := meshSetup(t, map[string]sap.Subscription{"SalesOrder.Created": createdOrders})
	if !conn.Streams("SalesOrder.Created") || conn.Streams("SalesOrder.Changed") {
		t.Fatal("Streams is wrong")
	}
	first, err := s4.CreateOrder("C-100", "M-2", "10")
	if err != nil {
		t.Fatal(err)
	}
	s4.Change(first, "PurchaseOrderByCustomer", "PO-1") // Changed: not taken
	second, _ := s4.CreateOrder("C-200", "TG11", "2")

	evs := collect(t, conn, "SalesOrder.Created", 2)
	var p map[string]any
	if err := json.Unmarshal(evs[0].Payload, &p); err != nil {
		t.Fatal(err)
	}
	// The payload is the order as the API reads it now, not the event's
	// few fields: the change after the event shows, and the items.
	items, _ := p["to_Item"].([]any)
	ce, _ := p["cloudEvent"].(map[string]any)
	if p["SalesOrder"] != first || p["TotalNetAmount"] != "2350.00" || p["PurchaseOrderByCustomer"] != "PO-1" || len(items) != 1 ||
		ce["type"] != saptest.SalesOrderCreated || ce["id"] != evs[0].ID || evs[0].Name != "SalesOrder.Created" {
		t.Fatalf("payload = %s", evs[0].Payload)
	}
	if !strings.Contains(string(evs[1].Payload), `"SalesOrder":"`+second+`"`) {
		t.Fatalf("second = %s", evs[1].Payload)
	}
	// Everything consumed was acknowledged, the Changed event too (dropped),
	// except the last one: the stream stopped inside its delivery.
	waitDepth(t, mesh, 1)
	if mesh.Acked[queue] != 2 {
		t.Fatalf("acked = %d", mesh.Acked[queue])
	}
}

func waitDepth(t *testing.T, mesh *saptest.Mesh, want int) {
	t.Helper()
	for i := 0; i < 100 && mesh.Depth(queue) != want; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if got := mesh.Depth(queue); got != want {
		t.Fatalf("queue depth = %d, want %d", got, want)
	}
}

func TestEventMeshRedeliversWhatWasNotStored(t *testing.T) {
	s4, mesh, conn := meshSetup(t, map[string]sap.Subscription{"SalesOrder.Created": createdOrders})
	id, _ := s4.CreateOrder("C-100", "M-1", "3")
	// The inbox fails: the message is not acknowledged.
	down := errors.New("inbox down")
	err := conn.Stream(context.Background(), "SalesOrder.Created", nil, func([]connector.Event, []byte) error { return down })
	if !errors.Is(err, down) {
		t.Fatalf("err = %v", err)
	}
	if mesh.Depth(queue) != 1 || mesh.Acked[queue] != 0 {
		t.Fatal("the message left the queue without being stored")
	}
	// Reopened, the same event comes again, with the same ID for the inbox
	// to recognize.
	evs := collect(t, conn, "SalesOrder.Created", 1)
	if !strings.Contains(string(evs[0].Payload), `"SalesOrder":"`+id+`"`) || mesh.Consumed[queue] != 2 {
		t.Fatalf("redelivered = %s, consumed %d", evs[0].Payload, mesh.Consumed[queue])
	}
}

func TestEventMeshBase64DataAndDeletedObjects(t *testing.T) {
	sub := sap.Subscription{Queue: queue, Types: []string{"sap.s4.beh.salesorder.v1.SalesOrder.*"}, Idle: "100ms",
		Read: &sap.Read{Service: "API_SALES_ORDER_SRV", EntitySet: "A_SalesOrder", Key: "SalesOrder"}}
	s4, mesh, conn := meshSetup(t, map[string]sap.Subscription{"SalesOrder.Any": sub})
	s4.Events = nil
	// An order that no longer exists keeps the event's data.
	mesh.Publisher(queue, "/default/sap.s4.beh/100")(saptest.SalesOrderDeleted, map[string]any{"SalesOrder": "999", "SoldToParty": "C-100"})
	// Data may come base64-encoded (data_base64).
	mesh.Publish(queue, []byte(`{"specversion":"1.0","id":"e-2","type":"sap.s4.beh.salesorder.v1.SalesOrder.Changed.v1","source":"/x","data_base64":"eyJTYWxlc09yZGVyIjoiOTk4In0="}`), "application/json")
	evs := collect(t, conn, "SalesOrder.Any", 2)
	if !strings.Contains(string(evs[0].Payload), `"SoldToParty":"C-100"`) || !strings.Contains(string(evs[1].Payload), `"SalesOrder":"998"`) || evs[1].ID != "e-2" {
		t.Fatalf("events = %s | %s", evs[0].Payload, evs[1].Payload)
	}
}

func TestEventMeshLeavesUnreadableMessagesOnTheQueue(t *testing.T) {
	_, mesh, conn := meshSetup(t, map[string]sap.Subscription{"SalesOrder.Created": createdOrders})
	mesh.Publish(queue, []byte(`not json`), "text/plain")
	err := conn.Stream(context.Background(), "SalesOrder.Created", nil, func([]connector.Event, []byte) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "not a CloudEvent") {
		t.Fatalf("err = %v", err)
	}
	if mesh.Depth(queue) != 1 {
		t.Fatal("an unreadable message was acknowledged; it belongs in the dead message queue")
	}
}

func TestEventMeshTokenRenewedAndQueueErrorsExplained(t *testing.T) {
	s4, mesh, conn := meshSetup(t, map[string]sap.Subscription{"SalesOrder.Created": createdOrders,
		"Missing": {Queue: "acme/none", Types: []string{"x.y"}}})
	collectOne := func() {
		s4.CreateOrder("C-100", "M-1", "1")
		collect(t, conn, "SalesOrder.Created", 1)
	}
	collectOne()
	mesh.ExpireTokens()
	collectOne()
	err := conn.Stream(context.Background(), "Missing", nil, func([]connector.Event, []byte) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "is there a queue acme/none") {
		t.Fatalf("err = %v", err)
	}
}

func TestEventMeshCheck(t *testing.T) {
	_, _, conn := meshSetup(t, map[string]sap.Subscription{"SalesOrder.Created": createdOrders})
	res := conn.Check(context.Background())
	if len(res) != 2 || !res[0].OK || res[0].Name != "event mesh" || !res[1].OK {
		t.Fatalf("check = %+v", res)
	}
	if err := conn.UseEventMesh(`{"clientid": "wrong", "clientsecret": "x"}`, &http.Client{}); err != nil {
		t.Fatal(err)
	}
	res = conn.Check(context.Background())
	if res[0].OK || !strings.Contains(res[0].Fix, "oa2 clientid") {
		t.Fatalf("check with a wrong client = %+v", res[0])
	}
}

func TestEventMeshConfigRejected(t *testing.T) {
	mesh := &sap.EventMesh{URL: "https://mesh.example", TokenURL: "https://auth.example/oauth/token", SecretRef: "openbao://m"}
	ok := sap.Subscription{Queue: queue, Types: []string{saptest.SalesOrderCreated}}
	for name, cfg := range map[string]sap.Config{
		"no event mesh":    {Subscriptions: map[string]sap.Subscription{"A": ok}},
		"plain http":       {EventMesh: &sap.EventMesh{URL: "http://mesh.example", TokenURL: mesh.TokenURL, SecretRef: "x"}, Subscriptions: map[string]sap.Subscription{"A": ok}},
		"no secret":        {EventMesh: &sap.EventMesh{URL: mesh.URL, TokenURL: mesh.TokenURL}, Subscriptions: map[string]sap.Subscription{"A": ok}},
		"shared queue":     {EventMesh: mesh, Subscriptions: map[string]sap.Subscription{"A": ok, "B": ok}},
		"no types":         {EventMesh: mesh, Subscriptions: map[string]sap.Subscription{"A": {Queue: queue}}},
		"bad queue":        {EventMesh: mesh, Subscriptions: map[string]sap.Subscription{"A": {Queue: "a/../b?x", Types: ok.Types}}},
		"polled and taken": {EventMesh: mesh, Subscriptions: map[string]sap.Subscription{"SalesOrder.Changed": ok}},
		"bad idle":         {EventMesh: mesh, Subscriptions: map[string]sap.Subscription{"A": {Queue: queue, Types: ok.Types, Idle: "1h"}}},
		"bad read":         {EventMesh: mesh, Subscriptions: map[string]sap.Subscription{"A": {Queue: queue, Types: ok.Types, Read: &sap.Read{Service: "S"}}}},
	} {
		cfg.BaseURL, cfg.Auth = "https://s4.example", rest.Auth{Type: "basic"}
		if name == "polled and taken" {
			cfg.Events = map[string]sap.Event{"SalesOrder.Changed": {Service: "API_SALES_ORDER_SRV", EntitySet: "A_SalesOrder", Key: "SalesOrder"}}
		}
		if _, err := sap.New(cfg, "u:p", nil); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// Only deployments whose flows take events from Event Mesh need its
// credentials.
func TestEventMeshCredentialsOnlyWhenUsed(t *testing.T) {
	cfg, _ := json.Marshal(sap.Config{BaseURL: "https://s4.example", Auth: rest.Auth{Type: "basic"},
		EventMesh:     &sap.EventMesh{URL: "https://mesh.example", TokenURL: "https://auth.example/oauth/token", SecretRef: "openbao://s4/mesh"},
		Subscriptions: map[string]sap.Subscription{"SalesOrder.Created": {Queue: queue, Types: []string{saptest.SalesOrderCreated}}}})
	secrets := connector.StaticSecrets{"openbao://s4/comm": "u:p"}
	writes := compiler.ConnectorConfig{Endpoint: "s4-prod", SecretRef: "openbao://s4/comm", Config: cfg, Interfaces: []string{"odata"}}
	inst, err := sap.Factory(context.Background(), writes, secrets)
	if err != nil {
		t.Fatalf("a write-only deployment needs the Event Mesh secret: %v", err)
	}
	if inst.(*sap.Conn).Streams("SalesOrder.Created") {
		t.Fatal("a write-only deployment subscribes")
	}
	events := writes
	events.Interfaces = []string{sap.InterfaceEventMesh, "odata"}
	if _, err := sap.Factory(context.Background(), events, secrets); err == nil || !strings.Contains(err.Error(), "openbao://s4/mesh") {
		t.Fatalf("err = %v", err)
	}
	secrets["openbao://s4/mesh"] = meshSecret
	inst, err = sap.Factory(context.Background(), events, secrets)
	if err != nil || !inst.(*sap.Conn).Streams("SalesOrder.Created") {
		t.Fatalf("err = %v", err)
	}
}
