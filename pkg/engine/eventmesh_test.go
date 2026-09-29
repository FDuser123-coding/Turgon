package engine

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/rest/dataversetest"
	"github.com/fduser123-coding/turgon/pkg/connector/sap/saptest"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

const s4Queue = "acme/s4/turgon/salesorders"

// newEventMeshFixture runs s4-sales-orders-to-dynamics: orders created in
// the fake S/4HANA are published to the fake Event Mesh's queue, and become
// orders in the fake Dataverse.
func newEventMeshFixture(t *testing.T) (*fixture, *saptest.S4, *saptest.Mesh, *dataversetest.Dataverse) {
	t.Helper()
	s4 := saptest.New("TURGON_COMM", "s3cret", "")
	srv := httptest.NewServer(s4)
	t.Cleanup(srv.Close)
	mesh := saptest.NewMesh("sb-turgon!b1|xbem-service-broker!b2", "mesh-secret")
	mesh.CreateQueue(s4Queue)
	s4.Events = mesh.Publisher(s4Queue, "/default/sap.s4.beh/100")
	msrv := httptest.NewServer(mesh)
	t.Cleanup(msrv.Close)
	dv := dataversetest.New("turgon-app", "s3cret")
	t.Cleanup(dv.Close)
	f := newFixtureFor(t, "s4-sales-orders-to-dynamics", connector.StaticSecrets{
		"openbao://s4-prod/comm-user":             "TURGON_COMM:s3cret",
		"openbao://s4-prod/event-mesh":            `{"clientid": "sb-turgon!b1|xbem-service-broker!b2", "clientsecret": "mesh-secret"}`,
		"openbao://dynamics-crm/app-registration": `{"clientId": "turgon-app", "clientSecret": "s3cret"}`,
	}, func(cfg string) string {
		cfg = strings.ReplaceAll(cfg, "https://my300000-api.s4hana.cloud.sap", srv.URL)
		cfg = strings.ReplaceAll(cfg, "https://enterprise-messaging-pubsub.cfapps.eu10.hana.ondemand.com", msrv.URL)
		cfg = strings.ReplaceAll(cfg, "https://acme.authentication.eu10.hana.ondemand.com/oauth/token", msrv.URL+saptest.TokenPath)
		cfg = strings.ReplaceAll(cfg, "https://login.microsoftonline.com/contoso.onmicrosoft.com/oauth2/v2.0/token", dv.TokenURL(dv.URL()))
		return strings.ReplaceAll(cfg, "https://contoso.crm4.dynamics.com/api/data/v9.2", dv.URL()+dataversetest.API)
	})
	return f, s4, mesh, dv
}

func TestS4OrderPushedThroughEventMeshBecomesDynamicsOrder(t *testing.T) {
	f, s4, mesh, dv := newEventMeshFixture(t)
	if !f.rt.Streams["s4-prod"]["SalesOrder.Created"] {
		t.Fatalf("streams = %v", f.rt.Streams)
	}
	if err := f.store.PutXref(context.Background(), "Customer", "s4-prod", "C-100", dataversetest.LovelaceGmbH); err != nil {
		t.Fatal(err)
	}
	delivered, stop := subscribe(t, f)
	id, err := s4.CreateOrder("C-100", "M-2", "3")
	if err != nil {
		t.Fatal(err)
	}
	wait(t, delivered)
	waitFor(t, "the acknowledgement", func() bool { return mesh.Depth(s4Queue) == 0 })

	d := &Dispatcher{Runtime: f.rt, Cursors: f.store, Starter: &recorder{}, Inbox: f.store}
	rec := d.Starter.(*recorder)
	if _, err := d.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(rec.ids) != 1 {
		t.Fatalf("runs = %v", rec.ids)
	}
	res, err, pending := f.run(rec.runs[rec.ids[0]], approve("04-write"))
	if err != nil {
		t.Fatal(err)
	}
	// The approver saw SAP's order, read through OData, in Dataverse's shape.
	if len(pending) != 1 || !strings.Contains(string(pending[0].Preview), `"name":"SAP-`+id+`"`) ||
		!strings.Contains(string(pending[0].Preview), `/accounts(`+dataversetest.LovelaceGmbH+`)`) {
		t.Fatalf("pending = %+v", pending)
	}
	o := dv.OrderByKey("s4-sales-order-" + id)
	if res.Writes[0].Status != writeguard.StatusCommitted || o == nil || o["totalamount"] != 705.0 || o["_customerid_value"] != dataversetest.LovelaceGmbH {
		t.Fatalf("Dataverse order = %v (writes %+v)", o, res.Writes)
	}

	// The worker stops before acknowledging (it crashed after storing the
	// event): Event Mesh delivers the message again, and the inbox knows it.
	stop()
	s4.Change(id, "PurchaseOrderByCustomer", "PO-7") // a Changed event: dropped
	body := `{"specversion":"1.0","id":"dup-1","type":"` + saptest.SalesOrderCreated + `","source":"/default/sap.s4.beh/100","data":{"SalesOrder":"` + id + `","SalesOrderType":"OR"}}`
	mesh.Publish(s4Queue, []byte(body), "application/json")
	mesh.Publish(s4Queue, []byte(body), "application/json")
	delivered, _ = subscribe(t, f)
	wait(t, delivered)
	waitFor(t, "the queue to drain", func() bool { return mesh.Depth(s4Queue) == 0 })
	if _, err := d.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(rec.ids) != 2 {
		t.Fatalf("runs after the duplicate = %v", rec.ids)
	}
	if mesh.Acked[s4Queue] != 4 {
		t.Fatalf("acked = %d", mesh.Acked[s4Queue])
	}
}

func TestS4CustomerWithoutLinkWaitsForASteward(t *testing.T) {
	f, s4, _, dv := newEventMeshFixture(t)
	delivered, _ := subscribe(t, f)
	if _, err := s4.CreateOrder("C-200", "TG11", "1"); err != nil {
		t.Fatal(err)
	}
	wait(t, delivered)
	d := &Dispatcher{Runtime: f.rt, Cursors: f.store, Starter: &recorder{}, Inbox: f.store}
	rec := d.Starter.(*recorder)
	if _, err := d.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err, _ := f.run(rec.runs[rec.ids[0]])
	if errType(err) != ErrTypeUnresolved {
		t.Fatalf("err = %v (%s)", err, errType(err))
	}
	if dv.Orders() != 0 {
		t.Fatal("an order was written for an unlinked customer")
	}
}
