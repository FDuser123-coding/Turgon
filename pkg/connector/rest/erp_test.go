package rest_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/fduser123-coding/turgon/pkg/connector/rest"
	"github.com/fduser123-coding/turgon/pkg/connector/rest/dataversetest"
	"github.com/fduser123-coding/turgon/pkg/connector/rest/netsuitetest"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// exampleConfig reads an example connection's config, so tests and
// examples cannot drift apart, and points it at base.
func exampleConfig(t *testing.T, name string, rewrite func(string) string) rest.Config {
	t.Helper()
	data, err := os.ReadFile("../../../examples/connections/" + name + ".yaml")
	if err != nil {
		t.Fatal(err)
	}
	var conn struct {
		Spec struct {
			Config json.RawMessage `json:"config"`
		} `json:"spec"`
	}
	if err := yaml.Unmarshal(data, &conn); err != nil {
		t.Fatal(err)
	}
	var cfg rest.Config
	dec := json.NewDecoder(strings.NewReader(rewrite(string(conn.Spec.Config))))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}

var (
	nsCreds  = rest.OAuth1Credentials{ConsumerKey: "ck", ConsumerSecret: "cs", TokenID: "ti", TokenSecret: "ts"}
	nsSecret = `{"consumerKey": "ck", "consumerSecret": "cs", "tokenId": "ti", "tokenSecret": "ts"}`
)

func newNetSuite(t *testing.T, secret string) (*netsuitetest.Account, *rest.Conn) {
	t.Helper()
	ns := netsuitetest.New("1234567_SB1", nsCreds)
	t.Cleanup(ns.Close)
	cfg := exampleConfig(t, "netsuite-erp", func(s string) string {
		return strings.ReplaceAll(s, "https://1234567-sb1.suitetalk.api.netsuite.com", ns.URL())
	})
	c, err := rest.New(cfg, secret, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	return ns, c
}

const nsOrder = `{"customerId": "1001", "orderDate": "2026-09-24", "memo": "Shopify SHOPIFY-7",
	"netsuiteItems": [{"item": {"id": "102"}, "quantity": 2}]}`

func TestNetSuiteUpsertByExternalIDAndDelete(t *testing.T) {
	ns, c := newNetSuite(t, nsSecret)
	ctx := context.Background()
	res, err := c.Commit(ctx, "create-sales-order", "SHOPIFY-7", json.RawMessage(nsOrder))
	if err != nil {
		t.Fatal(err)
	}
	var created struct{ ID, URL string }
	_ = json.Unmarshal(res, &created)
	o := ns.Order(created.ID)
	if o == nil || o["externalId"] != "SHOPIFY-7" || o["total"] != 470.0 || !strings.HasSuffix(created.URL, "/salesOrder/"+created.ID) {
		t.Fatalf("result %s, order %v", res, o)
	}
	// A retry (after a lost response) upserts the same order.
	again, err := c.Commit(ctx, "create-sales-order", "SHOPIFY-7", json.RawMessage(nsOrder))
	if err != nil || ns.Orders() != 1 || !strings.Contains(string(again), `"id":"`+created.ID+`"`) {
		t.Fatalf("retry: %s, %v, orders %d", again, err, ns.Orders())
	}
	if _, err := c.Commit(ctx, "delete-sales-order", "SHOPIFY-7#compensate", res); err != nil || ns.Orders() != 0 {
		t.Fatalf("delete: %v, orders %d", err, ns.Orders())
	}
	// NetSuite's own message comes through, and the error is final.
	_, err = c.Commit(ctx, "create-sales-order", "SHOPIFY-8", json.RawMessage(`{"customerId": "9999", "orderDate": "2026-09-24", "memo": "x", "netsuiteItems": [{"item": {"id": "102"}, "quantity": 1}]}`))
	if !errors.Is(err, writeguard.ErrInvalid) || !strings.Contains(err.Error(), "Invalid entity reference key 9999") {
		t.Fatalf("unknown customer: %v", err)
	}
}

func TestNetSuiteRejectsWrongTokens(t *testing.T) {
	_, c := newNetSuite(t, `{"consumerKey": "ck", "consumerSecret": "cs", "tokenId": "ti", "tokenSecret": "stolen"}`)
	_, err := c.Read(context.Background(), "get-customer", "1001")
	if err == nil || !strings.Contains(err.Error(), "signature does not match") {
		t.Fatalf("a wrong token secret was accepted: %v", err)
	}
	for _, r := range c.Check(context.Background()) {
		if r.OK {
			t.Errorf("check %s passed with wrong credentials", r.Name)
		}
	}
}

func TestNetSuiteReadsAndChangedOrders(t *testing.T) {
	ns, c := newNetSuite(t, nsSecret)
	ctx := context.Background()
	got, err := c.Read(ctx, "get-customer", "1001")
	if err != nil || !strings.Contains(string(got), `"name":"Lovelace GmbH"`) || !strings.Contains(string(got), `"customerNumber":"C-100"`) {
		t.Fatalf("read = %s, %v", got, err)
	}
	if _, err := c.Read(ctx, "get-customer", "4040"); !errors.Is(err, writeguard.ErrNotFound) {
		t.Fatalf("unknown customer: %v", err)
	}
	var mu sync.Mutex
	clock := time.Date(2026, 9, 24, 8, 0, 0, 0, time.UTC)
	ns.StartAt(5000, func() time.Time { mu.Lock(); defer mu.Unlock(); return clock })
	for i, key := range []string{"A", "B", "C"} {
		mu.Lock()
		clock = clock.Add(time.Duration(i) * time.Minute)
		mu.Unlock()
		if _, err := c.Commit(ctx, "create-sales-order", key, json.RawMessage(nsOrder)); err != nil {
			t.Fatal(err)
		}
	}
	evs, err := c.Poll(ctx, "SalesOrder.Changed", 0, 10)
	if err != nil || len(evs) != 3 || evs[0].ID != "5000" || evs[2].ID != "5002" {
		t.Fatalf("changed orders = %+v, %v", evs, err)
	}
	later, err := c.Poll(ctx, "SalesOrder.Changed", evs[0].Position, 10)
	if err != nil || len(later) != 2 || later[0].ID != "5001" {
		t.Fatalf("after the first: %+v, %v", later, err)
	}
	if n := ns.Requests["POST /services/rest/query/v1/suiteql"]; n != 2 {
		t.Fatalf("SuiteQL requests = %d", n)
	}
}

func newDataverse(t *testing.T) (*dataversetest.Dataverse, *rest.Conn) {
	t.Helper()
	dv := dataversetest.New("turgon-app", "s3cret")
	t.Cleanup(dv.Close)
	cfg := exampleConfig(t, "dynamics-crm", func(s string) string {
		s = strings.ReplaceAll(s, "https://login.microsoftonline.com/contoso.onmicrosoft.com/oauth2/v2.0/token", dv.TokenURL(dv.URL()))
		return strings.ReplaceAll(s, "https://contoso.crm4.dynamics.com/api/data/v9.2", dv.URL()+dataversetest.API)
	})
	c, err := rest.New(cfg, `{"clientId": "turgon-app", "clientSecret": "s3cret"}`, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	return dv, c
}

const dvOrder = `{"name": "HUBSPOT-1", "description": "From HUBSPOT-1, EUR",
	"accountBind": "/accounts(` + dataversetest.LovelaceGmbH + `)",
	"dynamicsLines": [{"isproductoverridden": true, "productdescription": "Deal HUBSPOT-1", "quantity": 1, "priceperunit": 1500.5}]}`

func TestDataverseUpsertByAlternateKeyAndDelete(t *testing.T) {
	dv, c := newDataverse(t)
	ctx := context.Background()
	preview, err := c.Simulate(ctx, "create-sales-order", json.RawMessage(dvOrder))
	if err != nil || !strings.Contains(string(preview), `"customerid_account@odata.bind":"/accounts(`) || !strings.Contains(string(preview), `"mode":"request"`) {
		t.Fatalf("preview = %s, %v", preview, err)
	}
	// A key with a quote cannot end the OData literal.
	res, err := c.Commit(ctx, "create-sales-order", "deal-o'brien", json.RawMessage(dvOrder))
	if err != nil {
		t.Fatal(err)
	}
	o := dv.OrderByKey("deal-o'brien")
	var created struct{ ID string }
	_ = json.Unmarshal(res, &created)
	if o == nil || o["salesorderid"] != created.ID || o["totalamount"] != 1500.5 {
		t.Fatalf("result %s, order %v", res, o)
	}
	if _, err := c.Commit(ctx, "create-sales-order", "deal-o'brien", json.RawMessage(dvOrder)); err != nil || dv.Orders() != 1 {
		t.Fatalf("retry: %v, orders %d", err, dv.Orders())
	}
	if _, err := c.Commit(ctx, "delete-sales-order", "k#compensate", res); err != nil || dv.Orders() != 0 {
		t.Fatalf("delete: %v, orders %d", err, dv.Orders())
	}
	// A preview catches a payload missing a field before anyone approves.
	if _, err := c.Simulate(ctx, "create-sales-order", json.RawMessage(`{"name": "x"}`)); !errors.Is(err, writeguard.ErrInvalid) {
		t.Fatalf("incomplete payload previewed: %v", err)
	}
}

func TestDataverseChangedOrdersAndChecks(t *testing.T) {
	dv, c := newDataverse(t)
	ctx := context.Background()
	var mu sync.Mutex
	clock := time.Date(2026, 9, 24, 8, 0, 0, 0, time.UTC)
	dv.SetClock(func() time.Time { mu.Lock(); defer mu.Unlock(); return clock })
	for i, key := range []string{"k1", "k2"} {
		mu.Lock()
		clock = clock.Add(time.Duration(i+1) * time.Minute)
		mu.Unlock()
		if _, err := c.Commit(ctx, "create-sales-order", key, json.RawMessage(dvOrder)); err != nil {
			t.Fatal(err)
		}
	}
	evs, err := c.Poll(ctx, "SalesOrder.Changed", 0, 10)
	if err != nil || len(evs) != 2 {
		t.Fatalf("changed orders = %+v, %v", evs, err)
	}
	var first map[string]any
	_ = json.Unmarshal(evs[0].Payload, &first)
	if first["turgon_externalref"] != "k1" {
		t.Fatalf("oldest first: %s", evs[0].Payload)
	}
	if later, err := c.Poll(ctx, "SalesOrder.Changed", evs[0].Position, 10); err != nil || len(later) != 1 {
		t.Fatalf("after the first: %+v, %v", later, err)
	}
	got, err := c.Read(ctx, "get-customer", dataversetest.HopperInc)
	if err != nil || !strings.Contains(string(got), `"name":"Hopper Industries"`) {
		t.Fatalf("read = %s, %v", got, err)
	}
	for _, r := range c.Check(ctx) {
		if !r.OK {
			t.Errorf("check %s: %s", r.Name, r.Detail)
		}
	}
}

func TestIDFromAndHeaderValidation(t *testing.T) {
	base := rest.Config{BaseURL: "https://x.example", Auth: rest.Auth{Type: "none"}}
	for name, op := range map[string]rest.Operation{
		"credential header": {Method: "POST", Path: "/x", Headers: map[string]string{"Authorization": "Bearer x"}},
		"unknown idFrom":    {Method: "POST", Path: "/x", IDFrom: "X-Id"},
	} {
		cfg := base
		cfg.Operations = map[string]rest.Operation{"op": op}
		if _, err := rest.New(cfg, "", http.DefaultClient); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	cfg := base
	cfg.Auth = rest.Auth{Type: "oauth1"}
	if _, err := rest.New(cfg, nsSecret, http.DefaultClient); err == nil {
		t.Error("oauth1 without a realm accepted")
	}
}

// A captured request cannot be replayed: its nonce was used.
func TestNetSuiteRefusesReplayedRequests(t *testing.T) {
	ns := netsuitetest.New("1234567_SB1", nsCreds)
	defer ns.Close()
	req, _ := http.NewRequest(http.MethodGet, ns.URL()+"/services/rest/record/v1/customer/1001", nil)
	req.Header.Set("Authorization", rest.SignOAuth1(req.Method, req.URL, "1234567_SB1", nsCreds, time.Now(), "nonce-1"))
	for i, want := range []int{http.StatusOK, http.StatusUnauthorized} {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("attempt %d: HTTP %d, want %d", i+1, resp.StatusCode, want)
		}
	}
}
