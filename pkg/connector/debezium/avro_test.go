package debezium

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iskorotkov/avro/v2"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/meta"
)

// The schemas Debezium's Avro converter registers for legacy.dbo.Orders:
// Connect's types and parameters ride along as connect.* properties.
const orderValueSchema = `{
  "type": "record", "name": "Envelope", "namespace": "legacy.dbo.Orders", "connect.name": "legacy.dbo.Orders.Envelope",
  "fields": [
    {"name": "before", "type": ["null", {"type": "record", "name": "Value", "connect.name": "legacy.dbo.Orders.Value", "fields": [
      {"name": "id", "type": "int"},
      {"name": "customer_email", "type": "string"},
      {"name": "total", "type": {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2,
        "connect.name": "org.apache.kafka.connect.data.Decimal", "connect.version": 1,
        "connect.parameters": {"scale": "2", "connect.decimal.precision": "12", "__debezium.source.column.type": "DECIMAL", "__debezium.source.column.length": "12"}}},
      {"name": "order_date", "type": {"type": "int", "connect.name": "io.debezium.time.Date", "connect.version": 1}},
      {"name": "updated_at", "type": {"type": "long", "connect.name": "io.debezium.time.NanoTimestamp", "connect.version": 1}},
      {"name": "items", "type": {"type": "string", "connect.name": "io.debezium.data.Json", "connect.version": 1}},
      {"name": "status", "type": {"type": "string", "connect.default": "open",
        "connect.parameters": {"__debezium.source.column.type": "NVARCHAR", "__debezium.source.column.length": "20"}}, "default": "open"},
      {"name": "note", "type": ["null", "string"], "default": null}
    ]}], "default": null},
    {"name": "after", "type": ["null", "Value"], "default": null},
    {"name": "source", "type": {"type": "record", "name": "Source", "namespace": "io.debezium.connector.sqlserver", "fields": [
      {"name": "version", "type": "string"}, {"name": "connector", "type": "string"}, {"name": "name", "type": "string"},
      {"name": "ts_ms", "type": "long"}, {"name": "snapshot", "type": ["null", "string"], "default": null},
      {"name": "db", "type": "string"}, {"name": "schema", "type": "string"}, {"name": "table", "type": "string"},
      {"name": "change_lsn", "type": ["null", "string"], "default": null}, {"name": "commit_lsn", "type": ["null", "string"], "default": null},
      {"name": "event_serial_no", "type": ["null", "long"], "default": null}
    ]}},
    {"name": "op", "type": "string"},
    {"name": "ts_ms", "type": ["null", "long"], "default": null}
  ]
}`

const orderKeySchema = `{"type": "record", "name": "Key", "namespace": "legacy.dbo.Orders", "fields": [{"name": "id", "type": "int"}]}`

// fakeRegistry serves schemas by ID, as Confluent's Schema Registry does.
type fakeRegistry struct {
	*httptest.Server
	fetches atomic.Int32
}

func newRegistryServer(t *testing.T, user, pass string, schemas map[int]string) *fakeRegistry {
	t.Helper()
	r := &fakeRegistry{}
	r.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if u, p, _ := req.BasicAuth(); user != "" && (u != user || p != pass) {
			http.Error(w, `{"error_code":40101,"message":"Unauthorized"}`, http.StatusUnauthorized)
			return
		}
		if req.URL.Path == "/subjects" {
			_ = json.NewEncoder(w).Encode([]string{"legacy.dbo.Orders-key", "legacy.dbo.Orders-value"})
			return
		}
		var id int
		if _, err := fmt.Sscanf(req.URL.Path, "/schemas/ids/%d", &id); err != nil || schemas[id] == "" {
			http.Error(w, `{"error_code":40403,"message":"Schema not found"}`, http.StatusNotFound)
			return
		}
		r.fetches.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": schemas[id]})
	}))
	t.Cleanup(r.Close)
	return r
}

// confluent encodes v with the schema as Confluent's serializer does.
func confluent(t *testing.T, id int, schema string, v any) []byte {
	t.Helper()
	s, err := avro.ParseWithCache(schema, "", &avro.SchemaCache{})
	if err != nil {
		t.Fatal(err)
	}
	body, err := avro.Marshal(s, v)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]byte, 5, 5+len(body))
	binary.BigEndian.PutUint32(out[1:], uint32(id))
	return append(out, body...)
}

func avroRow(o order) map[string]any {
	return map[string]any{"id": o.id, "customer_email": fmt.Sprintf("buyer%d@example.com", o.id), "total": big.NewRat(o.cents, 100),
		"order_date": 20720, "updated_at": int64(1790330000123456789), "items": `[{"sku":"M-1","qty":2}]`, "status": o.status,
		"note": nil}
}

func avroChange(t *testing.T, op string, before, after *order, lsn string) []byte {
	t.Helper()
	var b, a any
	if before != nil {
		b = map[string]any{"legacy.dbo.Orders.Value": avroRow(*before)}
	}
	if after != nil {
		a = map[string]any{"legacy.dbo.Orders.Value": avroRow(*after)}
	}
	return confluent(t, 2, orderValueSchema, map[string]any{
		"before": b, "after": a, "op": op, "ts_ms": map[string]any{"long": int64(1790330000200)},
		"source": map[string]any{"version": "3.7.0.Final", "connector": "sqlserver", "name": "legacy", "ts_ms": int64(1790330000100),
			"snapshot": map[string]any{"string": "false"}, "db": "erp", "schema": "dbo", "table": "Orders",
			"change_lsn": map[string]any{"string": lsn}, "commit_lsn": map[string]any{"string": lsn}, "event_serial_no": map[string]any{"long": int64(1)}},
	})
}

func avroKey(t *testing.T, id int) []byte {
	return confluent(t, 1, orderKeySchema, map[string]any{"id": id})
}

// Avro changes read with their registered schema give the same events as
// JSON ones: the same values, and the same IDs.
func TestAvroChangesReadLikeJSONOnes(t *testing.T) {
	reg := newRegistryServer(t, "", "", map[int]string{1: orderKeySchema, 2: orderValueSchema})
	conn, prod := setup(t, Event{Start: "earliest"})
	conn.reg = newRegistry(&Registry{URL: reg.URL}, "", "")

	produce(t, prod, 0, avroKey(t, 7), avroChange(t, "c", nil, &order{7, 1250, "open"}, "00000027:0000001a:0001"))
	produce(t, prod, 0, key(7), change("c", nil, &order{7, 1250, "open"}, "00000027:0000001a:0001"))
	produce(t, prod, 0, avroKey(t, 7), avroChange(t, "u", &order{7, 1250, "open"}, &order{7, -990, "paid"}, "00000027:0000001b:0001"))
	got, _ := stream(t, conn, nil, 3)
	var avroPayload, jsonPayload map[string]any
	_ = json.Unmarshal(got[0].Payload, &avroPayload)
	_ = json.Unmarshal(got[1].Payload, &jsonPayload)
	delete(avroPayload, "note") // an optional column the JSON fixture's schema lacks
	a, _ := json.Marshal(avroPayload)
	j, _ := json.Marshal(jsonPayload)
	if string(a) != string(j) {
		t.Fatalf("payloads differ:\navro %s\njson %s", a, j)
	}
	if got[0].ID != got[1].ID {
		t.Fatalf("the converter changed the change's ID: %s %s", got[0].ID, got[1].ID)
	}
	// Decimals stay exact (a negative one too), dates and timestamps ISO.
	upd := string(got[2].Payload)
	for _, want := range []string{`"total":-9.90`, `"status":"paid"`, `"order_date":"2026-09-24"`, `"updated_at":"2026-09-25T09:53:20.123456789Z"`,
		`"before":{"customer_email":"buyer7@example.com","id":7,"items":[{"qty":2,"sku":"M-1"}],"note":null,"order_date":"2026-09-24","status":"open","total":12.50`} {
		if !strings.Contains(upd, want) {
			t.Fatalf("update lacks %s: %s", want, upd)
		}
	}
	if n := reg.fetches.Load(); n != 2 {
		t.Fatalf("%d schema fetches; each ID is fetched once", n)
	}
}

func TestAvroWithoutARegistryIsExplained(t *testing.T) {
	conn, prod := setup(t, Event{Start: "earliest"})
	produce(t, prod, 0, avroKey(t, 7), avroChange(t, "c", nil, &order{7, 1250, "open"}, "00000027:0000001a:0001"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := conn.Stream(ctx, "Order.Changed", nil, func([]connector.Event, []byte) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "set the connection's schemaRegistry") {
		t.Fatalf("err = %v", err)
	}
}

func TestRegistryAuthAndErrors(t *testing.T) {
	reg := newRegistryServer(t, "key", "secret", map[int]string{1: orderKeySchema, 3: `{"type": "record", "name": "X", "fields": []}`})
	r := newRegistry(&Registry{URL: reg.URL, Auth: "basic"}, "key", "secret")
	if _, err := r.schema(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if n, err := r.subjects(context.Background()); err != nil || n != 2 {
		t.Fatalf("subjects: %d %v", n, err)
	}
	if _, err := r.schema(context.Background(), 9); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("missing schema: %v", err)
	}
	wrong := newRegistry(&Registry{URL: reg.URL, Auth: "basic"}, "key", "nope")
	if _, err := wrong.schema(context.Background(), 1); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("bad credentials: %v", err)
	}
	conn, _ := setup(t, Event{})
	conn.reg = wrong
	var check connector.CheckResult
	for _, r := range conn.Check(context.Background()) {
		if r.Name == "schema registry" {
			check = r
		}
	}
	if check.OK || !strings.Contains(check.Fix, `{"registry"`) {
		t.Fatalf("registry check %+v", check)
	}
	ok := map[string]Event{"E": {Topic: "t"}}
	for _, bad := range []Config{
		{Brokers: []string{"127.0.0.1:9092"}, Events: ok, SchemaRegistry: &Registry{URL: "http://registry.example:8081"}},
		{Brokers: []string{"127.0.0.1:9092"}, Events: ok, SchemaRegistry: &Registry{URL: "ftp://x"}},
		{Brokers: []string{"127.0.0.1:9092"}, Events: ok, SchemaRegistry: &Registry{URL: "https://r.example", Auth: "oauth"}},
	} {
		if _, err := New(bad, ""); err == nil {
			t.Errorf("%+v accepted", bad.SchemaRegistry)
		}
	}
	if _, err := New(Config{Brokers: []string{"127.0.0.1:9092"}, Events: ok, SchemaRegistry: &Registry{URL: "https://r.example", Auth: "basic"}}, `{}`); err == nil {
		t.Error("basic auth without credentials accepted")
	}
	if _, err := New(Config{Brokers: []string{"127.0.0.1:9092"}, Events: ok, SchemaRegistry: &Registry{URL: "https://r.example", Auth: "basic"}},
		`{"registry": {"username": "k", "password": "s"}}`); err != nil {
		t.Fatal(err)
	}
}

// Discovery reads the columns from the Avro schema, named as the JSON
// converter names them.
func TestDiscoverAvro(t *testing.T) {
	reg := newRegistryServer(t, "", "", map[int]string{1: orderKeySchema, 2: orderValueSchema})
	conn, prod := setup(t, Event{})
	conn.reg = newRegistry(&Registry{URL: reg.URL}, "", "")
	produce(t, prod, 0, avroKey(t, 7), avroChange(t, "c", nil, &order{7, 1250, "open"}, "00000027:0000001a:0001"))
	cat, err := conn.Discover(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	o, ok := cat.Object(topic)
	if !ok || o.Sampled {
		t.Fatalf("objects %+v", cat.Objects)
	}
	got := map[string]meta.Field{}
	for _, f := range o.Fields {
		got[f.Name] = f
	}
	want := map[string]meta.Field{
		"id":             {Name: "id", Type: "int32", Key: true},
		"customer_email": {Name: "customer_email", Type: "string", Required: true},
		"total":          {Name: "total", Type: "decimal(12,2)", Label: "source type DECIMAL", Required: true},
		"order_date":     {Name: "order_date", Type: "Date", Required: true},
		"updated_at":     {Name: "updated_at", Type: "NanoTimestamp", Required: true},
		"items":          {Name: "items", Type: "Json", Required: true},
		"status":         {Name: "status", Type: "string", Label: "source type NVARCHAR", Length: 20},
		"note":           {Name: "note", Type: "string"},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("fields\n got %v\nwant %v", got, want)
	}
}
