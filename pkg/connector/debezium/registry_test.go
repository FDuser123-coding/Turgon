package debezium

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/fduser123-coding/turgon/pkg/meta"
)

// typedRegistry serves schemas of any type by ID, and subjects' versions
// (what references name), as Confluent's Schema Registry does.
type typedRegistry struct {
	*httptest.Server
	subjectFetches atomic.Int32
}

func newTypedRegistry(t *testing.T, schemas map[int]registered, subjects map[string]registered) *typedRegistry {
	t.Helper()
	r := &typedRegistry{}
	r.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var id int
		if _, err := fmt.Sscanf(req.URL.Path, "/schemas/ids/%d", &id); err == nil {
			if s, ok := schemas[id]; ok {
				_ = json.NewEncoder(w).Encode(s)
				return
			}
		}
		if strings.HasPrefix(req.URL.Path, "/subjects/") {
			subject := strings.TrimPrefix(req.URL.Path, "/subjects/")
			subject = subject[:strings.Index(subject, "/versions/")]
			if s, ok := subjects[subject]; ok {
				r.subjectFetches.Add(1)
				_ = json.NewEncoder(w).Encode(s)
				return
			}
		}
		http.Error(w, `{"error_code":40403,"message":"Schema not found"}`, http.StatusNotFound)
	}))
	t.Cleanup(r.Close)
	return r
}

func framed(id int, body []byte) []byte {
	out := make([]byte, 5, 5+len(body))
	binary.BigEndian.PutUint32(out[1:], uint32(id))
	return append(out, body...)
}

// What Confluent's JSON Schema converter registers for legacy.dbo.Orders.
var orderJSONSchema = strings.ReplaceAll(`{"type": "object", "title": "legacy.dbo.Orders.Envelope", "properties": {
  "before": {"connect.index": 0, "oneOf": [{"type": "null"}, VALUE]},
  "after": {"connect.index": 1, "oneOf": [{"type": "null"}, VALUE]},
  "source": {"connect.index": 2, "type": "object", "title": "io.debezium.connector.sqlserver.Source", "properties": {}},
  "op": {"connect.index": 3, "type": "string"},
  "ts_ms": {"connect.index": 4, "oneOf": [{"type": "null"}, {"type": "integer", "connect.type": "int64"}]}
}}`, "VALUE", `{"type": "object", "title": "legacy.dbo.Orders.Value", "properties": {
    "id": {"type": "integer", "connect.index": 0, "connect.type": "int32"},
    "customer_email": {"type": "string", "connect.index": 1},
    "total": {"type": "number", "connect.index": 2, "title": "org.apache.kafka.connect.data.Decimal", "connect.version": 1,
      "connect.type": "bytes", "connect.parameters": {"scale": "2", "connect.decimal.precision": "12"}},
    "order_date": {"type": "integer", "connect.index": 3, "connect.type": "int32", "title": "io.debezium.time.Date", "connect.version": 1},
    "updated_at": {"type": "integer", "connect.index": 4, "connect.type": "int64", "title": "io.debezium.time.NanoTimestamp", "connect.version": 1},
    "items": {"type": "string", "connect.index": 5, "title": "io.debezium.data.Json", "connect.version": 1},
    "status": {"type": "string", "connect.index": 6, "default": "open"},
    "note": {"connect.index": 7, "oneOf": [{"type": "null"}, {"type": "string"}]}
  }}`)

const orderJSONKeySchema = `{"type": "object", "title": "legacy.dbo.Orders.Key", "properties": {"id": {"type": "integer", "connect.index": 0, "connect.type": "int32"}}}`

func source(lsn string) map[string]any {
	return map[string]any{"version": "3.7.0.Final", "connector": "sqlserver", "name": "legacy", "ts_ms": 1790330000100,
		"snapshot": "false", "db": "erp", "schema": "dbo", "table": "Orders", "change_lsn": lsn, "commit_lsn": lsn, "event_serial_no": 1}
}

func jsonSchemaChange(op string, before, after *order, lsn string, decimalAsBytes bool) []byte {
	r := func(o *order) any {
		if o == nil {
			return nil
		}
		row := o.row()
		row["total"] = json.Number(fmt.Sprintf("%.2f", float64(o.cents)/100))
		if decimalAsBytes {
			row["total"] = decimalBytes(o.cents)
		}
		row["note"] = nil
		return row
	}
	b, _ := json.Marshal(map[string]any{"before": r(before), "after": r(after), "op": op, "ts_ms": 1790330000200, "source": source(lsn)})
	return framed(5, b)
}

// The same change written by the JSON, the JSON Schema and the Protobuf
// converter is the same event, with the same ID.
func TestJSONSchemaAndProtobufChangesReadLikeJSONOnes(t *testing.T) {
	reg := newTypedRegistry(t, map[int]registered{
		5: {Schema: orderJSONSchema, SchemaType: "JSON"},
		6: {Schema: orderJSONKeySchema, SchemaType: "JSON"},
		7: {Schema: orderProto, SchemaType: "PROTOBUF", References: []reference{
			{Name: "confluent/meta.proto", Subject: "confluent/meta.proto", Version: 1},
			{Name: "confluent/type/decimal.proto", Subject: "confluent/type/decimal.proto", Version: 1},
			{Name: "google/type/date.proto", Subject: "google/type/date.proto", Version: 1}}},
		8: {Schema: orderProtoKey, SchemaType: "PROTOBUF"},
	}, map[string]registered{
		"confluent/meta.proto":         {Schema: protoFallbacks["confluent/meta.proto"]},
		"confluent/type/decimal.proto": {Schema: protoFallbacks["confluent/type/decimal.proto"]},
		"google/type/date.proto":       {Schema: protoFallbacks["google/type/date.proto"]},
	})
	conn, prod := setup(t, Event{Start: "earliest"})
	conn.reg = newRegistry(&Registry{URL: reg.URL}, "", "")

	o := &order{7, 1250, "open"}
	produce(t, prod, 0, key(7), change("c", nil, o, "00000027:0000001a:0001"))
	produce(t, prod, 0, framed(6, []byte(`{"id":7}`)), jsonSchemaChange("c", nil, o, "00000027:0000001a:0001", false))
	produce(t, prod, 0, protoKey(t, 7), protoChange(t, nil, o, "00000027:0000001a:0001"))
	paid := &order{7, -990, "paid"}
	produce(t, prod, 0, framed(6, []byte(`{"id":7}`)), jsonSchemaChange("u", o, paid, "00000027:0000001b:0001", true))
	produce(t, prod, 0, protoKey(t, 7), protoChange(t, o, paid, "00000027:0000001b:0001"))
	got, _ := stream(t, conn, nil, 5)

	payload := func(i int) string {
		var m map[string]any
		_ = json.Unmarshal(got[i].Payload, &m)
		delete(m, "note") // optional columns the JSON fixture's schema lacks
		delete(m, "priority")
		b, _ := json.Marshal(m)
		return string(b)
	}
	for i, name := range map[int]string{1: "JSON Schema", 2: "Protobuf"} {
		if payload(i) != payload(0) {
			t.Fatalf("%s payload differs:\n%s\njson %s", name, payload(i), payload(0))
		}
		if got[i].ID != got[0].ID {
			t.Fatalf("the %s converter changed the change's ID: %s %s", name, got[i].ID, got[0].ID)
		}
	}
	// A decimal as Connect's bytes (decimal.format=BASE64) stays exact too.
	for _, i := range []int{3, 4} {
		upd := string(got[i].Payload)
		for _, want := range []string{`"total":-9.90`, `"status":"paid"`, `"order_date":"2026-09-24"`, `"updated_at":"2026-09-25T09:53:20.123456789Z"`,
			`"items":[{"qty":2,"sku":"M-1"}]`, `"note":null`} {
			if !strings.Contains(upd, want) {
				t.Fatalf("update %d lacks %s: %s", i, want, upd)
			}
		}
		if got[i].ID != got[3].ID {
			t.Fatalf("update IDs differ: %s %s", got[i].ID, got[3].ID)
		}
	}
	if n := reg.subjectFetches.Load(); n != 3 {
		t.Fatalf("%d reference fetches; each is fetched once with its schema", n)
	}
}

// What Confluent's Protobuf converter registers for legacy.dbo.Orders,
// with time.precision.mode=connect (Connect's Date and Timestamp).
const orderProto = `syntax = "proto3";
package legacy.dbo.Orders;

import "confluent/meta.proto";
import "confluent/type/decimal.proto";
import "google/type/date.proto";
import "google/protobuf/timestamp.proto";

message Envelope {
  Value before = 1;
  Value after = 2;
  Source source = 3;
  string op = 4;
  optional int64 ts_ms = 5;

  message Value {
    int32 id = 1;
    string customer_email = 2;
    confluent.type.Decimal total = 3 [(confluent.field_meta) = { params: [{key: "precision", value: "12"}, {key: "scale", value: "2"}] }];
    google.type.Date order_date = 4;
    google.protobuf.Timestamp updated_at = 5;
    string items = 6 [(confluent.field_meta) = { params: [{key: "connect.name", value: "io.debezium.data.Json"}] }];
    string status = 7;
    optional string note = 8;
    int32 priority = 9 [(confluent.field_meta) = { params: [{key: "connect.type", value: "int16"}] }];
  }
  message Source {
    string version = 1;
    string connector = 2;
    string name = 3;
    int64 ts_ms = 4;
    optional string snapshot = 5;
    string db = 6;
    string schema = 7;
    string table = 8;
    optional string change_lsn = 9;
    optional string commit_lsn = 10;
    optional int64 event_serial_no = 11;
  }
}
`

const orderProtoKey = `syntax = "proto3";
package legacy.dbo.Orders;
message Key { int32 id = 1; }
`

func compileProto(t *testing.T, schema string) protoreflect.FileDescriptor {
	t.Helper()
	files := map[string]string{"t.proto": schema}
	for k, v := range protoFallbacks {
		files[k] = v
	}
	c := protocompile.Compiler{Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{Accessor: protocompile.SourceAccessorFromMap(files)})}
	res, err := c.Compile(context.Background(), "t.proto")
	if err != nil {
		t.Fatal(err)
	}
	return res[0]
}

// set fills a message from a map, as a test's shorthand.
func set(m *dynamicpb.Message, values map[string]any) *dynamicpb.Message {
	for k, v := range values {
		f := m.Descriptor().Fields().ByName(protoreflect.Name(k))
		switch x := v.(type) {
		case map[string]any:
			m.Set(f, protoreflect.ValueOfMessage(set(dynamicpb.NewMessage(f.Message()), x)))
		default:
			m.Set(f, protoreflect.ValueOf(x))
		}
	}
	return m
}

func protoChange(t *testing.T, before, after *order, lsn string) []byte {
	t.Helper()
	fd := compileProto(t, orderProto)
	env := fd.Messages().Get(0)
	row := func(o *order) map[string]any {
		b, _ := base64.StdEncoding.DecodeString(decimalBytes(o.cents))
		return map[string]any{"id": int32(o.id), "customer_email": fmt.Sprintf("buyer%d@example.com", o.id),
			"total":      map[string]any{"value": b, "precision": uint32(12), "scale": int32(2)},
			"order_date": map[string]any{"year": int32(2026), "month": int32(9), "day": int32(24)},
			"updated_at": map[string]any{"seconds": int64(1790330000), "nanos": int32(123456789)},
			"items":      `[{"sku":"M-1","qty":2}]`, "status": o.status}
	}
	values := map[string]any{"op": "c", "ts_ms": int64(1790330000200), "source": map[string]any{"version": "3.7.0.Final",
		"connector": "sqlserver", "name": "legacy", "ts_ms": int64(1790330000100), "snapshot": "false", "db": "erp", "schema": "dbo",
		"table": "Orders", "change_lsn": lsn, "commit_lsn": lsn, "event_serial_no": int64(1)}}
	if before != nil {
		values["before"] = row(before)
		values["op"] = "u"
	}
	if after != nil {
		values["after"] = row(after)
	}
	b, err := proto.Marshal(set(dynamicpb.NewMessage(env), values))
	if err != nil {
		t.Fatal(err)
	}
	return framed(7, append([]byte{0}, b...)) // message indexes: the first message
}

func protoKey(t *testing.T, id int) []byte {
	t.Helper()
	fd := compileProto(t, orderProtoKey)
	b, err := proto.Marshal(set(dynamicpb.NewMessage(fd.Messages().Get(0)), map[string]any{"id": int32(id)}))
	if err != nil {
		t.Fatal(err)
	}
	return framed(8, append([]byte{0}, b...))
}

func TestProtobufMessageIndexes(t *testing.T) {
	for _, c := range []struct {
		in   []byte
		want []int
	}{{[]byte{0, 9}, []int{0}}, {[]byte{4, 2, 6, 9}, []int{1, 3}}} {
		got, rest, err := messageIndexes(c.in)
		if err != nil || fmt.Sprint(got) != fmt.Sprint(c.want) || len(rest) != 1 || rest[0] != 9 {
			t.Fatalf("%v: %v %v %v", c.in, got, rest, err)
		}
	}
	for _, bad := range [][]byte{{}, {3}, {0x7f}} {
		if _, _, err := messageIndexes(bad); err == nil {
			t.Fatalf("%v accepted", bad)
		}
	}
}

// Discovery reads the columns from a JSON Schema or a Protobuf schema,
// named as the JSON converter names them.
func TestDiscoverJSONSchemaAndProtobuf(t *testing.T) {
	reg := newTypedRegistry(t, map[int]registered{
		5: {Schema: orderJSONSchema, SchemaType: "JSON"},
		6: {Schema: orderJSONKeySchema, SchemaType: "JSON"},
		7: {Schema: orderProto, SchemaType: "PROTOBUF"},
		8: {Schema: orderProtoKey, SchemaType: "PROTOBUF"},
	}, nil)
	for name, rec := range map[string]func() ([]byte, []byte){
		"json schema": func() ([]byte, []byte) {
			return framed(6, []byte(`{"id":7}`)), jsonSchemaChange("c", nil, &order{7, 1250, "open"}, "1", false)
		},
		"protobuf": func() ([]byte, []byte) { return protoKey(t, 7), protoChange(t, nil, &order{7, 1250, "open"}, "1") },
	} {
		conn, prod := setup(t, Event{})
		conn.reg = newRegistry(&Registry{URL: reg.URL}, "", "")
		k, v := rec()
		produce(t, prod, 0, k, v)
		cat, err := conn.Discover(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		o, ok := cat.Object(topic)
		if !ok || o.Sampled {
			t.Fatalf("%s: objects %+v", name, cat.Objects)
		}
		got := map[string]meta.Field{}
		for _, f := range o.Fields {
			got[f.Name] = f
		}
		want := map[string]meta.Field{
			"id":             {Name: "id", Type: "int32", Key: true},
			"customer_email": {Name: "customer_email", Type: "string", Required: true},
			"total":          {Name: "total", Type: "decimal(12,2)", Required: true},
			"items":          {Name: "items", Type: "Json", Required: true},
			"note":           {Name: "note", Type: "string"},
		}
		if name == "json schema" {
			want["order_date"] = meta.Field{Name: "order_date", Type: "Date", Required: true}
			want["updated_at"] = meta.Field{Name: "updated_at", Type: "NanoTimestamp", Required: true}
			want["status"] = meta.Field{Name: "status", Type: "string"}
		} else {
			want["order_date"] = meta.Field{Name: "order_date", Type: "Date", Required: true}
			want["updated_at"] = meta.Field{Name: "updated_at", Type: "Timestamp", Required: true}
			want["status"] = meta.Field{Name: "status", Type: "string", Required: true}
			want["priority"] = meta.Field{Name: "priority", Type: "int16", Required: true}
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s fields\n got %v\nwant %v", name, got, want)
		}
	}
}

// Wrappers (wrapper.for.nullables) read as their values, or null; a
// schema of another type is refused with what Turgon reads.
func TestProtobufWrappersAndUnknownSchemaTypes(t *testing.T) {
	const schema = `syntax = "proto3";
import "google/protobuf/wrappers.proto";
message Row { google.protobuf.StringValue note = 1; google.protobuf.Int64Value qty = 2; repeated string tags = 3; map<string, int32> stock = 4; }
`
	reg := newTypedRegistry(t, map[int]registered{1: {Schema: schema, SchemaType: "PROTOBUF"}, 2: {Schema: "x", SchemaType: "XML"}}, nil)
	r := newRegistry(&Registry{URL: reg.URL}, "", "")
	fd := compileProto(t, schema)
	m := set(dynamicpb.NewMessage(fd.Messages().Get(0)), map[string]any{"note": map[string]any{"value": "rush"}})
	l := m.Mutable(fd.Messages().Get(0).Fields().ByName("tags")).List()
	l.Append(protoreflect.ValueOfString("a"))
	m.Mutable(fd.Messages().Get(0).Fields().ByName("stock")).Map().Set(protoreflect.ValueOfString("M-1").MapKey(), protoreflect.ValueOfInt32(3))
	b, _ := proto.Marshal(m)
	d, got, err := r.decodeRecord(context.Background(), framed(1, append([]byte{0}, b...)))
	if err != nil {
		t.Fatal(err)
	}
	if j, _ := json.Marshal(got); string(j) != `{"note":"rush","qty":null,"stock":{"M-1":3},"tags":["a"]}` {
		t.Fatalf("decoded %s", j)
	}
	if fmt.Sprint(d.fields()) != "[note qty tags stock]" {
		t.Fatalf("fields %v", d.fields())
	}
	if _, _, err := r.decodeRecord(context.Background(), framed(2, nil)); err == nil || !strings.Contains(err.Error(), "Avro, JSON Schema and Protobuf") {
		t.Fatalf("XML: %v", err)
	}
	if _, _, err := r.decodeRecord(context.Background(), framed(1, []byte{0x7f})); err == nil {
		t.Fatal("bad message indexes accepted")
	}
}
