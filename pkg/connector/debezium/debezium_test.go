package debezium

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/fduser123-coding/turgon/pkg/connector"
)

const topic = "legacy.dbo.Orders"

// orderSchema is the schema the JSON converter writes for SQL Server's
// dbo.Orders: a DECIMAL(12,2), a DATE, a DATETIME2 and an NVARCHAR(MAX)
// holding JSON.
var orderSchema = []field{
	{Type: "int32", Field: "id"},
	{Type: "string", Field: "customer_email"},
	{Type: "bytes", Name: "org.apache.kafka.connect.data.Decimal", Field: "total", Parameters: map[string]string{"scale": "2", "connect.decimal.precision": "12"}},
	{Type: "int32", Name: "io.debezium.time.Date", Field: "order_date"},
	{Type: "int64", Name: "io.debezium.time.NanoTimestamp", Field: "updated_at"},
	{Type: "string", Name: "io.debezium.data.Json", Field: "items"},
	{Type: "string", Field: "status"},
}

// decimalBytes encodes n/100 as Connect does: unscaled, big-endian two's
// complement.
func decimalBytes(cents int64) string {
	n := big.NewInt(cents)
	var b []byte
	if cents >= 0 {
		b = n.Bytes()
		if len(b) == 0 || b[0]&0x80 != 0 {
			b = append([]byte{0}, b...)
		}
	} else {
		size := len(new(big.Int).Neg(n).Bytes()) + 1
		b = new(big.Int).Add(n, new(big.Int).Lsh(big.NewInt(1), uint(size*8))).Bytes()
	}
	return base64.StdEncoding.EncodeToString(b)
}

type order struct {
	id     int
	cents  int64
	status string
}

func (o order) row() map[string]any {
	return map[string]any{"id": o.id, "customer_email": fmt.Sprintf("buyer%d@example.com", o.id), "total": decimalBytes(o.cents),
		"order_date": 20720, "updated_at": int64(1790330000123456789), "items": `[{"sku":"M-1","qty":2}]`, "status": o.status}
}

// change builds a Debezium SQL Server change event with its schema.
func change(op string, before, after *order, lsn string) []byte {
	var b, a any
	if before != nil {
		b = before.row()
	}
	if after != nil {
		a = after.row()
	}
	env := map[string]any{
		"schema": map[string]any{"type": "struct", "name": "legacy.dbo.Orders.Envelope", "fields": []any{
			map[string]any{"type": "struct", "field": "before", "optional": true, "fields": orderSchema},
			map[string]any{"type": "struct", "field": "after", "optional": true, "fields": orderSchema},
			map[string]any{"type": "struct", "field": "source"},
			map[string]any{"type": "string", "field": "op"},
		}},
		"payload": map[string]any{"before": b, "after": a, "op": op, "ts_ms": 1790330000200,
			"source": map[string]any{"version": "3.7.0.Final", "connector": "sqlserver", "name": "legacy", "ts_ms": 1790330000100,
				"snapshot": "false", "db": "erp", "schema": "dbo", "table": "Orders", "change_lsn": lsn, "commit_lsn": lsn, "event_serial_no": 1}},
	}
	out, _ := json.Marshal(env)
	return out
}

func key(id int) []byte {
	return []byte(fmt.Sprintf(`{"schema":{"type":"struct","fields":[{"type":"int32","field":"id"}]},"payload":{"id":%d}}`, id))
}

func setup(t *testing.T, ev Event) (*Conn, *kgo.Client) {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, topic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	prod, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)
	ev.Topic = topic
	conn, err := New(Config{Brokers: c.ListenAddrs(), Events: map[string]Event{"Order.Changed": ev}}, "")
	if err != nil {
		t.Fatal(err)
	}
	return conn, prod
}

func produce(t *testing.T, prod *kgo.Client, partition int32, k, v []byte) {
	t.Helper()
	if err := prod.ProduceSync(context.Background(), &kgo.Record{Topic: topic, Partition: partition, Key: k, Value: v}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// stream runs until n events arrived, and returns them with the last
// position delivered.
func stream(t *testing.T, conn *Conn, resume []byte, n int) ([]connector.Event, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var got []connector.Event
	last := resume
	enough := errors.New("enough")
	err := conn.Stream(ctx, "Order.Changed", resume, func(evs []connector.Event, pos []byte) error {
		got = append(got, evs...)
		last = pos
		if len(got) >= n && n > 0 {
			return enough
		}
		if n == 0 {
			return enough
		}
		return nil
	})
	if !errors.Is(err, enough) {
		t.Fatalf("stream: %v (got %d)", err, len(got))
	}
	return got, last
}

func TestChangesAreDecodedAndResumeExactly(t *testing.T) {
	conn, prod := setup(t, Event{Start: "earliest", Match: map[string]any{"status": "open"}})
	produce(t, prod, 0, key(1), change("c", nil, &order{1, 123456, "open"}, "00000027:00000758:0003"))
	produce(t, prod, 0, key(1), nil)                                                                // a tombstone
	produce(t, prod, 1, key(2), change("c", nil, &order{2, -5, "draft"}, "00000027:00000760:0002")) // no match
	produce(t, prod, 1, key(3), change("u", &order{3, 100, "draft"}, &order{3, 100, "open"}, "00000027:00000770:0002"))
	produce(t, prod, 0, key(4), change("r", nil, &order{4, 1, "open"}, "00000027:00000780:0002")) // a snapshot read: not taken by default

	evs, pos := stream(t, conn, nil, 2)
	if len(evs) != 2 {
		t.Fatalf("events = %d", len(evs))
	}
	byID := map[string]map[string]any{}
	for _, e := range evs {
		var p map[string]any
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			t.Fatal(err)
		}
		byID[fmt.Sprint(p["id"])] = p
	}
	o1 := byID["1"]
	items, _ := o1["items"].([]any)
	meta, _ := o1["debezium"].(map[string]any)
	src, _ := meta["source"].(map[string]any)
	if o1["total"] != 1234.56 || o1["order_date"] != "2026-09-24" || o1["updated_at"] != "2026-09-25T09:53:20.123456789Z" ||
		len(items) != 1 || meta["op"] != "create" || src["table"] != "Orders" || src["connector"] != "sqlserver" {
		t.Fatalf("order 1 = %v", o1)
	}
	if b, _ := byID["3"]["debezium"].(map[string]any)["before"].(map[string]any); b["status"] != "draft" {
		t.Fatalf("update without its before image: %v", byID["3"])
	}

	// Resuming from the stored position reads only what came later; the
	// same change sent again by Debezium (after its own restart) keeps its ID.
	produce(t, prod, 1, key(3), change("u", &order{3, 100, "draft"}, &order{3, 100, "open"}, "00000027:00000770:0002"))
	produce(t, prod, 0, key(5), change("d", &order{5, 700, "open"}, nil, "00000027:00000790:0002"))
	again, _ := stream(t, conn, pos, 2)
	var p5 map[string]any
	for _, e := range again {
		if strings.Contains(string(e.Payload), `"id":5,`) {
			_ = json.Unmarshal(e.Payload, &p5)
		}
	}
	ids := map[string]bool{again[0].ID: true, again[1].ID: true}
	if len(again) != 2 || !ids[evsID(evs, "3")] || p5["id"] == nil {
		t.Fatalf("after resume: %v / %v", again, evs)
	}
	if p5["debezium"].(map[string]any)["op"] != "delete" || p5["total"] != 7.0 {
		t.Fatalf("delete = %v", p5)
	}
}

func evsID(evs []connector.Event, id string) string {
	for _, e := range evs {
		if strings.Contains(string(e.Payload), `"id":`+id+`,`) {
			return e.ID
		}
	}
	return ""
}

func TestLatestStoresItsStartBeforeReading(t *testing.T) {
	conn, prod := setup(t, Event{})
	produce(t, prod, 0, key(1), change("c", nil, &order{1, 100, "open"}, "1"))
	// Nothing to read: the first delivery is the position, at the end.
	evs, pos := stream(t, conn, nil, 0)
	if len(evs) != 0 || string(pos) != `{"0":1,"1":0}` {
		t.Fatalf("events %d, position %s", len(evs), pos)
	}
	produce(t, prod, 1, key(2), change("c", nil, &order{2, 100, "open"}, "2"))
	evs, _ = stream(t, conn, pos, 1)
	if !strings.Contains(string(evs[0].Payload), `"id":2`) {
		t.Fatalf("event = %s", evs[0].Payload)
	}
}

func TestProblemsAreExplained(t *testing.T) {
	conn, prod := setup(t, Event{Start: "earliest"})
	// A flattened event (ExtractNewRecordState) has no envelope.
	produce(t, prod, 0, key(1), []byte(`{"id":1,"status":"open"}`))
	err := conn.Stream(context.Background(), "Order.Changed", nil, func([]connector.Event, []byte) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "ExtractNewRecordState") || !strings.Contains(err.Error(), "offset 0") {
		t.Fatalf("err = %v", err)
	}
	missing, _ := New(Config{Brokers: conn.cfg.Brokers, Events: map[string]Event{"X": {Topic: "legacy.dbo.Missing"}}}, "")
	err = missing.Stream(context.Background(), "X", nil, func([]connector.Event, []byte) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "legacy.dbo.Missing") {
		t.Fatalf("err = %v", err)
	}
	res := missing.Check(context.Background())
	if len(res) != 2 || !res[0].OK || res[1].OK || !strings.Contains(res[1].Fix, "topic.prefix") {
		t.Fatalf("check = %+v", res)
	}
	if res := conn.Check(context.Background()); !res[1].OK || !strings.Contains(res[1].Detail, "2 partition(s)") {
		t.Fatalf("check = %+v", res)
	}
}

func TestDecodeValues(t *testing.T) {
	for cents, want := range map[int64]string{123456: "1234.56", -5: "-0.05", 0: "0.00", -12800: "-128.00", 32767: "327.67"} {
		got, err := decimal(decimalBytes(cents), 2)
		if err != nil || string(got) != want {
			t.Errorf("%d: %s %v, want %s", cents, got, err, want)
		}
	}
	v, _ := convertValue(field{Name: "io.debezium.data.VariableScaleDecimal"}, map[string]any{"scale": json.Number("3"), "value": decimalBytes(1234567)})
	if v != json.Number("1234.567") {
		t.Errorf("variable scale = %v", v)
	}
	v, _ = convertValue(field{Name: "io.debezium.time.MicroTime"}, json.Number("45296000123"))
	if v != "12:34:56.000123" {
		t.Errorf("time = %v", v)
	}
	// Without schemas, values stay as Debezium wrote them.
	env, err := decodeEnvelope([]byte(`{"op":"c","after":{"id":1,"total":"12.50"},"source":{"connector":"mysql","file":"binlog.000003","pos":1234,"row":0}}`))
	if err != nil || env.After["total"] != "12.50" {
		t.Fatalf("env = %+v, err = %v", env, err)
	}
}

func TestConfigRejected(t *testing.T) {
	ok := map[string]Event{"A": {Topic: topic}}
	for name, c := range map[string]Config{
		"no brokers":         {Events: ok},
		"remote without tls": {Brokers: []string{"kafka.example:9092"}, Events: ok},
		"plain without tls":  {Brokers: []string{"127.0.0.1:9092"}, SASL: "plain", Events: ok},
		"bad op":             {Brokers: []string{"127.0.0.1:9092"}, Events: map[string]Event{"A": {Topic: topic, Operations: []string{"truncate"}}}},
		"bad topic":          {Brokers: []string{"127.0.0.1:9092"}, Events: map[string]Event{"A": {Topic: "a b"}}},
	} {
		if _, err := New(c, ""); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := New(Config{Brokers: []string{"kafka.example:9093"}, TLS: true, SASL: "scram-sha-512", Events: ok}, `{"username":"turgon"}`); err == nil {
		t.Error("sasl without a password accepted")
	}
}
