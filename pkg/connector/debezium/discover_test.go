package debezium

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/meta"
)

// The table is described by its newest change's schema; a topic without
// changes yet is left out.
func TestDiscoverReadsTheSchemaOfTheNewestChange(t *testing.T) {
	conn, prod := setup(t, Event{Match: map[string]any{"status": "open"}})
	ctx := context.Background()
	cat, err := conn.Discover(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(cat.Objects) != 0 || len(meta.MissingUses(cat)) != 0 || cat.Events["Order.Changed"] != topic {
		t.Fatalf("empty topic: %+v", cat)
	}

	produce(t, prod, 0, key(1), change("c", nil, &order{1, 1250, "open"}, "00000027:0000001a:0001"))
	produce(t, prod, 1, key(2), change("u", &order{2, 990, "open"}, &order{2, 990, "paid"}, "00000027:0000001b:0001"))
	cat, err = conn.Discover(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	o, ok := cat.Object(topic)
	if !ok || o.Sampled || o.Kind != "change topic" || len(o.Fields) != len(orderSchema) {
		t.Fatalf("objects %+v", cat.Objects)
	}
	f := func(name string) meta.Field { v, _ := o.Field(name); return v }
	if id := f("id"); !id.Key || id.Type != "int32" {
		t.Fatalf("id %+v", id)
	}
	if total := f("total"); total.Type != "decimal(12,2)" || !total.Required {
		t.Fatalf("total %+v", total)
	}
	if d := f("order_date"); d.Type != "Date" {
		t.Fatalf("order_date %+v", d)
	}
	if by := meta.Usage(cat)[topic+".status"]; strings.Join(by, ";") != "event Order.Changed (match)" {
		t.Fatalf("status used by %v", by)
	}

	// A column dropped from the table: its next change's schema lacks it.
	orderSchema = orderSchema[:len(orderSchema)-1]
	defer func() { orderSchema = append(orderSchema, field{Type: "string", Field: "status"}) }()
	produce(t, prod, 0, key(3), change("c", nil, &order{3, 100, "open"}, "00000027:0000001c:0001"))
	after, err := conn.Discover(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	changes := meta.Compare(cat, after)
	if len(changes) != 1 || changes[0].Kind != meta.FieldRemoved || changes[0].Field != "status" || !changes[0].Breaks() {
		t.Fatalf("changes %+v", changes)
	}
}

// Without schemas, the newest changes' values describe the table.
func TestDiscoverWithoutSchemas(t *testing.T) {
	conn, prod := setup(t, Event{})
	full := change("c", nil, &order{1, 1250, "open"}, "00000027:0000001a:0001")
	var env map[string]json.RawMessage
	_ = json.Unmarshal(full, &env)
	produce(t, prod, 0, []byte(`{"id":1}`), env["payload"])
	cat, err := conn.Discover(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	o, ok := cat.Object(topic)
	if !ok || !o.Sampled {
		t.Fatalf("objects %+v", cat.Objects)
	}
	if id, _ := o.Field("id"); id.Type != "number" || id.Key {
		t.Fatalf("id %+v", id)
	}
}

func TestColumnFromConnectSchema(t *testing.T) {
	src := func(typ, length string) map[string]string {
		return map[string]string{"__debezium.source.column.type": typ, "__debezium.source.column.length": length}
	}
	for _, c := range []struct {
		f    field
		want meta.Field
	}{
		{field{Type: "string", Field: "currency", Parameters: src("BPCHAR", "3")}, meta.Field{Name: "currency", Type: "string", Label: "source type BPCHAR", Length: 3, Required: true}},
		{field{Type: "string", Field: "note", Optional: true, Parameters: src("TEXT", "2147483647")}, meta.Field{Name: "note", Type: "string", Label: "source type TEXT"}},
		{field{Type: "string", Field: "status", Default: "open", Parameters: src("TEXT", "2147483647")}, meta.Field{Name: "status", Type: "string", Label: "source type TEXT"}},
		{field{Type: "int32", Name: "io.debezium.time.Date", Field: "order_date", Parameters: src("DATE", "13")}, meta.Field{Name: "order_date", Type: "Date", Label: "source type DATE", Required: true}},
	} {
		if got := column(c.f, false); got != c.want {
			t.Errorf("%s: %+v, want %+v", c.f.Field, got, c.want)
		}
	}
}

// A topic that does not exist (the table is no longer captured) is missing.
func TestDiscoverMissingTopic(t *testing.T) {
	conn, _ := setup(t, Event{})
	conn.cfg.Events["Invoice.Changed"] = Event{Topic: "legacy.dbo.Invoices"}
	cat, err := conn.Discover(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	m := meta.MissingUses(cat)
	if len(m) != 1 || m[0].String() != "legacy.dbo.Invoices; used by event Invoice.Changed" {
		t.Fatalf("missing %v", m)
	}
}
