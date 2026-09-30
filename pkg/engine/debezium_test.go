package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

const legacyTopic = "legacy.dbo.Orders"

// legacyChange is a change of dbo.Orders as Debezium's SQL Server
// connector writes it with the JSON converter: the total a DECIMAL(12,2)
// (base64 unscaled bytes), the date a DATE (days since 1970), the items an
// NVARCHAR(MAX) of JSON.
func legacyChange(id int, status string, totalB64 string, lsn string) []byte {
	cols := []any{
		map[string]any{"type": "int32", "field": "id"},
		map[string]any{"type": "string", "field": "customer_email"},
		map[string]any{"type": "bytes", "name": "org.apache.kafka.connect.data.Decimal", "field": "total", "parameters": map[string]any{"scale": "2"}},
		map[string]any{"type": "string", "field": "currency"},
		map[string]any{"type": "int32", "name": "io.debezium.time.Date", "field": "order_date"},
		map[string]any{"type": "string", "name": "io.debezium.data.Json", "field": "items"},
		map[string]any{"type": "string", "field": "status"},
	}
	b, _ := json.Marshal(map[string]any{
		"schema": map[string]any{"type": "struct", "fields": []any{
			map[string]any{"type": "struct", "field": "before", "optional": true, "fields": cols},
			map[string]any{"type": "struct", "field": "after", "optional": true, "fields": cols},
		}},
		"payload": map[string]any{"op": "c", "before": nil, "ts_ms": 1790330000200,
			"after": map[string]any{"id": id, "customer_email": "Ada@Example.com", "total": totalB64, "currency": "eur",
				"order_date": 20720, "items": `[{"sku":"M-7","qty":3}]`, "status": status},
			"source": map[string]any{"connector": "sqlserver", "name": "legacy", "db": "erp", "schema": "dbo", "table": "Orders",
				"change_lsn": lsn, "commit_lsn": lsn, "event_serial_no": 1}},
	})
	return b
}

func TestLegacyOrderCapturedByDebeziumBecomesERPOrder(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, legacyTopic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	addr := cluster.ListenAddrs()[0]
	f := newFixtureFor(t, "legacy-orders-to-erp", connector.StaticSecrets{"openbao://legacy-erp/kafka": `{"username":"turgon","password":"x"}`},
		func(cfg string) string {
			cfg = strings.Replace(cfg, `"brokers":["kafka-1.internal:9093","kafka-2.internal:9093"]`, `"brokers":["`+addr+`"]`, 1)
			return strings.NewReplacer(`"tls":true`, `"tls":false`, `"sasl":"scram-sha-512"`, `"sasl":""`).Replace(cfg)
		})
	if !f.rt.Streams["legacy-erp"]["Order.Placed"] {
		t.Fatalf("streams = %v", f.rt.Streams)
	}
	if err := f.store.PutXref(context.Background(), "Customer", "legacy-erp", "ada@example.com", "C-100"); err != nil {
		t.Fatal(err)
	}
	prod, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	produce := func(v []byte, id int) {
		t.Helper()
		k := []byte(fmt.Sprintf(`{"id":%d}`, id))
		if err := prod.ProduceSync(context.Background(), &kgo.Record{Topic: legacyTopic, Key: k, Value: v}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	// The subscription starts at the topic's end and stores that first.
	delivered, stop := subscribe(t, f)
	waitFor(t, "the starting position", func() bool {
		pos, _ := f.store.StreamResume(context.Background(), InboxSource("legacy-erp", "Order.Placed"))
		return pos != nil
	})
	produce(legacyChange(41, "draft", "AOnA", "00000027:00000758:0003"), 41) // a draft: not taken
	produce(legacyChange(42, "open", "AOnA", "00000027:00000760:0003"), 42)  // 598.40
	wait(t, delivered)

	d := &Dispatcher{Runtime: f.rt, Cursors: f.store, Starter: &recorder{}, Inbox: f.store}
	rec := d.Starter.(*recorder)
	if _, err := d.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(rec.ids) != 1 {
		t.Fatalf("runs = %v", rec.ids)
	}
	res, err, _ := f.run(rec.runs[rec.ids[0]], approve("03-write"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Writes[0].Status != writeguard.StatusCommitted || f.orders(`external_id = 'LEGACY-42' AND customer_id = 'C-100' AND net_value = 598.40
		AND currency = 'EUR' AND order_date = '2026-09-24' AND lines = '[{"material":"M-7","quantity":3}]'`) != 1 {
		t.Fatalf("writes = %+v", res.Writes)
	}

	// Debezium restarts and sends the change again: the inbox knows it.
	stop()
	produce(legacyChange(42, "open", "AOnA", "00000027:00000760:0003"), 42)
	before, _ := f.store.StreamResume(context.Background(), InboxSource("legacy-erp", "Order.Placed"))
	subscribe(t, f)
	waitFor(t, "the replayed change to be read", func() bool {
		pos, _ := f.store.StreamResume(context.Background(), InboxSource("legacy-erp", "Order.Placed"))
		return string(pos) != string(before)
	})
	if _, err := d.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(rec.ids) != 1 {
		t.Fatalf("the replayed change started another run: %v", rec.ids)
	}
}
