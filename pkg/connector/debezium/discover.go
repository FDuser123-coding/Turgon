package debezium

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/meta"
)

var _ connector.Discoverer = (*Conn)(nil)

// Discover describes each event's table from the newest change on its
// topic: its columns as the change's Kafka Connect schema declares them
// (type, precision and scale, optional or not, and, with Debezium's
// column.propagate.source.type, the database's own type and length), and
// its key from the record key's schema. A topic written without schemas
// is described by the values of its newest changes instead. A topic with
// no change yet is left out until one arrives. objects is not used.
func (c *Conn) Discover(ctx context.Context, _ []string) (meta.Catalog, error) {
	cat := meta.Catalog{DiscoveredAt: time.Now().UTC(), Events: map[string]string{}}
	cl, err := kgo.NewClient(c.opts...)
	if err != nil {
		return cat, err
	}
	adm := kadm.NewClient(cl)
	defer cl.Close()
	names := make([]string, 0, len(c.cfg.Events))
	for n := range c.cfg.Events {
		names = append(names, n)
	}
	sort.Strings(names)
	described := map[string]bool{}
	for _, name := range names {
		ev := c.cfg.Events[name]
		by := "event " + name
		cat.Events[name] = ev.Topic
		cat.Uses = append(cat.Uses, meta.Use{Object: ev.Topic, By: by})
		for col := range ev.Match {
			cat.Uses = append(cat.Uses, meta.Use{Object: ev.Topic, Field: col, By: by + " (match)"})
		}
		if described[ev.Topic] {
			continue
		}
		starts, ends, err := offsets(ctx, adm, ev.Topic)
		switch {
		case errors.Is(err, kerr.UnknownTopicOrPartition) || (err != nil && strings.Contains(err.Error(), "does not exist")):
			continue // missing: the event's use shows it
		case err != nil:
			return cat, err
		}
		recs, err := c.newest(ctx, ev.Topic, starts, ends)
		if err != nil {
			return cat, err
		}
		if len(recs) == 0 {
			// No change yet: nothing to describe, and nothing missing.
			kept := cat.Uses[:0]
			for _, u := range cat.Uses {
				if u.Object != ev.Topic {
					kept = append(kept, u)
				}
			}
			cat.Uses = kept
			continue
		}
		o, err := describe(ev.Topic, recs)
		if err != nil {
			return cat, fmt.Errorf("debezium: discover %s: %w", ev.Topic, err)
		}
		described[ev.Topic] = true
		cat.Objects = append(cat.Objects, o)
	}
	cat.Normalize()
	return cat, nil
}

// newest reads the last record of each partition that holds one.
func (c *Conn) newest(ctx context.Context, topic string, starts, ends position) ([]*kgo.Record, error) {
	assign := map[int32]kgo.Offset{}
	for p, end := range ends {
		if end > starts[p] {
			assign[p] = kgo.NewOffset().At(end - 1)
		}
	}
	if len(assign) == 0 {
		return nil, nil
	}
	cl, err := kgo.NewClient(append(c.opts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: assign}))...)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	got := map[int32]*kgo.Record{}
	for len(got) < len(assign) {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil, fmt.Errorf("debezium: %s: the newest changes did not arrive: %w", topic, ctx.Err())
		}
		var ferr error
		fetches.EachError(func(_ string, p int32, err error) {
			if ferr == nil {
				ferr = fmt.Errorf("debezium: %s partition %d: %w", topic, p, err)
			}
		})
		if ferr != nil {
			return nil, ferr
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if r.Offset == ends[r.Partition]-1 {
				got[r.Partition] = r
			}
		})
	}
	out := make([]*kgo.Record, 0, len(got))
	for _, r := range got {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.After(out[j].Timestamp) })
	return out, nil
}

// describe makes the table's object from its newest changes (newest
// first): the schema of the newest one, or the values of all of them.
func describe(topic string, recs []*kgo.Record) (meta.Object, error) {
	o := meta.Object{Name: topic, Kind: "change topic"}
	keys := map[string]bool{}
	for _, f := range keySchema(recs[0].Key) {
		keys[f] = true
	}
	var top struct {
		Schema *field `json:"schema"`
	}
	if err := json.Unmarshal(recs[0].Value, &top); err == nil && top.Schema != nil {
		for _, f := range top.Schema.Fields {
			if (f.Field != "after" && f.Field != "before") || len(f.Fields) == 0 {
				continue
			}
			for _, col := range f.Fields {
				o.Fields = append(o.Fields, column(col, keys[col.Field]))
			}
			return o, nil
		}
	}
	// No schema: the values say what there is.
	var rows []map[string]any
	for _, r := range recs {
		env, err := decodeEnvelope(r.Value)
		if err != nil {
			return o, err
		}
		for _, row := range []map[string]any{env.After, env.Before} {
			if row != nil {
				rows = append(rows, row)
			}
		}
	}
	o.Sampled = true
	o.Fields = meta.SampleFields(rows)
	for i := range o.Fields {
		o.Fields[i].Key = keys[o.Fields[i].Name]
	}
	return o, nil
}

// column describes a column from its Connect schema field.
func column(f field, key bool) meta.Field {
	typ := f.Type
	if f.Name != "" {
		typ = f.Name[strings.LastIndex(f.Name, ".")+1:] // Decimal, MicroTimestamp, Json...
	}
	if f.Name == "org.apache.kafka.connect.data.Decimal" {
		typ = fmt.Sprintf("decimal(%s,%s)", or(f.Parameters["connect.decimal.precision"], "?"), or(f.Parameters["scale"], "?"))
	}
	// A column the database fills (a default, a sequence) is not required
	// of a writer.
	out := meta.Field{Name: f.Field, Type: typ, Key: key, Required: !f.Optional && !key && f.Default == nil}
	// With column.propagate.source.type, Debezium sends the database's
	// own type and length; a length is kept for bounded text only (other
	// types' are display widths, unbounded text's is 2^31-1).
	if src := f.Parameters["__debezium.source.column.type"]; src != "" {
		out.Label = "source type " + src
		if n, err := strconv.Atoi(f.Parameters["__debezium.source.column.length"]); err == nil && f.Type == "string" && f.Name == "" && n < math.MaxInt32 {
			out.Length = n
		}
	}
	return out
}

// keySchema names the key's columns from a JSON key with its schema.
func keySchema(key []byte) []string {
	var k struct {
		Schema *field `json:"schema"`
	}
	if json.Unmarshal(key, &k) != nil || k.Schema == nil {
		return nil
	}
	var out []string
	for _, f := range k.Schema.Fields {
		out = append(out, f.Field)
	}
	return out
}

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
