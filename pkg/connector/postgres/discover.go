package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/meta"
)

var _ connector.Discoverer = (*Conn)(nil)

// Discover reads the tables the configuration uses, and those named in
// objects ("schema.table", or "schema.*" for a whole schema), from the
// database's catalog: columns, primary keys and foreign keys.
func (c *Conn) Discover(ctx context.Context, objects []string) (meta.Catalog, error) {
	cat := meta.Catalog{DiscoveredAt: time.Now().UTC()}
	type use struct {
		table, column, by string
		creates           bool
	}
	var uses []use
	names := map[string]bool{}
	add := func(table, column, by string) {
		names[table] = true
		uses = append(uses, use{table: table, column: column, by: by})
	}
	if o := c.cfg.Outbox; o != nil {
		for _, col := range []string{"id", "event", "payload"} {
			add(o.Table, col, "outbox")
		}
	}
	for name, op := range c.cfg.Operations {
		by := "operation " + name
		add(op.Table, op.Key, by)
		for _, col := range op.Columns {
			add(op.Table, col, by)
		}
		if op.Action == "insert" {
			uses[len(uses)-1].creates = true
		}
		for col := range op.Set {
			add(op.Table, col, by)
		}
	}
	for name, ch := range c.cfg.Changes {
		add(ch.Table, "", "change event "+name)
	}
	var schemas []string
	for _, o := range objects {
		if s, ok := strings.CutSuffix(o, ".*"); ok {
			schemas = append(schemas, s)
		} else {
			names[o] = true
		}
	}
	list := make([]string, 0, len(names))
	for n := range names {
		list = append(list, n)
	}
	sort.Strings(list)

	// Names resolve as the connector's queries resolve them (search_path).
	oids := map[string]uint32{}
	canonical := map[uint32]string{}
	rows, err := c.pool.Query(ctx, `
		SELECT r.oid, n.nspname || '.' || r.relname, coalesce(q.name, '')
		FROM pg_class r JOIN pg_namespace n ON n.oid = r.relnamespace
		LEFT JOIN unnest($1::text[]) AS q(name) ON to_regclass(q.name) = r.oid
		WHERE r.relkind IN ('r', 'p', 'v', 'm') AND (q.name IS NOT NULL OR n.nspname = ANY($2))`, list, schemas)
	if err != nil {
		return cat, fmt.Errorf("postgres: discover: %w", err)
	}
	for rows.Next() {
		var oid uint32
		var full, asked string
		if err := rows.Scan(&oid, &full, &asked); err != nil {
			rows.Close()
			return cat, fmt.Errorf("postgres: discover: %w", err)
		}
		canonical[oid] = full
		if asked != "" {
			oids[asked] = oid
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return cat, fmt.Errorf("postgres: discover: %w", err)
	}
	cat.Events = map[string]string{}
	for name, ch := range c.cfg.Changes { // a change event carries its table's row
		if oid, ok := oids[ch.Table]; ok {
			cat.Events[name] = canonical[oid]
		} else {
			cat.Events[name] = ch.Table
		}
	}
	for _, u := range uses {
		if oid, ok := oids[u.table]; ok {
			cat.Uses = append(cat.Uses, meta.Use{Object: canonical[oid], Field: u.column, By: u.by, Creates: u.creates})
		} else {
			cat.Uses = append(cat.Uses, meta.Use{Object: u.table, Field: u.column, By: u.by, Creates: u.creates}) // missing: drift shows it
		}
	}
	all := make([]uint32, 0, len(canonical))
	for oid := range canonical {
		all = append(all, oid)
	}
	if len(all) == 0 {
		return cat, nil
	}

	cols, err := c.pool.Query(ctx, `
		SELECT a.attrelid, r.relkind::text, a.attname, format_type(a.atttypid, NULL),
		       a.attnotnull AND NOT a.atthasdef AND a.attidentity = '' AND a.attgenerated = '',
		       a.attgenerated <> '',
		       CASE WHEN a.atttypid IN ('varchar'::regtype, 'bpchar'::regtype) AND a.atttypmod > 4 THEN a.atttypmod - 4 ELSE 0 END,
		       coalesce(col_description(a.attrelid, a.attnum), '')
		FROM pg_attribute a JOIN pg_class r ON r.oid = a.attrelid
		WHERE a.attrelid = ANY($1) AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attrelid, a.attnum`, all)
	if err != nil {
		return cat, fmt.Errorf("postgres: discover: %w", err)
	}
	objs := map[uint32]*meta.Object{}
	for cols.Next() {
		var oid uint32
		var kind, name, typ, comment string
		var required, generated bool
		var length int
		if err := cols.Scan(&oid, &kind, &name, &typ, &required, &generated, &length, &comment); err != nil {
			cols.Close()
			return cat, fmt.Errorf("postgres: discover: %w", err)
		}
		o, ok := objs[oid]
		if !ok {
			k := map[string]string{"v": "view", "m": "view"}[kind]
			if k == "" {
				k = "table"
			}
			o = &meta.Object{Name: canonical[oid], Kind: k}
			objs[oid] = o
		}
		o.Fields = append(o.Fields, meta.Field{Name: name, Type: typ, Label: comment, Length: length, Required: required, ReadOnly: generated})
	}
	cols.Close()
	if err := cols.Err(); err != nil {
		return cat, fmt.Errorf("postgres: discover: %w", err)
	}

	keys, err := c.pool.Query(ctx, `
		SELECT con.conrelid, con.contype::text, a.attname, coalesce(fn.nspname || '.' || f.relname, ''),
		       coalesce(fa.attname, ''), con.conname
		FROM pg_constraint con
		CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, i)
		JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
		LEFT JOIN pg_class f ON f.oid = con.confrelid
		LEFT JOIN pg_namespace fn ON fn.oid = f.relnamespace
		LEFT JOIN pg_attribute fa ON fa.attrelid = con.confrelid AND fa.attnum = con.confkey[k.i]
		WHERE con.contype IN ('p', 'f') AND con.conrelid = ANY($1)`, all)
	if err != nil {
		return cat, fmt.Errorf("postgres: discover keys: %w", err)
	}
	for keys.Next() {
		var oid uint32
		var typ, col, to, toCol, name string
		if err := keys.Scan(&oid, &typ, &col, &to, &toCol, &name); err != nil {
			keys.Close()
			return cat, fmt.Errorf("postgres: discover keys: %w", err)
		}
		o := objs[oid]
		if o == nil {
			continue
		}
		if typ == "p" {
			for i := range o.Fields {
				if o.Fields[i].Name == col {
					o.Fields[i].Key = true
				}
			}
			continue
		}
		o.Links = append(o.Links, meta.Link{Name: name + "." + col, To: to, ToField: toCol})
	}
	keys.Close()
	if err := keys.Err(); err != nil {
		return cat, fmt.Errorf("postgres: discover keys: %w", err)
	}
	for _, o := range objs {
		cat.Objects = append(cat.Objects, *o)
	}
	outbox := ""
	if o := c.cfg.Outbox; o != nil {
		outbox = o.Table
		if oid, ok := oids[o.Table]; ok {
			outbox = canonical[oid]
		}
	}
	if err := c.sampleOutbox(ctx, &cat, outbox); err != nil {
		return cat, err
	}
	cat.Normalize()
	return cat, nil
}

// outboxSample is how many recent outbox rows describe each event.
const outboxSample = 500

// sampleOutbox describes each outbox event's payload: the application
// writes it as JSON, so its fields are those seen in the latest rows. An
// event's mapping reads them.
func (c *Conn) sampleOutbox(ctx context.Context, cat *meta.Catalog, table string) error {
	o := c.cfg.Outbox
	if o == nil {
		return nil
	}
	rows, err := c.pool.Query(ctx, fmt.Sprintf(`SELECT event, payload FROM %s ORDER BY id DESC LIMIT %d`, ident(o.Table), outboxSample))
	if err != nil {
		return nil // the table is missing: the outbox's uses show it
	}
	defer rows.Close()
	byEvent := map[string][]map[string]any{}
	for rows.Next() {
		var event string
		var payload []byte
		if err := rows.Scan(&event, &payload); err != nil {
			return fmt.Errorf("postgres: discover outbox: %w", err)
		}
		var doc map[string]any
		if json.Unmarshal(payload, &doc) == nil {
			byEvent[event] = append(byEvent[event], doc)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("postgres: discover outbox: %w", err)
	}
	for event, docs := range byEvent {
		name := table + "#" + event
		cat.Objects = append(cat.Objects, meta.Object{Name: name, Kind: "outbox payload", Sampled: true, Fields: meta.SampleFields(docs),
			Label: fmt.Sprintf("the %s payload in the latest %d outbox rows", event, len(docs))})
		cat.Events[event] = name
	}
	return nil
}
