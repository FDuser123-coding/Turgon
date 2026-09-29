package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/metrics"
)

// Change capture reads an event from the database's write-ahead log
// through logical replication (the "logical-replication" event
// interface): the application needs no outbox, and every committed insert,
// update or delete of the table is an event, in commit order.
//
// Each event has its own publication (the table, and only the operations
// the event wants) and its own pgoutput replication slot. Polls peek at the
// slot without consuming it; the slot advances only to the position the
// dispatcher has confirmed by polling past it, so a crash re-reads events
// (whose run IDs make them start once) and never skips one. The slot holds
// write-ahead log until it advances: a stopped worker makes the database
// keep WAL, which turgon_cdc_retained_wal_bytes shows and the chart alerts
// on. Setting max_slot_wal_keep_size bounds it.

// Change configures an event read by change capture.
type Change struct {
	// Table is the table whose changes are the event.
	Table string `json:"table"`
	// Operations are the changes that are events: insert, update, delete.
	// Default insert. The payload is the new row; for a delete, the old
	// row's replica identity (the primary key, or every column with
	// REPLICA IDENTITY FULL). Large values an update left unchanged are
	// not in the log and are left out of the payload.
	Operations []string `json:"operations,omitempty"`
	// Slot and Publication default to turgon_<event in snake_case>. Two
	// deployments reading the same database need different names.
	Slot        string `json:"slot,omitempty"`
	Publication string `json:"publication,omitempty"`
}

var pgName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// defaultName derives a slot or publication name from an event name.
func defaultName(event string) string {
	var b strings.Builder
	b.WriteString("turgon_")
	for _, r := range Snake(event) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	s := b.String()
	if len(s) > 63 {
		s = s[:63]
	}
	return s
}

func (ch Change) withDefaults(event string) Change {
	if len(ch.Operations) == 0 {
		ch.Operations = []string{"insert"}
	}
	if ch.Slot == "" {
		ch.Slot = defaultName(event)
	}
	if ch.Publication == "" {
		ch.Publication = defaultName(event)
	}
	return ch
}

func (ch Change) validate() error {
	if ch.Table == "" {
		return errors.New("table is required")
	}
	for _, op := range ch.Operations {
		if op != "insert" && op != "update" && op != "delete" {
			return fmt.Errorf("unknown operation %q (want insert, update or delete)", op)
		}
	}
	for _, n := range []string{ch.Slot, ch.Publication} {
		if !pgName.MatchString(n) {
			return fmt.Errorf("%q is not a valid slot or publication name (lower-case letters, digits and _, at most 63)", n)
		}
	}
	return nil
}

func (ch Change) wants(op string) bool {
	for _, o := range ch.Operations {
		if o == op {
			return true
		}
	}
	return false
}

// Positions order changes by the LSN where their transaction's commit
// record starts, then by their index in the transaction: LSNs of changes
// themselves do not follow commit order when transactions interleave.
const (
	indexBits = 15
	maxIndex  = 1<<indexBits - 1
	maxLSN    = 1<<(63-indexBits) - 1
)

func position(commit uint64, index int) int64 { return int64(commit<<indexBits | uint64(index)) }

func commitOf(pos int64) uint64 { return uint64(pos) >> indexBits }

func parseLSN(s string) (uint64, error) {
	hi, lo, ok := strings.Cut(s, "/")
	if !ok {
		return 0, fmt.Errorf("invalid LSN %q", s)
	}
	h, err := strconv.ParseUint(hi, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid LSN %q", s)
	}
	l, err := strconv.ParseUint(lo, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid LSN %q", s)
	}
	return h<<32 | l, nil
}

func formatLSN(lsn uint64) string { return fmt.Sprintf("%X/%X", lsn>>32, uint32(lsn)) }

// prepare makes sure the event's publication and slot exist, creating them
// if needed, and returns the table's OID.
func (c *Conn) prepare(ctx context.Context, event string, ch Change) (uint32, error) {
	c.mu.Lock()
	oid, ok := c.prepared[event]
	c.mu.Unlock()
	if ok {
		return oid, nil
	}
	var reloid *uint32
	if err := c.pool.QueryRow(ctx, `SELECT to_regclass($1)::oid`, ch.Table).Scan(&reloid); err != nil {
		return 0, err
	}
	if reloid == nil {
		return 0, fmt.Errorf("table %s does not exist", ch.Table)
	}
	var published bool
	err := c.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`, ch.Publication).Scan(&published)
	if err != nil {
		return 0, err
	}
	if !published {
		if err := c.replicaIdentity(ctx, *reloid, ch); err != nil {
			return 0, err
		}
		q := fmt.Sprintf(`CREATE PUBLICATION %s FOR TABLE %s WITH (publish = '%s')`,
			pgx.Identifier{ch.Publication}.Sanitize(), ident(ch.Table), strings.Join(ch.Operations, ", "))
		if _, err := c.pool.Exec(ctx, q); err != nil {
			return 0, fmt.Errorf("create publication %s: %w (a user owning the table can run: %s)", ch.Publication, err, q)
		}
	}
	var slot bool
	err = c.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, ch.Slot).Scan(&slot)
	if err != nil {
		return 0, err
	}
	if !slot {
		_, err := c.pool.Exec(ctx, `SELECT pg_create_logical_replication_slot($1, 'pgoutput')`, ch.Slot)
		var pe *pgconn.PgError
		if err != nil && !(errors.As(err, &pe) && pe.Code == "42710") { // created meanwhile by another worker
			return 0, fmt.Errorf("create replication slot %s: %w (it needs wal_level = logical and a user with the REPLICATION attribute)", ch.Slot, err)
		}
	}
	c.mu.Lock()
	c.prepared[event] = *reloid
	c.mu.Unlock()
	return *reloid, nil
}

// replicaIdentity refuses to publish updates or deletes of a table without
// a replica identity: Postgres would then reject the application's own
// updates and deletes of it.
func (c *Conn) replicaIdentity(ctx context.Context, reloid uint32, ch Change) error {
	if !ch.wants("update") && !ch.wants("delete") {
		return nil
	}
	var ident string
	var pk bool
	err := c.pool.QueryRow(ctx, `SELECT relreplident::text, EXISTS (SELECT 1 FROM pg_index WHERE indrelid = $1 AND indisprimary)
		FROM pg_class WHERE oid = $1`, reloid).Scan(&ident, &pk)
	if err != nil {
		return err
	}
	if ident == "n" || (ident == "d" && !pk) {
		return fmt.Errorf("%s has no primary key or replica identity: publishing its updates and deletes would make Postgres reject them. "+
			"Add a primary key, or ALTER TABLE %s REPLICA IDENTITY FULL", ch.Table, ch.Table)
	}
	return nil
}

// pollChanges reads the changes after a position.
func (c *Conn) pollChanges(ctx context.Context, event string, ch Change, after int64, limit int) ([]connector.Event, error) {
	reloid, err := c.prepare(ctx, event, ch)
	if err != nil {
		return nil, fmt.Errorf("postgres: %s: %w", event, err)
	}
	var confirmedText string
	var retained int64
	err = c.pool.QueryRow(ctx, `SELECT confirmed_flush_lsn::text, COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint
		FROM pg_replication_slots WHERE slot_name = $1`, ch.Slot).Scan(&confirmedText, &retained)
	if errors.Is(err, pgx.ErrNoRows) {
		c.forget(event)
		return nil, fmt.Errorf("postgres: %s: replication slot %s was dropped", event, ch.Slot)
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: %s: %w", event, err)
	}
	metrics.CDCRetainedWAL.WithLabelValues(ch.Slot).Set(float64(retained))
	confirmed, err := parseLSN(confirmedText)
	if err != nil {
		return nil, err
	}
	// Everything committed before the confirmed position's transaction
	// has started its runs: release it. The transaction itself is decoded
	// again, since the dispatcher may have stopped partway through it.
	if l := commitOf(after); after > 0 && l > confirmed {
		if err := c.advance(ctx, ch.Slot, l); err != nil {
			return nil, err
		}
	}
	var uptoText string
	if err := c.pool.QueryRow(ctx, `SELECT pg_current_wal_flush_lsn()::text`).Scan(&uptoText); err != nil {
		return nil, err
	}
	upto, err := parseLSN(uptoText)
	if err != nil {
		return nil, err
	}
	// A transaction is returned whole, so a limit on rows is only a guide.
	budget := limit*4 + 64
	rows, err := c.pool.Query(ctx, `SELECT data FROM pg_logical_slot_peek_binary_changes($1, $2::pg_lsn, $3,
		'proto_version', '1', 'publication_names', $4)`, ch.Slot, uptoText, budget, ch.Publication)
	if err != nil {
		return nil, busy(fmt.Errorf("postgres: %s: read slot %s: %w", event, ch.Slot, err))
	}
	defer rows.Close()
	var (
		out    []connector.Event
		rels   = map[uint32]*relation{}
		commit uint64
		index  int
		read   int
	)
	for rows.Next() {
		read++
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		if len(out) >= limit {
			continue // drain: the rest waits for the next poll
		}
		m, err := decode(data)
		if err != nil {
			return nil, fmt.Errorf("postgres: %s: %w", event, err)
		}
		switch m.kind {
		case 'B':
			commit, index = m.finalLSN, 0
			if commit > maxLSN {
				return nil, fmt.Errorf("postgres: %s: LSN %s is past what change capture positions can hold", event, formatLSN(commit))
			}
		case 'R':
			rels[m.rel.id] = m.rel
		case 'I', 'U', 'D':
			if m.relID != reloid {
				continue
			}
			index++
			if index > maxIndex {
				return nil, fmt.Errorf("postgres: %s: the transaction committed at %s changed more than %d rows of %s; "+
					"change capture cannot order more (use the outbox for bulk changes)", event, formatLSN(commit), maxIndex, ch.Table)
			}
			op := map[byte]string{'I': "insert", 'U': "update", 'D': "delete"}[m.kind]
			pos := position(commit, index)
			if pos <= after || !ch.wants(op) {
				continue
			}
			rel := rels[m.relID]
			if rel == nil {
				return nil, fmt.Errorf("postgres: %s: change to relation %d before its description", event, m.relID)
			}
			row := m.newRow
			if m.kind == 'D' {
				row = m.oldRow
			}
			payload, err := rowJSON(rel, row)
			if err != nil {
				return nil, fmt.Errorf("postgres: %s: %w", event, err)
			}
			// IDs avoid "/", which run IDs and URLs use as a separator.
			out = append(out, connector.Event{ID: fmt.Sprintf("%016X.%d", commit, index), Position: pos, Name: event, Payload: payload})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, busy(fmt.Errorf("postgres: %s: read slot %s: %w", event, ch.Slot, err))
	}
	// Nothing to deliver up to the flush position, and the slot was read
	// that far: release it, so a quiet table does not hold the log a busy
	// database writes. Transactions that commit later start after it.
	if len(out) == 0 && read < budget && upto > max(confirmed, commitOf(after)) {
		if err := c.advance(ctx, ch.Slot, upto); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (c *Conn) advance(ctx context.Context, slot string, lsn uint64) error {
	if _, err := c.pool.Exec(ctx, `SELECT pg_replication_slot_advance($1, $2::pg_lsn)`, slot, formatLSN(lsn)); err != nil {
		return busy(fmt.Errorf("postgres: advance slot %s: %w", slot, err))
	}
	return nil
}

func (c *Conn) forget(event string) {
	c.mu.Lock()
	delete(c.prepared, event)
	c.mu.Unlock()
}

// ErrSlotBusy: another worker is reading the slot. Only one reads at a
// time; the others find nothing new until it is released.
var ErrSlotBusy = errors.New("replication slot is in use by another worker")

func busy(err error) error {
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == "55006" { // object_in_use
		return fmt.Errorf("%w: %v", ErrSlotBusy, err)
	}
	return err
}

var textTypes = pgtype.NewMap()

// rowJSON turns a decoded row into a JSON object: numbers, booleans and
// JSON columns keep their type, timestamps become RFC 3339, everything else
// is its text form.
func rowJSON(rel *relation, row []tupleValue) (json.RawMessage, error) {
	if len(row) != len(rel.columns) {
		return nil, fmt.Errorf("%s.%s: row has %d columns, relation %d", rel.namespace, rel.name, len(row), len(rel.columns))
	}
	obj := make(map[string]any, len(row))
	for i, v := range row {
		col := rel.columns[i]
		switch v.kind {
		case 'u':
			continue
		case 'n':
			obj[col.name] = nil
			continue
		}
		obj[col.name] = textValue(col.oid, v.data)
	}
	return json.Marshal(obj)
}

func textValue(oid uint32, data []byte) any {
	s := string(data)
	switch oid {
	case pgtype.BoolOID:
		return s == "t"
	case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID, pgtype.OIDOID:
		return json.Number(s)
	case pgtype.Float4OID, pgtype.Float8OID, pgtype.NumericOID:
		if f, err := strconv.ParseFloat(s, 64); err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
			return s // NaN, Infinity
		}
		return json.Number(s)
	case pgtype.JSONOID, pgtype.JSONBOID:
		if json.Valid(data) {
			return json.RawMessage(data)
		}
	case pgtype.TimestamptzOID:
		var t time.Time
		if textTypes.Scan(oid, pgtype.TextFormatCode, data, &t) == nil {
			return t.UTC().Format(time.RFC3339Nano)
		}
	case pgtype.TimestampOID:
		var t time.Time
		if textTypes.Scan(oid, pgtype.TextFormatCode, data, &t) == nil {
			return t.Format("2006-01-02T15:04:05.999999999")
		}
	}
	return s
}

// changeEvents lists the events read by change capture, sorted.
func (c *Conn) changeEvents() []string {
	names := make([]string, 0, len(c.cfg.Changes))
	for name := range c.cfg.Changes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
