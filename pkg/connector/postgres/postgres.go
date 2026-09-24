// Package postgres is the prototype's native Postgres connector (architecture
// §13: JDBC reads and writes, events from a transactional outbox). Change
// data capture through Debezium replaces the outbox poller in production.
//
// Writes pass the payload to Postgres as one jsonb parameter and let
// jsonb_populate_record convert it to the table's column types; only table
// and column names come from configuration, and they are quoted.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// Name is the connector name this package implements.
const Name = "postgres"

// Config is the connection-specific configuration (Connection.spec.config).
type Config struct {
	Outbox     *Outbox              `json:"outbox,omitempty"`
	Operations map[string]Operation `json:"operations,omitempty"`
}

// Outbox is a table with columns id (bigint, increasing), event (text) and
// payload (jsonb), written in the same transaction as the business change.
type Outbox struct {
	Table string `json:"table"`
}

// Operation binds a manifest operation to a table.
type Operation struct {
	Table string `json:"table"`
	// Action is insert, update, delete, or select (a read by key).
	Action string `json:"action"`
	// Key is the column that identifies a row. For inserts it must have a
	// unique constraint; a repeated insert then returns the existing row.
	Key string `json:"key"`
	// Columns lists the columns written from the payload. Payload fields
	// are matched in snake_case, so netValue fills net_value. On insert, a
	// column absent from the payload keeps its database default.
	Columns []string `json:"columns,omitempty"`
	// Set assigns fixed values on update, e.g. {"status": "cancelled"}.
	Set map[string]any `json:"set,omitempty"`
}

// Factory builds a Postgres connector. The DSN is the resolved secret.
func Factory(ctx context.Context, cfg compiler.ConnectorConfig, secrets connector.SecretResolver) (connector.Instance, error) {
	var c Config
	if len(cfg.Config) > 0 {
		if err := json.Unmarshal(cfg.Config, &c); err != nil {
			return nil, fmt.Errorf("postgres %s: config: %w", cfg.Endpoint, err)
		}
	}
	for name, op := range c.Operations {
		if err := op.validate(); err != nil {
			return nil, fmt.Errorf("postgres %s: operation %s: %w", cfg.Endpoint, name, err)
		}
	}
	dsn, err := secrets.Resolve(ctx, cfg.SecretRef)
	if err != nil {
		return nil, fmt.Errorf("postgres %s: %w", cfg.Endpoint, err)
	}
	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres %s: invalid DSN in %s", cfg.Endpoint, cfg.SecretRef) // never echo the DSN
	}
	if cfg.Limits.MaxConcurrentCalls > 0 {
		pc.MaxConns = int32(cfg.Limits.MaxConcurrentCalls)
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("postgres %s: connect: %w", cfg.Endpoint, err)
	}
	return New(pool, c), nil
}

// Conn is a connector instance.
type Conn struct {
	pool *pgxpool.Pool
	cfg  Config
}

// New wraps an existing pool.
func New(pool *pgxpool.Pool, cfg Config) *Conn { return &Conn{pool: pool, cfg: cfg} }

var (
	_ connector.Instance   = (*Conn)(nil)
	_ connector.Source     = (*Conn)(nil)
	_ writeguard.Confirmer = (*Conn)(nil)
)

func (c *Conn) Close() { c.pool.Close() }

func (op Operation) validate() error {
	if op.Table == "" || op.Key == "" {
		return errors.New("table and key are required")
	}
	switch op.Action {
	case "insert":
		if len(op.Columns) == 0 {
			return errors.New("insert needs columns")
		}
	case "update":
		if len(op.Columns) == 0 && len(op.Set) == 0 {
			return errors.New("update needs columns or set")
		}
	case "delete", "select":
	default:
		return fmt.Errorf("unknown action %q", op.Action)
	}
	return nil
}

var _ writeguard.Reader = (*Conn)(nil)

// Read returns the row whose key column equals id. With Columns set, only
// those columns are returned.
func (c *Conn) Read(ctx context.Context, name, id string) (json.RawMessage, error) {
	op, err := c.operation(name)
	if err != nil {
		return nil, err
	}
	if op.Action != "select" {
		return nil, fmt.Errorf("postgres: operation %q is a %s, not a read", name, op.Action)
	}
	var raw []byte
	err = c.pool.QueryRow(ctx, fmt.Sprintf(`SELECT to_jsonb(t) FROM %s t WHERE t.%s::text = $1`,
		ident(op.Table), pgx.Identifier{op.Key}.Sanitize()), id).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, writeguard.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read %s: %w", op.Table, err)
	}
	if len(op.Columns) == 0 {
		return raw, nil
	}
	var row map[string]any
	if err := json.Unmarshal(raw, &row); err != nil {
		return nil, err
	}
	out := make(map[string]any, len(op.Columns))
	for _, col := range op.Columns {
		out[col] = row[col]
	}
	return json.Marshal(out)
}

func ident(name string) string {
	return pgx.Identifier(strings.Split(name, ".")).Sanitize()
}

func idents(names []string) string {
	q := make([]string, len(names))
	for i, n := range names {
		q[i] = pgx.Identifier{n}.Sanitize()
	}
	return strings.Join(q, ", ")
}

// Snake converts a field name to snake_case: netValue -> net_value,
// customerID -> customer_id.
func Snake(s string) string {
	var b strings.Builder
	prev := rune(0)
	for _, r := range s {
		if unicode.IsUpper(r) && (unicode.IsLower(prev) || unicode.IsDigit(prev)) {
			b.WriteByte('_')
		}
		b.WriteRune(unicode.ToLower(r))
		prev = r
	}
	return b.String()
}

// row converts a payload to column values: snake_case keys, fixed values
// from Set applied last.
func (op Operation) row(payload json.RawMessage) (map[string]any, error) {
	var in map[string]any
	if err := json.Unmarshal(payload, &in); err != nil {
		return nil, fmt.Errorf("payload must be a JSON object: %w", err)
	}
	out := make(map[string]any, len(in)+len(op.Set))
	for k, v := range in {
		out[Snake(k)] = v
	}
	for k, v := range op.Set {
		out[k] = v
	}
	return out, nil
}

func keyText(v any) (string, error) {
	switch k := v.(type) {
	case string:
		return k, nil
	case float64:
		return strconv.FormatFloat(k, 'f', -1, 64), nil
	case nil:
		return "", errors.New("missing key")
	default:
		b, _ := json.Marshal(k)
		return string(b), nil
	}
}

func (c *Conn) operation(name string) (Operation, error) {
	op, ok := c.cfg.Operations[name]
	if !ok {
		return Operation{}, fmt.Errorf("postgres: operation %q is not configured on this connection", name)
	}
	return op, nil
}

// exec runs op inside tx and returns the affected row as JSON.
func (c *Conn) exec(ctx context.Context, tx pgx.Tx, op Operation, payload json.RawMessage) (json.RawMessage, error) {
	row, err := op.row(payload)
	if err != nil {
		return nil, err
	}
	key, err := keyText(row[op.Key])
	if err != nil {
		return nil, fmt.Errorf("postgres: %s: %w (column %s)", op.Table, err, op.Key)
	}
	data, err := json.Marshal(row)
	if err != nil {
		return nil, err
	}
	tbl, keyCol := ident(op.Table), pgx.Identifier{op.Key}.Sanitize()

	var result []byte
	switch op.Action {
	case "insert":
		// Only columns present in the payload are written; the rest keep
		// their database defaults, as a mapping that yields no value means.
		var present []string
		for _, c := range op.Columns {
			if _, ok := row[c]; ok {
				present = append(present, c)
			}
		}
		cols := idents(present)
		q := fmt.Sprintf(`INSERT INTO %s AS t (%s) SELECT %s FROM jsonb_populate_record(NULL::%s, $1::jsonb)
			ON CONFLICT (%s) DO NOTHING RETURNING to_jsonb(t)`, tbl, cols, cols, tbl, keyCol)
		err = tx.QueryRow(ctx, q, data).Scan(&result)
		if errors.Is(err, pgx.ErrNoRows) {
			// Already written by an earlier attempt: return that row.
			err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT to_jsonb(t) FROM %s t WHERE t.%s::text = $1`, tbl, keyCol), key).Scan(&result)
		}
	case "update":
		set := append([]string{}, op.Columns...)
		for k := range op.Set {
			set = append(set, k)
		}
		sort.Strings(set)
		cols := idents(set)
		q := fmt.Sprintf(`UPDATE %s AS t SET (%s) = (SELECT %s FROM jsonb_populate_record(NULL::%s, $1::jsonb))
			WHERE t.%s::text = $2 RETURNING to_jsonb(t)`, tbl, cols, cols, tbl, keyCol)
		err = tx.QueryRow(ctx, q, data, key).Scan(&result)
	case "delete":
		err = tx.QueryRow(ctx, fmt.Sprintf(`DELETE FROM %s AS t WHERE t.%s::text = $1 RETURNING to_jsonb(t)`, tbl, keyCol), key).Scan(&result)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("postgres: %s: no row with %s = %s", op.Table, op.Key, key)
	}
	if err != nil {
		return nil, permanent(fmt.Errorf("postgres: %s %s: %w", op.Action, op.Table, err))
	}
	return result, nil
}

// permanent marks errors that retrying cannot fix (SQLSTATE class 22, data
// exceptions, and 23, integrity violations) as invalid writes.
func permanent(err error) error {
	var pe *pgconn.PgError
	if errors.As(err, &pe) && (strings.HasPrefix(pe.Code, "22") || strings.HasPrefix(pe.Code, "23")) {
		return fmt.Errorf("%w: %v", writeguard.ErrInvalid, err)
	}
	return err
}

// Simulate runs the write in a transaction and rolls it back, returning the
// row it would produce: constraints, triggers and type conversion all run.
func (c *Conn) Simulate(ctx context.Context, name string, payload json.RawMessage) (json.RawMessage, error) {
	op, err := c.operation(name)
	if err != nil {
		return nil, err
	}
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	row, err := c.exec(ctx, tx, op, payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"mode": "rollback", "row": row})
}

// Commit performs the write.
func (c *Conn) Commit(ctx context.Context, name, _ string, payload json.RawMessage) (json.RawMessage, error) {
	op, err := c.operation(name)
	if err != nil {
		return nil, err
	}
	var result json.RawMessage
	err = pgx.BeginFunc(ctx, c.pool, func(tx pgx.Tx) error {
		result, err = c.exec(ctx, tx, op, payload)
		return err
	})
	return result, err
}

// Confirm reads the written row back (read-your-writes).
func (c *Conn) Confirm(ctx context.Context, name string, result json.RawMessage) error {
	op, err := c.operation(name)
	if err != nil || op.Action == "delete" {
		return err
	}
	var row map[string]any
	if err := json.Unmarshal(result, &row); err != nil {
		return err
	}
	key, err := keyText(row[op.Key])
	if err != nil {
		return err
	}
	var ok bool
	err = c.pool.QueryRow(ctx, fmt.Sprintf(`SELECT true FROM %s t WHERE t.%s::text = $1`, ident(op.Table), pgx.Identifier{op.Key}.Sanitize()), key).Scan(&ok)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("row %s = %s not found", op.Key, key)
	}
	return err
}

// Poll reads events from the outbox.
func (c *Conn) Poll(ctx context.Context, event string, after int64, limit int) ([]connector.Event, error) {
	if c.cfg.Outbox == nil {
		return nil, errors.New("postgres: this connection has no outbox configured")
	}
	rows, err := c.pool.Query(ctx, fmt.Sprintf(
		`SELECT id, payload FROM %s WHERE event = $1 AND id > $2 ORDER BY id LIMIT $3`, ident(c.cfg.Outbox.Table)),
		event, after, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: poll outbox: %w", err)
	}
	defer rows.Close()
	var out []connector.Event
	for rows.Next() {
		var id int64
		var payload []byte
		if err := rows.Scan(&id, &payload); err != nil {
			return nil, err
		}
		out = append(out, connector.Event{ID: strconv.FormatInt(id, 10), Position: id, Name: event, Payload: payload})
	}
	return out, rows.Err()
}
