// Package pgstore keeps Turgon's operational state in Postgres (architecture
// §7.6, "Metadata Postgres"): the write guard's idempotency records, event
// source cursors and the identity cross-reference that links a master
// record to its IDs in every system.
package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/identity"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// schema is applied idempotently by Migrate into the connection's
// search_path (production sets search_path=turgon). Migrations follow
// expand-then-contract (architecture §11): only additive statements here.
const schema = `
-- Turgon was called Porter while it was prototyped: rename a state
-- database created under the old table names, keeping its contents.
DO $$
DECLARE t text;
BEGIN
	FOREACH t IN ARRAY ARRAY['writes', 'cursors', 'xref', 'audit'] LOOP
		IF to_regclass('porter_' || t) IS NOT NULL AND to_regclass('turgon_' || t) IS NULL THEN
			EXECUTE format('ALTER TABLE %I RENAME TO %I', 'porter_' || t, 'turgon_' || t);
		END IF;
	END LOOP;
END $$;
-- The old audit triggers go with their function; auditSchema adds the new ones.
DROP FUNCTION IF EXISTS porter_audit_append_only() CASCADE;

CREATE TABLE IF NOT EXISTS turgon_writes (
	key        text PRIMARY KEY,
	status     text NOT NULL CHECK (status IN ('inflight', 'done')),
	outcome    jsonb,
	claimed_at timestamptz NOT NULL DEFAULT now(),
	done_at    timestamptz
);

CREATE TABLE IF NOT EXISTS turgon_cursors (
	name       text PRIMARY KEY,
	position   bigint NOT NULL,
	updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS turgon_xref (
	entity    text NOT NULL,
	system    text NOT NULL,
	source_id text NOT NULL,
	master_id text NOT NULL,
	PRIMARY KEY (entity, system, source_id)
);
-- The source record's identifying attributes, which later records are
-- matched against (pkg/identity).
ALTER TABLE turgon_xref ADD COLUMN IF NOT EXISTS attributes jsonb NOT NULL DEFAULT '{}';
CREATE INDEX IF NOT EXISTS turgon_xref_email ON turgon_xref (entity, (attributes->>'email'));
CREATE INDEX IF NOT EXISTS turgon_xref_domain ON turgon_xref (entity, (attributes->>'domain'));
CREATE INDEX IF NOT EXISTS turgon_xref_name ON turgon_xref (entity, left(attributes->>'name', 3));

-- Events delivered by webhook, until the dispatcher starts their runs. One
-- row per source event: a provider's redelivery is dropped here.
CREATE TABLE IF NOT EXISTS turgon_inbox (
	seq         bigserial PRIMARY KEY,
	source      text NOT NULL,
	event_id    text NOT NULL,
	payload     jsonb NOT NULL,
	received_at timestamptz NOT NULL DEFAULT now(),
	UNIQUE (source, event_id)
);
CREATE INDEX IF NOT EXISTS turgon_inbox_received ON turgon_inbox (received_at);
`

// Migrate creates or upgrades Turgon's schema.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	// Serialize concurrent migrations from replicas starting together.
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7070)`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, schema); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, auditSchema)
		return err
	})
}

// Store implements writeguard.Store, cursors and the cross-reference.
type Store struct {
	pool *pgxpool.Pool
	// Lease is how long an in-flight claim blocks other callers before it is
	// considered abandoned (a worker crashed mid-write). Default 5 minutes.
	Lease time.Duration
	// Timeout bounds each store call. Default 10 seconds.
	Timeout time.Duration
}

// New returns a store over pool. Call Migrate first.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, Lease: 5 * time.Minute, Timeout: 10 * time.Second}
}

var _ writeguard.Store = (*Store)(nil)

func (s *Store) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.Timeout)
}

// Begin claims key, taking over a claim whose lease has expired.
func (s *Store) Begin(key string) (*writeguard.Outcome, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	var claimed bool
	err := s.pool.QueryRow(ctx, `
		INSERT INTO turgon_writes (key, status, claimed_at) VALUES ($1, 'inflight', now())
		ON CONFLICT (key) DO UPDATE SET claimed_at = now()
			WHERE turgon_writes.status = 'inflight'
			  AND turgon_writes.claimed_at < now() - make_interval(secs => $2)
		RETURNING true`, key, s.Lease.Seconds()).Scan(&claimed)
	if err == nil {
		return nil, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("pgstore: claim %s: %w", key, err)
	}
	out, err := s.lookup(ctx, key)
	if err != nil {
		return nil, err
	}
	if out == nil {
		return nil, writeguard.ErrInFlight
	}
	return out, nil
}

// Lookup returns the outcome of a completed write, or nil.
func (s *Store) Lookup(key string) (*writeguard.Outcome, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	return s.lookup(ctx, key)
}

func (s *Store) lookup(ctx context.Context, key string) (*writeguard.Outcome, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx, `SELECT outcome FROM turgon_writes WHERE key = $1 AND status = 'done'`, key).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("pgstore: lookup %s: %w", key, err)
	}
	var out writeguard.Outcome
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("pgstore: decode %s: %w", key, err)
	}
	return &out, nil
}

// Complete records the outcome.
func (s *Store) Complete(key string, out writeguard.Outcome) error {
	raw, err := json.Marshal(out)
	if err != nil {
		return err
	}
	ctx, cancel := s.ctx()
	defer cancel()
	_, err = s.pool.Exec(ctx, `
		INSERT INTO turgon_writes (key, status, outcome, done_at) VALUES ($1, 'done', $2, now())
		ON CONFLICT (key) DO UPDATE SET status = 'done', outcome = $2, done_at = now()`, key, raw)
	return err
}

// Abort releases an in-flight claim.
func (s *Store) Abort(key string) error {
	ctx, cancel := s.ctx()
	defer cancel()
	_, err := s.pool.Exec(ctx, `DELETE FROM turgon_writes WHERE key = $1 AND status = 'inflight'`, key)
	return err
}

// Cursor returns a named source position, or 0 if none is stored.
func (s *Store) Cursor(ctx context.Context, name string) (int64, error) {
	var pos int64
	err := s.pool.QueryRow(ctx, `SELECT position FROM turgon_cursors WHERE name = $1`, name).Scan(&pos)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return pos, err
}

// SetCursor stores a source position. Positions only move forward.
func (s *Store) SetCursor(ctx context.Context, name string, pos int64) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO turgon_cursors (name, position) VALUES ($1, $2)
		ON CONFLICT (name) DO UPDATE SET position = GREATEST(turgon_cursors.position, $2), updated_at = now()`, name, pos)
	return err
}

// Deliver stores webhook events for a source ("endpoint/event") and
// returns how many were new. Deliveries to one source are serialized, so
// sequence numbers commit in order and a reader never passes one that is
// still being written.
func (s *Store) Deliver(ctx context.Context, source string, events []connector.Event) (int, error) {
	n := 0
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7071, hashtext($1))`, source); err != nil {
			return err
		}
		for _, ev := range events {
			tag, err := tx.Exec(ctx, `
				INSERT INTO turgon_inbox (source, event_id, payload) VALUES ($1, $2, $3)
				ON CONFLICT (source, event_id) DO NOTHING`, source, ev.ID, []byte(ev.Payload))
			if err != nil {
				return err
			}
			n += int(tag.RowsAffected())
		}
		return nil
	})
	return n, err
}

// Inbox returns up to limit delivered events for a source after a
// sequence number, in order; their positions are sequence numbers.
func (s *Store) Inbox(ctx context.Context, source, name string, after int64, limit int) ([]connector.Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT seq, event_id, payload FROM turgon_inbox
		WHERE source = $1 AND seq > $2 ORDER BY seq LIMIT $3`, source, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []connector.Event
	for rows.Next() {
		ev := connector.Event{Name: name}
		var payload []byte
		if err := rows.Scan(&ev.Position, &ev.ID, &payload); err != nil {
			return nil, err
		}
		ev.Payload = payload
		out = append(out, ev)
	}
	return out, rows.Err()
}

// PruneInbox deletes deliveries received before a time. Their runs have
// long started; keeping them for a while drops late redeliveries.
func (s *Store) PruneInbox(ctx context.Context, before time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM turgon_inbox WHERE received_at < $1`, before)
	return tag.RowsAffected(), err
}

// Xref looks up the master ID for a record in a source system.
func (s *Store) Xref(ctx context.Context, entity, system, sourceID string) (string, bool, error) {
	var master string
	err := s.pool.QueryRow(ctx, `SELECT master_id FROM turgon_xref WHERE entity = $1 AND system = $2 AND source_id = $3`,
		entity, system, sourceID).Scan(&master)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	return master, err == nil, err
}

// PutXref links a source record to a master ID.
func (s *Store) PutXref(ctx context.Context, entity, system, sourceID, masterID string) error {
	return s.Link(ctx, entity, system, sourceID, masterID, nil)
}

// Link links a source record to a master ID and keeps its identifying
// attributes for matching later records. Attributes already known for the
// record are kept unless new ones are given.
func (s *Store) Link(ctx context.Context, entity, system, sourceID, masterID string, attrs identity.Attributes) error {
	if attrs == nil {
		attrs = identity.Attributes{}
	}
	b, err := json.Marshal(attrs)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO turgon_xref (entity, system, source_id, master_id, attributes) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (entity, system, source_id) DO UPDATE SET master_id = $4,
			attributes = CASE WHEN $5::jsonb = '{}'::jsonb THEN turgon_xref.attributes ELSE $5::jsonb END`,
		entity, system, sourceID, masterID, b)
	return err
}

// Candidates returns linked records of an entity that share an email,
// domain, name prefix or exact identifier with attrs (blocking, so a match
// never scans every record).
func (s *Store) Candidates(ctx context.Context, entity string, attrs identity.Attributes) ([]identity.Candidate, error) {
	var exact []string
	for k, v := range attrs {
		if strings.HasPrefix(k, "exact:") {
			exact = append(exact, k+"="+v)
		}
	}
	name := attrs["name"]
	if len([]rune(name)) > 3 {
		name = string([]rune(name)[:3])
	}
	rows, err := s.pool.Query(ctx, `
		SELECT master_id, attributes FROM turgon_xref
		WHERE entity = $1 AND (
			($2 <> '' AND attributes->>'email' = $2) OR
			($3 <> '' AND attributes->>'domain' = $3) OR
			($4 <> '' AND left(attributes->>'name', 3) = $4) OR
			EXISTS (SELECT 1 FROM jsonb_each_text(attributes) a WHERE a.key || '=' || a.value = ANY($5)))
		LIMIT 500`, entity, attrs["email"], attrs["domain"], name, exact)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []identity.Candidate
	for rows.Next() {
		var c identity.Candidate
		var raw []byte
		if err := rows.Scan(&c.Master, &raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &c.Attributes); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
