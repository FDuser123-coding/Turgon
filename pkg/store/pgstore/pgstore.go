// Package pgstore keeps Porter's operational state in Postgres (architecture
// §7.6, "Metadata Postgres"): the write guard's idempotency records, event
// source cursors and the identity cross-reference that links a master
// record to its IDs in every system.
package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// schema is applied idempotently by Migrate into the connection's
// search_path (production sets search_path=porter). Migrations follow
// expand-then-contract (architecture §11): only additive statements here.
const schema = `

CREATE TABLE IF NOT EXISTS porter_writes (
	key        text PRIMARY KEY,
	status     text NOT NULL CHECK (status IN ('inflight', 'done')),
	outcome    jsonb,
	claimed_at timestamptz NOT NULL DEFAULT now(),
	done_at    timestamptz
);

CREATE TABLE IF NOT EXISTS porter_cursors (
	name       text PRIMARY KEY,
	position   bigint NOT NULL,
	updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS porter_xref (
	entity    text NOT NULL,
	system    text NOT NULL,
	source_id text NOT NULL,
	master_id text NOT NULL,
	PRIMARY KEY (entity, system, source_id)
);
`

// Migrate creates or upgrades Porter's schema.
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
		INSERT INTO porter_writes (key, status, claimed_at) VALUES ($1, 'inflight', now())
		ON CONFLICT (key) DO UPDATE SET claimed_at = now()
			WHERE porter_writes.status = 'inflight'
			  AND porter_writes.claimed_at < now() - make_interval(secs => $2)
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
	err := s.pool.QueryRow(ctx, `SELECT outcome FROM porter_writes WHERE key = $1 AND status = 'done'`, key).Scan(&raw)
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
		INSERT INTO porter_writes (key, status, outcome, done_at) VALUES ($1, 'done', $2, now())
		ON CONFLICT (key) DO UPDATE SET status = 'done', outcome = $2, done_at = now()`, key, raw)
	return err
}

// Abort releases an in-flight claim.
func (s *Store) Abort(key string) error {
	ctx, cancel := s.ctx()
	defer cancel()
	_, err := s.pool.Exec(ctx, `DELETE FROM porter_writes WHERE key = $1 AND status = 'inflight'`, key)
	return err
}

// Cursor returns a named source position, or 0 if none is stored.
func (s *Store) Cursor(ctx context.Context, name string) (int64, error) {
	var pos int64
	err := s.pool.QueryRow(ctx, `SELECT position FROM porter_cursors WHERE name = $1`, name).Scan(&pos)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return pos, err
}

// SetCursor stores a source position. Positions only move forward.
func (s *Store) SetCursor(ctx context.Context, name string, pos int64) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO porter_cursors (name, position) VALUES ($1, $2)
		ON CONFLICT (name) DO UPDATE SET position = GREATEST(porter_cursors.position, $2), updated_at = now()`, name, pos)
	return err
}

// Xref looks up the master ID for a record in a source system.
func (s *Store) Xref(ctx context.Context, entity, system, sourceID string) (string, bool, error) {
	var master string
	err := s.pool.QueryRow(ctx, `SELECT master_id FROM porter_xref WHERE entity = $1 AND system = $2 AND source_id = $3`,
		entity, system, sourceID).Scan(&master)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	return master, err == nil, err
}

// PutXref links a source record to a master ID.
func (s *Store) PutXref(ctx context.Context, entity, system, sourceID, masterID string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO porter_xref (entity, system, source_id, master_id) VALUES ($1, $2, $3, $4)
		ON CONFLICT (entity, system, source_id) DO UPDATE SET master_id = $4`, entity, system, sourceID, masterID)
	return err
}
