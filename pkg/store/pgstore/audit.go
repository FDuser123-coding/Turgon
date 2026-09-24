package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fduser123-coding/turgon/pkg/audit"
)

// auditSchema stores each entry's canonical JSON line, so hashes verify
// byte for byte after the round trip, plus columns for querying. Triggers
// make the table append-only; the hash chain still detects tampering by
// anyone able to disable them.
const auditSchema = `
CREATE TABLE IF NOT EXISTS porter_audit (
	seq    bigint PRIMARY KEY,
	time   timestamptz NOT NULL,
	actor  text NOT NULL,
	action text NOT NULL,
	line   text NOT NULL
);

CREATE OR REPLACE FUNCTION porter_audit_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE EXCEPTION 'porter_audit is append-only';
END $$;

DROP TRIGGER IF EXISTS porter_audit_no_change ON porter_audit;
CREATE TRIGGER porter_audit_no_change BEFORE UPDATE OR DELETE ON porter_audit
	FOR EACH ROW EXECUTE FUNCTION porter_audit_append_only();
DROP TRIGGER IF EXISTS porter_audit_no_truncate ON porter_audit;
CREATE TRIGGER porter_audit_no_truncate BEFORE TRUNCATE ON porter_audit
	FOR EACH STATEMENT EXECUTE FUNCTION porter_audit_append_only();
`

// AuditLog is a hash-chained audit log in Postgres, shared by every worker
// and the console. Appends are serialized with a transaction-scoped
// advisory lock so concurrent writers extend one chain.
type AuditLog struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

// NewAuditLog returns the audit log. Call Migrate first.
func NewAuditLog(pool *pgxpool.Pool) *AuditLog {
	return &AuditLog{pool: pool, now: time.Now}
}

var _ audit.Recorder = (*AuditLog)(nil)

// auditLockKey is an arbitrary constant naming the advisory lock.
const auditLockKey = 0x706f72746572 // "porter"

// Record appends an entry.
func (a *AuditLog) Record(actor, action string, data any) (audit.Entry, error) {
	raw, err := audit.Encode(data)
	if err != nil {
		return audit.Entry{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var e audit.Entry
	err = pgx.BeginFunc(ctx, a.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(auditLockKey)); err != nil {
			return err
		}
		seq, prev := uint64(0), audit.Genesis
		var line string
		err := tx.QueryRow(ctx, `SELECT line FROM porter_audit ORDER BY seq DESC LIMIT 1`).Scan(&line)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
		case err != nil:
			return err
		default:
			var last audit.Entry
			if err := json.Unmarshal([]byte(line), &last); err != nil {
				return fmt.Errorf("audit: last entry unreadable: %w", err)
			}
			seq, prev = last.Seq, last.Hash
		}
		e = audit.Next(seq, prev, a.now(), actor, action, raw)
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO porter_audit (seq, time, actor, action, line) VALUES ($1, $2, $3, $4, $5)`,
			int64(e.Seq), e.Time, e.Actor, e.Action, string(b))
		return err
	})
	if err != nil {
		return audit.Entry{}, fmt.Errorf("audit: %w", err)
	}
	return e, nil
}

// Verify checks the whole chain and returns the last entry, or nil if empty.
func (a *AuditLog) Verify(ctx context.Context) (*audit.Entry, error) {
	rows, err := a.pool.Query(ctx, `SELECT seq, line FROM porter_audit ORDER BY seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	seq, prev := uint64(0), audit.Genesis
	var last *audit.Entry
	for rows.Next() {
		var rowSeq int64
		var line string
		if err := rows.Scan(&rowSeq, &line); err != nil {
			return last, err
		}
		var e audit.Entry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			return last, fmt.Errorf("%w: row %d is not a valid entry", audit.ErrTampered, rowSeq)
		}
		if uint64(rowSeq) != e.Seq {
			return last, fmt.Errorf("%w: row %d holds entry %d", audit.ErrTampered, rowSeq, e.Seq)
		}
		if err := audit.Link(seq, prev, e); err != nil {
			return last, err
		}
		seq, prev = e.Seq, e.Hash
		last = &e
	}
	return last, rows.Err()
}

// Tail returns the newest entries, newest first.
func (a *AuditLog) Tail(ctx context.Context, limit int) ([]audit.Entry, error) {
	rows, err := a.pool.Query(ctx, `SELECT line FROM porter_audit ORDER BY seq DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []audit.Entry{}
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, err
		}
		var e audit.Entry
		if json.Unmarshal([]byte(line), &e) == nil {
			out = append(out, e)
		}
	}
	return out, rows.Err()
}
