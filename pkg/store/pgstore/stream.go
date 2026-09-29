package pgstore

import (
	"context"
	"errors"
	"hash/fnv"

	"github.com/jackc/pgx/v5"

	"github.com/fduser123-coding/turgon/pkg/connector"
)

// DeliverStream stores events that arrived over a subscription and the
// position to resume after them, in one transaction: a crash either keeps
// both or neither, so the subscription reopens exactly after what the
// inbox holds.
func (s *Store) DeliverStream(ctx context.Context, source string, events []connector.Event, resume []byte) (int, error) {
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
		if len(resume) == 0 {
			return nil
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO turgon_stream_positions (source, resume) VALUES ($1, $2)
			ON CONFLICT (source) DO UPDATE SET resume = excluded.resume, updated_at = now()`, source, resume)
		return err
	})
	return n, err
}

// StreamResume returns where a subscription resumes; nil if it never
// stored a position.
func (s *Store) StreamResume(ctx context.Context, source string) ([]byte, error) {
	var resume []byte
	err := s.pool.QueryRow(ctx, `SELECT resume FROM turgon_stream_positions WHERE source = $1`, source).Scan(&resume)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return resume, err
}

// TryLock takes a session advisory lock on a connection of its own, held
// until release or until that connection is lost.
func (s *Store) TryLock(ctx context.Context, name string) (func(), bool, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	key := int64(h.Sum64())
	var ok bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(7072, $1::int)`, int32(key)).Scan(&ok); err != nil {
		conn.Release()
		return nil, false, err
	}
	if !ok {
		conn.Release()
		return nil, false, nil
	}
	return func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock(7072, $1::int)`, int32(key))
		conn.Release()
	}, true, nil
}
