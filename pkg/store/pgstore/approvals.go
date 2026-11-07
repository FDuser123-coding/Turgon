package pgstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

const approvalSchema = `
-- The open request for approval of each write, and the runs (for later
-- changes of the same record) waiting on its decision.
CREATE TABLE IF NOT EXISTS turgon_approvals (
	key     text PRIMARY KEY,
	holder  text NOT NULL,
	until   timestamptz NOT NULL,
	waiters text[] NOT NULL DEFAULT '{}'
);
`

var _ writeguard.ApprovalLedger = (*Store)(nil)

// OpenApproval implements writeguard.ApprovalLedger. A request past its
// time (its run ended without closing it) is taken over.
func (s *Store) OpenApproval(key, run string, until time.Time) (string, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	// A request closed between the insert and the read is gone: ask again.
	for attempt := 0; attempt < 3; attempt++ {
		holder, err := s.openApproval(ctx, key, run, until)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("pgstore: open approval %s: %w", key, err)
		}
		return holder, nil
	}
	return "", fmt.Errorf("pgstore: open approval %s: the request kept closing", key)
}

func (s *Store) openApproval(ctx context.Context, key, run string, until time.Time) (string, error) {
	var holder string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// The first run to ask inserts the row; every other one waits for
		// its lock and reads who holds it.
		var inserted bool
		err := tx.QueryRow(ctx, `INSERT INTO turgon_approvals (key, holder, until) VALUES ($1, $2, $3)
			ON CONFLICT (key) DO NOTHING RETURNING true`, key, run, until).Scan(&inserted)
		if err == nil {
			holder = ""
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		var expired bool
		if err := tx.QueryRow(ctx, `SELECT holder, until <= now() FROM turgon_approvals WHERE key = $1 FOR UPDATE`, key).Scan(&holder, &expired); err != nil {
			return err
		}
		if holder == run || expired {
			holder = ""
			_, err = tx.Exec(ctx, `UPDATE turgon_approvals SET holder = $2, until = $3, waiters = array_remove(waiters, $2) WHERE key = $1`, key, run, until)
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE turgon_approvals SET waiters = array_append(array_remove(waiters, $2), $2) WHERE key = $1`, key, run)
		return err
	})
	return holder, err
}

// CloseApproval implements writeguard.ApprovalLedger.
func (s *Store) CloseApproval(key, run string) ([]string, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	var waiters []string
	err := s.pool.QueryRow(ctx, `DELETE FROM turgon_approvals WHERE key = $1 AND holder = $2 RETURNING waiters`, key, run).Scan(&waiters)
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = s.pool.Exec(ctx, `UPDATE turgon_approvals SET waiters = array_remove(waiters, $2) WHERE key = $1`, key, run)
		if err != nil {
			return nil, fmt.Errorf("pgstore: close approval %s: %w", key, err)
		}
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("pgstore: close approval %s: %w", key, err)
	}
	return waiters, nil
}
