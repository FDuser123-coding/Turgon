package pgstore

import (
	"context"
	"time"

	"github.com/fduser123-coding/turgon/pkg/verifier"
)

// Reviews stores mapping field reviews made in the console.
type Reviews struct{ s *Store }

// Reviews returns the store's mapping reviews.
func (s *Store) Reviews() Reviews { return Reviews{s} }

// Put records a review, replacing an earlier one of the same expression.
func (r Reviews) Put(ctx context.Context, rv verifier.Review) error {
	ctx, cancel := context.WithTimeout(ctx, r.s.Timeout)
	defer cancel()
	_, err := r.s.pool.Exec(ctx, `
		INSERT INTO turgon_mapping_reviews (mapping, target, expr_sha256, decision, reviewer, note, reviewed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (mapping, target, expr_sha256) DO UPDATE
		SET decision = EXCLUDED.decision, reviewer = EXCLUDED.reviewer, note = EXCLUDED.note, reviewed_at = EXCLUDED.reviewed_at`,
		rv.Mapping, rv.Target, verifier.ExpressionDigest(rv.Expression), rv.Decision, rv.Reviewer, rv.Note, time.Now().UTC())
	return err
}

// All returns every review, for the verifier.
func (r Reviews) All(ctx context.Context) (verifier.Reviews, error) {
	ctx, cancel := context.WithTimeout(ctx, r.s.Timeout)
	defer cancel()
	rows, err := r.s.pool.Query(ctx, `SELECT mapping, target, expr_sha256, decision, reviewer, note, reviewed_at FROM turgon_mapping_reviews`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := verifier.Reviews{}
	for rows.Next() {
		var rv verifier.Review
		var digest string
		if err := rows.Scan(&rv.Mapping, &rv.Target, &digest, &rv.Decision, &rv.Reviewer, &rv.Note, &rv.At); err != nil {
			return nil, err
		}
		out[verifier.ReviewKey{Mapping: rv.Mapping, Target: rv.Target, ExpressionSHA256: digest}] = rv
	}
	return out, rows.Err()
}
