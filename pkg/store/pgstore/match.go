package pgstore

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/fduser123-coding/turgon/pkg/identity"
)

// LinkedRecords returns an entity's linked records that kept identifying
// attributes: the training data for its matching model.
func (s *Store) LinkedRecords(ctx context.Context, entity string) ([]identity.Record, error) {
	rows, err := s.pool.Query(ctx, `SELECT master_id, attributes FROM turgon_xref WHERE entity = $1 AND attributes <> '{}'::jsonb`, entity)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []identity.Record
	for rows.Next() {
		var r identity.Record
		var raw []byte
		if err := rows.Scan(&r.Master, &raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &r.Attributes); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// PutMatchModel stores an entity's trained model.
func (s *Store) PutMatchModel(ctx context.Context, entity string, m identity.Model, rep identity.TrainReport, by string) error {
	mj, err := json.Marshal(m)
	if err != nil {
		return err
	}
	rj, err := json.Marshal(rep)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	_, err = s.pool.Exec(ctx, `
		INSERT INTO turgon_match_models (entity, model, report, trained_by, trained_at) VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (entity) DO UPDATE SET model = EXCLUDED.model, report = EXCLUDED.report, trained_by = EXCLUDED.trained_by, trained_at = now()`,
		entity, mj, rj, by)
	return err
}

// MatchModel returns an entity's trained model, if one was stored.
func (s *Store) MatchModel(ctx context.Context, entity string) (identity.Model, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	var raw []byte
	err := s.pool.QueryRow(ctx, `SELECT model FROM turgon_match_models WHERE entity = $1`, entity).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return identity.Model{}, false, nil
	}
	if err != nil {
		return identity.Model{}, false, err
	}
	var m identity.Model
	if err := json.Unmarshal(raw, &m); err != nil {
		return identity.Model{}, false, err
	}
	return m, true, nil
}
