package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/fduser123-coding/turgon/pkg/meta"
)

// Snapshot describes one stored catalog of an endpoint.
type Snapshot = meta.Snapshot

// SaveCatalog stores an endpoint's catalog. When the endpoint still holds
// what its latest snapshot holds (the same digest), that snapshot is kept,
// with this discovery's uses and time; otherwise a new one is added.
// created reports which.
func (s *Store) SaveCatalog(ctx context.Context, c meta.Catalog) (snap Snapshot, created bool, err error) {
	c.Normalize()
	raw, err := json.Marshal(c)
	if err != nil {
		return Snapshot{}, false, err
	}
	digest := c.Digest()
	at := c.DiscoveredAt
	if at.IsZero() {
		at = time.Now().UTC()
	}
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Two discoveries of one endpoint at once store one snapshot.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7073, hashtext($1))`, c.Endpoint); err != nil {
			return err
		}
		var id int64
		var last string
		err := tx.QueryRow(ctx, `SELECT id, digest FROM turgon_meta_snapshots WHERE endpoint = $1 ORDER BY id DESC LIMIT 1`, c.Endpoint).Scan(&id, &last)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err == nil && last == digest {
			return tx.QueryRow(ctx, `
				UPDATE turgon_meta_snapshots SET checked_at = $2, catalog = jsonb_set($3::jsonb, '{discoveredAt}', to_jsonb(discovered_at)), connector = $4
				WHERE id = $1
				RETURNING id, endpoint, connector, digest, discovered_at, checked_at, CASE WHEN jsonb_typeof(catalog->'objects') = 'array' THEN jsonb_array_length(catalog->'objects') ELSE 0 END`,
				id, at, raw, c.Connector).Scan(&snap.ID, &snap.Endpoint, &snap.Connector, &snap.Digest, &snap.DiscoveredAt, &snap.CheckedAt, &snap.Objects)
		}
		created = true
		return tx.QueryRow(ctx, `
			INSERT INTO turgon_meta_snapshots (endpoint, connector, digest, discovered_at, checked_at, catalog) VALUES ($1, $2, $3, $4, $4, $5)
			RETURNING id, endpoint, connector, digest, discovered_at, checked_at, CASE WHEN jsonb_typeof(catalog->'objects') = 'array' THEN jsonb_array_length(catalog->'objects') ELSE 0 END`,
			c.Endpoint, c.Connector, digest, at, raw).Scan(&snap.ID, &snap.Endpoint, &snap.Connector, &snap.Digest, &snap.DiscoveredAt, &snap.CheckedAt, &snap.Objects)
	})
	return snap, created, err
}

// LatestCatalog returns an endpoint's latest stored catalog.
func (s *Store) LatestCatalog(ctx context.Context, endpoint string) (meta.Catalog, bool, error) {
	return s.catalog(ctx, `SELECT catalog FROM turgon_meta_snapshots WHERE endpoint = $1 ORDER BY id DESC LIMIT 1`, endpoint)
}

// Catalog returns a stored catalog by its snapshot's id.
func (s *Store) Catalog(ctx context.Context, id int64) (meta.Catalog, bool, error) {
	return s.catalog(ctx, `SELECT catalog FROM turgon_meta_snapshots WHERE id = $1`, id)
}

func (s *Store) catalog(ctx context.Context, q string, arg any) (meta.Catalog, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	var raw []byte
	err := s.pool.QueryRow(ctx, q, arg).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return meta.Catalog{}, false, nil
	}
	if err != nil {
		return meta.Catalog{}, false, err
	}
	var c meta.Catalog
	if err := json.Unmarshal(raw, &c); err != nil {
		return meta.Catalog{}, false, err
	}
	return c, true, nil
}

// Snapshots lists the stored snapshots, newest first; of one endpoint, or
// of all when endpoint is empty.
func (s *Store) Snapshots(ctx context.Context, endpoint string) ([]Snapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, `
		SELECT id, endpoint, connector, digest, discovered_at, checked_at, CASE WHEN jsonb_typeof(catalog->'objects') = 'array' THEN jsonb_array_length(catalog->'objects') ELSE 0 END
		FROM turgon_meta_snapshots WHERE $1 = '' OR endpoint = $1 ORDER BY id DESC`, endpoint)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Snapshot
	for rows.Next() {
		var sn Snapshot
		if err := rows.Scan(&sn.ID, &sn.Endpoint, &sn.Connector, &sn.Digest, &sn.DiscoveredAt, &sn.CheckedAt, &sn.Objects); err != nil {
			return nil, err
		}
		out = append(out, sn)
	}
	return out, rows.Err()
}
