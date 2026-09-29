package pgstore

import (
	"context"
	"encoding/json"
	"maps"

	"github.com/jackc/pgx/v5"

	"github.com/fduser123-coding/turgon/pkg/identity"
)

// XrefLink links one source record to a master record.
type XrefLink struct {
	Source, Master string
	Attributes     identity.Attributes
}

// XrefConflict is a source record already linked to another master record.
type XrefConflict struct {
	Source, Linked, Proposed string
}

// XrefCounts is what a batch of links did.
type XrefCounts struct {
	// Added links are new; Replaced ones pointed elsewhere and were moved
	// (with replace); Unchanged ones already pointed there (their
	// attributes are refreshed).
	Added, Replaced, Unchanged int
	// Conflicts point elsewhere and were left alone (without replace).
	Conflicts []XrefConflict
}

// LinkMany links a batch of one system's records in one transaction. A
// record already linked to another master keeps its link unless replace is
// set: a steward's decision is not overwritten by a load. With dryRun
// nothing is written; the counts say what would be.
func (s *Store) LinkMany(ctx context.Context, entity, system string, links []XrefLink, replace, dryRun bool) (XrefCounts, error) {
	var out XrefCounts
	// One link per record: the last one given wins.
	last := make(map[string]int, len(links))
	for i, l := range links {
		last[l.Source] = i
	}
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		sources := make([]string, 0, len(last))
		for src := range last {
			sources = append(sources, src)
		}
		rows, err := tx.Query(ctx, `SELECT source_id, master_id, attributes FROM turgon_xref WHERE entity = $1 AND system = $2 AND source_id = ANY($3)`,
			entity, system, sources)
		if err != nil {
			return err
		}
		type current struct {
			master string
			attrs  identity.Attributes
		}
		linked := map[string]current{}
		for rows.Next() {
			var src string
			var cur current
			var raw []byte
			if err := rows.Scan(&src, &cur.master, &raw); err != nil {
				return err
			}
			if err := json.Unmarshal(raw, &cur.attrs); err != nil {
				return err
			}
			linked[src] = cur
		}
		if err := rows.Err(); err != nil {
			return err
		}
		var write [][]any
		for i, l := range links {
			if last[l.Source] != i {
				continue
			}
			cur, ok := linked[l.Source]
			attrs := l.Attributes
			if attrs == nil {
				attrs = identity.Attributes{}
			}
			switch {
			case !ok:
				out.Added++
			case cur.master == l.Master:
				out.Unchanged++
				if len(attrs) == 0 || maps.Equal(cur.attrs, attrs) {
					continue // nothing to write
				}
			case replace:
				out.Replaced++
			default:
				out.Conflicts = append(out.Conflicts, XrefConflict{Source: l.Source, Linked: cur.master, Proposed: l.Master})
				continue
			}
			b, err := json.Marshal(attrs)
			if err != nil {
				return err
			}
			write = append(write, []any{l.Source, l.Master, string(b)})
		}
		if dryRun || len(write) == 0 {
			return nil
		}
		// COPY into a scratch table, then one upsert: a load of 100,000
		// links is a few statements, not 100,000.
		if _, err := tx.Exec(ctx, `CREATE TEMP TABLE turgon_xref_load (source_id text, master_id text, attributes jsonb) ON COMMIT DROP`); err != nil {
			return err
		}
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"turgon_xref_load"}, []string{"source_id", "master_id", "attributes"}, pgx.CopyFromRows(write)); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO turgon_xref (entity, system, source_id, master_id, attributes)
			SELECT $1, $2, source_id, master_id, attributes FROM turgon_xref_load
			ON CONFLICT (entity, system, source_id) DO UPDATE SET master_id = excluded.master_id,
				attributes = CASE WHEN excluded.attributes = '{}'::jsonb THEN turgon_xref.attributes ELSE excluded.attributes END`,
			entity, system)
		return err
	})
	return out, err
}
