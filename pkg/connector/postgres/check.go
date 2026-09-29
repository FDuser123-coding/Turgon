package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/fduser123-coding/turgon/pkg/connector"
)

var _ connector.Checker = (*Conn)(nil)

// Check verifies reachability, the outbox table, change capture, and every
// configured operation's table, columns, key and privileges.
func (c *Conn) Check(ctx context.Context) []connector.CheckResult {
	var out []connector.CheckResult
	cfg := c.pool.Config().ConnConfig
	target := fmt.Sprintf("%s:%d/%s", cfg.Host, cfg.Port, cfg.Database)
	var version string
	if err := c.pool.QueryRow(ctx, `SELECT current_setting('server_version')`).Scan(&version); err != nil {
		fix := connector.NetworkFix(err, target)
		if fix == "" {
			fix = "Check the DSN in the connection's secret."
		}
		return append(out, connector.Fail("connect", err.Error(), fix))
	}
	out = append(out, connector.Pass("connect", fmt.Sprintf("PostgreSQL %s at %s", version, target)))

	if o := c.cfg.Outbox; o != nil {
		out = append(out, c.checkTable(ctx, "outbox "+o.Table, o.Table, []string{"id", "event", "payload"}, "", []string{"SELECT"},
			fmt.Sprintf("Create it: CREATE TABLE %s (id bigserial PRIMARY KEY, event text NOT NULL, payload jsonb NOT NULL); "+
				"and write to it in the same transaction as each business change.", o.Table))...)
	}
	out = append(out, c.checkChanges(ctx)...)
	names := make([]string, 0, len(c.cfg.Operations))
	for name := range c.cfg.Operations {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		op := c.cfg.Operations[name]
		privs := map[string][]string{"insert": {"INSERT", "SELECT"}, "update": {"UPDATE", "SELECT"}, "delete": {"DELETE", "SELECT"}}[op.Action]
		cols := append([]string{op.Key}, op.Columns...)
		for k := range op.Set {
			cols = append(cols, k)
		}
		unique := ""
		if op.Action == "insert" {
			unique = op.Key
		}
		out = append(out, c.checkTable(ctx, "operation "+name, op.Table, cols, unique, privs, "")...)
	}
	return out
}

func (c *Conn) checkTable(ctx context.Context, label, table string, cols []string, unique string, privs []string, createFix string) []connector.CheckResult {
	var exists bool
	if err := c.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists); err != nil {
		return []connector.CheckResult{connector.Fail(label, err.Error(), "")}
	}
	if !exists {
		fix := createFix
		if fix == "" {
			fix = fmt.Sprintf("Create %s, or correct the table name in the connection's config.", table)
		}
		return []connector.CheckResult{connector.Fail(label, "table "+table+" does not exist", fix)}
	}
	var missing []string
	for _, col := range cols {
		var found bool
		err := c.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_attribute
			WHERE attrelid = to_regclass($1) AND attname = $2 AND attnum > 0 AND NOT attisdropped)`, table, col).Scan(&found)
		if err != nil || !found {
			missing = append(missing, col)
		}
	}
	if len(missing) > 0 {
		return []connector.CheckResult{connector.Fail(label, fmt.Sprintf("%s has no column %s", table, strings.Join(missing, ", ")),
			"Add the columns, or correct the column names in the connection's config.")}
	}
	var denied []string
	for _, p := range privs {
		var ok bool
		if err := c.pool.QueryRow(ctx, `SELECT has_table_privilege(to_regclass($1), $2)`, table, p).Scan(&ok); err != nil || !ok {
			denied = append(denied, p)
		}
	}
	if len(denied) > 0 {
		return []connector.CheckResult{connector.Fail(label, fmt.Sprintf("the connection's user lacks %s on %s", strings.Join(denied, ", "), table),
			fmt.Sprintf("GRANT %s ON %s TO <the connection's user>;", strings.Join(denied, ", "), table))}
	}
	if unique != "" {
		var ok bool
		err := c.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_index i
			JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
			WHERE i.indrelid = to_regclass($1) AND i.indisunique AND i.indnkeyatts = 1 AND a.attname = $2)`, table, unique).Scan(&ok)
		if err != nil || !ok {
			return []connector.CheckResult{connector.Fail(label, fmt.Sprintf("%s.%s has no unique constraint", table, unique),
				fmt.Sprintf("Retried writes rely on it to never duplicate rows: CREATE UNIQUE INDEX ON %s (%s);", table, unique))}
		}
	}
	return []connector.CheckResult{connector.Pass(label, table)}
}

// checkChanges verifies what change capture needs: logical WAL, a user
// allowed to create replication slots, readable tables whose updates and
// deletes can be published, and slots that are not holding back the log.
func (c *Conn) checkChanges(ctx context.Context) []connector.CheckResult {
	events := c.changeEvents()
	if len(events) == 0 {
		return nil
	}
	var out []connector.CheckResult
	var level string
	var replication bool
	err := c.pool.QueryRow(ctx, `SELECT current_setting('wal_level'),
		(SELECT rolreplication OR rolsuper FROM pg_roles WHERE rolname = current_user)`).Scan(&level, &replication)
	switch {
	case err != nil:
		return append(out, connector.Fail("change capture", err.Error(), ""))
	case level != "logical":
		out = append(out, connector.Fail("change capture", "wal_level is "+level,
			"Set wal_level = logical (ALTER SYSTEM SET wal_level = logical, then restart Postgres; on RDS and Cloud SQL, "+
				"the logical replication flag). Also set max_slot_wal_keep_size, so a stopped worker cannot fill the disk."))
	case !replication:
		out = append(out, connector.Fail("change capture", "the connection's user cannot create replication slots",
			"ALTER ROLE <the connection's user> REPLICATION; (on RDS: GRANT rds_replication)."))
	default:
		out = append(out, connector.Pass("change capture", "wal_level = logical, replication allowed"))
	}
	for _, event := range events {
		ch := c.cfg.Changes[event]
		label := "changes " + event
		res := c.checkTable(ctx, label, ch.Table, nil, "", []string{"SELECT"}, "")
		if !res[0].OK {
			out = append(out, res...)
			continue
		}
		var reloid uint32
		if err := c.pool.QueryRow(ctx, `SELECT to_regclass($1)::oid`, ch.Table).Scan(&reloid); err != nil {
			out = append(out, connector.Fail(label, err.Error(), ""))
			continue
		}
		if err := c.replicaIdentity(ctx, reloid, ch); err != nil {
			out = append(out, connector.Fail(label, err.Error(), "Add a primary key to "+ch.Table+", or set REPLICA IDENTITY FULL."))
			continue
		}
		var retained *int64
		err := c.pool.QueryRow(ctx, `SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint
			FROM pg_replication_slots WHERE slot_name = $1`, ch.Slot).Scan(&retained)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			out = append(out, connector.Pass(label, fmt.Sprintf("%s; publication %s and slot %s are created on the first poll", ch.Table, ch.Publication, ch.Slot)))
		case err != nil:
			out = append(out, connector.Fail(label, err.Error(), ""))
		case retained != nil && *retained > retainedWarn:
			out = append(out, connector.Fail(label, fmt.Sprintf("slot %s holds %d MiB of write-ahead log", ch.Slot, *retained>>20),
				"Start the worker that reads it, or drop the slot if the event is no longer used: SELECT pg_drop_replication_slot('"+ch.Slot+"');"))
		default:
			out = append(out, connector.Pass(label, fmt.Sprintf("%s via slot %s", ch.Table, ch.Slot)))
		}
	}
	return out
}

// retainedWarn is how much log a slot may hold before check flags it.
const retainedWarn = 1 << 30
