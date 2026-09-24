package postgres

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/fduser123-coding/turgon/pkg/connector"
)

var _ connector.Checker = (*Conn)(nil)

// Check verifies reachability, the outbox table and every configured
// operation's table, columns, key and privileges.
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
