// Package pgtest gives integration tests an isolated Postgres schema.
// Tests are skipped unless PORTER_TEST_DATABASE_URL is set.
package pgtest

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var counter atomic.Int64

// Pool returns a pool whose search_path is a fresh schema dropped at the
// end of the test, plus that schema's name.
func Pool(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	url := os.Getenv("PORTER_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("PORTER_TEST_DATABASE_URL not set; skipping Postgres integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("t_%d_%d", os.Getpid(), counter.Add(1))
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	return pool, schema
}

// URL returns the test database URL with search_path set to schema.
func URL(t *testing.T, schema string) string {
	t.Helper()
	url := os.Getenv("PORTER_TEST_DATABASE_URL")
	sep := "?"
	for _, c := range url {
		if c == '?' {
			sep = "&"
		}
	}
	return url + sep + "search_path=" + schema
}
