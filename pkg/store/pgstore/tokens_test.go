package pgstore

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	keyA = "a:MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="
	keyB = "b:ZmVkY2JhOTg3NjU0MzIxMGZlZGNiYTk4NzY1NDMyMTA="
)

func TestTokenVaultSealsLocksAndRotatesKeys(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	v, err := NewTokenVault(s.pool, keyA)
	if err != nil {
		t.Fatal(err)
	}
	put := func(v *TokenVault, key string, val []byte) error {
		return v.UpdateToken(ctx, key, func([]byte) ([]byte, error) { return val, nil })
	}
	get := func(v *TokenVault, key string) ([]byte, error) {
		var got []byte
		err := v.UpdateToken(ctx, key, func(cur []byte) ([]byte, error) { got = cur; return nil, nil })
		return got, err
	}
	if got, err := get(v, "rest/quickbooks@x"); err != nil || got != nil {
		t.Fatalf("empty vault: %q %v", got, err)
	}
	if err := put(v, "rest/quickbooks@x", []byte(`{"refreshToken":"AB1-secret"}`)); err != nil {
		t.Fatal(err)
	}
	// Postgres holds only the sealed value.
	var sealed []byte
	if err := s.pool.QueryRow(ctx, `SELECT sealed FROM turgon_tokens WHERE key = $1`, "rest/quickbooks@x").Scan(&sealed); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed, []byte("AB1-secret")) {
		t.Fatal("the token is stored in the clear")
	}
	// A sealed value is bound to its key: moved to another key, it does not open.
	if _, err := s.pool.Exec(ctx, `INSERT INTO turgon_tokens (key, key_id, sealed) SELECT 'rest/other@y', key_id, sealed FROM turgon_tokens WHERE key = 'rest/quickbooks@x'`); err != nil {
		t.Fatal(err)
	}
	if _, err := get(v, "rest/other@y"); err == nil {
		t.Fatal("a value moved to another key opened")
	}
	// Key rotation: the new key seals, the old one still opens.
	rotated, err := NewTokenVault(s.pool, keyB+","+keyA)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := get(rotated, "rest/quickbooks@x"); err != nil || string(got) != `{"refreshToken":"AB1-secret"}` {
		t.Fatalf("old key, read with the rotated vault: %q %v", got, err)
	}
	if err := put(rotated, "rest/quickbooks@x", []byte(`{"refreshToken":"AB1-next"}`)); err != nil {
		t.Fatal(err)
	}
	onlyB, _ := NewTokenVault(s.pool, keyB)
	if got, err := get(onlyB, "rest/quickbooks@x"); err != nil || !strings.Contains(string(got), "AB1-next") {
		t.Fatalf("after rotation, the new key alone: %q %v", got, err)
	}
	if _, err := get(v, "rest/quickbooks@x"); err == nil || !strings.Contains(err.Error(), "no longer holds") {
		t.Fatalf("a vault without the sealing key: %v", err)
	}
	// An error from fn stores nothing.
	if err := rotated.UpdateToken(ctx, "rest/quickbooks@x", func([]byte) ([]byte, error) { return []byte("x"), errors.New("refresh failed") }); err == nil {
		t.Fatal("the error was lost")
	}
	if got, _ := get(onlyB, "rest/quickbooks@x"); !strings.Contains(string(got), "AB1-next") {
		t.Fatalf("a failed update stored: %q", got)
	}
	for _, bad := range []string{"", "a", "a:short", "a:" + strings.Repeat("A", 44), keyA + "," + keyA} {
		if _, err := NewTokenVault(s.pool, bad); err == nil {
			t.Errorf("keys %q accepted", bad)
		}
	}
}

// Workers spend a token one at a time: each sees what the previous stored.
func TestTokenVaultSerializesWorkers(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	v, _ := NewTokenVault(s.pool, keyA)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := v.UpdateToken(ctx, "counter", func(cur []byte) ([]byte, error) {
				time.Sleep(5 * time.Millisecond) // a refresh in flight
				return append(cur, 'x'), nil
			})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	var got []byte
	_ = v.UpdateToken(ctx, "counter", func(cur []byte) ([]byte, error) { got = cur; return nil, nil })
	if string(got) != "xxxxxxxx" {
		t.Fatalf("concurrent updates = %q, want 8 in turn", got)
	}
}

// A key pasted without its id is not echoed back in the error.
func TestTokenKeyErrorsHideTheKey(t *testing.T) {
	raw := "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="
	for _, keys := range []string{raw, keyA + "," + raw, "bad id!:" + raw} {
		_, err := NewTokenVault(nil, keys)
		if err == nil || strings.Contains(err.Error(), raw) || strings.Contains(err.Error(), "MDEy") {
			t.Errorf("keys %q: %v", keys, err)
		}
	}
}
