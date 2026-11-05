package pgstore

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fduser123-coding/turgon/pkg/connector"
)

const tokenSchema = `
-- Credentials systems rotate while Turgon uses them (OAuth 2.0 refresh
-- tokens), sealed with AES-256-GCM under a key Postgres never sees.
CREATE TABLE IF NOT EXISTS turgon_tokens (
	key        text PRIMARY KEY,
	key_id     text NOT NULL,
	sealed     bytea NOT NULL,
	updated_at timestamptz NOT NULL DEFAULT now()
);
`

// TokenVault is a connector.TokenStore in Turgon's database. Values are
// sealed with AES-256-GCM, bound to their key; the first of the vault's
// keys seals, every key opens, so keys can be rotated: put the new one
// first and drop the old once every token was rotated since.
type TokenVault struct {
	pool    *pgxpool.Pool
	current string
	keys    map[string]cipher.AEAD
}

var _ connector.TokenStore = (*TokenVault)(nil)

var tokenKeyIDRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// NewTokenVault parses keys as "id:base64key,id:base64key" (32-byte keys,
// e.g. openssl rand -base64 32), as TURGON_TOKEN_KEYS holds them.
func NewTokenVault(pool *pgxpool.Pool, keys string) (*TokenVault, error) {
	if strings.TrimSpace(keys) == "" {
		return nil, errors.New("token keys: none set; TURGON_TOKEN_KEYS=id:base64key seals rotated tokens (openssl rand -base64 32)")
	}
	v := &TokenVault{pool: pool, keys: map[string]cipher.AEAD{}}
	for i, part := range strings.Split(keys, ",") {
		id, b64, ok := strings.Cut(strings.TrimSpace(part), ":")
		if !ok || !tokenKeyIDRE.MatchString(id) {
			// Name the entry by its place: without a colon it may be the key.
			return nil, fmt.Errorf("token keys: entry %d is not id:base64key (the id is 1-64 letters, digits, '.', '_' or '-')", i+1)
		}
		key, err := base64.StdEncoding.DecodeString(b64)
		if err != nil || len(key) != 32 {
			return nil, fmt.Errorf("token key %s: must be 32 bytes, base64-encoded (openssl rand -base64 32)", id)
		}
		if _, dup := v.keys[id]; dup {
			return nil, fmt.Errorf("token key %s is listed twice", id)
		}
		block, _ := aes.NewCipher(key)
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, err
		}
		v.keys[id] = aead
		if v.current == "" {
			v.current = id
		}
	}
	return v, nil
}

// UpdateToken implements connector.TokenStore: the row is locked for the
// transaction, so workers spend a token one at a time.
func (v *TokenVault) UpdateToken(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	return pgx.BeginFunc(ctx, v.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7072, hashtext($1))`, key); err != nil {
			return err
		}
		var keyID string
		var sealed []byte
		err := tx.QueryRow(ctx, `SELECT key_id, sealed FROM turgon_tokens WHERE key = $1`, key).Scan(&keyID, &sealed)
		var current []byte
		switch {
		case errors.Is(err, pgx.ErrNoRows):
		case err != nil:
			return err
		default:
			aead, ok := v.keys[keyID]
			if !ok {
				return fmt.Errorf("token %s is sealed with key %s, which TURGON_TOKEN_KEYS no longer holds", key, keyID)
			}
			n := aead.NonceSize()
			if len(sealed) < n {
				return fmt.Errorf("token %s: the sealed value is damaged", key)
			}
			if current, err = aead.Open(nil, sealed[:n], sealed[n:], []byte(key)); err != nil {
				return fmt.Errorf("token %s cannot be opened with key %s: %w", key, keyID, err)
			}
		}
		next, err := fn(current)
		if err != nil || next == nil {
			return err
		}
		aead := v.keys[v.current]
		nonce := make([]byte, aead.NonceSize())
		if _, err := rand.Read(nonce); err != nil {
			return err
		}
		out := aead.Seal(nonce, nonce, next, []byte(key))
		_, err = tx.Exec(ctx, `
			INSERT INTO turgon_tokens (key, key_id, sealed, updated_at) VALUES ($1, $2, $3, now())
			ON CONFLICT (key) DO UPDATE SET key_id = EXCLUDED.key_id, sealed = EXCLUDED.sealed, updated_at = now()`, key, v.current, out)
		return err
	})
}
