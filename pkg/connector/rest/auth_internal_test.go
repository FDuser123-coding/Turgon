package rest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
)

// A 401 for a request that carried an older token (another request
// refreshed it meanwhile) leaves the new one alone.
func TestInvalidateDropsOnlyTheTokenTheRequestCarried(t *testing.T) {
	a := &authenticator{cfg: Auth{Type: "oauth2"}, token: "new", expires: time.Now().Add(time.Hour)}
	old, _ := http.NewRequest(http.MethodGet, "https://api.example.com/", nil)
	old.Header.Set("Authorization", "Bearer old")
	a.invalidate(old)
	if a.token != "new" {
		t.Fatal("a stale 401 dropped the fresh token")
	}
	cur, _ := http.NewRequest(http.MethodGet, "https://api.example.com/", nil)
	cur.Header.Set("Authorization", "Bearer new")
	a.invalidate(cur)
	if a.token != "" || a.rejected != "new" {
		t.Fatalf("token %q, rejected %q", a.token, a.rejected)
	}
	s := &authenticator{cfg: Auth{Type: "session"}, token: "B1SESSION=2", expires: time.Now().Add(time.Hour)}
	stale, _ := http.NewRequest(http.MethodGet, "https://b1.example.com/", nil)
	stale.Header.Set("Cookie", "B1SESSION=1")
	s.invalidate(stale)
	if s.token == "" {
		t.Fatal("a stale 401 ended the new session")
	}
}

// A token stored under the key of earlier releases (without the seed) is
// adopted when it descends from the secret's seed.
func TestStoredTokenUnderTheOldKeyIsAdopted(t *testing.T) {
	a := &authenticator{cfg: Auth{Type: "oauth2-refresh", TokenURL: "https://idp.example.com/token"}, clientID: "app", refreshToken: "seed"}
	store := &connector.MemoryTokens{}
	a.useTokens(store, "rest/qbo")
	sum := sha256.Sum256([]byte("seed"))
	old, _ := json.Marshal(tokenState{Seed: hex.EncodeToString(sum[:8]), RefreshToken: "rotated", AccessToken: "live", Expires: time.Now().Add(time.Hour)})
	_ = store.UpdateToken(context.Background(), "rest/qbo@"+seedless(a), func([]byte) ([]byte, error) { return old, nil })
	tok, err := a.refreshed(context.Background()) // no token server: it must not be needed
	if err != nil || tok != "live" {
		t.Fatalf("%q, %v", tok, err)
	}
	var st tokenState
	_ = store.UpdateToken(context.Background(), a.tokenKey, func(cur []byte) ([]byte, error) { return nil, json.Unmarshal(cur, &st) })
	if st.RefreshToken != "rotated" || a.legacyKeys != nil {
		t.Fatalf("stored %+v, legacy keys %q", st, a.legacyKeys)
	}
}

// seedless is the key suffix of releases before the seed was in the key.
func seedless(a *authenticator) string {
	sum := sha256.Sum256([]byte(a.cfg.TokenURL + "\x00" + a.clientID))
	return hex.EncodeToString(sum[:8])
}

// A token kept by the endpoint (a stack's slot) is adopted by the
// connection's key, which is then the one written; a token the connection
// already has wins over it.
func TestStoredTokenUnderTheEndpointIsAdopted(t *testing.T) {
	sum := sha256.Sum256([]byte("seed"))
	state := func(refresh, access string) []byte {
		b, _ := json.Marshal(tokenState{Seed: hex.EncodeToString(sum[:8]), RefreshToken: refresh, AccessToken: access, Expires: time.Now().Add(time.Hour)})
		return b
	}
	put := func(store *connector.MemoryTokens, key string, b []byte) {
		_ = store.UpdateToken(context.Background(), key, func([]byte) ([]byte, error) { return b, nil })
	}
	newAuth := func(store *connector.MemoryTokens) *authenticator {
		a := &authenticator{cfg: Auth{Type: "oauth2-refresh", TokenURL: "https://idp.example.com/token"}, clientID: "app", refreshToken: "seed"}
		a.useTokens(store, "rest/quickbooks", "rest/erp")
		return a
	}

	store := &connector.MemoryTokens{}
	a := newAuth(store)
	put(store, strings.Replace(a.tokenKey, "rest/quickbooks@", "rest/erp@", 1), state("rotated-by-erp", "erp-live"))
	if tok, err := a.refreshed(context.Background()); err != nil || tok != "erp-live" {
		t.Fatalf("%q, %v", tok, err)
	}
	var st tokenState
	_ = store.UpdateToken(context.Background(), a.tokenKey, func(cur []byte) ([]byte, error) { return nil, json.Unmarshal(cur, &st) })
	if st.RefreshToken != "rotated-by-erp" {
		t.Fatalf("connection's token = %+v", st)
	}

	store = &connector.MemoryTokens{}
	a = newAuth(store)
	put(store, a.tokenKey, state("rotated-by-connection", "conn-live"))
	put(store, strings.Replace(a.tokenKey, "rest/quickbooks@", "rest/erp@", 1), state("rotated-by-erp", "erp-live"))
	if tok, err := a.refreshed(context.Background()); err != nil || tok != "conn-live" {
		t.Fatalf("%q, %v", tok, err)
	}
}

func TestTokenLifetimeAsNumberOrString(t *testing.T) {
	for raw, want := range map[string]seconds{`600`: 600, `"600"`: 600} {
		var tok struct {
			ExpiresIn seconds `json:"expires_in"`
		}
		if err := json.Unmarshal([]byte(`{"expires_in":`+raw+`}`), &tok); err != nil || tok.ExpiresIn != want {
			t.Errorf("%s: %d, %v", raw, tok.ExpiresIn, err)
		}
	}
	var tok struct {
		ExpiresIn seconds `json:"expires_in"`
	}
	if err := json.Unmarshal([]byte(`{"expires_in":"soon"}`), &tok); err == nil {
		t.Error("a lifetime that is not a number was accepted")
	}
}
