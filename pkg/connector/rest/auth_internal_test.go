package rest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
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
	_ = store.UpdateToken(context.Background(), a.legacyKey, func([]byte) ([]byte, error) { return old, nil })
	tok, err := a.refreshed(context.Background()) // no token server: it must not be needed
	if err != nil || tok != "live" {
		t.Fatalf("%q, %v", tok, err)
	}
	var st tokenState
	_ = store.UpdateToken(context.Background(), a.tokenKey, func(cur []byte) ([]byte, error) { return nil, json.Unmarshal(cur, &st) })
	if st.RefreshToken != "rotated" || a.legacyKey != "" {
		t.Fatalf("stored %+v, legacy key %q", st, a.legacyKey)
	}
}
