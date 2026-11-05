package rest_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/connector/rest"
	"github.com/fduser123-coding/turgon/pkg/connector/rest/qbotest"
)

// failingStore loses the first write fn asks for, as a commit that fails
// (or a caller that gives up) after the server rotated the token would.
type failingStore struct {
	connector.MemoryTokens
	fail int
}

func (f *failingStore) UpdateToken(ctx context.Context, key string, fn func([]byte) ([]byte, error)) error {
	if f.fail > 0 {
		var wrote bool
		err := f.MemoryTokens.UpdateToken(ctx, key, func(cur []byte) ([]byte, error) {
			next, err := fn(cur)
			wrote = next != nil && err == nil
			return nil, err // stores nothing
		})
		if wrote {
			f.fail--
			return errors.New("the commit failed")
		}
		return err
	}
	return f.MemoryTokens.UpdateToken(ctx, key, fn)
}

// A refresh whose rotated token could not be stored keeps it in memory and
// stores it next time, instead of spending the token the server rotated.
func TestRotatedTokenSurvivesAFailedStore(t *testing.T) {
	q, seed := qbotest.New("qb-app", "s3cret")
	t.Cleanup(q.Close)
	store := &failingStore{fail: 1}
	c := newQuickBooksConn(t, q, qboSecret(seed), store)
	ctx := context.Background()
	if _, err := c.Poll(ctx, "SalesReceipt.Changed", 0, 10); err == nil {
		t.Fatal("the failed store went unnoticed")
	}
	if _, err := c.Poll(ctx, "SalesReceipt.Changed", 0, 10); err != nil {
		t.Fatalf("after the failed store: %v", err)
	}
	q.ExpireAccessTokens()
	// Another worker finds the rotated token in the store.
	other := newQuickBooksConn(t, q, qboSecret(seed), store)
	if _, err := other.Poll(ctx, "SalesReceipt.Changed", 0, 10); err != nil {
		t.Fatalf("another worker: %v", err)
	}
	if q.Refreshes != 2 || q.Spent != 0 {
		t.Fatalf("refreshes %d, spent %d", q.Refreshes, q.Spent)
	}
}

// Two connections of one endpoint (one app, two authorizations) keep
// their own tokens: neither resets the other's to a spent seed.
func TestTwoAuthorizationsOfOneEndpointKeepTheirTokens(t *testing.T) {
	q, seed1 := qbotest.New("qb-app", "s3cret")
	t.Cleanup(q.Close)
	seed2 := q.Authorize()
	store := &connector.MemoryTokens{}
	a := newQuickBooksConn(t, q, qboSecret(seed1), store)
	b := newQuickBooksConn(t, q, qboSecret(seed2), store)
	ctx := context.Background()
	for round := 0; round < 2; round++ {
		for _, c := range []*rest.Conn{a, b} {
			if _, err := c.Poll(ctx, "SalesReceipt.Changed", 0, 10); err != nil {
				t.Fatalf("round %d: %v", round, err)
			}
		}
		q.ExpireAccessTokens()
		a = newQuickBooksConn(t, q, qboSecret(seed1), store) // restarted workers
		b = newQuickBooksConn(t, q, qboSecret(seed2), store)
	}
	if q.Spent != 0 {
		t.Fatalf("spent %d", q.Spent)
	}
}

// A token endpoint that redirects does not get the client's credentials
// posted again to wherever it points.
func TestTokenRequestsDoNotFollowRedirects(t *testing.T) {
	var leaked atomic.Int32
	elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaked.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(elsewhere.Close)
	tokens := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL+"/token", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(tokens.Close)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(api.Close)
	cfg := []byte(`{"baseURL": "` + api.URL + `", "auth": {"type": "oauth2", "tokenURL": "` + tokens.URL + `/token", "clientAuth": "body"},
		"operations": {"get": {"method": "GET", "path": "/x/{{id}}"}}}`)
	c, err := rest.New(mustConfig(t, cfg), `{"clientId": "app", "clientSecret": "s3cret"}`, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Read(context.Background(), "get", "1"); err == nil || !strings.Contains(err.Error(), "307") {
		t.Fatalf("a redirected token request: %v", err)
	}
	if leaked.Load() != 0 {
		t.Fatal("the credentials followed the redirect")
	}
}

// A session's cookies go only to the host that set them.
func TestSessionLoginMustShareTheAPIsOrigin(t *testing.T) {
	for _, login := range []string{"https://login.example.com/Login", "http://b1.example.com/Login", "https://b1.example.com:50001/Login"} {
		cfg := []byte(`{"baseURL": "https://b1.example.com/b1s/v2", "auth": {"type": "session", "loginURL": "` + login + `"},
			"operations": {"get": {"method": "GET", "path": "/x/{{id}}"}}}`)
		if _, err := rest.New(mustConfig(t, cfg), `{"UserName": "u"}`, http.DefaultClient); err == nil || !strings.Contains(err.Error(), "share scheme and host") {
			t.Errorf("login %s: %v", login, err)
		}
	}
}

func mustConfig(t *testing.T, raw []byte) rest.Config {
	t.Helper()
	var cfg rest.Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}
