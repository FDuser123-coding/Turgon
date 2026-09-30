package secrets

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeBao serves a KV v2 mount and the kubernetes and approle logins.
type fakeBao struct {
	mu      sync.Mutex
	secrets map[string]map[string]any // path -> data
	tokens  map[string]time.Time      // token -> expiry
	reads   int
	logins  int
	ns      string
	now     func() time.Time
}

func newFake(t *testing.T) (*fakeBao, *httptest.Server) {
	f := &fakeBao{secrets: map[string]map[string]any{}, tokens: map[string]time.Time{"root": time.Now().Add(time.Hour)}, now: time.Now}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeBao) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fail := func(status int, msg string) {
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"errors": []string{msg}})
	}
	if f.ns != "" && r.Header.Get("X-Vault-Namespace") != f.ns {
		fail(http.StatusForbidden, "permission denied")
		return
	}
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/login"):
		var in map[string]string
		_ = json.NewDecoder(r.Body).Decode(&in)
		ok := (r.URL.Path == "/v1/auth/kubernetes/login" && in["role"] == "turgon" && in["jwt"] == "sa-token") ||
			(r.URL.Path == "/v1/auth/approle/login" && in["role_id"] == "rid" && in["secret_id"] == "sid")
		if !ok {
			fail(http.StatusBadRequest, "invalid role or credentials")
			return
		}
		f.logins++
		tok := "t" + string(rune('a'+f.logins))
		f.tokens[tok] = f.now().Add(30 * time.Second)
		_ = json.NewEncoder(w).Encode(map[string]any{"auth": map[string]any{"client_token": tok, "lease_duration": 30, "renewable": true}})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/secret/data/"):
		exp, ok := f.tokens[r.Header.Get("X-Vault-Token")]
		if !ok || f.now().After(exp) {
			fail(http.StatusForbidden, "permission denied")
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/v1/secret/data/")
		if path == "deleted" { // what Vault and OpenBao answer for a soft-deleted latest version
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"data": nil,
				"metadata": map[string]any{"deletion_time": "2026-09-30T11:40:00Z", "destroyed": false, "version": 2}}})
			return
		}
		if path == "forbidden" {
			fail(http.StatusForbidden, "1 error occurred:\n\t* permission denied")
			return
		}
		data, ok := f.secrets[path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"errors": []string{}})
			return
		}
		f.reads++
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"data": data, "metadata": map[string]any{"version": 3}}})
	default:
		fail(http.StatusNotFound, "no handler")
	}
}

func TestTokenAuthReadsKVAndCaches(t *testing.T) {
	f, srv := newFake(t)
	f.secrets["shop-db"] = map[string]any{"dsn": "postgres://shop"}
	f.secrets["salesforce"] = map[string]any{"prod-jwt": map[string]any{"clientId": "3MVG", "username": "turgon@example.com"}}
	f.secrets["team/erp/db"] = map[string]any{"dsn": "postgres://erp"}
	o, err := NewOpenBao(Config{Address: srv.URL, Token: "root"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for ref, want := range map[string]string{
		"openbao://shop-db/dsn":         "postgres://shop",
		"vault://team/erp/db/dsn":       "postgres://erp",
		"openbao://salesforce/prod-jwt": `{"clientId":"3MVG","username":"turgon@example.com"}`,
	} {
		if got, err := o.Resolve(ctx, ref); err != nil || got != want {
			t.Errorf("%s = %q, %v; want %q", ref, got, err, want)
		}
	}
	// Read again within the TTL: from the cache.
	reads := f.reads
	if _, err := o.Resolve(ctx, "openbao://shop-db/dsn"); err != nil || f.reads != reads {
		t.Fatalf("reads %d -> %d, err %v", reads, f.reads, err)
	}
	o.now = func() time.Time { return time.Now().Add(6 * time.Minute) }
	if _, err := o.Resolve(ctx, "openbao://shop-db/dsn"); err != nil || f.reads != reads+1 {
		t.Fatalf("an expired entry was not read again: %d reads, %v", f.reads, err)
	}
	if w := o.Where("openbao://team/erp/db/dsn"); w != "secret/data/team/erp/db key dsn" {
		t.Fatalf("where = %s", w)
	}
}

func TestErrorsNameTheFix(t *testing.T) {
	f, srv := newFake(t)
	f.secrets["shop-db"] = map[string]any{"dsn": "x", "user": "y"}
	o, _ := NewOpenBao(Config{Address: srv.URL, Token: "root"})
	ctx := context.Background()
	for ref, want := range map[string]string{
		"openbao://shop-db/password": `has no key "password" (it has dsn, user)`,
		"openbao://missing/dsn":      "openbao 404",
		"openbao://forbidden/dsn":    "openbao 403: permission denied",
		"openbao://deleted/dsn":      "deleted at 2026-09-30T11:40:00Z",
		"env://shop-db/dsn":          "openbao:// or vault://",
		"openbao://justakey":         "openbao://<path>/<key>",
		"openbao://../sys/policy/x":  "openbao://<path>/<key>",
		"openbao://a/../../sys/key":  "openbao://<path>/<key>",
	} {
		if _, err := o.Resolve(ctx, ref); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want %q", ref, err, want)
		}
	}
	if res := o.Check(ctx, []string{"openbao://deleted/dsn"}); !strings.Contains(res[1].Fix, "kv undelete") {
		t.Fatalf("deleted: %+v", res)
	}
	res := o.Check(ctx, []string{"openbao://shop-db/dsn", "openbao://forbidden/dsn", "openbao://missing/dsn"})
	if len(res) != 4 || !res[0].OK || !res[1].OK || res[2].OK || !strings.Contains(res[2].Fix, "allow read on secret/data/forbidden") ||
		res[3].OK || !strings.Contains(res[3].Fix, "bao kv put -mount=secret missing") {
		t.Fatalf("check = %+v", res)
	}
	// No value ever appears in a check.
	for _, r := range res {
		if strings.Contains(r.Detail+r.Fix, `"x"`) || r.Detail == "x" {
			t.Fatalf("a value leaked: %+v", r)
		}
	}
	bad, _ := NewOpenBao(Config{Address: srv.URL, Token: "expired"})
	if res := bad.Check(ctx, []string{"openbao://shop-db/dsn"}); res[1].OK {
		t.Fatalf("check with a bad token = %+v", res)
	}
}

func TestKubernetesAuthSignsInAgainBeforeExpiry(t *testing.T) {
	f, srv := newFake(t)
	f.secrets["shop-db"] = map[string]any{"dsn": "postgres://shop"}
	dir := t.TempDir()
	jwt := filepath.Join(dir, "token")
	if err := os.WriteFile(jwt, []byte("sa-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := NewOpenBao(Config{Address: srv.URL, Auth: "kubernetes", Role: "turgon", JWTFile: jwt, TTL: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := o.Resolve(ctx, "openbao://shop-db/dsn"); err != nil || f.logins != 1 {
		t.Fatalf("err %v, logins %d", err, f.logins)
	}
	// Within two thirds of the 30s lease the token is reused; after, the
	// worker signs in again with the (rotated) service account token.
	o.now = func() time.Time { return time.Now().Add(10 * time.Second) }
	_, _ = o.Resolve(ctx, "openbao://shop-db/dsn")
	o.now = func() time.Time { return time.Now().Add(25 * time.Second) }
	_, _ = o.Resolve(ctx, "openbao://shop-db/dsn")
	if f.logins != 2 {
		t.Fatalf("logins = %d, want 2", f.logins)
	}
	// A token revoked early is replaced once.
	f.mu.Lock()
	f.tokens = map[string]time.Time{}
	f.mu.Unlock()
	if _, err := o.Resolve(ctx, "openbao://shop-db/dsn"); err != nil || f.logins != 3 {
		t.Fatalf("after revocation: err %v, logins %d", err, f.logins)
	}
	wrong, _ := NewOpenBao(Config{Address: srv.URL, Auth: "kubernetes", Role: "other", JWTFile: jwt})
	res := wrong.Check(ctx, nil)
	if res[0].OK || !strings.Contains(res[0].Fix, "auth/kubernetes/role/other") {
		t.Fatalf("check = %+v", res)
	}
}

func TestAppRoleAndNamespace(t *testing.T) {
	f, srv := newFake(t)
	f.ns = "acme/prod"
	f.secrets["shop-db"] = map[string]any{"dsn": "postgres://shop"}
	sid := filepath.Join(t.TempDir(), "secret-id")
	_ = os.WriteFile(sid, []byte("sid"), 0o600)
	o, err := NewOpenBao(Config{Address: srv.URL, Auth: "approle", Role: "rid", SecretIDFile: sid, Namespace: "acme/prod"})
	if err != nil {
		t.Fatal(err)
	}
	if v, err := o.Resolve(context.Background(), "openbao://shop-db/dsn"); err != nil || v != "postgres://shop" {
		t.Fatalf("v %q err %v", v, err)
	}
}

func TestConfigRejected(t *testing.T) {
	for name, c := range map[string]Config{
		"no address":         {Token: "x"},
		"plain http remote":  {Address: "http://openbao.example:8200", Token: "x"},
		"no token":           {Address: "https://openbao.example"},
		"kubernetes no role": {Address: "https://openbao.example", Auth: "kubernetes"},
		"approle no secret":  {Address: "https://openbao.example", Auth: "approle", Role: "rid"},
		"unknown auth":       {Address: "https://openbao.example", Auth: "ldap"},
		"bad mount":          {Address: "https://openbao.example", Token: "x", Mount: "../sys"},
	} {
		if _, err := NewOpenBao(c); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
