package console

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/fduser123-coding/turgon/internal/oidctest"
)

type oidcEnv struct {
	idp     *oidctest.Provider
	console *httptest.Server
	auth    *OIDCAuth
	client  *http.Client
}

func newOIDC(t *testing.T, cfg OIDCConfig) *oidcEnv {
	t.Helper()
	idp := oidctest.New("turgon-console", "s3cret",
		oidctest.Account{Subject: "u-1", Email: "olga@example.com", Groups: []string{"turgon-users", "turgon-approvers", "turgon-operators"}},
		oidctest.Account{Subject: "u-2", Email: "mallory@example.com", Groups: []string{"contractors"}},
	)
	t.Cleanup(idp.Close)
	var handler http.Handler
	console := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handler.ServeHTTP(w, r) }))
	t.Cleanup(console.Close)
	idp.RedirectURIs = []string{console.URL + "/auth/callback"}

	cfg.Issuer, cfg.ClientID, cfg.ClientSecret, cfg.URL = idp.Issuer, "turgon-console", "s3cret", console.URL
	if cfg.SessionKey == nil {
		cfg.SessionKey = []byte(strings.Repeat("k", 32))
	}
	auth, err := NewOIDCAuth(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	assets := fstest.MapFS{"index.html": {Data: []byte("<!doctype html><title>Turgon</title>")}}
	handler = New(Config{Runs: newRuns(), Auth: auth, Assets: assets})
	jar, _ := cookiejar.New(nil)
	return &oidcEnv{idp: idp, console: console, auth: auth, client: &http.Client{Jar: jar}}
}

func (e *oidcEnv) get(t *testing.T, path string) (*http.Response, string) {
	t.Helper()
	resp, err := e.client.Get(e.console.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func TestOIDCSignInKeepsTheUserAndTheirRoles(t *testing.T) {
	e := newOIDC(t, OIDCConfig{ViewerGroup: "turgon-users", ApproverGroup: "turgon-approvers", OperatorGroup: "turgon-operators", StewardGroup: "turgon-stewards"})

	// The API says to sign in; a page load goes through the provider and
	// comes back to the page.
	resp, body := e.get(t, "/api/me")
	if resp.StatusCode != http.StatusUnauthorized || !strings.Contains(body, `"login":"/auth/login?next=%2F"`) {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	resp, body = e.get(t, "/runs/shop-orders-to-erp%2F7?tab=writes")
	if resp.StatusCode != 200 || resp.Request.URL.Path != "/runs/shop-orders-to-erp/7" || resp.Request.URL.RawQuery != "tab=writes" || !strings.Contains(body, "<title>Turgon") {
		t.Fatalf("landed on %s (%d)", resp.Request.URL, resp.StatusCode)
	}
	_, body = e.get(t, "/api/me")
	var me Me
	if err := json.Unmarshal([]byte(body), &me); err != nil || me.ID != "olga@example.com" || strings.Join(me.Roles, ",") != "viewer,approver,operator" || me.Logout != "/auth/logout" {
		t.Fatalf("me %s", body)
	}
	// The session cookie is HttpOnly and SameSite=Lax.
	u, _ := url.Parse(e.console.URL)
	if cs := e.client.Jar.Cookies(u); len(cs) != 1 || cs[0].Name != sessionCookie {
		t.Fatalf("cookies %v", cs)
	}

	// Signing out ends the session.
	resp, body = e.get(t, "/auth/logout")
	if resp.Request.URL.Path != "/auth/signed-out" || !strings.Contains(body, "signed out") {
		t.Fatalf("logout %s", resp.Request.URL)
	}
	if resp, _ := e.get(t, "/api/me"); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("after logout: %d", resp.StatusCode)
	}
}

func TestOIDCRefusesUsersOutsideTheViewerGroup(t *testing.T) {
	e := newOIDC(t, OIDCConfig{ViewerGroup: "turgon-users", ApproverGroup: "turgon-approvers"})
	e.idp.Current = 1 // mallory, a contractor
	resp, body := e.get(t, "/")
	if resp.StatusCode != http.StatusForbidden || !strings.Contains(body, "may not use this console") {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if resp, _ := e.get(t, "/api/me"); resp.StatusCode != http.StatusUnauthorized {
		t.Fatal("a refused user got a session")
	}
}

// ID tokens the console must not accept.
func TestOIDCRejectsBadIDTokens(t *testing.T) {
	for name, mutate := range map[string]func(map[string]any){
		"another client's token": func(c map[string]any) { c["aud"] = "other-app" },
		"expired":                func(c map[string]any) { c["exp"] = time.Now().Add(-time.Minute).Unix() },
		"replayed nonce":         func(c map[string]any) { c["nonce"] = "from-another-login" },
		"another issuer":         func(c map[string]any) { c["iss"] = "https://evil.example" },
	} {
		e := newOIDC(t, OIDCConfig{})
		e.idp.Claims = mutate
		resp, body := e.get(t, "/")
		if resp.StatusCode != http.StatusBadRequest || !strings.Contains(body, "not valid for this console") {
			t.Errorf("%s: %d %s", name, resp.StatusCode, body)
		}
		if resp, _ := e.get(t, "/api/me"); resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s: signed in", name)
		}
	}
}

func TestOIDCCallbackNeedsTheLoginItStarted(t *testing.T) {
	e := newOIDC(t, OIDCConfig{})
	// A callback nobody started here (a login CSRF attempt).
	if resp, body := e.get(t, "/auth/callback?code=x&state=y"); resp.StatusCode != http.StatusBadRequest || !strings.Contains(body, "not started here") {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	// A started login, answered with another state.
	e.client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, _ := e.get(t, "/auth/login?next=/audit")
	if resp.StatusCode != http.StatusFound || !strings.HasPrefix(resp.Header.Get("Location"), e.idp.Issuer+"/authorize?") {
		t.Fatalf("login %d %s", resp.StatusCode, resp.Header.Get("Location"))
	}
	auth, _ := url.Parse(resp.Header.Get("Location"))
	if auth.Query().Get("code_challenge_method") != "S256" || auth.Query().Get("nonce") == "" {
		t.Fatalf("no PKCE or nonce: %s", auth)
	}
	if resp, body := e.get(t, "/auth/callback?code=x&state=forged"); resp.StatusCode != http.StatusBadRequest || !strings.Contains(body, "does not match") {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	// The provider's error is shown, escaped.
	e.get(t, "/auth/login")
	if resp, body := e.get(t, "/auth/callback?error=access_denied<script>"); resp.StatusCode != http.StatusForbidden || strings.Contains(body, "<script>") {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
}

func TestOIDCSessionsCannotBeForged(t *testing.T) {
	e := newOIDC(t, OIDCConfig{ApproverGroup: "turgon-approvers"})
	e.get(t, "/")
	u, _ := url.Parse(e.console.URL)
	good := e.client.Jar.Cookies(u)[0].Value
	try := func(value string) int {
		req, _ := http.NewRequest(http.MethodGet, e.console.URL+"/api/me", nil)
		req.AddCookie(&http.Cookie{Name: sessionCookie, Value: value})
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if try(good) != 200 {
		t.Fatal("the real session was refused")
	}
	// Grant yourself the operator role: the signature no longer matches.
	payload, sig, _ := strings.Cut(good, ".")
	forged := strings.Replace(decode(t, payload), `"approver"]`, `"approver","operator"]`, 1)
	if try(encode(forged)+"."+sig) != http.StatusUnauthorized {
		t.Fatal("a forged session was accepted")
	}
	// A login cookie is signed for another purpose.
	if try(e.auth.seal(loginCookie, session{User: User{ID: "x", Roles: []string{RoleViewer}}, Expires: time.Now().Add(time.Hour).Unix()})) != http.StatusUnauthorized {
		t.Fatal("a login cookie was accepted as a session")
	}
	// Sessions end.
	e.auth.now = func() time.Time { return time.Now().Add(9 * time.Hour) }
	if try(good) != http.StatusUnauthorized {
		t.Fatal("an expired session was accepted")
	}
}

func TestOIDCRedirectsStayOnTheConsole(t *testing.T) {
	for next, want := range map[string]string{
		"//evil.example/x": "/", "https://evil.example": "/", "/\\evil.example": "/", "/auth/logout": "/", "/audit?x=1": "/audit?x=1",
	} {
		if got := safeNext(next); got != want {
			t.Errorf("safeNext(%q) = %q, want %q", next, got, want)
		}
	}
	e := newOIDC(t, OIDCConfig{})
	resp, _ := e.get(t, "/auth/login?next="+url.QueryEscape("//evil.example/steal"))
	if resp.Request.URL.Host != strings.TrimPrefix(e.console.URL, "http://") || resp.Request.URL.Path != "/" {
		t.Fatalf("landed on %s", resp.Request.URL)
	}
}

func TestOIDCConfiguration(t *testing.T) {
	for name, cfg := range map[string]OIDCConfig{
		"short session key": {Issuer: "https://x", ClientID: "c", URL: "https://turgon.example", SessionKey: []byte("short")},
		"plain http":        {Issuer: "https://x", ClientID: "c", URL: "http://turgon.example", SessionKey: []byte(strings.Repeat("k", 32))},
		"no client":         {Issuer: "https://x", URL: "https://turgon.example", SessionKey: []byte(strings.Repeat("k", 32))},
	} {
		if _, err := NewOIDCAuth(context.Background(), cfg); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func decode(t *testing.T, s string) string {
	t.Helper()
	b, err := base64RawURL.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func encode(s string) string { return base64RawURL.EncodeToString([]byte(s)) }
