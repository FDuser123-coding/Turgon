// Package oidctest is a fake OpenID Connect provider for tests and demos:
// discovery, the authorization endpoint, a token endpoint that checks
// client secrets and PKCE, and RS256-signed ID tokens with an email and
// groups. It signs in whichever account is current, or, when an account
// is given as ?account=<n>, that one.
package oidctest

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/go-jose/go-jose/v4"
)

// Account is a user the provider signs in.
type Account struct {
	Subject string
	Email   string
	Groups  []string
}

// Provider is the fake identity provider.
type Provider struct {
	ClientID, ClientSecret string
	// RedirectURIs are the registered callback URLs.
	RedirectURIs []string
	Accounts     []Account
	// Current is the account signed in without a choice; -1 shows a page to
	// pick one.
	Current int
	// Claims, if set, changes the ID token's claims before signing, to
	// test what a console must refuse.
	Claims func(map[string]any)
	// Issuer is the base URL; set by New or by the caller for Handler.
	Issuer string

	mu    sync.Mutex
	codes map[string]grant
	key   *rsa.PrivateKey
	kid   string
	srv   *httptest.Server
}

type grant struct {
	account                    Account
	redirect, nonce, challenge string
	expires                    time.Time
}

// New starts a provider on a local port.
func New(clientID, secret string, accounts ...Account) *Provider {
	p := NewProvider(clientID, secret, accounts...)
	p.srv = httptest.NewServer(p)
	p.Issuer = p.srv.URL
	return p
}

// NewProvider returns a provider to serve yourself; set Issuer.
func NewProvider(clientID, secret string, accounts ...Account) *Provider {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return &Provider{ClientID: clientID, ClientSecret: secret, Accounts: accounts, codes: map[string]grant{}, key: key, kid: "turgon-test-1"}
}

func (p *Provider) Close() { p.srv.Close() }

func (p *Provider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/.well-known/openid-configuration":
		writeJSON(w, http.StatusOK, map[string]any{
			"issuer": p.Issuer, "authorization_endpoint": p.Issuer + "/authorize", "token_endpoint": p.Issuer + "/token",
			"jwks_uri": p.Issuer + "/jwks", "response_types_supported": []string{"code"}, "subject_types_supported": []string{"public"},
			"id_token_signing_alg_values_supported": []string{"RS256"}, "code_challenge_methods_supported": []string{"S256"},
			"scopes_supported": []string{"openid", "email", "profile", "groups"},
		})
	case "/jwks":
		writeJSON(w, http.StatusOK, jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &p.key.PublicKey, KeyID: p.kid, Algorithm: "RS256", Use: "sig"}}})
	case "/authorize":
		p.authorize(w, r)
	case "/token":
		p.token(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (p *Provider) authorize(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	registered := false
	for _, u := range p.RedirectURIs {
		registered = registered || u == q.Get("redirect_uri")
	}
	switch {
	case q.Get("client_id") != p.ClientID || !registered:
		http.Error(w, "unknown client or redirect_uri", http.StatusBadRequest)
		return
	case q.Get("response_type") != "code" || q.Get("code_challenge_method") != "S256" || q.Get("code_challenge") == "":
		http.Error(w, "the code flow with PKCE (S256) is required", http.StatusBadRequest)
		return
	}
	i := p.Current
	if a := q.Get("account"); a != "" {
		i, _ = strconv.Atoi(a)
	}
	if i < 0 || i >= len(p.Accounts) {
		// Let a person pick the account, as a login page would.
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, `<!doctype html><title>Fake identity provider</title><h1>Sign in as</h1><ul>`)
		for n, a := range p.Accounts {
			q.Set("account", strconv.Itoa(n))
			fmt.Fprintf(w, `<li><a href="/authorize?%s">%s</a> (%s)</li>`, html.EscapeString(q.Encode()), html.EscapeString(a.Email), html.EscapeString(fmt.Sprint(a.Groups)))
		}
		fmt.Fprint(w, `</ul>`)
		return
	}
	code := random()
	p.mu.Lock()
	p.codes[code] = grant{account: p.Accounts[i], redirect: q.Get("redirect_uri"), nonce: q.Get("nonce"), challenge: q.Get("code_challenge"), expires: time.Now().Add(time.Minute)}
	p.mu.Unlock()
	back, _ := url.Parse(q.Get("redirect_uri"))
	bq := back.Query()
	bq.Set("code", code)
	bq.Set("state", q.Get("state"))
	back.RawQuery = bq.Encode()
	http.Redirect(w, r, back.String(), http.StatusFound)
}

func (p *Provider) token(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil || r.Method != http.MethodPost {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	id, secret, ok := r.BasicAuth()
	if !ok {
		id, secret = r.PostForm.Get("client_id"), r.PostForm.Get("client_secret")
	}
	if id != p.ClientID || secret != p.ClientSecret {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid_client"})
		return
	}
	code := r.PostForm.Get("code")
	p.mu.Lock()
	g, found := p.codes[code]
	delete(p.codes, code) // one use
	p.mu.Unlock()
	sum := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
	switch {
	case r.PostForm.Get("grant_type") != "authorization_code" || !found || time.Now().After(g.expires) || g.redirect != r.PostForm.Get("redirect_uri"):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_grant"})
		return
	case base64.RawURLEncoding.EncodeToString(sum[:]) != g.challenge:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_grant", "error_description": "PKCE verification failed"})
		return
	}
	now := time.Now()
	claims := map[string]any{
		"iss": p.Issuer, "sub": g.account.Subject, "aud": p.ClientID, "iat": now.Unix(), "exp": now.Add(5 * time.Minute).Unix(),
		"nonce": g.nonce, "email": g.account.Email, "email_verified": true, "groups": g.account.Groups,
	}
	if p.Claims != nil {
		p.Claims(claims)
	}
	payload, _ := json.Marshal(claims)
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: p.key}, (&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", p.kid))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	jws, err := signer.Sign(payload)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	idToken, _ := jws.CompactSerialize()
	writeJSON(w, http.StatusOK, map[string]any{"access_token": random(), "token_type": "Bearer", "expires_in": 300, "id_token": idToken})
}

func random() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
