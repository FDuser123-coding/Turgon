package rest

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
)

// Auth says how requests are authenticated. The credential itself is the
// connection's secret:
//
//	bearer  a token, sent as "Authorization: Bearer <token>"
//	header  a token, sent in Header (e.g. X-Shopify-Access-Token)
//	basic   "user:password"
//	oauth2  {"clientId": "...", "clientSecret": "..."}; the client
//	        credentials grant against TokenURL, refreshed before expiry
//	oauth2-refresh  {"clientId", "clientSecret", "refreshToken"}; the
//	        refresh token grant against TokenURL, for APIs a person
//	        authorized (QuickBooks Online, Xero). The server rotates the
//	        refresh token, so the current one is kept in a
//	        connector.TokenStore (Turgon's database); the secret seeds it
//	oauth1  {"consumerKey", "consumerSecret", "tokenId", "tokenSecret"};
//	        OAuth 1.0a request signing with HMAC-SHA256, as NetSuite's
//	        token-based authentication takes it, with Realm (the account ID)
//	none    no credential
type Auth struct {
	Type     string   `json:"type"`
	Header   string   `json:"header,omitempty"`
	TokenURL string   `json:"tokenURL,omitempty"`
	Scopes   []string `json:"scopes,omitempty"`
	Realm    string   `json:"realm,omitempty"`
	// ClientAuth is how an oauth2 client authenticates to TokenURL: basic
	// (default; form-encoded as RFC 6749 says) or body (client_id and
	// client_secret in the form, for servers that take Basic credentials
	// as they stand, like SAP's XSUAA with its "!" and "|" client IDs).
	ClientAuth string `json:"clientAuth,omitempty"`
	// LoginURL is where a session login is posted (type session): the
	// secret, a JSON object, is the body, as SAP Business One's Service
	// Layer takes {"CompanyDB", "UserName", "Password"} at /Login. The
	// cookies it sets authenticate later requests until the session ends
	// (SessionTimeout minutes in the answer, else 30) or a 401 says so.
	LoginURL string `json:"loginURL,omitempty"`
	// Cookies names the login's cookies to send (default: all it sets).
	Cookies []string `json:"cookies,omitempty"`
}

func (a Auth) validate() error {
	switch a.Type {
	case "bearer", "basic", "none":
	case "oauth1":
		if a.Realm == "" {
			return fmt.Errorf("auth: oauth1 needs a realm (the NetSuite account ID, e.g. 1234567_SB1)")
		}
	case "header":
		if a.Header == "" || strings.EqualFold(a.Header, "Authorization") {
			return fmt.Errorf("auth: header needs a header name other than Authorization (use bearer)")
		}
	case "session":
		u, err := url.Parse(a.LoginURL)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
			return fmt.Errorf("auth: session needs an http(s) loginURL")
		}
	case "oauth2", "oauth2-refresh":
		u, err := url.Parse(a.TokenURL)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
			return fmt.Errorf("auth: %s needs an http(s) tokenURL", a.Type)
		}
		if a.ClientAuth != "" && a.ClientAuth != "basic" && a.ClientAuth != "body" {
			return fmt.Errorf("auth: clientAuth must be basic or body, got %q", a.ClientAuth)
		}
	default:
		return fmt.Errorf("auth: type must be bearer, header, basic, oauth2, oauth2-refresh, oauth1, session or none, got %q", a.Type)
	}
	return nil
}

// SameOrigin checks that a session's cookies go only where they were set:
// the API at base must have the login URL's scheme and host (and port).
func (a Auth) SameOrigin(base *url.URL) error {
	if a.Type != "session" {
		return nil
	}
	login, err := url.Parse(a.LoginURL)
	if err != nil {
		return fmt.Errorf("auth: session needs an http(s) loginURL")
	}
	if !strings.EqualFold(login.Scheme, base.Scheme) || !strings.EqualFold(login.Host, base.Host) {
		return fmt.Errorf("auth: the session's loginURL (%s://%s) and the API (%s://%s) must share scheme and host: the login's cookies are sent to the API",
			login.Scheme, login.Host, base.Scheme, base.Host)
	}
	return nil
}

type authenticator struct {
	cfg    Auth
	secret string
	http   *http.Client

	clientID, clientSecret string
	oauth1                 oauth1Creds
	now                    func() time.Time

	mu      sync.Mutex
	token   string
	expires time.Time

	// oauth2-refresh: the seed refresh token from the secret, and where the
	// rotated ones are kept (connector.TokenStore), under tokenKey.
	refreshToken string
	tokens       connector.TokenStore
	tokenKey     string
	legacyKey    string    // where releases before the seed was in the key kept it
	rejected     string    // an access token the API refused, not to be reused
	pending      *rotation // a rotation the store has not taken yet
}

// rotation is a refresh whose new tokens could not be stored: the server
// has spent the old refresh token, so the new one is kept in memory and
// stored on the next call.
type rotation struct {
	spent string
	state tokenState
}

// grantTimeout bounds a token request and the storing of what it returns.
// They run apart from the caller's context: a refresh token the server has
// rotated is lost unless its successor is stored, whatever the caller does.
const grantTimeout = 30 * time.Second

func newAuthenticator(cfg Auth, secret string, client *http.Client) (*authenticator, error) {
	a := &authenticator{cfg: cfg, secret: strings.TrimSpace(secret), http: noRedirects(client)}
	switch cfg.Type {
	case "bearer", "header":
		if a.secret == "" {
			return nil, fmt.Errorf("auth: the secret must hold the %s token", cfg.Type)
		}
	case "basic":
		if !strings.Contains(a.secret, ":") {
			return nil, fmt.Errorf(`auth: the basic secret must be "user:password"`)
		}
	case "oauth2":
		var creds struct {
			ClientID     string `json:"clientId"`
			ClientSecret string `json:"clientSecret"`
		}
		if err := json.Unmarshal([]byte(a.secret), &creds); err != nil || creds.ClientID == "" || creds.ClientSecret == "" {
			return nil, fmt.Errorf(`auth: the oauth2 secret must be {"clientId": "...", "clientSecret": "..."}`)
		}
		a.clientID, a.clientSecret = creds.ClientID, creds.ClientSecret
	case "oauth2-refresh":
		var creds struct {
			ClientID     string `json:"clientId"`
			ClientSecret string `json:"clientSecret"`
			RefreshToken string `json:"refreshToken"`
		}
		if err := json.Unmarshal([]byte(a.secret), &creds); err != nil || creds.ClientID == "" || creds.ClientSecret == "" || creds.RefreshToken == "" {
			return nil, fmt.Errorf(`auth: the oauth2-refresh secret must be {"clientId": "...", "clientSecret": "...", "refreshToken": "..."}`)
		}
		a.clientID, a.clientSecret, a.refreshToken = creds.ClientID, creds.ClientSecret, creds.RefreshToken
	case "session":
		var login map[string]any
		if err := json.Unmarshal([]byte(a.secret), &login); err != nil || len(login) == 0 {
			return nil, fmt.Errorf(`auth: the session secret must be the login's JSON body, e.g. {"CompanyDB": "...", "UserName": "...", "Password": "..."}`)
		}
	case "oauth1":
		c := &a.oauth1
		if err := json.Unmarshal([]byte(a.secret), c); err != nil || c.ConsumerKey == "" || c.ConsumerSecret == "" || c.TokenID == "" || c.TokenSecret == "" {
			return nil, fmt.Errorf(`auth: the oauth1 secret must be {"consumerKey": "...", "consumerSecret": "...", "tokenId": "...", "tokenSecret": "..."}`)
		}
	}
	return a, nil
}

func (a *authenticator) refreshable() bool {
	return a.cfg.Type == "oauth2" || a.cfg.Type == "oauth2-refresh" || a.cfg.Type == "session"
}

// noRedirects copies client for token and login requests, which carry
// credentials in their body: a 307 or 308 would post them again to
// wherever it points.
func noRedirects(client *http.Client) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	c := *client
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &c
}

// invalidate drops the token req was sent with, after the API refused it.
// A request that carried an older token (another one refreshed it since)
// leaves the current one alone. A nil req drops whatever is current.
func (a *authenticator) invalidate(req *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.token == "" {
		return
	}
	if req != nil {
		sent := req.Header.Get("Cookie")
		if a.cfg.Type != "session" {
			sent = strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")
		}
		if sent != a.token {
			return
		}
	}
	a.rejected, a.token = a.token, ""
}

func (a *authenticator) apply(ctx context.Context, req *http.Request) error {
	switch a.cfg.Type {
	case "bearer":
		req.Header.Set("Authorization", "Bearer "+a.secret)
	case "header":
		req.Header.Set(a.cfg.Header, a.secret)
	case "basic":
		req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(a.secret)))
	case "oauth2":
		token, err := a.current(ctx)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+token)
	case "oauth2-refresh":
		token, err := a.refreshed(ctx)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+token)
	case "session":
		cookie, err := a.session(ctx)
		if err != nil {
			return err
		}
		req.Header.Set("Cookie", cookie)
	case "oauth1":
		now := time.Now
		if a.now != nil {
			now = a.now
		}
		req.Header.Set("Authorization", SignOAuth1(req.Method, req.URL, a.cfg.Realm, a.oauth1, now(), nonce()))
	}
	return nil
}

type oauth1Creds struct {
	ConsumerKey    string `json:"consumerKey"`
	ConsumerSecret string `json:"consumerSecret"`
	TokenID        string `json:"tokenId"`
	TokenSecret    string `json:"tokenSecret"`
}

// OAuth1Credentials are the four secrets of a token-based integration.
type OAuth1Credentials = oauth1Creds

func nonce() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// oauthEscape percent-encodes as RFC 5849 §3.6 requires: everything but
// unreserved characters, with uppercase hex.
func oauthEscape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z') || ('0' <= c && c <= '9') || c == '-' || c == '.' || c == '_' || c == '~' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// SignOAuth1 returns the Authorization header of an OAuth 1.0a request
// signed with HMAC-SHA256 (RFC 5849 §3.4, with SHA-256 as NetSuite takes
// it). The query string is signed; a JSON body is not.
func SignOAuth1(method string, u *url.URL, realm string, c OAuth1Credentials, t time.Time, nonce string) string {
	return signOAuth1(sha256.New, "HMAC-SHA256", method, u, realm, c, t, nonce)
}

func signOAuth1(h func() hash.Hash, sigMethod, method string, u *url.URL, realm string, c OAuth1Credentials, t time.Time, nonce string) string {
	oauth := map[string]string{
		"oauth_consumer_key":     c.ConsumerKey,
		"oauth_token":            c.TokenID,
		"oauth_signature_method": sigMethod,
		"oauth_timestamp":        strconv.FormatInt(t.Unix(), 10),
		"oauth_nonce":            nonce,
		"oauth_version":          "1.0",
	}
	var pairs []string
	for k, v := range oauth {
		pairs = append(pairs, oauthEscape(k)+"="+oauthEscape(v))
	}
	for k, vs := range u.Query() {
		for _, v := range vs {
			pairs = append(pairs, oauthEscape(k)+"="+oauthEscape(v))
		}
	}
	sort.Strings(pairs)
	base := strings.ToUpper(method) + "&" + oauthEscape(strings.ToLower(u.Scheme)+"://"+strings.ToLower(u.Host)+u.EscapedPath()) + "&" + oauthEscape(strings.Join(pairs, "&"))
	mac := hmac.New(h, []byte(oauthEscape(c.ConsumerSecret)+"&"+oauthEscape(c.TokenSecret)))
	mac.Write([]byte(base))
	oauth["oauth_signature"] = base64.StdEncoding.EncodeToString(mac.Sum(nil))
	keys := make([]string, 0, len(oauth))
	for k := range oauth {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	header := `OAuth realm="` + oauthEscape(realm) + `"`
	for _, k := range keys {
		header += ", " + k + `="` + oauthEscape(oauth[k]) + `"`
	}
	return header
}

// current returns a valid access token, fetching one when needed.
func (a *authenticator) current(ctx context.Context) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.token != "" && time.Now().Before(a.expires) {
		return a.token, nil
	}
	form := url.Values{"grant_type": {"client_credentials"}}
	if len(a.cfg.Scopes) > 0 {
		form.Set("scope", strings.Join(a.cfg.Scopes, " "))
	}
	if a.cfg.ClientAuth == "body" {
		form.Set("client_id", a.clientID)
		form.Set("client_secret", a.clientSecret)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	if a.cfg.ClientAuth != "body" {
		req.SetBasicAuth(url.QueryEscape(a.clientID), url.QueryEscape(a.clientSecret))
	}
	resp, err := a.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("oauth2 token: %w", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return "", &APIError{Method: http.MethodPost, Path: a.cfg.TokenURL, Status: resp.StatusCode, Body: strings.TrimSpace(string(data))}
	}
	var tok struct {
		AccessToken string  `json:"access_token"`
		ExpiresIn   seconds `json:"expires_in"`
	}
	if err := json.Unmarshal(data, &tok); err != nil || tok.AccessToken == "" {
		return "", fmt.Errorf("oauth2 token: no access_token in the response")
	}
	life := time.Duration(tok.ExpiresIn) * time.Second
	if life <= 0 {
		life = time.Hour
	}
	// Refresh early so a request never carries an expiring token.
	margin := time.Minute
	if life < 4*margin {
		margin = life / 4
	}
	a.token, a.expires = tok.AccessToken, time.Now().Add(life-margin)
	return a.token, nil
}

// seconds is a token's lifetime as servers send it: a number, or a number
// in a string as Exact Online sends it ("600").
type seconds int

func (s *seconds) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*s = 0 // the default lifetime
		return nil
	}
	n, err := strconv.Atoi(strings.Trim(string(b), `"`))
	if err != nil {
		return fmt.Errorf("expires_in %s is not a number of seconds", b)
	}
	*s = seconds(n)
	return nil
}

// tokenState is what the token store keeps for an oauth2-refresh
// connection: the refresh token in force, the access token it last gave
// (so workers share it instead of each spending the refresh token), and a
// hash of the seed it descends from.
type tokenState struct {
	Seed         string    `json:"seed"`
	RefreshToken string    `json:"refreshToken"`
	AccessToken  string    `json:"accessToken,omitempty"`
	Expires      time.Time `json:"expires,omitempty"`
}

// ErrNoTokenStore is returned by an oauth2-refresh connection without a
// token store: spending the refresh token would lose the one the server
// rotates it to.
var ErrNoTokenStore = errors.New("oauth2-refresh keeps the refresh tokens the server rotates in Turgon's database: " +
	"give the command --database-url (TURGON_DATABASE_URL) and set TURGON_TOKEN_KEYS")

// refreshed returns an access token, refreshing it with the refresh token
// in force. The refresh and the rotated token's storing happen under the
// store's lock, so two workers never spend the same single-use token. The
// stored token is keyed by its seed (the refresh token in the secret): an
// app authorized again starts from its new seed, and two connections of
// one endpoint never share a token.
func (a *authenticator) refreshed(ctx context.Context) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.token != "" && time.Now().Before(a.expires) {
		return a.token, nil
	}
	if a.tokens == nil {
		return "", ErrNoTokenStore
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), grantTimeout)
	defer cancel()
	sum := sha256.Sum256([]byte(a.refreshToken))
	seed := hex.EncodeToString(sum[:8])
	var legacy *tokenState
	if a.legacyKey != "" {
		err := a.tokens.UpdateToken(ctx, a.legacyKey, func(cur []byte) ([]byte, error) {
			var st tokenState
			if cur != nil && json.Unmarshal(cur, &st) == nil && st.Seed == seed && st.RefreshToken != "" {
				legacy = &st
			}
			return nil, nil
		})
		if err != nil {
			return "", err
		}
	}
	var token string
	var expires time.Time
	err := a.tokens.UpdateToken(ctx, a.tokenKey, func(cur []byte) ([]byte, error) {
		var st tokenState
		changed := false
		switch {
		case cur != nil:
			if err := json.Unmarshal(cur, &st); err != nil {
				return nil, fmt.Errorf("oauth2-refresh: the stored token is not readable: %w", err)
			}
		case legacy != nil:
			st, changed = *legacy, true
		}
		if st.Seed != seed || st.RefreshToken == "" {
			st, changed = tokenState{Seed: seed, RefreshToken: a.refreshToken}, true
		}
		if p := a.pending; p != nil {
			if st.RefreshToken == p.spent {
				st, changed = p.state, true // the rotation the store missed
			} else {
				a.pending = nil // the store moved on without it
			}
		}
		if st.AccessToken == "" || st.AccessToken == a.rejected || !time.Now().Before(st.Expires) {
			access, refresh, life, err := a.refreshGrant(ctx, st.RefreshToken)
			if err != nil {
				return nil, err
			}
			margin := time.Minute
			if life < 4*margin {
				margin = life / 4
			}
			spent := st.RefreshToken
			st.AccessToken, st.Expires = access, time.Now().Add(life-margin)
			if refresh != "" {
				st.RefreshToken = refresh
			}
			// Until the store commits, the new tokens live here. A pending
			// rotation this one follows is superseded: its token is spent.
			if a.pending != nil {
				spent = a.pending.spent
			}
			a.pending, changed = &rotation{spent: spent, state: st}, true
		}
		token, expires = st.AccessToken, st.Expires
		if !changed {
			return nil, nil // another worker refreshed it
		}
		return json.Marshal(st)
	})
	if err != nil {
		return "", err
	}
	a.pending, a.legacyKey = nil, ""
	a.token, a.expires = token, expires
	return a.token, nil
}

// refreshGrant spends a refresh token (RFC 6749 §6).
func (a *authenticator) refreshGrant(ctx context.Context, refresh string) (access, next string, life time.Duration, err error) {
	form := url.Values{"grant_type": {"refresh_token"}, "refresh_token": {refresh}}
	if a.cfg.ClientAuth == "body" {
		form.Set("client_id", a.clientID)
		form.Set("client_secret", a.clientSecret)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", "", 0, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	if a.cfg.ClientAuth != "body" {
		req.SetBasicAuth(url.QueryEscape(a.clientID), url.QueryEscape(a.clientSecret))
	}
	resp, err := a.http.Do(req)
	if err != nil {
		return "", "", 0, fmt.Errorf("oauth2 refresh: %w", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		e := &APIError{Method: http.MethodPost, Path: a.cfg.TokenURL, Status: resp.StatusCode, Body: strings.TrimSpace(string(data))}
		if strings.Contains(e.Body, "invalid_grant") {
			return "", "", 0, fmt.Errorf("%w (the refresh token was revoked, expired or already spent: authorize the app again and put its new refresh token in the secret)", e)
		}
		return "", "", 0, e
	}
	var tok struct {
		AccessToken  string  `json:"access_token"`
		RefreshToken string  `json:"refresh_token"`
		ExpiresIn    seconds `json:"expires_in"`
	}
	if err := json.Unmarshal(data, &tok); err != nil || tok.AccessToken == "" {
		return "", "", 0, fmt.Errorf("oauth2 refresh: no access_token in the response")
	}
	life = time.Duration(tok.ExpiresIn) * time.Second
	if life <= 0 {
		life = time.Hour
	}
	return tok.AccessToken, tok.RefreshToken, life, nil
}

// session logs in when there is no live session, and returns the Cookie
// header its cookies make.
func (a *authenticator) session(ctx context.Context) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.token != "" && time.Now().Before(a.expires) {
		return a.token, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.LoginURL, strings.NewReader(a.secret))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := a.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("session login: %w", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return "", &APIError{Method: http.MethodPost, Path: a.cfg.LoginURL, Status: resp.StatusCode, Body: strings.TrimSpace(string(data))}
	}
	keep := map[string]bool{}
	for _, n := range a.cfg.Cookies {
		keep[n] = true
	}
	var parts []string
	for _, c := range resp.Cookies() {
		if len(keep) == 0 || keep[c.Name] {
			parts = append(parts, c.Name+"="+c.Value)
		}
	}
	if len(parts) == 0 {
		return "", fmt.Errorf("session login: %s set no session cookie", a.cfg.LoginURL)
	}
	var body struct {
		SessionTimeout float64 `json:"SessionTimeout"`
	}
	_ = json.Unmarshal(data, &body)
	life := 30 * time.Minute
	if body.SessionTimeout > 0 {
		life = time.Duration(body.SessionTimeout * float64(time.Minute))
	}
	// Log in again early: the server ends an idle session at its timeout.
	a.token, a.expires = strings.Join(parts, "; "), time.Now().Add(life*3/4)
	return a.token, nil
}

// Authenticator authenticates requests as an Auth says, for other HTTP
// connectors (SAP OData) that share these methods.
type Authenticator struct{ a *authenticator }

// NewAuthenticator checks cfg and the secret it needs.
func NewAuthenticator(cfg Auth, secret string, client *http.Client) (*Authenticator, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	a, err := newAuthenticator(cfg, secret, client)
	if err != nil {
		return nil, err
	}
	return &Authenticator{a}, nil
}

// Apply sets the request's credentials, fetching a token when needed.
func (x *Authenticator) Apply(ctx context.Context, req *http.Request) error {
	return x.a.apply(ctx, req)
}

// Refreshable reports whether a rejected credential is worth fetching again.
func (x *Authenticator) Refreshable() bool { return x.a.refreshable() }

// Invalidate drops the token req carried, after the API refused it; a
// token refreshed since is kept.
func (x *Authenticator) Invalidate(req *http.Request) { x.a.invalidate(req) }

// UseTokens gives an oauth2-refresh authenticator the store for the
// tokens the server rotates, under key (the endpoint).
func (x *Authenticator) UseTokens(store connector.TokenStore, key string) { x.a.useTokens(store, key) }

// useTokens keys the stored token by the endpoint, the token URL, the
// client and the seed: another app, or another authorization, starts from
// its own seed, and going back to an earlier seed finds where it led.
func (a *authenticator) useTokens(store connector.TokenStore, key string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	legacy := sha256.Sum256([]byte(a.cfg.TokenURL + "\x00" + a.clientID))
	sum := sha256.Sum256([]byte(a.cfg.TokenURL + "\x00" + a.clientID + "\x00" + a.refreshToken))
	a.tokens = store
	a.legacyKey = key + "@" + hex.EncodeToString(legacy[:8])
	a.tokenKey = key + "@" + hex.EncodeToString(sum[:12])
}
