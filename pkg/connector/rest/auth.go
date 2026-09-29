package rest

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
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
)

// Auth says how requests are authenticated. The credential itself is the
// connection's secret:
//
//	bearer  a token, sent as "Authorization: Bearer <token>"
//	header  a token, sent in Header (e.g. X-Shopify-Access-Token)
//	basic   "user:password"
//	oauth2  {"clientId": "...", "clientSecret": "..."}; the client
//	        credentials grant against TokenURL, refreshed before expiry
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
	case "oauth2":
		u, err := url.Parse(a.TokenURL)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
			return fmt.Errorf("auth: oauth2 needs an http(s) tokenURL")
		}
		if a.ClientAuth != "" && a.ClientAuth != "basic" && a.ClientAuth != "body" {
			return fmt.Errorf("auth: clientAuth must be basic or body, got %q", a.ClientAuth)
		}
	default:
		return fmt.Errorf("auth: type must be bearer, header, basic, oauth2, oauth1 or none, got %q", a.Type)
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
}

func newAuthenticator(cfg Auth, secret string, client *http.Client) (*authenticator, error) {
	a := &authenticator{cfg: cfg, secret: strings.TrimSpace(secret), http: client}
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
	case "oauth1":
		c := &a.oauth1
		if err := json.Unmarshal([]byte(a.secret), c); err != nil || c.ConsumerKey == "" || c.ConsumerSecret == "" || c.TokenID == "" || c.TokenSecret == "" {
			return nil, fmt.Errorf(`auth: the oauth1 secret must be {"consumerKey": "...", "consumerSecret": "...", "tokenId": "...", "tokenSecret": "..."}`)
		}
	}
	return a, nil
}

func (a *authenticator) refreshable() bool { return a.cfg.Type == "oauth2" }

func (a *authenticator) invalidate() {
	a.mu.Lock()
	a.token = ""
	a.mu.Unlock()
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
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
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

// Invalidate drops a cached token.
func (x *Authenticator) Invalidate() { x.a.invalidate() }
