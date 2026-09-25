package console

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// OIDCAuth signs people in with the customer's OpenID Connect provider
// (Microsoft Entra ID, Okta, Keycloak, Google...) directly, without a proxy
// in front of the console. It uses the authorization code flow with PKCE,
// checks the ID token's signature, issuer, audience, expiry and nonce, and
// keeps the user in a signed, HttpOnly session cookie. Roles come from the
// groups claim at sign-in and last as long as the session.
type OIDCAuth struct {
	cfg      OIDCConfig
	provider *oidc.Provider
	verifier *oidc.IDTokenVerifier
	oauth    oauth2.Config
	secure   bool
	now      func() time.Time
}

// OIDCConfig configures sign-in.
type OIDCConfig struct {
	Issuer       string
	ClientID     string
	ClientSecret string
	// URL is the console's public URL; the provider redirects back to
	// URL/auth/callback, which must be registered with it.
	URL string
	// Scopes beyond openid, email and profile, e.g. "groups" for Okta.
	Scopes []string
	// GroupsClaim holds the user's groups (default "groups"); Entra ID can
	// put group object IDs or names there.
	GroupsClaim string
	// Group names that grant roles, as for ProxyAuth.
	ApproverGroup, StewardGroup, OperatorGroup, ViewerGroup string
	// SessionKey signs session cookies: at least 32 random bytes, the same
	// on every console replica.
	SessionKey []byte
	// SessionTTL is how long a sign-in lasts (default 8 hours).
	SessionTTL time.Duration
	// Client makes the requests to the provider (default http.DefaultClient).
	Client *http.Client
}

const (
	sessionCookie = "turgon_session"
	loginCookie   = "turgon_login"
	loginTTL      = 10 * time.Minute
)

// NewOIDCAuth discovers the provider's endpoints and keys.
func NewOIDCAuth(ctx context.Context, cfg OIDCConfig) (*OIDCAuth, error) {
	if cfg.Issuer == "" || cfg.ClientID == "" || cfg.URL == "" {
		return nil, errors.New("oidc: issuer, client ID and the console URL are required")
	}
	if len(cfg.SessionKey) < 32 {
		return nil, errors.New("oidc: the session key must be at least 32 bytes")
	}
	base, err := url.Parse(strings.TrimRight(cfg.URL, "/"))
	if err != nil || base.Host == "" || (base.Scheme != "https" && !(base.Scheme == "http" && isLocal(base.Hostname()))) {
		return nil, fmt.Errorf("oidc: console URL %q must be https (http only for localhost)", cfg.URL)
	}
	if cfg.GroupsClaim == "" {
		cfg.GroupsClaim = "groups"
	}
	if cfg.SessionTTL <= 0 {
		cfg.SessionTTL = 8 * time.Hour
	}
	if cfg.Client != nil {
		ctx = oidc.ClientContext(ctx, cfg.Client)
	}
	provider, err := oidc.NewProvider(ctx, cfg.Issuer)
	if err != nil {
		return nil, fmt.Errorf("oidc: discover %s: %w", cfg.Issuer, err)
	}
	return &OIDCAuth{
		cfg:      cfg,
		provider: provider,
		verifier: provider.Verifier(&oidc.Config{ClientID: cfg.ClientID}),
		oauth: oauth2.Config{
			ClientID: cfg.ClientID, ClientSecret: cfg.ClientSecret, Endpoint: provider.Endpoint(),
			RedirectURL: base.String() + "/auth/callback",
			Scopes:      append([]string{oidc.ScopeOpenID, "email", "profile"}, cfg.Scopes...),
		},
		secure: base.Scheme == "https",
		now:    time.Now,
	}, nil
}

func isLocal(host string) bool {
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

// session is what the session cookie carries.
type session struct {
	User    User  `json:"u"`
	Expires int64 `json:"e"`
}

// login is what the short-lived login cookie carries between the redirect
// to the provider and the callback.
type login struct {
	State    string `json:"s"`
	Nonce    string `json:"n"`
	Verifier string `json:"v"`
	Next     string `json:"x"`
	Expires  int64  `json:"e"`
}

// seal signs a value for a cookie: base64url(JSON).base64url(HMAC).
func (o *OIDCAuth) seal(purpose string, v any) string {
	b, _ := json.Marshal(v)
	p := base64.RawURLEncoding.EncodeToString(b)
	return p + "." + base64.RawURLEncoding.EncodeToString(o.mac(purpose, p))
}

func (o *OIDCAuth) open(purpose, s string, v any) error {
	p, sig, ok := strings.Cut(s, ".")
	got, err := base64.RawURLEncoding.DecodeString(sig)
	if !ok || err != nil || !hmac.Equal(got, o.mac(purpose, p)) {
		return errors.New("bad signature")
	}
	b, err := base64.RawURLEncoding.DecodeString(p)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// mac binds a cookie to its purpose, so a login cookie is never a session.
func (o *OIDCAuth) mac(purpose, payload string) []byte {
	m := hmac.New(sha256.New, o.cfg.SessionKey)
	m.Write([]byte(purpose + "\x00" + payload))
	return m.Sum(nil)
}

var base64RawURL = base64.RawURLEncoding

func random() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

func (o *OIDCAuth) setCookie(w http.ResponseWriter, name, value string, ttl time.Duration) {
	http.SetCookie(w, &http.Cookie{
		Name: name, Value: value, Path: "/", MaxAge: int(ttl.Seconds()),
		HttpOnly: true, Secure: o.secure, SameSite: http.SameSiteLaxMode,
	})
}

// Authenticate implements Authenticator from the session cookie.
func (o *OIDCAuth) Authenticate(r *http.Request) (User, error) {
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return User{}, ErrUnauthenticated
	}
	var s session
	if o.open(sessionCookie, c.Value, &s) != nil || o.now().Unix() >= s.Expires || s.User.ID == "" {
		return User{}, ErrUnauthenticated
	}
	return s.User, nil
}

// LoginURL is where a browser without a session is sent.
func (o *OIDCAuth) LoginURL(next string) string {
	return "/auth/login?next=" + url.QueryEscape(next)
}

// LogoutURL ends the session.
func (o *OIDCAuth) LogoutURL() string { return "/auth/logout" }

// ServeAuth serves /auth/login, /auth/callback and /auth/logout.
func (o *OIDCAuth) ServeAuth(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/auth/login":
		o.login(w, r)
	case "/auth/callback":
		o.callback(w, r)
	case "/auth/logout":
		o.setCookie(w, sessionCookie, "", -time.Second)
		http.Redirect(w, r, "/auth/signed-out", http.StatusFound)
	case "/auth/signed-out":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, `<!doctype html><title>Signed out</title><p>You are signed out of Turgon. <a href="/">Sign in again</a></p>`)
	default:
		http.NotFound(w, r)
	}
}

// safeNext keeps redirects on this site: a path, never //host or a URL.
func safeNext(next string) string {
	if !strings.HasPrefix(next, "/") || strings.HasPrefix(next, "//") || strings.HasPrefix(next, "/\\") || strings.HasPrefix(next, "/auth/") {
		return "/"
	}
	return next
}

func (o *OIDCAuth) login(w http.ResponseWriter, r *http.Request) {
	l := login{State: random(), Nonce: random(), Verifier: oauth2.GenerateVerifier(),
		Next: safeNext(r.URL.Query().Get("next")), Expires: o.now().Add(loginTTL).Unix()}
	o.setCookie(w, loginCookie, o.seal(loginCookie, l), loginTTL)
	http.Redirect(w, r, o.oauth.AuthCodeURL(l.State, oidc.Nonce(l.Nonce), oauth2.S256ChallengeOption(l.Verifier)), http.StatusFound)
}

func (o *OIDCAuth) fail(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<!doctype html><title>Sign-in failed</title><p>Sign-in failed: %s. <a href="/">Try again</a></p>`, msg)
}

func (o *OIDCAuth) callback(w http.ResponseWriter, r *http.Request) {
	c, err := r.Cookie(loginCookie)
	var l login
	if err != nil || o.open(loginCookie, c.Value, &l) != nil || o.now().Unix() >= l.Expires {
		o.fail(w, http.StatusBadRequest, "the sign-in took too long or was not started here")
		return
	}
	o.setCookie(w, loginCookie, "", -time.Second) // one use
	q := r.URL.Query()
	if e := q.Get("error"); e != "" {
		o.fail(w, http.StatusForbidden, "the identity provider refused ("+htmlText(e)+")")
		return
	}
	if !hmac.Equal([]byte(q.Get("state")), []byte(l.State)) {
		o.fail(w, http.StatusBadRequest, "the sign-in response does not match the request")
		return
	}
	ctx := r.Context()
	if o.cfg.Client != nil {
		ctx = oidc.ClientContext(ctx, o.cfg.Client)
	}
	tok, err := o.oauth.Exchange(ctx, q.Get("code"), oauth2.VerifierOption(l.Verifier))
	if err != nil {
		o.fail(w, http.StatusBadGateway, "the identity provider did not accept the code")
		return
	}
	raw, _ := tok.Extra("id_token").(string)
	idt, err := o.verifier.Verify(ctx, raw)
	if err != nil || !hmac.Equal([]byte(idt.Nonce), []byte(l.Nonce)) {
		o.fail(w, http.StatusBadRequest, "the ID token is not valid for this console")
		return
	}
	var claims map[string]any
	if err := idt.Claims(&claims); err != nil {
		o.fail(w, http.StatusBadRequest, "the ID token has unreadable claims")
		return
	}
	u := o.user(claims, idt.Subject)
	if !u.Has(RoleViewer) {
		o.fail(w, http.StatusForbidden, "your account may not use this console")
		return
	}
	exp := o.now().Add(o.cfg.SessionTTL)
	o.setCookie(w, sessionCookie, o.seal(sessionCookie, session{User: u, Expires: exp.Unix()}), o.cfg.SessionTTL)
	http.Redirect(w, r, l.Next, http.StatusFound)
}

// user maps claims to a console user: the email (or the preferred user
// name, or the subject) and roles from the groups claim.
func (o *OIDCAuth) user(claims map[string]any, subject string) User {
	id := subject
	for _, k := range []string{"email", "preferred_username"} {
		if s, _ := claims[k].(string); s != "" {
			id = s
			break
		}
	}
	groups := map[string]bool{}
	switch g := claims[o.cfg.GroupsClaim].(type) {
	case []any:
		for _, x := range g {
			if s, ok := x.(string); ok {
				groups[s] = true
			}
		}
	case string: // some providers send one group, or a space-separated list
		for _, s := range strings.Fields(g) {
			groups[s] = true
		}
	}
	u := User{ID: id}
	if o.cfg.ViewerGroup == "" || groups[o.cfg.ViewerGroup] {
		u.Roles = append(u.Roles, RoleViewer)
	}
	for role, group := range map[string]string{RoleApprover: o.cfg.ApproverGroup, RoleSteward: o.cfg.StewardGroup, RoleOperator: o.cfg.OperatorGroup} {
		if group != "" && groups[group] {
			u.Roles = append(u.Roles, role)
		}
	}
	sortRoles(u.Roles)
	return u
}

func sortRoles(r []string) {
	order := map[string]int{RoleViewer: 0, RoleApprover: 1, RoleSteward: 2, RoleOperator: 3}
	for i := 1; i < len(r); i++ {
		for j := i; j > 0 && order[r[j]] < order[r[j-1]]; j-- {
			r[j], r[j-1] = r[j-1], r[j]
		}
	}
}

func htmlText(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", "'", "&#39;").Replace(s)
}
