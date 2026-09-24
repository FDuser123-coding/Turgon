package rest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
//	none    no credential
type Auth struct {
	Type     string   `json:"type"`
	Header   string   `json:"header,omitempty"`
	TokenURL string   `json:"tokenURL,omitempty"`
	Scopes   []string `json:"scopes,omitempty"`
}

func (a Auth) validate() error {
	switch a.Type {
	case "bearer", "basic", "none":
	case "header":
		if a.Header == "" || strings.EqualFold(a.Header, "Authorization") {
			return fmt.Errorf("auth: header needs a header name other than Authorization (use bearer)")
		}
	case "oauth2":
		u, err := url.Parse(a.TokenURL)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
			return fmt.Errorf("auth: oauth2 needs an http(s) tokenURL")
		}
	default:
		return fmt.Errorf("auth: type must be bearer, header, basic, oauth2 or none, got %q", a.Type)
	}
	return nil
}

type authenticator struct {
	cfg    Auth
	secret string
	http   *http.Client

	clientID, clientSecret string

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
	}
	return nil
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(url.QueryEscape(a.clientID), url.QueryEscape(a.clientSecret))
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
