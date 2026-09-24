package salesforce

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Credentials is the secret a connection's secretRef resolves to, as JSON.
// With PrivateKey and Username it uses the OAuth 2.0 JWT bearer flow for a
// dedicated integration user (recommended); with ClientSecret it uses the
// client credentials flow against the org's My Domain URL.
type Credentials struct {
	LoginURL     string `json:"loginUrl"`
	ClientID     string `json:"clientId"`
	Username     string `json:"username,omitempty"`
	PrivateKey   string `json:"privateKey,omitempty"`
	ClientSecret string `json:"clientSecret,omitempty"`
	// Audience overrides the JWT audience; defaults to LoginURL, which is
	// correct for login.salesforce.com and test.salesforce.com.
	Audience string `json:"audience,omitempty"`
}

func parseCredentials(secret string) (Credentials, *rsa.PrivateKey, error) {
	var c Credentials
	if err := json.Unmarshal([]byte(secret), &c); err != nil {
		return c, nil, errors.New("salesforce credentials must be a JSON object") // never echo the secret
	}
	if c.LoginURL == "" || c.ClientID == "" {
		return c, nil, errors.New("salesforce credentials need loginUrl and clientId")
	}
	c.LoginURL = strings.TrimRight(c.LoginURL, "/")
	switch {
	case c.PrivateKey != "" && c.Username != "":
		key, err := parseKey(c.PrivateKey)
		return c, key, err
	case c.ClientSecret != "":
		return c, nil, nil
	default:
		return c, nil, errors.New("salesforce credentials need username and privateKey (JWT bearer) or clientSecret (client credentials)")
	}
}

func parseKey(pemText string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(pemText))
	if block == nil {
		return nil, errors.New("salesforce privateKey is not PEM")
	}
	if k, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return k, nil
	}
	k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, errors.New("salesforce privateKey is not an RSA key")
	}
	rk, ok := k.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("salesforce privateKey is not an RSA key")
	}
	return rk, nil
}

// session holds the current access token and refreshes it on demand.
type session struct {
	creds Credentials
	key   *rsa.PrivateKey
	http  *http.Client
	now   func() time.Time

	mu       sync.Mutex
	token    string
	instance string
}

func (s *session) current(ctx context.Context) (token, instance string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token == "" {
		if err := s.login(ctx); err != nil {
			return "", "", err
		}
	}
	return s.token, s.instance, nil
}

// invalidate drops a token the API rejected, if it is still the current one.
func (s *session) invalidate(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token == token {
		s.token = ""
	}
}

func (s *session) login(ctx context.Context) error {
	form := url.Values{}
	if s.key != nil {
		assertion, err := s.assertion()
		if err != nil {
			return err
		}
		form.Set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
		form.Set("assertion", assertion)
	} else {
		form.Set("grant_type", "client_credentials")
		form.Set("client_id", s.creds.ClientID)
		form.Set("client_secret", s.creds.ClientSecret)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.creds.LoginURL+"/services/oauth2/token", strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := s.http.Do(req)
	if err != nil {
		return fmt.Errorf("salesforce login: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error       string `json:"error"`
			Description string `json:"error_description"`
		}
		_ = json.Unmarshal(body, &e)
		return fmt.Errorf("salesforce login: %s: %s %s", resp.Status, e.Error, e.Description)
	}
	var tok struct {
		AccessToken string `json:"access_token"`
		InstanceURL string `json:"instance_url"`
	}
	if err := json.Unmarshal(body, &tok); err != nil || tok.AccessToken == "" || tok.InstanceURL == "" {
		return errors.New("salesforce login: malformed token response")
	}
	s.token, s.instance = tok.AccessToken, strings.TrimRight(tok.InstanceURL, "/")
	return nil
}

// assertion builds the RS256-signed JWT for the bearer flow.
func (s *session) assertion() (string, error) {
	aud := s.creds.Audience
	if aud == "" {
		aud = s.creds.LoginURL
	}
	enc := base64.RawURLEncoding
	header := enc.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	claims, _ := json.Marshal(map[string]any{
		"iss": s.creds.ClientID, "sub": s.creds.Username, "aud": aud,
		"exp": s.now().Add(3 * time.Minute).Unix(),
	})
	signing := header + "." + enc.EncodeToString(claims)
	sum := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, s.key, crypto.SHA256, sum[:])
	if err != nil {
		return "", err
	}
	return signing + "." + enc.EncodeToString(sig), nil
}
