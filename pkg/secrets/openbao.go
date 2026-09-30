// Package secrets resolves secret references against a secret manager.
//
// OpenBao resolves references against OpenBao or HashiCorp Vault (the same
// API) from a KV version 2 secrets engine: "openbao://shop-db/dsn" (or
// "vault://shop-db/dsn") is the key "dsn" of the secret at path "shop-db",
// read from <mount>/data/shop-db. A secret's value may be a string or a JSON
// object (Salesforce credentials), which is returned as JSON.
//
// Turgon signs in with a token, as a Kubernetes service account (the
// kubernetes auth method, with a projected, audience-bound token), or with
// an AppRole. Values are cached for a few minutes and never logged.
package secrets

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
)

// Config says where OpenBao is and how Turgon signs in.
type Config struct {
	// Address is the server, e.g. https://openbao.internal:8200.
	Address string
	// Mount is the KV v2 secrets engine's path (default "secret").
	Mount string
	// Namespace is the OpenBao or Vault Enterprise namespace, if any.
	Namespace string
	// CACert is a PEM file of the CAs that sign the server's certificate
	// (default: the system's).
	CACert string
	// Auth is token, kubernetes or approle.
	Auth string
	// Token is the token for token auth.
	Token string
	// Role is the kubernetes auth role, or the AppRole's role ID.
	Role string
	// AuthMount is the auth method's path (default "kubernetes" or
	// "approle").
	AuthMount string
	// JWTFile is the service account token for kubernetes auth (default
	// the projected token the Helm chart mounts, else the pod's own).
	JWTFile string
	// SecretIDFile holds the AppRole's secret ID.
	SecretIDFile string
	// TTL is how long read values are reused (default 5m). A rotated
	// secret reaches a connector when the connector is next built: at the
	// worker's next start.
	TTL time.Duration
}

// DefaultJWTFile is where the Helm chart mounts the projected token.
const DefaultJWTFile = "/var/run/secrets/turgon/openbao/token"

// ConfigFromEnv reads TURGON_OPENBAO_* variables, falling back to
// OpenBao's (BAO_*) and Vault's (VAULT_*) own.
func ConfigFromEnv() Config {
	get := func(names ...string) string {
		for _, n := range names {
			if v := os.Getenv(n); v != "" {
				return v
			}
		}
		return ""
	}
	c := Config{
		Address:      get("TURGON_OPENBAO_ADDR", "BAO_ADDR", "VAULT_ADDR"),
		Mount:        get("TURGON_OPENBAO_MOUNT"),
		Namespace:    get("TURGON_OPENBAO_NAMESPACE", "BAO_NAMESPACE", "VAULT_NAMESPACE"),
		CACert:       get("TURGON_OPENBAO_CACERT", "BAO_CACERT", "VAULT_CACERT"),
		Auth:         get("TURGON_OPENBAO_AUTH"),
		Token:        get("TURGON_OPENBAO_TOKEN", "BAO_TOKEN", "VAULT_TOKEN"),
		Role:         get("TURGON_OPENBAO_ROLE"),
		AuthMount:    get("TURGON_OPENBAO_AUTH_MOUNT"),
		JWTFile:      get("TURGON_OPENBAO_JWT_FILE"),
		SecretIDFile: get("TURGON_OPENBAO_SECRET_ID_FILE"),
	}
	if d, err := time.ParseDuration(get("TURGON_OPENBAO_TTL")); err == nil {
		c.TTL = d
	}
	return c
}

// OpenBao is a connector.SecretResolver backed by OpenBao or Vault.
type OpenBao struct {
	cfg  Config
	http *http.Client
	now  func() time.Time

	mu      sync.Mutex
	token   string
	renewAt time.Time // when a login token must be replaced
	cache   map[string]cached
}

type cached struct {
	data    map[string]any
	fetched time.Time
}

var _ connector.SecretResolver = (*OpenBao)(nil)

var pathRE = regexp.MustCompile(`^[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)*$`)

// validPath accepts slash-separated names, never "." or "..": a reference
// must not climb out of the mount (openbao://../sys/...).
func validPath(p string) bool {
	if !pathRE.MatchString(p) {
		return false
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

// NewOpenBao checks the configuration; it does not contact the server.
func NewOpenBao(cfg Config) (*OpenBao, error) {
	u, err := url.Parse(cfg.Address)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, errors.New("openbao: the address must be an http(s) URL (TURGON_OPENBAO_ADDR)")
	}
	if u.Scheme == "http" && !loopback(u.Hostname()) {
		return nil, errors.New("openbao: the address must use https: tokens and secrets cross the network")
	}
	cfg.Address = strings.TrimRight(cfg.Address, "/")
	if cfg.Mount == "" {
		cfg.Mount = "secret"
	}
	if !validPath(cfg.Mount) {
		return nil, fmt.Errorf("openbao: invalid mount %q", cfg.Mount)
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 5 * time.Minute
	}
	switch cfg.Auth {
	case "", "token":
		cfg.Auth = "token"
		if cfg.Token == "" {
			return nil, errors.New("openbao: token auth needs a token (TURGON_OPENBAO_TOKEN)")
		}
	case "kubernetes":
		if cfg.Role == "" {
			return nil, errors.New("openbao: kubernetes auth needs a role (TURGON_OPENBAO_ROLE)")
		}
		if cfg.AuthMount == "" {
			cfg.AuthMount = "kubernetes"
		}
		if cfg.JWTFile == "" {
			cfg.JWTFile = DefaultJWTFile
			if _, err := os.Stat(cfg.JWTFile); err != nil {
				cfg.JWTFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
			}
		}
	case "approle":
		if cfg.Role == "" || cfg.SecretIDFile == "" {
			return nil, errors.New("openbao: approle auth needs the role ID (TURGON_OPENBAO_ROLE) and a secret ID file (TURGON_OPENBAO_SECRET_ID_FILE)")
		}
		if cfg.AuthMount == "" {
			cfg.AuthMount = "approle"
		}
	default:
		return nil, fmt.Errorf("openbao: auth must be token, kubernetes or approle, got %q", cfg.Auth)
	}
	if cfg.AuthMount != "" && !validPath(cfg.AuthMount) {
		return nil, fmt.Errorf("openbao: invalid auth mount %q", cfg.AuthMount)
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.CACert != "" {
		pem, err := os.ReadFile(cfg.CACert)
		if err != nil {
			return nil, fmt.Errorf("openbao: CA certificate: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("openbao: %s holds no PEM certificate", cfg.CACert)
		}
		tlsCfg.RootCAs = pool
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsCfg
	return &OpenBao{cfg: cfg, http: &http.Client{Timeout: 15 * time.Second, Transport: transport}, now: time.Now, cache: map[string]cached{}}, nil
}

func loopback(host string) bool {
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

// Where returns where a reference is read from, for `turgon secrets`.
func (o *OpenBao) Where(ref string) string {
	path, key, err := parseRef(ref)
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("%s/data/%s key %s", o.cfg.Mount, path, key)
}

// parseRef splits "openbao://a/b/key" into the path "a/b" and the key.
func parseRef(ref string) (path, key string, err error) {
	scheme, rest, ok := strings.Cut(ref, "://")
	if !ok || (scheme != "openbao" && scheme != "vault") {
		return "", "", fmt.Errorf("secret %s: OpenBao reads openbao:// or vault:// references", ref)
	}
	i := strings.LastIndexByte(rest, '/')
	if i <= 0 || i == len(rest)-1 || !validPath(rest) {
		return "", "", fmt.Errorf("secret %s: a reference is openbao://<path>/<key>", ref)
	}
	return rest[:i], rest[i+1:], nil
}

// Resolve returns a reference's value.
func (o *OpenBao) Resolve(ctx context.Context, ref string) (string, error) {
	path, key, err := parseRef(ref)
	if err != nil {
		return "", err
	}
	data, err := o.read(ctx, path)
	if err != nil {
		return "", fmt.Errorf("secret %s: %w", ref, err)
	}
	v, ok := data[key]
	if !ok {
		keys := make([]string, 0, len(data))
		for k := range data {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		return "", fmt.Errorf("secret %s: %s/data/%s has no key %q (it has %s)", ref, o.cfg.Mount, path, key, strings.Join(keys, ", "))
	}
	switch v := v.(type) {
	case string:
		return v, nil
	case nil:
		return "", fmt.Errorf("secret %s: the value is null", ref)
	default:
		b, err := json.Marshal(v)
		return string(b), err
	}
}

// read returns a secret's data, from the cache while it is fresh.
func (o *OpenBao) read(ctx context.Context, path string) (map[string]any, error) {
	o.mu.Lock()
	c, ok := o.cache[path]
	o.mu.Unlock()
	if ok && o.now().Sub(c.fetched) < o.cfg.TTL {
		return c.data, nil
	}
	var body struct {
		Data struct {
			Data     map[string]any `json:"data"`
			Metadata struct {
				DeletionTime string `json:"deletion_time"`
				Destroyed    bool   `json:"destroyed"`
			} `json:"metadata"`
		} `json:"data"`
	}
	target := "/v1/" + o.cfg.Mount + "/data/" + path
	if err := o.call(ctx, http.MethodGet, target, nil, &body, true); err != nil {
		return nil, err
	}
	if body.Data.Data == nil {
		return nil, fmt.Errorf("%s/data/%s: the latest version is deleted or destroyed", o.cfg.Mount, path)
	}
	o.mu.Lock()
	o.cache[path] = cached{data: body.Data.Data, fetched: o.now()}
	o.mu.Unlock()
	return body.Data.Data, nil
}

// Check signs in and reads each reference's secret, for `turgon check`. It
// reports which secrets it could read, never their values.
func (o *OpenBao) Check(ctx context.Context, refs []string) []connector.CheckResult {
	var out []connector.CheckResult
	if _, err := o.currentToken(ctx); err != nil {
		return append(out, connector.Fail("openbao", err.Error(), o.loginFix(err)))
	}
	out = append(out, connector.Pass("openbao", fmt.Sprintf("%s: signed in (%s auth)", o.cfg.Address, o.cfg.Auth)))
	for _, ref := range refs {
		if _, err := o.Resolve(ctx, ref); err != nil {
			out = append(out, connector.Fail("secret "+ref, err.Error(), o.readFix(err, ref)))
		} else {
			out = append(out, connector.Pass("secret "+ref, o.Where(ref)))
		}
	}
	return out
}

// APIError is an error response from OpenBao.
type APIError struct {
	Status int
	Errors []string
	// Deleted is when a KV secret's latest version was deleted (a 404
	// that is not a missing secret).
	Deleted string
}

func (e *APIError) Error() string {
	if e.Deleted != "" {
		return fmt.Sprintf("openbao %d: the latest version was deleted at %s", e.Status, e.Deleted)
	}
	var msgs []string
	for _, m := range e.Errors {
		// "1 error occurred:\n\t* permission denied\n\n" -> "permission denied"
		for _, line := range strings.Split(m, "\n") {
			line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "*"))
			if line != "" && !strings.HasSuffix(line, "occurred:") {
				msgs = append(msgs, line)
			}
		}
	}
	msg := strings.Join(msgs, "; ")
	if msg == "" {
		msg = http.StatusText(e.Status)
	}
	return fmt.Sprintf("openbao %d: %s", e.Status, msg)
}

func (o *OpenBao) loginFix(err error) string {
	var api *APIError
	if !errors.As(err, &api) {
		if strings.Contains(err.Error(), "certificate") {
			return "Set TURGON_OPENBAO_CACERT to the CA that signed the server's certificate."
		}
		return connector.NetworkFix(err, o.cfg.Address)
	}
	switch o.cfg.Auth {
	case "kubernetes":
		return fmt.Sprintf("OpenBao rejected the service account. The role %s (auth/%s/role/%s) must bind this pod's service account and namespace, and its audience must match the token's.", o.cfg.Role, o.cfg.AuthMount, o.cfg.Role)
	case "approle":
		return "OpenBao rejected the AppRole: check the role ID and that the secret ID has not expired or run out of uses."
	}
	return "OpenBao rejected the token: it may have expired or been revoked."
}

func (o *OpenBao) readFix(err error, ref string) string {
	path, _, _ := parseRef(ref)
	var api *APIError
	if errors.As(err, &api) {
		switch api.Status {
		case http.StatusForbidden:
			return fmt.Sprintf("The policy of the token Turgon signs in with must allow read on %s/data/%s.", o.cfg.Mount, path)
		case http.StatusNotFound:
			if api.Deleted != "" {
				return fmt.Sprintf("Restore it (bao kv undelete -mount=%s -versions=<n> %s) or write a new version.", o.cfg.Mount, path)
			}
			return fmt.Sprintf("Write the secret: bao kv put -mount=%s %s <key>=<value>.", o.cfg.Mount, path)
		}
	}
	return ""
}

// call sends a request with the current token; a rejected login token is
// replaced once.
func (o *OpenBao) call(ctx context.Context, method, path string, in, out any, authed bool) error {
	for retried := false; ; retried = true {
		var token string
		if authed {
			t, err := o.currentToken(ctx)
			if err != nil {
				return err
			}
			token = t
		}
		err := o.send(ctx, method, path, token, in, out)
		var api *APIError
		if authed && !retried && o.cfg.Auth != "token" && errors.As(err, &api) && api.Status == http.StatusForbidden {
			o.dropToken(token) // it may have expired early (revoked, lease cut)
			continue
		}
		return err
	}
}

func (o *OpenBao) send(ctx context.Context, method, path, token string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, o.cfg.Address+path, body)
	if err != nil {
		return err
	}
	if token != "" {
		req.Header.Set("X-Vault-Token", token)
	}
	if o.cfg.Namespace != "" {
		req.Header.Set("X-Vault-Namespace", o.cfg.Namespace)
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := o.http.Do(req)
	if err != nil {
		return fmt.Errorf("openbao: %w", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 300 {
		api := &APIError{Status: resp.StatusCode}
		var e struct {
			Errors []string `json:"errors"`
			Data   struct {
				Metadata struct {
					DeletionTime string `json:"deletion_time"`
					Destroyed    bool   `json:"destroyed"`
				} `json:"metadata"`
			} `json:"data"`
		}
		if json.Unmarshal(data, &e) == nil {
			api.Errors = e.Errors
			if md := e.Data.Metadata; md.DeletionTime != "" || md.Destroyed {
				api.Deleted = md.DeletionTime
				if md.Destroyed {
					api.Deleted += " (destroyed)"
				}
			}
		}
		return api
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

// currentToken returns a valid token, signing in when needed.
func (o *OpenBao) currentToken(ctx context.Context) (string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.cfg.Auth == "token" {
		return o.cfg.Token, nil
	}
	if o.token != "" && o.now().Before(o.renewAt) {
		return o.token, nil
	}
	var in map[string]string
	switch o.cfg.Auth {
	case "kubernetes":
		jwt, err := os.ReadFile(o.cfg.JWTFile)
		if err != nil {
			return "", fmt.Errorf("openbao: service account token: %w", err)
		}
		in = map[string]string{"role": o.cfg.Role, "jwt": strings.TrimSpace(string(jwt))}
	case "approle":
		sid, err := os.ReadFile(o.cfg.SecretIDFile)
		if err != nil {
			return "", fmt.Errorf("openbao: AppRole secret ID: %w", err)
		}
		in = map[string]string{"role_id": o.cfg.Role, "secret_id": strings.TrimSpace(string(sid))}
	}
	var out struct {
		Auth struct {
			ClientToken   string `json:"client_token"`
			LeaseDuration int    `json:"lease_duration"`
		} `json:"auth"`
	}
	if err := o.send(ctx, http.MethodPost, "/v1/auth/"+o.cfg.AuthMount+"/login", "", in, &out); err != nil {
		return "", fmt.Errorf("openbao: %s login: %w", o.cfg.Auth, err)
	}
	if out.Auth.ClientToken == "" {
		return "", fmt.Errorf("openbao: %s login returned no token", o.cfg.Auth)
	}
	life := time.Duration(out.Auth.LeaseDuration) * time.Second
	if life <= 0 {
		life = time.Hour
	}
	// Sign in again at two thirds of the token's life, before it expires.
	o.token, o.renewAt = out.Auth.ClientToken, o.now().Add(life*2/3)
	return o.token, nil
}

func (o *OpenBao) dropToken(token string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.token == token {
		o.token = ""
	}
}
