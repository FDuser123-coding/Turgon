package secrets

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// GCPConfig configures Google Secret Manager, read over its REST API.
// Credentials come from Application Default Credentials: on GKE, Workload
// Identity (the Kubernetes service account annotated with
// iam.gke.io/gcp-service-account); on Compute Engine, the VM's service
// account; elsewhere GOOGLE_APPLICATION_CREDENTIALS.
type GCPConfig struct {
	// Project is the project ID (default: the credentials' project).
	Project string
	// Prefix is put before every secret name, e.g. "turgon-".
	Prefix string
	// Endpoint overrides https://secretmanager.googleapis.com (a regional
	// or private endpoint, or a test server).
	Endpoint string
	TTL      time.Duration
	// TokenSource and HTTPClient, if set, replace Application Default
	// Credentials and the HTTP client (tests).
	TokenSource oauth2.TokenSource
	HTTPClient  *http.Client
}

type gcpFetcher struct {
	project, endpoint string
	http              *http.Client
}

// NewGCP returns a resolver for Google Secret Manager.
func NewGCP(ctx context.Context, c GCPConfig) (*Cloud, error) {
	ts := c.TokenSource
	if ts == nil {
		creds, err := google.FindDefaultCredentials(ctx, "https://www.googleapis.com/auth/cloud-platform")
		if err != nil {
			return nil, fmt.Errorf("gcp: %w (on GKE, use Workload Identity; elsewhere set GOOGLE_APPLICATION_CREDENTIALS)", err)
		}
		ts = creds.TokenSource
		if c.Project == "" {
			c.Project = creds.ProjectID
		}
	}
	if c.Project == "" {
		return nil, errors.New("gcp: no project: set TURGON_GCP_PROJECT")
	}
	if c.Endpoint == "" {
		c.Endpoint = "https://secretmanager.googleapis.com"
	}
	u, err := url.Parse(c.Endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "https" && !loopback(u.Hostname())) {
		return nil, errors.New("gcp: the endpoint must be an https URL")
	}
	base := c.HTTPClient
	if base == nil {
		base = &http.Client{Timeout: 15 * time.Second}
	}
	client := &http.Client{Timeout: base.Timeout, Transport: &oauth2.Transport{Source: oauth2.ReuseTokenSource(nil, ts), Base: base.Transport}}
	return newCloud(&gcpFetcher{project: c.Project, endpoint: strings.TrimRight(c.Endpoint, "/"), http: client}, c.Prefix, c.TTL), nil
}

func (*gcpFetcher) kind() string { return "Google Secret Manager" }

var (
	gcpNameRE   = regexp.MustCompile(`^[A-Za-z0-9_-]{1,255}$`)
	gcpReplacer = strings.NewReplacer("/", "--", ".", "-")
)

func (*gcpFetcher) secretName(path string) (string, error) {
	return mapName("Google Secret Manager", path, gcpReplacer, gcpNameRE)
}

func (g *gcpFetcher) fetch(ctx context.Context, name string) (string, error) {
	target := fmt.Sprintf("%s/v1/projects/%s/secrets/%s/versions/latest:access", g.endpoint, url.PathEscape(g.project), url.PathEscape(name))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return "", err
	}
	resp, err := g.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("google secret manager %s: %w", name, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error struct {
				Status  string `json:"status"`
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(body, &e)
		return "", fmt.Errorf("google secret manager %s: %d %s: %s", name, resp.StatusCode, e.Error.Status, e.Error.Message)
	}
	var out struct {
		Payload struct {
			Data string `json:"data"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("google secret manager %s: %w", name, err)
	}
	data, err := base64.StdEncoding.DecodeString(out.Payload.Data)
	if err != nil {
		return "", fmt.Errorf("google secret manager %s: payload: %w", name, err)
	}
	return string(data), nil
}

func (g *gcpFetcher) fix(err error, name string) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "NOT_FOUND"):
		return fmt.Sprintf("Create it: echo -n '{\"<field>\": \"...\"}' | gcloud secrets create %s --project %s --data-file=- (a disabled or destroyed latest version reads as missing too).", name, g.project)
	case strings.Contains(msg, "PERMISSION_DENIED"):
		return fmt.Sprintf("Grant roles/secretmanager.secretAccessor on the secret %s to the Google service account Turgon runs as.", name)
	case strings.Contains(msg, "UNAUTHENTICATED") || strings.Contains(msg, "oauth2"):
		return "Google rejected Turgon's credentials. On GKE, annotate its service account with iam.gke.io/gcp-service-account and allow it to impersonate that account."
	}
	return ""
}
