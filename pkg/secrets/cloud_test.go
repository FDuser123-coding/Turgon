package secrets

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"golang.org/x/oauth2"
)

// store is what each fake manager serves: secret name -> value.
type store struct {
	mu     sync.Mutex
	values map[string]string
	denied map[string]bool
	reads  int
}

func (s *store) get(name string) (string, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.denied[name] {
		return "", http.StatusForbidden
	}
	v, ok := s.values[name]
	if !ok {
		return "", http.StatusNotFound
	}
	s.reads++
	return v, http.StatusOK
}

func newStore() *store {
	return &store{values: map[string]string{
		"turgon/shop-db":         `{"dsn": "postgres://shop", "user": "shop"}`,
		"turgon/salesforce":      `{"prod-jwt": {"clientId": "3MVG", "username": "turgon@example.com"}}`,
		"turgon/plain":           `just a password`,
		"turgon-shop-db":         `{"dsn": "postgres://shop"}`,
		"turgon-team--erp--db-1": `{"dsn": "postgres://erp"}`,
	}, denied: map[string]bool{"turgon/forbidden": true, "turgon-forbidden": true}}
}

// awsServer speaks Secrets Manager's JSON 1.1 protocol and insists on a
// SigV4 signature for the region and service.
func awsServer(t *testing.T, s *store) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if r.Header.Get("X-Amz-Target") != "secretsmanager.GetSecretValue" ||
			!strings.HasPrefix(auth, "AWS4-HMAC-SHA256 Credential=AKIDTEST/") || !strings.Contains(auth, "/eu-central-1/secretsmanager/aws4_request") {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"__type":"InvalidSignatureException","message":"bad request"}`)
			return
		}
		var in struct{ SecretId string }
		_ = json.NewDecoder(r.Body).Decode(&in)
		v, status := s.get(in.SecretId)
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		switch status {
		case http.StatusOK:
			_ = json.NewEncoder(w).Encode(map[string]any{"Name": in.SecretId, "SecretString": v, "VersionStages": []string{"AWSCURRENT"}})
		case http.StatusForbidden:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"__type":"AccessDeniedException","Message":"User is not authorized to perform: secretsmanager:GetSecretValue"}`)
		default:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"__type":"ResourceNotFoundException","Message":"Secrets Manager can't find the specified secret."}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestAWS(t *testing.T) {
	s := newStore()
	srv := awsServer(t, s)
	c, err := NewAWS(context.Background(), AWSConfig{Region: "eu-central-1", Prefix: "turgon/", Endpoint: srv.URL,
		Credentials: credentials.NewStaticCredentialsProvider("AKIDTEST", "secret", "")})
	if err != nil {
		t.Fatal(err)
	}
	commonChecks(t, c, s, "AWS Secrets Manager secret turgon/shop-db, field dsn", map[string]string{
		"openbao://forbidden/dsn": "AccessDeniedException",
		"openbao://missing/dsn":   "ResourceNotFoundException",
	}, map[string]string{
		"openbao://forbidden/dsn": "secretsmanager:GetSecretValue on arn:aws:secretsmanager:eu-central-1",
		"openbao://missing/dsn":   "aws secretsmanager create-secret --name turgon/missing",
	})
}

// commonChecks runs what every manager must do alike.
func commonChecks(t *testing.T, c *Cloud, s *store, where string, errs, fixes map[string]string) {
	t.Helper()
	ctx := context.Background()
	for ref, want := range map[string]string{
		"openbao://shop-db/dsn":       "postgres://shop",
		"vault://shop-db/user":        "shop",
		"aws://salesforce/prod-jwt":   `{"clientId":"3MVG","username":"turgon@example.com"}`,
		"openbao://team/erp/db.1/dsn": "postgres://erp", // Azure and GCP names only
	} {
		if strings.Contains(ref, "team/erp") && c.f.kind() == "AWS Secrets Manager" {
			continue
		}
		if strings.Contains(ref, "user") && c.f.kind() != "AWS Secrets Manager" {
			continue
		}
		if strings.Contains(ref, "salesforce") && c.f.kind() != "AWS Secrets Manager" {
			continue
		}
		if got, err := c.Resolve(ctx, ref); err != nil || got != want {
			t.Errorf("%s = %q, %v; want %q", ref, got, err, want)
		}
	}
	if w := c.Where("openbao://shop-db/dsn"); w != where {
		t.Errorf("where = %q", w)
	}
	// Cached within the TTL; Forget reads the current value.
	reads := s.reads
	_, _ = c.Resolve(ctx, "openbao://shop-db/dsn")
	if s.reads != reads {
		t.Errorf("not cached: %d reads", s.reads-reads)
	}
	name, _ := c.f.secretName(c.prefix + "shop-db")
	s.mu.Lock()
	s.values[name] = `{"dsn": "postgres://rotated"}`
	s.mu.Unlock()
	c.Forget()
	if got, _ := c.Resolve(ctx, "openbao://shop-db/dsn"); got != "postgres://rotated" {
		t.Errorf("after Forget: %q", got)
	}
	c.now = func() time.Time { return time.Now().Add(time.Hour) }
	defer func() { c.now = time.Now }()
	if _, err := c.Resolve(ctx, "openbao://shop-db/nope"); err == nil || !strings.Contains(err.Error(), `has no field "nope" (it has dsn`) {
		t.Errorf("missing field: %v", err)
	}
	for ref, want := range errs {
		if _, err := c.Resolve(ctx, ref); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want %q", ref, err, want)
		}
	}
	refs := []string{"openbao://shop-db/dsn"}
	for ref := range fixes {
		refs = append(refs, ref)
	}
	res := c.Check(ctx, refs)
	if !res[0].OK {
		t.Errorf("check = %+v", res[0])
	}
	for _, r := range res[1:] {
		if r.OK || !strings.Contains(r.Fix, fixes[strings.TrimPrefix(r.Name, "secret ")]) {
			t.Errorf("check %s = %+v", r.Name, r)
		}
		if strings.Contains(r.Detail+r.Fix, "postgres://") {
			t.Errorf("a value leaked: %+v", r)
		}
	}
}

func TestPlainValuesAndNames(t *testing.T) {
	s := newStore()
	srv := awsServer(t, s)
	c, _ := NewAWS(context.Background(), AWSConfig{Region: "eu-central-1", Prefix: "turgon/", Endpoint: srv.URL,
		Credentials: credentials.NewStaticCredentialsProvider("AKIDTEST", "secret", "")})
	if _, err := c.Resolve(context.Background(), "openbao://plain/password"); err == nil || !strings.Contains(err.Error(), "holds a plain value; store a JSON object") {
		t.Fatalf("plain: %v", err)
	}
	for _, ref := range []string{"openbao://../x/k", "openbao://a b/k", "nothing", "openbao://onlykey"} {
		if _, err := c.Resolve(context.Background(), ref); err == nil {
			t.Errorf("%s: resolved", ref)
		}
	}
	for path, want := range map[string]string{"team/erp/db.1": "team--erp--db-1", "shop_db": "shop-db"} {
		if got, err := (&azureFetcher{}).secretName(path); err != nil || got != want {
			t.Errorf("azure %s = %q %v", path, got, err)
		}
	}
	if got, _ := (&gcpFetcher{}).secretName("team/erp/shop_db.1"); got != "team--erp--shop_db-1" {
		t.Errorf("gcp name = %q", got)
	}
	if _, err := (&azureFetcher{}).secretName("a@b"); err == nil {
		t.Error("an invalid Azure name was accepted")
	}
}

// fakeCred is an Azure credential that hands out one token.
type fakeCred struct{ scopes []string }

func (f *fakeCred) GetToken(_ context.Context, o policy.TokenRequestOptions) (azcore.AccessToken, error) {
	f.scopes = o.Scopes
	return azcore.AccessToken{Token: "az-token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

func TestAzure(t *testing.T) {
	s := newStore()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Key Vault's challenge: an unauthenticated request learns where to
		// get a token.
		if r.Header.Get("Authorization") != "Bearer az-token" {
			w.Header().Set("WWW-Authenticate", `Bearer authorization="https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000", resource="https://vault.azure.net"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		name := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/secrets/"), "/")
		v, status := s.get(name)
		w.Header().Set("Content-Type", "application/json")
		switch status {
		case http.StatusOK:
			_ = json.NewEncoder(w).Encode(map[string]any{"value": v, "id": "https://" + r.Host + "/secrets/" + name + "/v1"})
		case http.StatusForbidden:
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"error":{"code":"Forbidden","message":"The user does not have secrets get permission"}}`)
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"error":{"code":"SecretNotFound","message":"A secret with (name/id) was not found in this key vault."}}`)
		}
	}))
	t.Cleanup(srv.Close)
	cred := &fakeCred{}
	c, err := NewAzure(AzureConfig{VaultURL: srv.URL, Prefix: "turgon-", Credential: cred, Transport: srv.Client(), InsecureChallenge: true})
	if err != nil {
		t.Fatal(err)
	}
	commonChecks(t, c, s, "Azure Key Vault secret turgon-shop-db, field dsn", map[string]string{
		"openbao://forbidden/dsn": "403 Forbidden",
		"openbao://missing/dsn":   "404 SecretNotFound",
	}, map[string]string{
		"openbao://forbidden/dsn": "Key Vault Secrets User",
		"openbao://missing/dsn":   "az keyvault secret set --vault-name 127 --name turgon-missing",
	})
	if len(cred.scopes) != 1 || cred.scopes[0] != "https://vault.azure.net/.default" {
		t.Errorf("token scopes = %v", cred.scopes)
	}
	if _, err := NewAzure(AzureConfig{VaultURL: "http://vault.example", Credential: cred}); err == nil {
		t.Error("an http vault URL was accepted")
	}
}

func TestGCP(t *testing.T) {
	s := newStore()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer gcp-token" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"error":{"code":401,"status":"UNAUTHENTICATED","message":"Request had invalid authentication credentials."}}`)
			return
		}
		name := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/projects/acme-prod/secrets/"), "/versions/latest:access")
		v, status := s.get(name)
		switch status {
		case http.StatusOK:
			_ = json.NewEncoder(w).Encode(map[string]any{"name": "projects/123/secrets/" + name + "/versions/4",
				"payload": map[string]any{"data": base64.StdEncoding.EncodeToString([]byte(v))}})
		case http.StatusForbidden:
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"error":{"code":403,"status":"PERMISSION_DENIED","message":"Permission 'secretmanager.versions.access' denied"}}`)
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"error":{"code":404,"status":"NOT_FOUND","message":"Secret [projects/123/secrets/x] not found or has no versions."}}`)
		}
	}))
	t.Cleanup(srv.Close)
	c, err := NewGCP(context.Background(), GCPConfig{Project: "acme-prod", Prefix: "turgon-", Endpoint: srv.URL,
		TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "gcp-token"})})
	if err != nil {
		t.Fatal(err)
	}
	commonChecks(t, c, s, "Google Secret Manager secret turgon-shop-db, field dsn", map[string]string{
		"openbao://forbidden/dsn": "PERMISSION_DENIED",
		"openbao://missing/dsn":   "NOT_FOUND",
	}, map[string]string{
		"openbao://forbidden/dsn": "roles/secretmanager.secretAccessor",
		"openbao://missing/dsn":   "gcloud secrets create turgon-missing --project acme-prod",
	})
	bad, _ := NewGCP(context.Background(), GCPConfig{Project: "acme-prod", Endpoint: srv.URL,
		TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "wrong"})})
	res := bad.Check(context.Background(), []string{"openbao://shop-db/dsn"})
	if res[0].OK || !strings.Contains(res[0].Fix, "iam.gke.io/gcp-service-account") {
		t.Fatalf("bad token: %+v", res)
	}
}
