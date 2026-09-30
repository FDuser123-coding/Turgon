//go:build live

// Live tests against a real OpenBao (or Vault) server and a real
// kube-apiserver, run by hand:
//
//	make envtest
//	TURGON_BAO_BIN=/path/to/bao KUBEBUILDER_ASSETS=$PWD/bin/envtest \
//	  go test -tags live -run Live ./pkg/secrets/
//
// The server runs in dev mode on a free port; nothing outside the test is
// touched.
package secrets

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// startBao runs a dev server and returns its address; root is the token.
func startBao(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("TURGON_BAO_BIN")
	if bin == "" {
		t.Skip("set TURGON_BAO_BIN to an OpenBao (bao) or Vault binary")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	cmd := exec.Command(bin, "server", "-dev", "-dev-root-token-id=root", "-dev-listen-address="+addr)
	var logs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &logs, &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	url := "http://" + addr
	for i := 0; i < 100; i++ {
		if resp, err := http.Get(url + "/v1/sys/health"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return url
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("the server did not start:\n%s", logs.String())
	return ""
}

// admin calls the server's API with the root token.
func admin(t *testing.T, url, method, path string, body any) {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(method, url+path, bytes.NewReader(b))
	req.Header.Set("X-Vault-Token", "root")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		var e map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&e)
		t.Fatalf("%s %s: %d %v", method, path, resp.StatusCode, e)
	}
}

func TestLiveKubernetesAuth(t *testing.T) {
	url := startBao(t)
	te := &envtest.Environment{}
	cfg, err := te.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = te.Stop() })
	kc := kubernetes.NewForConfigOrDie(cfg)
	ctx := context.Background()

	// The service account Turgon's pods run as, and one OpenBao uses to
	// review tokens (system:auth-delegator), as in a real cluster.
	for _, ns := range []string{"turgon", "openbao", "other"} {
		if _, err := kc.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	for _, sa := range []struct{ ns, name string }{{"turgon", "turgon"}, {"openbao", "reviewer"}, {"other", "turgon"}} {
		if _, err := kc.CoreV1().ServiceAccounts(sa.ns).Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: sa.name}}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := kc.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "openbao-reviewer"},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "system:auth-delegator"},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "reviewer", Namespace: "openbao"}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	token := func(ns, sa string, audiences ...string) string {
		exp := int64(3600)
		tr, err := kc.CoreV1().ServiceAccounts(ns).CreateToken(ctx, sa, &authv1.TokenRequest{
			Spec: authv1.TokenRequestSpec{Audiences: audiences, ExpirationSeconds: &exp}}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return tr.Status.Token
	}

	admin(t, url, http.MethodPost, "/v1/sys/auth/kubernetes", map[string]any{"type": "kubernetes"})
	admin(t, url, http.MethodPost, "/v1/auth/kubernetes/config", map[string]any{
		"kubernetes_host": cfg.Host, "kubernetes_ca_cert": string(cfg.CAData), "token_reviewer_jwt": token("openbao", "reviewer"),
	})
	admin(t, url, http.MethodPut, "/v1/sys/policies/acl/turgon-read", map[string]any{"policy": `path "secret/data/shop-db" { capabilities = ["read"] }`})
	admin(t, url, http.MethodPost, "/v1/auth/kubernetes/role/turgon", map[string]any{
		"bound_service_account_names": []string{"turgon"}, "bound_service_account_namespaces": []string{"turgon"},
		"audience": "openbao", "token_policies": []string{"turgon-read"}, "token_ttl": "60s",
	})
	admin(t, url, http.MethodPost, "/v1/secret/data/shop-db", map[string]any{"data": map[string]any{"dsn": "postgres://shop"}})
	admin(t, url, http.MethodPost, "/v1/secret/data/erp-db", map[string]any{"data": map[string]any{"dsn": "postgres://erp"}})

	dir := t.TempDir()
	resolver := func(jwt string) *OpenBao {
		f := filepath.Join(dir, fmt.Sprintf("token-%d", time.Now().UnixNano()))
		if err := os.WriteFile(f, []byte(jwt), 0o600); err != nil {
			t.Fatal(err)
		}
		o, err := NewOpenBao(Config{Address: url, Auth: "kubernetes", Role: "turgon", JWTFile: f})
		if err != nil {
			t.Fatal(err)
		}
		return o
	}

	// The pod's projected token, bound to the audience "openbao".
	o := resolver(token("turgon", "turgon", "openbao"))
	if v, err := o.Resolve(ctx, "openbao://shop-db/dsn"); err != nil || v != "postgres://shop" {
		t.Fatalf("resolve = %q, %v", v, err)
	}
	// The policy grants shop-db only.
	if _, err := o.Resolve(ctx, "openbao://erp-db/dsn"); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("erp-db: %v", err)
	}
	// A token for another audience (the API server's), or from a service
	// account the role does not bind, is refused.
	for name, jwt := range map[string]string{
		"default audience": token("turgon", "turgon"),
		"other namespace":  token("other", "turgon", "openbao"),
	} {
		res := resolver(jwt).Check(ctx, nil)
		if res[0].OK || !strings.Contains(res[0].Fix, "auth/kubernetes/role/turgon") {
			t.Errorf("%s: %+v", name, res)
		}
	}
}

func TestLiveAppRoleAndKV(t *testing.T) {
	url := startBao(t)
	ctx := context.Background()
	admin(t, url, http.MethodPost, "/v1/sys/auth/approle", map[string]any{"type": "approle"})
	admin(t, url, http.MethodPut, "/v1/sys/policies/acl/turgon-read", map[string]any{"policy": `path "secret/data/*" { capabilities = ["read"] }`})
	admin(t, url, http.MethodPost, "/v1/auth/approle/role/turgon", map[string]any{"token_policies": []string{"turgon-read"}, "token_ttl": "60s"})
	get := func(path string) map[string]any {
		req, _ := http.NewRequest(http.MethodGet, url+path, nil)
		req.Header.Set("X-Vault-Token", "root")
		if req.URL.Path == "/v1/auth/approle/role/turgon/secret-id" {
			req.Method = http.MethodPost
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out struct{ Data map[string]any }
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return out.Data
	}
	roleID := get("/v1/auth/approle/role/turgon/role-id")["role_id"].(string)
	secretID := get("/v1/auth/approle/role/turgon/secret-id")["secret_id"].(string)
	sid := filepath.Join(t.TempDir(), "secret-id")
	_ = os.WriteFile(sid, []byte(secretID), 0o600)
	admin(t, url, http.MethodPost, "/v1/secret/data/salesforce", map[string]any{"data": map[string]any{
		"prod-jwt": map[string]any{"loginUrl": "https://login.salesforce.com", "clientId": "3MVG"}}})
	admin(t, url, http.MethodPost, "/v1/secret/data/gone", map[string]any{"data": map[string]any{"dsn": "x"}})
	admin(t, url, http.MethodDelete, "/v1/secret/data/gone", nil)

	o, err := NewOpenBao(Config{Address: url, Auth: "approle", Role: roleID, SecretIDFile: sid})
	if err != nil {
		t.Fatal(err)
	}
	if v, err := o.Resolve(ctx, "openbao://salesforce/prod-jwt"); err != nil || v != `{"clientId":"3MVG","loginUrl":"https://login.salesforce.com"}` {
		t.Fatalf("salesforce = %q, %v", v, err)
	}
	res := o.Check(ctx, []string{"openbao://gone/dsn", "openbao://missing/dsn"})
	if !res[0].OK || res[1].OK || !strings.Contains(res[1].Detail, "deleted at") || !strings.Contains(res[1].Fix, "undelete") ||
		res[2].OK || !strings.Contains(res[2].Fix, "kv put") {
		t.Fatalf("check = %+v", res)
	}
}
