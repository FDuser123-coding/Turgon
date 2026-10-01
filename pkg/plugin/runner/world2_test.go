package runner

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
	"github.com/fduser123-coding/turgon/pkg/audit"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/engine"
	"github.com/fduser123-coding/turgon/pkg/netguard"
	"github.com/fduser123-coding/turgon/pkg/plugin"
)

// riskAPI is a TLS server standing in for api.acme-risk.example.
type riskAPI struct {
	*httptest.Server
	mu   sync.Mutex
	auth []string
}

func newRiskAPI(t *testing.T) *riskAPI {
	r := &riskAPI{}
	r.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		r.auth = append(r.auth, req.Header.Get("Authorization"))
		r.mu.Unlock()
		if req.Header.Get("Authorization") != "Bearer s3cret-key" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch {
		case strings.HasSuffix(req.URL.Path, "/C-100"):
			_, _ = io.WriteString(w, `{"score": 35}`)
		case strings.HasSuffix(req.URL.Path, "/fail"):
			http.Error(w, "boom", http.StatusInternalServerError)
		default:
			body, _ := io.ReadAll(req.Body)
			w.Header().Set("X-Seen", req.Method)
			_, _ = w.Write(append([]byte("echo "), body...))
		}
	}))
	t.Cleanup(r.Close)
	return r
}

// options lets plugins reach the test server: its address range allowed,
// its certificate trusted, the API key in place.
func (r *riskAPI) options() Options {
	pool := x509.NewCertPool()
	pool.AddCert(r.Certificate())
	return Options{
		Network: netguard.Guard{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}, TLSConfig: &tls.Config{RootCAs: pool}},
		Secrets: connector.StaticSecrets{plugin.SecretRef("credit-check", "acme-api-key"): "s3cret-key", plugin.SecretRef("probe2", "acme-api-key"): "s3cret-key"},
	}
}

func (r *riskAPI) host() string { u, _ := url.Parse(r.URL); return u.Host }

// withWorld2 grants network, secrets and events.
func withWorld2(d *compiler.PluginDeployment, host string, topics ...string) {
	d.Grants.Network = &v1alpha1.NetworkPermissions{Allow: []string{host}}
	d.Grants.Secrets = []string{"acme-api-key"}
	d.Secrets = map[string]string{"acme-api-key": plugin.SecretRef(d.Name, "acme-api-key")}
	d.Grants.Events = &v1alpha1.EventPermissions{Publish: topics}
}

func TestCreditCheckCallsTheRiskServiceAndPublishes(t *testing.T) {
	ctx := context.Background()
	api := newRiskAPI(t)
	cc := deployment(t, "credit-check", "../../../examples/plugins/credit-check/credit-check.wasm",
		v1alpha1.EntityPermissions{Read: []string{"Customer"}, Propose: []string{"SalesOrder.creditStatus"}})
	withWorld2(&cc, api.host(), "credit.checked")
	// The plugin calls https://api.acme-risk.example: point that name at the
	// test server.
	opts := api.options()
	opts.Network.Resolver = resolver{"api.acme-risk.example": netip.MustParseAddr("127.0.0.1")}
	cc.Grants.Network.Allow = []string{"api.acme-risk.example"}
	// A second plugin listens to what credit-check publishes.
	listener := deployment(t, "listener", "../testdata/probe2.wasm", v1alpha1.EntityPermissions{})
	listener.Subscribes = []string{"plugin.credit-check.credit.checked"}
	var log bytes.Buffer
	prop := &fakeProposer{}
	r, err := New(ctx, spec(cc, listener), prop, audit.New(&log), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(ctx)
	r.client = clientTo(r.client, api) // api.acme-risk.example:443 -> the test server
	in := order
	in.Record = json.RawMessage(`{"external_id": "SHOP-8", "customer_id": "C-100", "net_value": 400}`)
	read := &fakeReader{rows: map[string]string{"Customer/C-100": `{"id": "C-100", "credit_limit": "1000.00"}`}}
	if err := r.Bind(read).Run(ctx, in); err != nil {
		t.Fatal(err)
	}
	// Within the limit, but a risk score of 35: review.
	if len(prop.reqs) != 1 || string(prop.reqs[0].Payload) != `{"creditStatus":"review","externalId":"SHOP-8"}` {
		t.Fatalf("proposals %+v", prop.reqs)
	}
	// The API key went to the risk service, and nowhere else.
	if len(api.auth) != 1 || api.auth[0] != "Bearer s3cret-key" {
		t.Fatalf("auth seen %v", api.auth)
	}
	if strings.Contains(log.String(), "s3cret") {
		t.Fatal("the secret reached the audit log")
	}
	// What credit-check published reached the listener (which, being a
	// probe, does not understand it: its failure is audited and does not
	// fail credit-check).
	lines := log.String()
	if !strings.Contains(lines, `"published":["credit.checked"]`) || !strings.Contains(lines, `"requests":["api.acme-risk.example"]`) ||
		!strings.Contains(lines, `"actor":"plugin:listener@1.2.0","action":"plugin.failed"`) || !strings.Contains(lines, `"event":"plugin.credit-check.credit.checked"`) {
		t.Fatalf("audit:\n%s", lines)
	}
}

type resolver map[string]netip.Addr

func (r resolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	if a, ok := r[host]; ok {
		return []netip.Addr{a}, nil
	}
	return nil, &net404{host}
}

type net404 struct{ host string }

func (e *net404) Error() string { return "no such host " + e.host }

// clientTo sends requests for any host to the test server, keeping the
// guard's dial checks and the server's certificate for the right name.
func clientTo(c *http.Client, api *riskAPI) *http.Client {
	t := c.Transport.(*http.Transport).Clone()
	dial := t.DialContext
	t.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return dial(ctx, network, api.Listener.Addr().String())
	}
	t.TLSClientConfig = t.TLSClientConfig.Clone()
	t.TLSClientConfig.ServerName = "example.com" // httptest's certificate
	return &http.Client{Transport: t, CheckRedirect: c.CheckRedirect, Timeout: c.Timeout}
}

func TestPluginHTTPWithinItsGrants(t *testing.T) {
	ctx := context.Background()
	api := newRiskAPI(t)
	d := deployment(t, "probe2", "../testdata/probe2.wasm", v1alpha1.EntityPermissions{})
	withWorld2(&d, api.host(), "audit")
	r, err := New(ctx, spec(d), &fakeProposer{}, audit.New(&bytes.Buffer{}), api.options())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(ctx)
	b := r.Bind(&fakeReader{})
	base := api.URL
	run := func(cmd string) string {
		t.Helper()
		in := order
		in.Plugin = "probe2"
		h := &host{r: b, p: b.plugins["probe2"], in: in}
		c := &capture{host: h}
		if err := b.plugins["probe2"].module.Handle(ctx, []byte(cmd), c); err != nil {
			t.Fatalf("%s: %v", cmd, err)
		}
		return c.result
	}
	for cmd, want := range map[string]string{
		"http POST " + base + "/x Authorization Bearer {{secret:acme-api-key}}\n{\"a\":1}": `ok 200 [Content-Length=12,Content-Type=text/plain; charset=utf-8,Date=`,
		"http GET " + base + "/fail Authorization Bearer {{secret:acme-api-key}}":          "ok 500 ",
		"http GET " + base + "/x":                                           "ok 401 ",
		"http GET https://evil.example.com/":                                "err denied",
		"http GET " + base + "/x Authorization Bearer {{secret:other-key}}": "err denied",
		"http GET " + base + "/x Host evil.example.com":                     "err invalid the header Host is set by Turgon",
		"http GET " + strings.Replace(base, "https", "http", 1) + "/x":      "ok 400 ", // plain http: allowed only to ranges operators allow, as here
		"http TRACE " + base + "/x":                                         `err invalid method "TRACE"`,
		"many-http " + base + "/x":                                          "after 16: err invalid at most 16 requests per event",
		"publish audit {\"ok\":true}":                                       "ok published",
		"publish other x":                                                   "err denied",
	} {
		if got := run(cmd); !strings.HasPrefix(got, want) {
			t.Errorf("%s:\n got %q\nwant %q", strings.SplitN(cmd, "\n", 2)[0], got, want)
		}
	}
	// A secret that cannot be read is unavailable, never shown.
	r2, _ := New(ctx, spec(d), &fakeProposer{}, audit.New(&bytes.Buffer{}), Options{Network: api.options().Network, Secrets: connector.StaticSecrets{}})
	defer r2.Close(ctx)
	b = r2.Bind(&fakeReader{})
	if got := run("http GET " + base + "/x Authorization Bearer {{secret:acme-api-key}}"); got != "err unavailable secret acme-api-key is not available" {
		t.Errorf("missing secret: %q", got)
	}
	// Without the operators allowing loopback, the same host is refused.
	r3, _ := New(ctx, spec(d), &fakeProposer{}, audit.New(&bytes.Buffer{}), Options{Secrets: api.options().Secrets})
	defer r3.Close(ctx)
	b = r3.Bind(&fakeReader{})
	if got := run("http GET " + base + "/x"); !strings.Contains(got, "not a public address") {
		t.Errorf("loopback: %q", got)
	}
}

var _ = engine.PluginInput{}
