package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTemporalTLSConfig(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.TLS.PeerCertificates) == 0 {
			http.Error(w, "no client certificate", http.StatusUnauthorized)
		}
	}))
	srv.TLS = &tls.Config{ClientAuth: tls.RequestClientCert}
	srv.StartTLS()
	defer srv.Close()

	dir := t.TempDir()
	write := func(name, typ string, der []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	// The test server's certificate is self-signed, so it is its own CA;
	// its key pair doubles as a client certificate.
	cert := srv.TLS.Certificates[0]
	ca := write("ca.pem", "CERTIFICATE", cert.Certificate[0])
	key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	keyFile := write("key.pem", "PRIVATE KEY", key)

	f := temporalFlags{caFile: ca, certFile: ca, keyFile: keyFile, serverName: "example.com"}
	cfg, err := f.tlsConfig()
	if err != nil {
		t.Fatal(err)
	}
	c := &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}}
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatalf("handshake with the configured CA: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("client certificate not presented: %s", resp.Status)
	}

	// Without the CA the server is not trusted.
	cfg, err = (&temporalFlags{}).tlsConfig()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (&http.Client{Transport: &http.Transport{TLSClientConfig: cfg}}).Get(srv.URL); err == nil {
		t.Fatal("an unknown CA was trusted")
	}

	for _, bad := range []struct {
		f    temporalFlags
		want string
	}{
		{temporalFlags{certFile: ca}, "go together"},
		{temporalFlags{caFile: keyFile}, "no PEM certificates"},
		{temporalFlags{caFile: filepath.Join(dir, "missing")}, "--temporal-ca"},
		{temporalFlags{certFile: ca, keyFile: ca}, "client certificate"},
	} {
		if _, err := bad.f.tlsConfig(); err == nil || !strings.Contains(err.Error(), bad.want) {
			t.Errorf("%+v: got %v, want an error with %q", bad.f, err, bad.want)
		}
	}
}
