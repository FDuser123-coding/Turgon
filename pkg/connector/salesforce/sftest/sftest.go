// Package sftest is an in-process fake of the Salesforce REST API for tests:
// OAuth (JWT bearer with signature checks, client credentials), SOQL queries
// with pagination, sObject reads and updates, and Salesforce-style errors.
package sftest

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Server is a fake Salesforce org.
type Server struct {
	*httptest.Server
	Key      *rsa.PrivateKey
	ClientID string
	Username string
	// PageSize is the number of records per query page. Default 2000.
	PageSize int
	// FailPatch, if set, can reject an update with a Salesforce error code.
	FailPatch func(sobject, id string, fields map[string]any) (status int, code, msg string)

	mu      sync.Mutex
	records map[string]map[string]map[string]any // sobject -> id -> record
	tokens  map[string]bool
	cursors map[string][]map[string]any
	nextID  int
	Logins  int
	Patches int
}

// New starts a fake org.
func New() *Server {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	s := &Server{
		Key: key, ClientID: "3MVG9test", Username: "porter-integration@example.com", PageSize: 2000,
		records: map[string]map[string]map[string]any{}, tokens: map[string]bool{}, cursors: map[string][]map[string]any{},
	}
	s.Server = httptest.NewServer(http.HandlerFunc(s.serve))
	return s
}

// PrivateKeyPEM returns the integration user's key as PKCS#8 PEM.
func (s *Server) PrivateKeyPEM() string {
	der, _ := x509.MarshalPKCS8PrivateKey(s.Key)
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
}

// Credentials returns the JSON secret a connection would resolve.
func (s *Server) Credentials() string {
	b, _ := json.Marshal(map[string]string{
		"loginUrl": s.URL, "clientId": s.ClientID, "username": s.Username, "privateKey": s.PrivateKeyPEM(),
	})
	return string(b)
}

// ExpireTokens invalidates every issued access token.
func (s *Server) ExpireTokens() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens = map[string]bool{}
}

// Put creates or replaces a record and stamps SystemModstamp.
func (s *Server) Put(sobject, id string, fields map[string]any, modstamp time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := map[string]any{}
	for k, v := range fields {
		rec[k] = v
	}
	rec["Id"] = id
	rec["SystemModstamp"] = modstamp.UTC().Format("2006-01-02T15:04:05.000+0000")
	if s.records[sobject] == nil {
		s.records[sobject] = map[string]map[string]any{}
	}
	s.records[sobject][id] = rec
}

// Get returns a copy of a record.
func (s *Server) Get(sobject, id string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]any{}
	for k, v := range s.records[sobject][id] {
		out[k] = v
	}
	return out
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode([]map[string]string{{"errorCode": code, "message": msg}})
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/services/oauth2/token" {
		s.token(w, r)
		return
	}
	s.mu.Lock()
	ok := s.tokens[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
	s.mu.Unlock()
	if !ok {
		writeErr(w, http.StatusUnauthorized, "INVALID_SESSION_ID", "Session expired or invalid")
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	// services/data/vXX.X/{query|sobjects}/...
	if len(parts) < 4 || parts[0] != "services" || parts[1] != "data" {
		writeErr(w, http.StatusNotFound, "NOT_FOUND", "unknown path")
		return
	}
	switch {
	case parts[3] == "query" && len(parts) == 4:
		s.query(w, r.URL.Query().Get("q"))
	case parts[3] == "query" && len(parts) == 5:
		s.page(w, parts[4])
	case parts[3] == "sobjects" && len(parts) == 6 && r.Method == http.MethodGet:
		s.read(w, parts[4], parts[5], r.URL.Query().Get("fields"))
	case parts[3] == "sobjects" && len(parts) == 6 && r.Method == http.MethodPatch:
		s.patch(w, r, parts[4], parts[5])
	default:
		writeErr(w, http.StatusNotFound, "NOT_FOUND", "unknown path")
	}
}

func (s *Server) token(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	switch r.Form.Get("grant_type") {
	case "urn:ietf:params:oauth:grant-type:jwt-bearer":
		if err := s.checkAssertion(r.Form.Get("assertion")); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_grant", "error_description": err.Error()})
			return
		}
	case "client_credentials":
		if r.Form.Get("client_id") != s.ClientID || r.Form.Get("client_secret") != "secret" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_client"})
			return
		}
	default:
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "unsupported_grant_type"})
		return
	}
	s.mu.Lock()
	s.Logins++
	tok := fmt.Sprintf("00Dtoken%d", s.Logins)
	s.tokens[tok] = true
	s.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]string{"access_token": tok, "instance_url": s.URL, "token_type": "Bearer"})
}

func (s *Server) checkAssertion(jwt string) error {
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		return fmt.Errorf("malformed assertion")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(&s.Key.PublicKey, crypto.SHA256, sum[:], sig); err != nil {
		return fmt.Errorf("invalid signature")
	}
	raw, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var c struct {
		Iss, Sub, Aud string
		Exp           int64
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		return err
	}
	switch {
	case c.Iss != s.ClientID:
		return fmt.Errorf("client identifier invalid")
	case c.Sub != s.Username:
		return fmt.Errorf("user hasn't approved this consumer")
	case c.Aud != s.URL:
		return fmt.Errorf("audience is invalid")
	case time.Unix(c.Exp, 0).Before(time.Now()):
		return fmt.Errorf("expired assertion")
	}
	return nil
}

var (
	fromRE  = regexp.MustCompile(`FROM (\w+) WHERE`)
	stampRE = regexp.MustCompile(`SystemModstamp > (\S+)`)
	eqRE    = regexp.MustCompile(`\((\w+) = '([^']*)'\)`)
)

// query supports the SOQL the connector issues:
// SELECT ... FROM X WHERE [(Field = 'v') AND] SystemModstamp > T ORDER BY SystemModstamp, Id
func (s *Server) query(w http.ResponseWriter, soql string) {
	m := fromRE.FindStringSubmatch(soql)
	st := stampRE.FindStringSubmatch(soql)
	if m == nil || st == nil || !strings.HasSuffix(soql, "ORDER BY SystemModstamp, Id") {
		writeErr(w, http.StatusBadRequest, "MALFORMED_QUERY", "unsupported query: "+soql)
		return
	}
	after, err := time.Parse("2006-01-02T15:04:05.000Z", st[1])
	if err != nil {
		writeErr(w, http.StatusBadRequest, "MALFORMED_QUERY", "bad datetime literal "+st[1])
		return
	}
	eq := eqRE.FindStringSubmatch(soql)
	s.mu.Lock()
	var out []map[string]any
	for _, rec := range s.records[m[1]] {
		ts, _ := time.Parse("2006-01-02T15:04:05.000-0700", rec["SystemModstamp"].(string))
		if !ts.After(after) || (eq != nil && fmt.Sprint(rec[eq[1]]) != eq[2]) {
			continue
		}
		cp := map[string]any{"attributes": map[string]any{"type": m[1]}}
		for k, v := range rec {
			cp[k] = v
		}
		out = append(out, cp)
	}
	s.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i]["SystemModstamp"].(string), out[j]["SystemModstamp"].(string)
		if a != b {
			return a < b
		}
		return out[i]["Id"].(string) < out[j]["Id"].(string)
	})
	s.respondPage(w, out)
}

func (s *Server) respondPage(w http.ResponseWriter, recs []map[string]any) {
	size := s.PageSize
	page := recs
	res := map[string]any{"totalSize": len(recs), "done": true}
	if len(recs) > size {
		page = recs[:size]
		s.mu.Lock()
		s.nextID++
		id := fmt.Sprintf("01g%d-%d", s.nextID, size)
		s.cursors[id] = recs[size:]
		s.mu.Unlock()
		res["done"] = false
		res["nextRecordsUrl"] = "/services/data/v61.0/query/" + id
	}
	if page == nil {
		page = []map[string]any{}
	}
	res["records"] = page
	_ = json.NewEncoder(w).Encode(res)
}

func (s *Server) page(w http.ResponseWriter, id string) {
	s.mu.Lock()
	recs, ok := s.cursors[id]
	delete(s.cursors, id)
	s.mu.Unlock()
	if !ok {
		writeErr(w, http.StatusBadRequest, "INVALID_QUERY_LOCATOR", "invalid query locator")
		return
	}
	s.respondPage(w, recs)
}

func (s *Server) read(w http.ResponseWriter, sobject, id, fields string) {
	s.mu.Lock()
	rec, ok := s.records[sobject][id]
	out := map[string]any{"attributes": map[string]any{"type": sobject}, "Id": id}
	if ok {
		for _, f := range strings.Split(fields, ",") {
			if f != "" {
				out[f] = rec[f]
			}
		}
	}
	s.mu.Unlock()
	if !ok {
		writeErr(w, http.StatusNotFound, "NOT_FOUND", "The requested resource does not exist")
		return
	}
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) patch(w http.ResponseWriter, r *http.Request, sobject, id string) {
	var fields map[string]any
	if err := json.NewDecoder(r.Body).Decode(&fields); err != nil {
		writeErr(w, http.StatusBadRequest, "JSON_PARSER_ERROR", err.Error())
		return
	}
	if s.FailPatch != nil {
		if status, code, msg := s.FailPatch(sobject, id, fields); status != 0 {
			writeErr(w, status, code, msg)
			return
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[sobject][id]
	if !ok {
		writeErr(w, http.StatusNotFound, "NOT_FOUND", "The requested resource does not exist")
		return
	}
	for k, v := range fields {
		rec[k] = v
	}
	rec["SystemModstamp"] = time.Now().UTC().Format("2006-01-02T15:04:05.000+0000")
	s.Patches++
	w.WriteHeader(http.StatusNoContent)
}
