package debezium

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Registry is a Confluent-compatible schema registry (Confluent Schema
// Registry, Apicurio's ccompat API, Karapace, Redpanda). With it, changes
// Debezium wrote in the Confluent wire format (a zero byte, the schema's
// 4-byte ID, the body) are read with their schema: Avro, JSON Schema or
// Protobuf, whichever converter wrote them. JSON changes without the wire
// format are still read as before.
type Registry struct {
	URL string `json:"url"`
	// Auth is "basic" for a registry that needs credentials (Confluent
	// Cloud's API key and secret); the connection's secret then holds them
	// as {"registry": {"username": ..., "password": ...}}.
	Auth string `json:"auth,omitempty"`
}

func (r Registry) validate() error {
	u, err := url.Parse(r.URL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return fmt.Errorf("schemaRegistry: url %q must be an http(s) URL", r.URL)
	}
	if u.Scheme == "http" {
		if ip := net.ParseIP(u.Hostname()); u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return errors.New("schemaRegistry: https is required for a registry that is not on this host")
		}
	}
	switch r.Auth {
	case "", "basic":
	default:
		return fmt.Errorf("schemaRegistry: auth must be basic, got %q", r.Auth)
	}
	return nil
}

// decoder reads the records written with one registered schema.
type decoder interface {
	// decode reads a record's body (what follows the schema ID) as plain
	// JSON values: exact decimals, ISO dates and times, JSON documents.
	decode(body []byte) (map[string]any, error)
	// columns describes the row of a change event's schema.
	columns(keys map[string]bool) ([]metaColumn, bool)
	// fields names a key schema's fields.
	fields() []string
}

// registry fetches schemas by ID and keeps their decoders: an ID always
// names the same schema.
type registry struct {
	url        string
	user, pass string
	client     *http.Client
	mu         sync.Mutex
	decoders   map[uint32]decoder
}

func newRegistry(r *Registry, user, pass string) *registry {
	if r == nil {
		return nil
	}
	return &registry{url: strings.TrimRight(r.URL, "/"), user: user, pass: pass,
		client: &http.Client{Timeout: 10 * time.Second}, decoders: map[uint32]decoder{}}
}

// registered is a schema as the registry gives it.
type registered struct {
	Schema     string      `json:"schema"`
	SchemaType string      `json:"schemaType"`
	References []reference `json:"references"`
}

// reference names a schema another one imports (a Protobuf import, a JSON
// Schema $ref): by its name in the importing schema, and its subject and
// version in the registry.
type reference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

func (r *registry) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.url+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json, application/json")
	if r.user != "" {
		req.SetBasicAuth(r.user, r.pass)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.Unmarshal(body, out)
}

func (r *registry) decoder(ctx context.Context, id uint32) (decoder, error) {
	r.mu.Lock()
	d, ok := r.decoders[id]
	r.mu.Unlock()
	if ok {
		return d, nil
	}
	var s registered
	if err := r.get(ctx, fmt.Sprintf("/schemas/ids/%d", id), &s); err != nil {
		return nil, fmt.Errorf("schema registry: schema %d: %w", id, err)
	}
	var err error
	switch s.SchemaType {
	case "", "AVRO":
		d, err = newAvroDecoder(id, s.Schema)
	case "JSON":
		d, err = newJSONSchemaDecoder(id, s.Schema)
	case "PROTOBUF":
		var files map[string]string
		if files, err = r.imports(ctx, s.References, map[string]string{}); err == nil {
			d, err = newProtoDecoder(ctx, id, s.Schema, files)
		}
	default:
		err = fmt.Errorf("is %s; Turgon reads Avro, JSON Schema and Protobuf", s.SchemaType)
	}
	if err != nil {
		return nil, fmt.Errorf("schema registry: schema %d: %w", id, err)
	}
	r.mu.Lock()
	r.decoders[id] = d
	r.mu.Unlock()
	return d, nil
}

// imports fetches the schemas a schema references, and theirs, by name.
func (r *registry) imports(ctx context.Context, refs []reference, into map[string]string) (map[string]string, error) {
	for _, ref := range refs {
		if _, ok := into[ref.Name]; ok {
			continue
		}
		version := "latest"
		if ref.Version > 0 {
			version = fmt.Sprint(ref.Version)
		}
		var s registered
		if err := r.get(ctx, "/subjects/"+url.PathEscape(ref.Subject)+"/versions/"+version, &s); err != nil {
			return nil, fmt.Errorf("reference %s (subject %s, version %s): %w", ref.Name, ref.Subject, version, err)
		}
		into[ref.Name] = s.Schema
		if _, err := r.imports(ctx, s.References, into); err != nil {
			return nil, err
		}
	}
	return into, nil
}

// decodeRecord reads a record in the Confluent wire format.
func (r *registry) decodeRecord(ctx context.Context, b []byte) (decoder, map[string]any, error) {
	id, body, _ := wireFormat(b)
	d, err := r.decoder(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	m, err := d.decode(body)
	return d, m, err
}

// subjects lists the registry's subjects: a call that proves it is
// reachable and accepts the credentials.
func (r *registry) subjects(ctx context.Context) (int, error) {
	var list []string
	if err := r.get(ctx, "/subjects", &list); err != nil {
		return 0, err
	}
	return len(list), nil
}

// wireFormat splits a Confluent wire-format record into its schema ID and
// body.
func wireFormat(b []byte) (uint32, []byte, bool) {
	if len(b) < 5 || b[0] != 0 {
		return 0, nil, false
	}
	return binary.BigEndian.Uint32(b[1:5]), b[5:], true
}
