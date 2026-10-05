package debezium

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/iskorotkov/avro/v2"
)

// Registry is a Confluent-compatible schema registry (Confluent Schema
// Registry, Apicurio's ccompat API, Karapace, Redpanda). With it, changes
// Debezium wrote with an Avro converter in the Confluent wire format (a
// zero byte, the schema's 4-byte ID, the Avro body) are read with their
// schema; JSON changes are still read as before.
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

// avroAPI decodes within bounds: a corrupt or hostile record fails
// instead of allocating without limit.
var avroAPI = avro.Config{MaxByteSliceSize: 16 << 20, MaxSliceAllocSize: 1_000_000, MaxMapAllocSize: 100_000}.Freeze()

// registry fetches schemas by ID and keeps them: an ID always names the
// same schema.
type registry struct {
	url        string
	user, pass string
	client     *http.Client
	mu         sync.Mutex
	schemas    map[uint32]avro.Schema
}

func newRegistry(r *Registry, user, pass string) *registry {
	if r == nil {
		return nil
	}
	return &registry{url: strings.TrimRight(r.URL, "/"), user: user, pass: pass,
		client: &http.Client{Timeout: 10 * time.Second}, schemas: map[uint32]avro.Schema{}}
}

func (r *registry) schema(ctx context.Context, id uint32) (avro.Schema, error) {
	r.mu.Lock()
	s, ok := r.schemas[id]
	r.mu.Unlock()
	if ok {
		return s, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/schemas/ids/%d", r.url, id), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json, application/json")
	if r.user != "" {
		req.SetBasicAuth(r.user, r.pass)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("schema registry: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("schema registry: schema %d: HTTP %d %s", id, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out struct {
		Schema     string `json:"schema"`
		SchemaType string `json:"schemaType"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("schema registry: schema %d: %w", id, err)
	}
	if out.SchemaType != "" && out.SchemaType != "AVRO" {
		return nil, fmt.Errorf("schema registry: schema %d is %s; Turgon reads Avro and JSON changes", id, out.SchemaType)
	}
	// A cache of its own: two versions of a table's schema share a name.
	s, err = avro.ParseWithCache(out.Schema, "", &avro.SchemaCache{})
	if err != nil {
		return nil, fmt.Errorf("schema registry: schema %d: %w", id, err)
	}
	r.mu.Lock()
	r.schemas[id] = s
	r.mu.Unlock()
	return s, nil
}

// subjects lists the registry's subjects: a call that proves it is
// reachable and accepts the credentials.
func (r *registry) subjects(ctx context.Context) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.url+"/subjects", nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json, application/json")
	if r.user != "" {
		req.SetBasicAuth(r.user, r.pass)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP %d %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var list []string
	if err := json.Unmarshal(body, &list); err != nil {
		return 0, fmt.Errorf("not a schema registry's subject list: %w", err)
	}
	return len(list), nil
}

// wireFormat splits a Confluent wire-format record into its schema ID and
// Avro body.
func wireFormat(b []byte) (uint32, []byte, bool) {
	if len(b) < 5 || b[0] != 0 {
		return 0, nil, false
	}
	return binary.BigEndian.Uint32(b[1:5]), b[5:], true
}

// decodeAvro reads an Avro record as JSON values: the same values the JSON
// converter's records give (exact decimals, ISO dates and timestamps, JSON
// documents), so events do not depend on the converter.
func (r *registry) decodeAvro(ctx context.Context, b []byte) (avro.Schema, map[string]any, error) {
	id, body, _ := wireFormat(b)
	s, err := r.schema(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	var raw any
	if err := avroAPI.Unmarshal(s, body, &raw); err != nil {
		return nil, nil, fmt.Errorf("avro (schema %d): %w", id, err)
	}
	v, err := plainAvro(s, raw)
	if err != nil {
		return nil, nil, err
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("avro (schema %d): not a record", id)
	}
	return s, m, nil
}

// connectName is the Kafka Connect type an Avro schema carries.
func connectName(s avro.Schema) string {
	if p, ok := s.(interface{ Prop(string) any }); ok {
		if n, ok := p.Prop("connect.name").(string); ok {
			return n
		}
	}
	return ""
}

func plainAvro(s avro.Schema, v any) (any, error) {
	if r, ok := s.(*avro.RefSchema); ok { // a named type used again ("after": Value)
		return plainAvro(r.Schema(), v)
	}
	switch s := s.(type) {
	case *avro.UnionSchema:
		if v == nil {
			return nil, nil
		}
		if m, ok := v.(map[string]any); ok && len(m) == 1 {
			for name, inner := range m {
				for _, t := range s.Types() {
					if avroTypeName(t) == name {
						return plainAvro(t, inner)
					}
				}
			}
		}
		for _, t := range s.Types() {
			if t.Type() != avro.Null {
				return plainAvro(t, v)
			}
		}
		return v, nil
	case *avro.RecordSchema:
		m, ok := v.(map[string]any)
		if !ok {
			return v, nil
		}
		if connectName(s) == "io.debezium.data.VariableScaleDecimal" {
			scale, _ := toInt(m["scale"])
			b, _ := m["value"].([]byte)
			return decimal(base64.StdEncoding.EncodeToString(b), int(scale))
		}
		for _, f := range s.Fields() {
			if fv, ok := m[f.Name()]; ok {
				pv, err := plainAvro(f.Type(), fv)
				if err != nil {
					return nil, fmt.Errorf("%s: %w", f.Name(), err)
				}
				m[f.Name()] = pv
			}
		}
		return m, nil
	case *avro.ArraySchema:
		a, _ := v.([]any)
		for i := range a {
			pv, err := plainAvro(s.Items(), a[i])
			if err != nil {
				return nil, err
			}
			a[i] = pv
		}
		return v, nil
	case *avro.MapSchema:
		m, _ := v.(map[string]any)
		for k := range m {
			pv, err := plainAvro(s.Values(), m[k])
			if err != nil {
				return nil, err
			}
			m[k] = pv
		}
		return v, nil
	case *avro.PrimitiveSchema:
		switch x := v.(type) {
		case *big.Rat:
			scale := 0
			if d, ok := s.Logical().(*avro.DecimalLogicalSchema); ok {
				scale = d.Scale()
			}
			return json.Number(x.FloatString(scale)), nil
		case time.Time:
			if l := s.Logical(); l != nil && l.Type() == avro.Date {
				return x.UTC().Format("2006-01-02"), nil
			}
			return x.UTC().Format(time.RFC3339Nano), nil
		case time.Duration:
			return clock(x), nil
		case []byte:
			return base64.StdEncoding.EncodeToString(x), nil
		}
		// Debezium's own logical types travel as plain ints and strings
		// named by connect.name: convert them as the JSON path does.
		if n := connectName(s); n != "" {
			return convertValue(field{Name: n}, v)
		}
		return v, nil
	}
	return v, nil
}

func avroTypeName(s avro.Schema) string {
	if r, ok := s.(*avro.RefSchema); ok {
		return r.Schema().FullName()
	}
	if n, ok := s.(avro.NamedSchema); ok {
		return n.FullName()
	}
	return string(s.Type())
}

// avroEnvelope makes a change event's envelope from its decoded record.
func avroEnvelope(m map[string]any) (envelope, error) {
	env := envelope{}
	env.Op, _ = m["op"].(string)
	env.Source, _ = m["source"].(map[string]any)
	if env.Op == "" || env.Source == nil {
		return envelope{}, errors.New("not a Debezium change event: op and source are required (is the ExtractNewRecordState transform on? Turgon needs the full envelope)")
	}
	env.Before, _ = m["before"].(map[string]any)
	env.After, _ = m["after"].(map[string]any)
	if ts, err := toInt(m["ts_ms"]); err == nil {
		env.TsMs = ts
	}
	return env, nil
}

// avroColumns describes the row record of a change event's Avro schema.
func avroColumns(s avro.Schema, keys map[string]bool) ([]metaColumn, bool) {
	rec, ok := s.(*avro.RecordSchema)
	if !ok {
		return nil, false
	}
	for _, f := range rec.Fields() {
		if f.Name() != "after" && f.Name() != "before" {
			continue
		}
		row := unionRecord(f.Type())
		if row == nil {
			continue
		}
		var out []metaColumn
		for _, col := range row.Fields() {
			t, optional := col.Type(), false
			if u, ok := t.(*avro.UnionSchema); ok {
				for _, m := range u.Types() {
					if m.Type() == avro.Null {
						optional = true
					} else {
						t = m
					}
				}
			}
			c := field{Field: col.Name(), Type: connectType(t), Name: connectName(t), Optional: optional}
			if col.HasDefault() && col.Default() != nil {
				c.Default = col.Default()
			}
			if d, ok := t.(*avro.PrimitiveSchema); ok {
				if dl, ok := d.Logical().(*avro.DecimalLogicalSchema); ok {
					c.Name = "org.apache.kafka.connect.data.Decimal"
					c.Parameters = map[string]string{"scale": strconv.Itoa(dl.Scale()), "connect.decimal.precision": strconv.Itoa(dl.Precision())}
				}
			}
			if p, ok := t.(interface{ Prop(string) any }); ok {
				if params, ok := p.Prop("connect.parameters").(map[string]any); ok {
					if c.Parameters == nil {
						c.Parameters = map[string]string{}
					}
					for k, v := range params {
						if s, ok := v.(string); ok {
							c.Parameters[k] = s
						}
					}
				}
			}
			out = append(out, metaColumn{c, keys[col.Name()]})
		}
		return out, true
	}
	return nil, false
}

type metaColumn struct {
	f   field
	key bool
}

// connectType names an Avro type as Kafka Connect does, so a table reads
// the same through the Avro and the JSON converter.
func connectType(s avro.Schema) string {
	if p, ok := s.(interface{ Prop(string) any }); ok {
		if t, ok := p.Prop("connect.type").(string); ok {
			return t // int8, int16
		}
	}
	switch s.Type() {
	case avro.Int:
		return "int32"
	case avro.Long:
		return "int64"
	case avro.Float:
		return "float"
	case avro.Double:
		return "double"
	}
	return string(s.Type())
}

func unionRecord(s avro.Schema) *avro.RecordSchema {
	if ref, ok := s.(*avro.RefSchema); ok {
		s = ref.Schema()
	}
	if r, ok := s.(*avro.RecordSchema); ok {
		return r
	}
	if u, ok := s.(*avro.UnionSchema); ok {
		for _, t := range u.Types() {
			if r := unionRecord(t); r != nil {
				return r
			}
		}
	}
	return nil
}
