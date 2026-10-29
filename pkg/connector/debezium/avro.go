package debezium

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/iskorotkov/avro/v2"
)

// avroAPI decodes within bounds: a corrupt or hostile record fails
// instead of allocating without limit.
var avroAPI = avro.Config{MaxByteSliceSize: 16 << 20, MaxSliceAllocSize: 1_000_000, MaxMapAllocSize: 100_000}.Freeze()

// avroDecoder reads records of one Avro schema.
type avroDecoder struct {
	id uint32
	s  avro.Schema
}

func newAvroDecoder(id uint32, schema string) (decoder, error) {
	// A cache of its own: two versions of a table's schema share a name.
	s, err := avro.ParseWithCache(schema, "", &avro.SchemaCache{})
	if err != nil {
		return nil, err
	}
	return avroDecoder{id, s}, nil
}

// decode reads an Avro record as JSON values: the same values the JSON
// converter's records give (exact decimals, ISO dates and timestamps, JSON
// documents), so events do not depend on the converter.
func (d avroDecoder) decode(body []byte) (map[string]any, error) {
	var raw any
	if err := avroAPI.Unmarshal(d.s, body, &raw); err != nil {
		return nil, fmt.Errorf("avro (schema %d): %w", d.id, err)
	}
	v, err := plainAvro(d.s, raw)
	if err != nil {
		return nil, err
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("avro (schema %d): not a record", d.id)
	}
	return m, nil
}

func (d avroDecoder) columns(keys map[string]bool) ([]metaColumn, bool) {
	return avroColumns(d.s, keys)
}

func (d avroDecoder) fields() []string {
	var out []string
	if r, ok := d.s.(*avro.RecordSchema); ok {
		for _, f := range r.Fields() {
			out = append(out, f.Name())
		}
	}
	return out
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

// recordEnvelope makes a change event's envelope from its decoded record.
func recordEnvelope(m map[string]any) (envelope, error) {
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
