package debezium

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

// jsonSchema is the part of a JSON Schema that Confluent's JSON Schema
// converter writes for a Kafka Connect schema: Connect's type, name
// (title) and parameters ride along as connect.* keywords, and an optional
// field is a oneOf with null.
type jsonSchema struct {
	Type       any                    `json:"type"`
	Title      string                 `json:"title"`
	Properties map[string]*jsonSchema `json:"properties"`
	OneOf      []*jsonSchema          `json:"oneOf"`
	AnyOf      []*jsonSchema          `json:"anyOf"`
	Items      *jsonSchema            `json:"items"`
	Default    any                    `json:"default"`
	Connect    string                 `json:"connect.type"`
	Index      *int                   `json:"connect.index"`
	Params     map[string]string      `json:"connect.parameters"`
}

// jsonSchemaDecoder reads records of one JSON Schema: their bodies are the
// change event as plain JSON, without the JSON converter's schema and
// payload.
type jsonSchemaDecoder struct {
	id uint32
	s  *jsonSchema
}

func newJSONSchemaDecoder(id uint32, schema string) (decoder, error) {
	var s jsonSchema
	if err := json.Unmarshal([]byte(schema), &s); err != nil {
		return nil, fmt.Errorf("not a JSON Schema: %w", err)
	}
	return jsonSchemaDecoder{id, &s}, nil
}

// nonNull is the schema a oneOf (or a type list) with null allows besides
// null, and whether null is allowed.
func (s *jsonSchema) nonNull() (*jsonSchema, bool) {
	if s == nil {
		return nil, false
	}
	alts := s.OneOf
	if len(alts) == 0 {
		alts = s.AnyOf
	}
	if len(alts) > 0 {
		var other *jsonSchema
		null := false
		for _, a := range alts {
			if a.Type == "null" {
				null = true
			} else if other == nil {
				other = a
			}
		}
		if other == nil {
			return s, null
		}
		inner, innerNull := other.nonNull()
		return inner, null || innerNull
	}
	if types, ok := s.Type.([]any); ok {
		for _, t := range types {
			if t == "null" {
				return s, true
			}
		}
	}
	return s, false
}

// ordered lists an object's properties in Connect's field order.
func (s *jsonSchema) ordered() []string {
	names := make([]string, 0, len(s.Properties))
	for n := range s.Properties {
		names = append(names, n)
	}
	sort.Slice(names, func(i, j int) bool {
		a, b := s.Properties[names[i]].Index, s.Properties[names[j]].Index
		if a != nil && b != nil && *a != *b {
			return *a < *b
		}
		if (a == nil) != (b == nil) {
			return a != nil
		}
		return names[i] < names[j]
	})
	return names
}

func (d jsonSchemaDecoder) decode(body []byte) (map[string]any, error) {
	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("json (schema %d): %w", d.id, err)
	}
	root, _ := d.s.nonNull()
	for name, v := range m {
		cv, err := plainJSON(root.Properties[name], v)
		if err != nil {
			return nil, fmt.Errorf("json (schema %d): %s: %w", d.id, name, err)
		}
		m[name] = cv
	}
	return m, nil
}

// plainJSON converts a value from Connect's encodings, as its schema names
// them, to what it means; the same conversions as the JSON converter's.
func plainJSON(s *jsonSchema, v any) (any, error) {
	s, _ = s.nonNull()
	if s == nil || v == nil {
		return v, nil
	}
	if s.Title != "" {
		cv, err := convertValue(field{Name: s.Title, Parameters: s.Params}, v)
		if err != nil {
			return nil, err
		}
		if _, isMap := cv.(map[string]any); !isMap {
			return cv, nil // a logical type: converted
		}
		v = cv
	}
	switch x := v.(type) {
	case map[string]any:
		for k, fv := range x {
			if ps, ok := s.Properties[k]; ok {
				cv, err := plainJSON(ps, fv)
				if err != nil {
					return nil, fmt.Errorf("%s: %w", k, err)
				}
				x[k] = cv
			}
		}
	case []any:
		for i := range x {
			cv, err := plainJSON(s.Items, x[i])
			if err != nil {
				return nil, err
			}
			x[i] = cv
		}
	}
	return v, nil
}

func (d jsonSchemaDecoder) columns(keys map[string]bool) ([]metaColumn, bool) {
	root, _ := d.s.nonNull()
	for _, side := range []string{"after", "before"} {
		row, _ := root.Properties[side].nonNull()
		if row == nil || len(row.Properties) == 0 {
			continue
		}
		var out []metaColumn
		for _, name := range row.ordered() {
			col, optional := row.Properties[name].nonNull()
			f := field{Field: name, Type: jsonConnectType(col), Name: col.Title, Optional: optional, Default: col.Default,
				Parameters: col.Params}
			out = append(out, metaColumn{f, keys[name]})
		}
		return out, true
	}
	return nil, false
}

func (d jsonSchemaDecoder) fields() []string {
	root, _ := d.s.nonNull()
	return root.ordered()
}

// jsonConnectType names a column's type as the JSON converter does, so a
// table reads the same through either converter.
func jsonConnectType(s *jsonSchema) string {
	switch s.Connect {
	case "float32":
		return "float"
	case "float64":
		return "double"
	case "":
	default:
		return s.Connect // int8 ... int64, bytes, map
	}
	switch s.Type {
	case "integer":
		return "int64"
	case "number":
		return "double"
	case "object":
		return "struct"
	case "array":
		return "array"
	case "string", "boolean":
		return s.Type.(string)
	}
	return "string"
}
