package debezium

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// envelope is a Debezium change event's value.
type envelope struct {
	Op     string
	Before map[string]any
	After  map[string]any
	Source map[string]any
	TsMs   int64
}

// field is a Kafka Connect schema field, as the JSON converter writes it
// with schemas enabled.
type field struct {
	Type       string            `json:"type"`
	Name       string            `json:"name"`
	Field      string            `json:"field"`
	Optional   bool              `json:"optional"`
	Default    any               `json:"default"`
	Parameters map[string]string `json:"parameters"`
	Fields     []field           `json:"fields"`
}

// decodeEnvelope reads a change event written by Kafka Connect's JSON
// converter, with its schema ({"schema": ..., "payload": ...}) or without.
// With the schema, the row's values are converted from Connect's encodings
// to what they mean: a Decimal's base64 bytes to a number, a Date's days
// since 1970 to a date, a MicroTimestamp to a timestamp, a Json column to
// its document. Without it they are passed as Debezium wrote them, so set
// decimal.handling.mode=string and time.precision.mode=connect, or keep
// schemas enabled (Kafka Connect's default).
func decodeEnvelope(value []byte) (envelope, error) {
	var top map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(value))
	dec.UseNumber()
	if err := dec.Decode(&top); err != nil {
		return envelope{}, fmt.Errorf("not a JSON change event: %w (use the JSON converter)", err)
	}
	payload, withSchema := top["payload"]
	var rowSchema []field
	if withSchema && len(top["schema"]) > 0 && string(top["schema"]) != "null" {
		var s field
		if err := json.Unmarshal(top["schema"], &s); err != nil {
			return envelope{}, fmt.Errorf("schema: %w", err)
		}
		for _, f := range s.Fields {
			if (f.Field == "after" || f.Field == "before") && len(f.Fields) > 0 {
				rowSchema = f.Fields
			}
		}
	} else {
		payload = value
	}
	var raw struct {
		Op     string          `json:"op"`
		Before json.RawMessage `json:"before"`
		After  json.RawMessage `json:"after"`
		Source map[string]any  `json:"source"`
		TsMs   json.Number     `json:"ts_ms"`
	}
	dec = json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&raw); err != nil {
		return envelope{}, fmt.Errorf("not a Debezium change event: %w", err)
	}
	if raw.Op == "" || raw.Source == nil {
		return envelope{}, errors.New("not a Debezium change event: op and source are required (is the ExtractNewRecordState transform on? Turgon needs the full envelope)")
	}
	env := envelope{Op: raw.Op, Source: raw.Source}
	env.TsMs, _ = raw.TsMs.Int64()
	var err error
	if env.Before, err = row(raw.Before, rowSchema); err != nil {
		return envelope{}, fmt.Errorf("before: %w", err)
	}
	if env.After, err = row(raw.After, rowSchema); err != nil {
		return envelope{}, fmt.Errorf("after: %w", err)
	}
	return env, nil
}

func row(raw json.RawMessage, schema []field) (map[string]any, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var r map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&r); err != nil {
		return nil, err
	}
	for _, f := range schema {
		v, ok := r[f.Field]
		if !ok || v == nil {
			continue
		}
		cv, err := convertValue(f, v)
		if err != nil {
			return nil, fmt.Errorf("column %s (%s): %w", f.Field, f.Name, err)
		}
		r[f.Field] = cv
	}
	return r, nil
}

// convertValue decodes one value of a Connect logical type.
func convertValue(f field, v any) (any, error) {
	switch f.Name {
	case "org.apache.kafka.connect.data.Decimal":
		s, ok := v.(string)
		if !ok {
			return v, nil // decimal.handling.mode double or string: already plain
		}
		scale, err := strconv.Atoi(f.Parameters["scale"])
		if err != nil {
			return nil, fmt.Errorf("decimal without a scale")
		}
		return decimal(s, scale)
	case "io.debezium.data.VariableScaleDecimal":
		m, ok := v.(map[string]any)
		if !ok {
			return v, nil
		}
		scale, err := toInt(m["scale"])
		if err != nil {
			return nil, err
		}
		s, _ := m["value"].(string)
		return decimal(s, int(scale))
	case "org.apache.kafka.connect.data.Date", "io.debezium.time.Date":
		days, err := toInt(v)
		if err != nil {
			return nil, err
		}
		return time.Unix(days*86400, 0).UTC().Format("2006-01-02"), nil
	case "org.apache.kafka.connect.data.Timestamp", "io.debezium.time.Timestamp":
		ms, err := toInt(v)
		if err != nil {
			return nil, err
		}
		return time.UnixMilli(ms).UTC().Format(time.RFC3339Nano), nil
	case "io.debezium.time.MicroTimestamp":
		us, err := toInt(v)
		if err != nil {
			return nil, err
		}
		return time.UnixMicro(us).UTC().Format(time.RFC3339Nano), nil
	case "io.debezium.time.NanoTimestamp":
		ns, err := toInt(v)
		if err != nil {
			return nil, err
		}
		return time.Unix(0, ns).UTC().Format(time.RFC3339Nano), nil
	case "org.apache.kafka.connect.data.Time", "io.debezium.time.Time":
		ms, err := toInt(v)
		if err != nil {
			return nil, err
		}
		return clock(time.Duration(ms) * time.Millisecond), nil
	case "io.debezium.time.MicroTime":
		us, err := toInt(v)
		if err != nil {
			return nil, err
		}
		return clock(time.Duration(us) * time.Microsecond), nil
	case "io.debezium.time.NanoTime":
		ns, err := toInt(v)
		if err != nil {
			return nil, err
		}
		return clock(time.Duration(ns)), nil
	case "io.debezium.data.Json":
		s, ok := v.(string)
		if !ok {
			return v, nil
		}
		var doc any
		d := json.NewDecoder(strings.NewReader(s))
		d.UseNumber()
		if err := d.Decode(&doc); err != nil {
			return nil, err
		}
		return doc, nil
	}
	return v, nil
}

// decimal turns Connect's unscaled big-endian two's-complement bytes into
// an exact decimal number.
func decimal(b64 string, scale int) (json.Number, error) {
	b, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return "", fmt.Errorf("decimal: %w", err)
	}
	n := new(big.Int).SetBytes(b)
	if len(b) > 0 && b[0]&0x80 != 0 { // negative: two's complement
		n.Sub(n, new(big.Int).Lsh(big.NewInt(1), uint(len(b)*8)))
	}
	s := n.String()
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	if scale > 0 {
		for len(s) <= scale {
			s = "0" + s
		}
		s = s[:len(s)-scale] + "." + s[len(s)-scale:]
	} else if scale < 0 {
		s += strings.Repeat("0", -scale)
	}
	if neg {
		s = "-" + s
	}
	return json.Number(s), nil
}

func toInt(v any) (int64, error) {
	switch n := v.(type) {
	case json.Number:
		return n.Int64()
	case float64:
		return int64(n), nil
	case int:
		return int64(n), nil
	case int32:
		return int64(n), nil
	case int64:
		return n, nil
	}
	return 0, fmt.Errorf("expected a number, got %v", v)
}

func clock(d time.Duration) string {
	t := time.Unix(0, 0).UTC().Add(d)
	s := t.Format("15:04:05.999999999")
	return s
}
