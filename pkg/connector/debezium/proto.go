package debezium

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// protoDecoder reads records of one Protobuf schema, as Confluent's
// Protobuf converter writes them: after the schema ID, the indexes of the
// record's message in the schema's file, then the message.
type protoDecoder struct {
	id uint32
	fd protoreflect.FileDescriptor
}

// Imports a Protobuf schema of Confluent's converter has; the registry
// normally holds them as references, and these stand in when it does not.
var protoFallbacks = map[string]string{
	"confluent/meta.proto": `syntax = "proto3";
package confluent;
import "google/protobuf/descriptor.proto";
message Meta { string doc = 1; map<string, string> params = 2; repeated string tags = 3; }
extend google.protobuf.FileOptions { Meta file_meta = 1088; }
extend google.protobuf.MessageOptions { Meta message_meta = 1088; }
extend google.protobuf.FieldOptions { Meta field_meta = 1088; }
extend google.protobuf.EnumOptions { Meta enum_meta = 1088; }
extend google.protobuf.EnumValueOptions { Meta enum_value_meta = 1088; }
`,
	"confluent/type/decimal.proto": `syntax = "proto3";
package confluent.type;
message Decimal { bytes value = 1; uint32 precision = 2; int32 scale = 3; }
`,
	"google/type/date.proto": `syntax = "proto3";
package google.type;
message Date { int32 year = 1; int32 month = 2; int32 day = 3; }
`,
	"google/type/timeofday.proto": `syntax = "proto3";
package google.type;
message TimeOfDay { int32 hours = 1; int32 minutes = 2; int32 seconds = 3; int32 nanos = 4; }
`,
}

// fieldMetaNumber is confluent.field_meta's extension number: the field's
// Connect parameters (connect.type, a decimal's precision and scale).
const fieldMetaNumber = 1088

func newProtoDecoder(ctx context.Context, id uint32, schema string, imports map[string]string) (decoder, error) {
	name := fmt.Sprintf("turgon-registry/%d.proto", id)
	files := map[string]string{name: schema}
	for k, v := range protoFallbacks {
		files[k] = v
	}
	for k, v := range imports {
		files[k] = v
	}
	c := protocompile.Compiler{Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
		Accessor: protocompile.SourceAccessorFromMap(files)})}
	res, err := c.Compile(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("protobuf: %w", err)
	}
	return protoDecoder{id, res[0]}, nil
}

// messageIndexes reads the record's message indexes: a count, then each
// index, as zigzag varints; a lone 0 is the file's first message.
func messageIndexes(b []byte) ([]int, []byte, error) {
	n, k := binary.Varint(b)
	if k <= 0 || n < 0 || n > 32 {
		return nil, nil, errors.New("protobuf: bad message indexes")
	}
	b = b[k:]
	if n == 0 {
		return []int{0}, b, nil
	}
	out := make([]int, n)
	for i := range out {
		v, k := binary.Varint(b)
		if k <= 0 || v < 0 {
			return nil, nil, errors.New("protobuf: bad message indexes")
		}
		out[i], b = int(v), b[k:]
	}
	return out, b, nil
}

func (d protoDecoder) message(idx []int) (protoreflect.MessageDescriptor, error) {
	msgs := d.fd.Messages()
	var md protoreflect.MessageDescriptor
	for _, i := range idx {
		if i >= msgs.Len() {
			return nil, fmt.Errorf("protobuf (schema %d): no message at index %v", d.id, idx)
		}
		md = msgs.Get(i)
		msgs = md.Messages()
	}
	return md, nil
}

func (d protoDecoder) decode(body []byte) (map[string]any, error) {
	idx, body, err := messageIndexes(body)
	if err != nil {
		return nil, err
	}
	md, err := d.message(idx)
	if err != nil {
		return nil, err
	}
	msg := dynamicpb.NewMessage(md)
	if err := proto.Unmarshal(body, msg); err != nil {
		return nil, fmt.Errorf("protobuf (schema %d): %w", d.id, err)
	}
	return plainMessage(msg)
}

// plainMessage reads a message as JSON values, by its fields' names. A
// field with presence that is not set is null.
func plainMessage(m protoreflect.Message) (map[string]any, error) {
	out := map[string]any{}
	fields := m.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		if f.HasPresence() && !m.Has(f) {
			out[string(f.Name())] = nil
			continue
		}
		v, err := plainField(f, m.Get(f))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", f.Name(), err)
		}
		out[string(f.Name())] = v
	}
	return out, nil
}

func plainField(f protoreflect.FieldDescriptor, v protoreflect.Value) (any, error) {
	switch {
	case f.IsMap():
		out := map[string]any{}
		var err error
		v.Map().Range(func(k protoreflect.MapKey, mv protoreflect.Value) bool {
			var pv any
			if pv, err = plainSingle(f.MapValue(), mv); err != nil {
				return false
			}
			out[k.String()] = pv
			return true
		})
		return out, err
	case f.IsList():
		l := v.List()
		out := make([]any, l.Len())
		for i := range out {
			pv, err := plainSingle(f, l.Get(i))
			if err != nil {
				return nil, err
			}
			out[i] = pv
		}
		return out, nil
	}
	pv, err := plainSingle(f, v)
	if err != nil {
		return nil, err
	}
	// Debezium's logical types travel as plain numbers and strings; their
	// Connect name, when the converter kept it, says how to read them.
	if name := fieldMeta(f)["connect.name"]; name != "" && f.Kind() != protoreflect.MessageKind {
		return convertValue(field{Name: name, Parameters: fieldMeta(f)}, pv)
	}
	return pv, nil
}

func plainSingle(f protoreflect.FieldDescriptor, v protoreflect.Value) (any, error) {
	switch f.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return plainWellKnown(v.Message())
	case protoreflect.EnumKind:
		if ev := f.Enum().Values().ByNumber(v.Enum()); ev != nil {
			return string(ev.Name()), nil
		}
		return int32(v.Enum()), nil
	case protoreflect.BytesKind:
		return base64.StdEncoding.EncodeToString(v.Bytes()), nil
	}
	return v.Interface(), nil // bool, int32, int64, uint32, uint64, float32, float64, string
}

// plainWellKnown reads the messages Confluent's converter writes Connect's
// logical types as, and wrappers, as their values; others as objects.
func plainWellKnown(m protoreflect.Message) (any, error) {
	md := m.Descriptor()
	get := func(name string) protoreflect.Value { return m.Get(md.Fields().ByName(protoreflect.Name(name))) }
	switch md.FullName() {
	case "confluent.type.Decimal":
		return decimal(base64.StdEncoding.EncodeToString(get("value").Bytes()), int(get("scale").Int()))
	case "google.type.Date":
		return fmt.Sprintf("%04d-%02d-%02d", get("year").Int(), get("month").Int(), get("day").Int()), nil
	case "google.type.TimeOfDay":
		d := time.Duration(get("hours").Int())*time.Hour + time.Duration(get("minutes").Int())*time.Minute +
			time.Duration(get("seconds").Int())*time.Second + time.Duration(get("nanos").Int())
		return clock(d), nil
	case "google.protobuf.Timestamp":
		return time.Unix(get("seconds").Int(), get("nanos").Int()).UTC().Format(time.RFC3339Nano), nil
	case "google.protobuf.StringValue", "google.protobuf.BoolValue", "google.protobuf.Int32Value", "google.protobuf.Int64Value",
		"google.protobuf.UInt32Value", "google.protobuf.UInt64Value", "google.protobuf.FloatValue", "google.protobuf.DoubleValue",
		"google.protobuf.BytesValue":
		f := md.Fields().ByName("value")
		return plainSingle(f, m.Get(f))
	}
	return plainMessage(m)
}

// fieldMeta is a field's confluent.field_meta params, read from its
// options' wire form, so it does not matter how the import was resolved.
func fieldMeta(f protoreflect.FieldDescriptor) map[string]string {
	opts := f.Options()
	if opts == nil {
		return nil
	}
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(opts)
	if err != nil {
		return nil
	}
	var out map[string]string
	walk(b, func(num protowire.Number, meta []byte) {
		if num != fieldMetaNumber {
			return
		}
		walk(meta, func(num protowire.Number, entry []byte) {
			if num != 2 { // params
				return
			}
			var k, v string
			walk(entry, func(num protowire.Number, s []byte) {
				if num == 1 {
					k = string(s)
				} else if num == 2 {
					v = string(s)
				}
			})
			if out == nil {
				out = map[string]string{}
			}
			out[k] = v
		})
	})
	return out
}

// walk calls fn for each length-delimited field of a message's wire form.
func walk(b []byte, fn func(protowire.Number, []byte)) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return
		}
		b = b[n:]
		if typ == protowire.BytesType {
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return
			}
			fn(num, v)
			b = b[n:]
			continue
		}
		n = protowire.ConsumeFieldValue(num, typ, b)
		if n < 0 {
			return
		}
		b = b[n:]
	}
}

// envelopeMessage is the file's message with a before or after row.
func (d protoDecoder) envelopeMessage() (protoreflect.MessageDescriptor, protoreflect.MessageDescriptor) {
	msgs := d.fd.Messages()
	for i := 0; i < msgs.Len(); i++ {
		for _, side := range []protoreflect.Name{"after", "before"} {
			if f := msgs.Get(i).Fields().ByName(side); f != nil && f.Kind() == protoreflect.MessageKind {
				return msgs.Get(i), f.Message()
			}
		}
	}
	return nil, nil
}

func (d protoDecoder) columns(keys map[string]bool) ([]metaColumn, bool) {
	_, row := d.envelopeMessage()
	if row == nil {
		return nil, false
	}
	var out []metaColumn
	fields := row.Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		params := fieldMeta(f)
		c := field{Field: string(f.Name()), Type: protoConnectType(f, params), Name: params["connect.name"],
			Optional: f.HasOptionalKeyword() || wrapper(f), Parameters: params}
		if f.Kind() == protoreflect.MessageKind && !f.IsList() && !f.IsMap() {
			switch f.Message().FullName() {
			case "confluent.type.Decimal":
				c.Name = "org.apache.kafka.connect.data.Decimal"
				c.Parameters = map[string]string{"scale": params["scale"], "connect.decimal.precision": params["precision"]}
			case "google.type.Date":
				c.Name = "org.apache.kafka.connect.data.Date"
			case "google.type.TimeOfDay":
				c.Name = "org.apache.kafka.connect.data.Time"
			case "google.protobuf.Timestamp":
				c.Name = "org.apache.kafka.connect.data.Timestamp"
			}
		}
		out = append(out, metaColumn{c, keys[c.Field]})
	}
	return out, true
}

func (d protoDecoder) fields() []string {
	var out []string
	if d.fd.Messages().Len() == 0 {
		return nil
	}
	fields := d.fd.Messages().Get(0).Fields()
	for i := 0; i < fields.Len(); i++ {
		out = append(out, string(fields.Get(i).Name()))
	}
	return out
}

// wrapper is whether a field is a wrapper message (google.protobuf.*Value),
// as the converter writes an optional column with wrapper.for.nullables. A
// message field has presence whether or not its column is optional, so
// only a wrapper or the optional keyword says it is.
func wrapper(f protoreflect.FieldDescriptor) bool {
	return f.Kind() == protoreflect.MessageKind && f.Message().ParentFile().Path() == "google/protobuf/wrappers.proto"
}

// protoConnectType names a field's type as the JSON converter does.
func protoConnectType(f protoreflect.FieldDescriptor, params map[string]string) string {
	if t := params["connect.type"]; t != "" {
		return t // int8, int16
	}
	switch {
	case f.IsMap():
		return "map"
	case f.IsList():
		return "array"
	}
	switch f.Kind() {
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind, protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return "int32"
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind, protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return "int64"
	case protoreflect.FloatKind:
		return "float"
	case protoreflect.DoubleKind:
		return "double"
	case protoreflect.BoolKind:
		return "boolean"
	case protoreflect.BytesKind:
		return "bytes"
	case protoreflect.EnumKind:
		return "string"
	case protoreflect.MessageKind:
		switch f.Message().FullName() {
		case "google.protobuf.StringValue":
			return "string"
		case "google.protobuf.Int32Value", "google.protobuf.UInt32Value":
			return "int32"
		case "google.protobuf.Int64Value", "google.protobuf.UInt64Value":
			return "int64"
		case "google.protobuf.BoolValue":
			return "boolean"
		case "google.protobuf.DoubleValue":
			return "double"
		case "google.protobuf.FloatValue":
			return "float"
		case "google.protobuf.BytesValue":
			return "bytes"
		}
		return "struct"
	}
	return "string"
}
