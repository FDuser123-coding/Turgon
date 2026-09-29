package pubsub

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// The messages as Salesforce's pubsub_api.proto declares them, so the hand
// encoding is checked against a real protobuf implementation.
func descriptors(t *testing.T) protoreflect.FileDescriptor {
	t.Helper()
	str, byt, i32, enum, msg := descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_TYPE_BYTES,
		descriptorpb.FieldDescriptorProto_TYPE_INT32, descriptorpb.FieldDescriptorProto_TYPE_ENUM, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	one, rep := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL, descriptorpb.FieldDescriptorProto_LABEL_REPEATED
	field := func(name string, num int32, typ descriptorpb.FieldDescriptorProto_Type, label descriptorpb.FieldDescriptorProto_Label, typeName string) *descriptorpb.FieldDescriptorProto {
		f := &descriptorpb.FieldDescriptorProto{Name: proto.String(name), Number: proto.Int32(num), Type: typ.Enum(), Label: label.Enum()}
		if typeName != "" {
			f.TypeName = proto.String(typeName)
		}
		return f
	}
	message := func(name string, fields ...*descriptorpb.FieldDescriptorProto) *descriptorpb.DescriptorProto {
		return &descriptorpb.DescriptorProto{Name: proto.String(name), Field: fields}
	}
	file := &descriptorpb.FileDescriptorProto{
		Name: proto.String("pubsub_api.proto"), Package: proto.String("eventbus.v1"), Syntax: proto.String("proto3"),
		EnumType: []*descriptorpb.EnumDescriptorProto{{Name: proto.String("ReplayPreset"), Value: []*descriptorpb.EnumValueDescriptorProto{
			{Name: proto.String("LATEST"), Number: proto.Int32(0)}, {Name: proto.String("EARLIEST"), Number: proto.Int32(1)}, {Name: proto.String("CUSTOM"), Number: proto.Int32(2)}}}},
		MessageType: []*descriptorpb.DescriptorProto{
			message("EventHeader", field("key", 1, str, one, ""), field("value", 2, byt, one, "")),
			message("ProducerEvent", field("id", 1, str, one, ""), field("schema_id", 2, str, one, ""), field("payload", 3, byt, one, ""),
				field("headers", 4, msg, rep, ".eventbus.v1.EventHeader")),
			message("ConsumerEvent", field("event", 1, msg, one, ".eventbus.v1.ProducerEvent"), field("replay_id", 2, byt, one, "")),
			message("FetchRequest", field("topic_name", 1, str, one, ""), field("replay_preset", 2, enum, one, ".eventbus.v1.ReplayPreset"),
				field("replay_id", 3, byt, one, ""), field("num_requested", 4, i32, one, ""), field("auth_refresh", 5, str, one, "")),
			message("FetchResponse", field("events", 1, msg, rep, ".eventbus.v1.ConsumerEvent"), field("latest_replay_id", 2, byt, one, ""),
				field("rpc_id", 3, str, one, ""), field("pending_num_requested", 4, i32, one, "")),
			message("SchemaRequest", field("schema_id", 1, str, one, "")),
			message("SchemaInfo", field("schema_json", 1, str, one, ""), field("schema_id", 2, str, one, ""), field("rpc_id", 3, str, one, "")),
		},
	}
	fd, err := protodesc.NewFile(file, nil)
	if err != nil {
		t.Fatal(err)
	}
	return fd
}

func TestEncodingMatchesTheProto(t *testing.T) {
	fd := descriptors(t)
	md := func(name string) protoreflect.MessageDescriptor { return fd.Messages().ByName(protoreflect.Name(name)) }

	// Ours -> protobuf.
	req := FetchRequest{TopicName: "/data/OpportunityChangeEvent", ReplayPreset: Custom, ReplayID: []byte{0x5f, 1, 2}, NumRequested: 100}
	m := dynamicpb.NewMessage(md("FetchRequest"))
	if err := proto.Unmarshal(req.Encode(), m); err != nil {
		t.Fatal(err)
	}
	get := func(m *dynamicpb.Message, f string) protoreflect.Value {
		return m.Get(m.Descriptor().Fields().ByName(protoreflect.Name(f)))
	}
	if get(m, "topic_name").String() != req.TopicName || get(m, "replay_preset").Enum() != 2 ||
		!bytes.Equal(get(m, "replay_id").Bytes(), req.ReplayID) || get(m, "num_requested").Int() != 100 {
		t.Fatalf("FetchRequest decoded by protobuf: %v", m)
	}

	// Protobuf -> ours, with an unknown field (headers) to skip.
	resp := dynamicpb.NewMessage(md("FetchResponse"))
	events := resp.Mutable(md("FetchResponse").Fields().ByName("events")).List()
	for _, id := range []string{"e1", "e2"} {
		ce := dynamicpb.NewMessage(md("ConsumerEvent"))
		pe := dynamicpb.NewMessage(md("ProducerEvent"))
		pe.Set(md("ProducerEvent").Fields().ByName("id"), protoreflect.ValueOfString(id))
		pe.Set(md("ProducerEvent").Fields().ByName("schema_id"), protoreflect.ValueOfString("s1"))
		pe.Set(md("ProducerEvent").Fields().ByName("payload"), protoreflect.ValueOfBytes([]byte{2, 4, 6}))
		h := dynamicpb.NewMessage(md("EventHeader"))
		h.Set(md("EventHeader").Fields().ByName("key"), protoreflect.ValueOfString("k"))
		pe.Mutable(md("ProducerEvent").Fields().ByName("headers")).List().Append(protoreflect.ValueOfMessage(h))
		ce.Set(md("ConsumerEvent").Fields().ByName("event"), protoreflect.ValueOfMessage(pe))
		ce.Set(md("ConsumerEvent").Fields().ByName("replay_id"), protoreflect.ValueOfBytes([]byte("r-"+id)))
		events.Append(protoreflect.ValueOfMessage(ce))
	}
	resp.Set(md("FetchResponse").Fields().ByName("latest_replay_id"), protoreflect.ValueOfBytes([]byte("r-e2")))
	resp.Set(md("FetchResponse").Fields().ByName("pending_num_requested"), protoreflect.ValueOfInt32(98))
	wire, err := proto.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeFetchResponse(wire)
	if err != nil || len(got.Events) != 2 || got.Events[1].Event.ID != "e2" || got.Events[0].Event.SchemaID != "s1" ||
		!bytes.Equal(got.Events[1].Event.Payload, []byte{2, 4, 6}) || string(got.Events[0].ReplayID) != "r-e1" ||
		string(got.LatestReplayID) != "r-e2" || got.PendingNumRequested != 98 {
		t.Fatalf("FetchResponse: %+v %v", got, err)
	}
	// And our encoding of it reads back through protobuf identically.
	back := dynamicpb.NewMessage(md("FetchResponse"))
	if err := proto.Unmarshal(got.Encode(), back); err != nil {
		t.Fatal(err)
	}
	if back.Get(md("FetchResponse").Fields().ByName("events")).List().Len() != 2 {
		t.Fatalf("re-encoded: %v", back)
	}

	info := dynamicpb.NewMessage(md("SchemaInfo"))
	if err := proto.Unmarshal(SchemaInfo{SchemaJSON: `{"type":"string"}`, SchemaID: "s1"}.Encode(), info); err != nil ||
		get(info, "schema_json").String() != `{"type":"string"}` || get(info, "schema_id").String() != "s1" {
		t.Fatalf("SchemaInfo: %v %v", info, err)
	}
	sr := dynamicpb.NewMessage(md("SchemaRequest"))
	sr.Set(md("SchemaRequest").Fields().ByName("schema_id"), protoreflect.ValueOfString("s9"))
	wire, _ = proto.Marshal(sr)
	if r, err := DecodeSchemaRequest(wire); err != nil || r.SchemaID != "s9" {
		t.Fatalf("SchemaRequest: %+v %v", r, err)
	}
	if _, err := DecodeFetchResponse([]byte{0x0a, 0xff}); err == nil {
		t.Fatal("truncated message accepted")
	}
}
