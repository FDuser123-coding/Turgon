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
	i64 := descriptorpb.FieldDescriptorProto_TYPE_INT64
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
			{Name: proto.String("LATEST"), Number: proto.Int32(0)}, {Name: proto.String("EARLIEST"), Number: proto.Int32(1)}, {Name: proto.String("CUSTOM"), Number: proto.Int32(2)}}},
			{Name: proto.String("ErrorCode"), Value: []*descriptorpb.EnumValueDescriptorProto{
				{Name: proto.String("UNKNOWN"), Number: proto.Int32(0)}, {Name: proto.String("PUBLISH"), Number: proto.Int32(1)}, {Name: proto.String("COMMIT"), Number: proto.Int32(2)}}}},
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
			message("Error", field("code", 1, enum, one, ".eventbus.v1.ErrorCode"), field("msg", 2, str, one, "")),
			message("CommitReplayRequest", field("commit_request_id", 1, str, one, ""), field("replay_id", 2, byt, one, "")),
			message("CommitReplayResponse", field("commit_request_id", 1, str, one, ""), field("replay_id", 2, byt, one, ""),
				field("error", 3, msg, one, ".eventbus.v1.Error"), field("process_time", 4, i64, one, "")),
			message("ManagedFetchRequest", field("subscription_id", 1, str, one, ""), field("developer_name", 2, str, one, ""),
				field("num_requested", 3, i32, one, ""), field("auth_refresh", 4, str, one, ""),
				field("commit_replay_id_request", 5, msg, one, ".eventbus.v1.CommitReplayRequest")),
			message("ManagedFetchResponse", field("events", 1, msg, rep, ".eventbus.v1.ConsumerEvent"), field("latest_replay_id", 2, byt, one, ""),
				field("rpc_id", 3, str, one, ""), field("pending_num_requested", 4, i32, one, ""),
				field("commit_response", 5, msg, one, ".eventbus.v1.CommitReplayResponse")),
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

func TestManagedEncodingMatchesTheProto(t *testing.T) {
	fd := descriptors(t)
	md := func(name string) protoreflect.MessageDescriptor { return fd.Messages().ByName(protoreflect.Name(name)) }
	fieldOf := func(m protoreflect.Message, f string) protoreflect.FieldDescriptor {
		return m.Descriptor().Fields().ByName(protoreflect.Name(f))
	}

	// A commit, as ours encodes it, read by protobuf.
	req := ManagedFetchRequest{DeveloperName: "Turgon_Won_Deals", NumRequested: 50, Commit: &CommitReplayRequest{CommitRequestID: "c-1", ReplayID: []byte{0x5f, 9}}}
	m := dynamicpb.NewMessage(md("ManagedFetchRequest"))
	if err := proto.Unmarshal(req.Encode(), m); err != nil {
		t.Fatal(err)
	}
	commit := m.Get(fieldOf(m, "commit_replay_id_request")).Message()
	if m.Get(fieldOf(m, "developer_name")).String() != "Turgon_Won_Deals" || m.Get(fieldOf(m, "num_requested")).Int() != 50 ||
		commit.Get(fieldOf(commit, "commit_request_id")).String() != "c-1" || !bytes.Equal(commit.Get(fieldOf(commit, "replay_id")).Bytes(), []byte{0x5f, 9}) {
		t.Fatalf("ManagedFetchRequest decoded by protobuf: %v", m)
	}
	if back, err := DecodeManagedFetchRequest(req.Encode()); err != nil || back.DeveloperName != req.DeveloperName || back.Commit == nil || back.Commit.CommitRequestID != "c-1" {
		t.Fatalf("ManagedFetchRequest round trip: %+v %v", back, err)
	}

	// A response with an event and a failed commit, encoded by protobuf.
	resp := dynamicpb.NewMessage(md("ManagedFetchResponse"))
	ce := dynamicpb.NewMessage(md("ConsumerEvent"))
	pe := dynamicpb.NewMessage(md("ProducerEvent"))
	pe.Set(fieldOf(pe, "id"), protoreflect.ValueOfString("e1"))
	ce.Set(fieldOf(ce, "event"), protoreflect.ValueOfMessage(pe))
	ce.Set(fieldOf(ce, "replay_id"), protoreflect.ValueOfBytes([]byte("r-e1")))
	resp.Mutable(fieldOf(resp, "events")).List().Append(protoreflect.ValueOfMessage(ce))
	resp.Set(fieldOf(resp, "latest_replay_id"), protoreflect.ValueOfBytes([]byte("r-e1")))
	cr := dynamicpb.NewMessage(md("CommitReplayResponse"))
	cr.Set(fieldOf(cr, "commit_request_id"), protoreflect.ValueOfString("c-1"))
	cr.Set(fieldOf(cr, "replay_id"), protoreflect.ValueOfBytes([]byte("r-e0")))
	e := dynamicpb.NewMessage(md("Error"))
	e.Set(fieldOf(e, "code"), protoreflect.ValueOfEnum(2))
	e.Set(fieldOf(e, "msg"), protoreflect.ValueOfString("commit failed"))
	cr.Set(fieldOf(cr, "error"), protoreflect.ValueOfMessage(e))
	cr.Set(fieldOf(cr, "process_time"), protoreflect.ValueOfInt64(1791100000000))
	resp.Set(fieldOf(resp, "commit_response"), protoreflect.ValueOfMessage(cr))
	wire, err := proto.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeManagedFetchResponse(wire)
	if err != nil || len(got.Events) != 1 || got.Events[0].Event.ID != "e1" || string(got.LatestReplayID) != "r-e1" || got.Commit == nil ||
		got.Commit.CommitRequestID != "c-1" || string(got.Commit.ReplayID) != "r-e0" || got.Commit.ErrorCode != 2 ||
		got.Commit.ErrorMessage != "commit failed" || got.Commit.ProcessTime != 1791100000000 || !got.Commit.Failed() {
		t.Fatalf("ManagedFetchResponse: %+v %+v %v", got, got.Commit, err)
	}
	back := dynamicpb.NewMessage(md("ManagedFetchResponse"))
	if err := proto.Unmarshal(got.Encode(), back); err != nil || !proto.Equal(back, resp) {
		t.Fatalf("re-encoded: %v %v", back, err)
	}
}
