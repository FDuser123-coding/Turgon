// Package pubsub speaks the part of Salesforce's Pub/Sub API (gRPC service
// eventbus.v1.PubSub) that subscribers use: Subscribe, ManagedSubscribe,
// GetSchema and GetTopic. Its
// few messages are encoded by hand with protowire, which keeps generated
// code and protoc out of the build; field numbers follow Salesforce's
// pubsub_api.proto.
package pubsub

import (
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protowire"
)

// Method names.
const (
	Service   = "eventbus.v1.PubSub"
	Subscribe = "/" + Service + "/Subscribe"
	GetSchema = "/" + Service + "/GetSchema"
	GetTopic  = "/" + Service + "/GetTopic"
	// ManagedSubscribe subscribes through a managed subscription: Salesforce
	// keeps the position the subscriber commits.
	ManagedSubscribe = "/" + Service + "/ManagedSubscribe"
)

// Replay presets.
const (
	Latest   = 0
	Earliest = 1
	Custom   = 2
)

// FetchRequest asks for events (the first on a stream also names the topic
// and where to start).
type FetchRequest struct {
	TopicName    string
	ReplayPreset int
	ReplayID     []byte
	NumRequested int32
}

// ProducerEvent is an event as published: its payload is Avro binary
// written with the schema SchemaID names.
type ProducerEvent struct {
	ID       string
	SchemaID string
	Payload  []byte
}

// ConsumerEvent is an event with the opaque position after it.
type ConsumerEvent struct {
	Event    ProducerEvent
	ReplayID []byte
}

// FetchResponse carries events, or none (a keepalive) with the latest
// position.
type FetchResponse struct {
	Events              []ConsumerEvent
	LatestReplayID      []byte
	RPCID               string
	PendingNumRequested int32
}

// SchemaRequest and SchemaInfo fetch an event schema.
type SchemaRequest struct{ SchemaID string }

type SchemaInfo struct {
	SchemaJSON string
	SchemaID   string
}

// Frame is an encoded message; Codec passes it through gRPC unchanged.
type Frame []byte

// Codec is a gRPC codec for Frames.
type Codec struct{}

func (Codec) Name() string { return "proto" }

func (Codec) Marshal(v any) ([]byte, error) {
	f, ok := v.(*Frame)
	if !ok {
		return nil, fmt.Errorf("pubsub: cannot encode %T", v)
	}
	return *f, nil
}

func (Codec) Unmarshal(data []byte, v any) error {
	f, ok := v.(*Frame)
	if !ok {
		return fmt.Errorf("pubsub: cannot decode into %T", v)
	}
	*f = append((*f)[:0], data...)
	return nil
}

// CallOption makes a call use Codec.
func CallOption() grpc.CallOption { return grpc.ForceCodec(Codec{}) }

// SubscribeStream describes the bidirectional Subscribe stream.
var SubscribeStream = &grpc.StreamDesc{StreamName: "Subscribe", ServerStreams: true, ClientStreams: true}

// ManagedSubscribeStream describes the bidirectional ManagedSubscribe stream.
var ManagedSubscribeStream = &grpc.StreamDesc{StreamName: "ManagedSubscribe", ServerStreams: true, ClientStreams: true}

func appendString(b []byte, num protowire.Number, s string) []byte {
	if s == "" {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendString(b, s)
}

func appendBytes(b []byte, num protowire.Number, v []byte) []byte {
	if len(v) == 0 {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendBytes(b, v)
}

func appendVarint(b []byte, num protowire.Number, v uint64) []byte {
	if v == 0 {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.VarintType)
	return protowire.AppendVarint(b, v)
}

// fields calls fn for each field of a message; fn gets the raw bytes of
// length-delimited fields and the value of varints.
func fields(b []byte, fn func(num protowire.Number, typ protowire.Type, raw []byte, v uint64) error) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return errors.New("pubsub: malformed message")
		}
		b = b[n:]
		var raw []byte
		var v uint64
		switch typ {
		case protowire.BytesType:
			raw, n = protowire.ConsumeBytes(b)
		case protowire.VarintType:
			v, n = protowire.ConsumeVarint(b)
		default:
			n = protowire.ConsumeFieldValue(num, typ, b)
		}
		if n < 0 {
			return errors.New("pubsub: malformed message")
		}
		b = b[n:]
		if err := fn(num, typ, raw, v); err != nil {
			return err
		}
	}
	return nil
}

func (r FetchRequest) Encode() Frame {
	var b []byte
	b = appendString(b, 1, r.TopicName)
	b = appendVarint(b, 2, uint64(r.ReplayPreset))
	b = appendBytes(b, 3, r.ReplayID)
	b = appendVarint(b, 4, uint64(r.NumRequested))
	return b
}

func DecodeFetchRequest(b []byte) (FetchRequest, error) {
	var r FetchRequest
	err := fields(b, func(num protowire.Number, _ protowire.Type, raw []byte, v uint64) error {
		switch num {
		case 1:
			r.TopicName = string(raw)
		case 2:
			r.ReplayPreset = int(v)
		case 3:
			r.ReplayID = append([]byte{}, raw...)
		case 4:
			r.NumRequested = int32(v)
		}
		return nil
	})
	return r, err
}

func (e ProducerEvent) encode() []byte {
	var b []byte
	b = appendString(b, 1, e.ID)
	b = appendString(b, 2, e.SchemaID)
	return appendBytes(b, 3, e.Payload)
}

func (ce ConsumerEvent) encode() []byte {
	var e []byte
	e = protowire.AppendTag(e, 1, protowire.BytesType)
	e = protowire.AppendBytes(e, ce.Event.encode())
	return appendBytes(e, 2, ce.ReplayID)
}

func decodeConsumerEvent(b []byte) (ConsumerEvent, error) {
	var ce ConsumerEvent
	err := fields(b, func(num protowire.Number, _ protowire.Type, raw []byte, _ uint64) error {
		switch num {
		case 1:
			return fields(raw, func(num protowire.Number, _ protowire.Type, raw []byte, _ uint64) error {
				switch num {
				case 1:
					ce.Event.ID = string(raw)
				case 2:
					ce.Event.SchemaID = string(raw)
				case 3:
					ce.Event.Payload = append([]byte{}, raw...)
				}
				return nil
			})
		case 2:
			ce.ReplayID = append([]byte{}, raw...)
		}
		return nil
	})
	return ce, err
}

func appendEvents(b []byte, evs []ConsumerEvent) []byte {
	for _, ev := range evs {
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendBytes(b, ev.encode())
	}
	return b
}

func (r FetchResponse) Encode() Frame {
	b := appendEvents(nil, r.Events)
	b = appendBytes(b, 2, r.LatestReplayID)
	b = appendString(b, 3, r.RPCID)
	b = appendVarint(b, 4, uint64(r.PendingNumRequested))
	return b
}

func DecodeFetchResponse(b []byte) (FetchResponse, error) {
	var r FetchResponse
	err := fields(b, func(num protowire.Number, _ protowire.Type, raw []byte, v uint64) error {
		switch num {
		case 1:
			ce, err := decodeConsumerEvent(raw)
			if err != nil {
				return err
			}
			r.Events = append(r.Events, ce)
		case 2:
			r.LatestReplayID = append([]byte{}, raw...)
		case 3:
			r.RPCID = string(raw)
		case 4:
			r.PendingNumRequested = int32(v)
		}
		return nil
	})
	return r, err
}

func (r SchemaRequest) Encode() Frame { return appendString(nil, 1, r.SchemaID) }

func DecodeSchemaRequest(b []byte) (SchemaRequest, error) {
	var r SchemaRequest
	err := fields(b, func(num protowire.Number, _ protowire.Type, raw []byte, _ uint64) error {
		if num == 1 {
			r.SchemaID = string(raw)
		}
		return nil
	})
	return r, err
}

func (s SchemaInfo) Encode() Frame {
	return appendString(appendString(nil, 1, s.SchemaJSON), 2, s.SchemaID)
}

func DecodeSchemaInfo(b []byte) (SchemaInfo, error) {
	var s SchemaInfo
	err := fields(b, func(num protowire.Number, _ protowire.Type, raw []byte, _ uint64) error {
		switch num {
		case 1:
			s.SchemaJSON = string(raw)
		case 2:
			s.SchemaID = string(raw)
		}
		return nil
	})
	return s, err
}

// TopicRequest and TopicInfo describe a topic and what the caller may do
// with it.
type TopicRequest struct{ TopicName string }

type TopicInfo struct {
	TopicName    string
	CanSubscribe bool
	SchemaID     string
}

func (r TopicRequest) Encode() Frame { return appendString(nil, 1, r.TopicName) }

func DecodeTopicRequest(b []byte) (TopicRequest, error) {
	var r TopicRequest
	err := fields(b, func(num protowire.Number, _ protowire.Type, raw []byte, _ uint64) error {
		if num == 1 {
			r.TopicName = string(raw)
		}
		return nil
	})
	return r, err
}

func (t TopicInfo) Encode() Frame {
	b := appendString(nil, 1, t.TopicName)
	if t.CanSubscribe {
		b = appendVarint(b, 4, 1)
	}
	return appendString(b, 5, t.SchemaID)
}

func DecodeTopicInfo(b []byte) (TopicInfo, error) {
	var t TopicInfo
	err := fields(b, func(num protowire.Number, _ protowire.Type, raw []byte, v uint64) error {
		switch num {
		case 1:
			t.TopicName = string(raw)
		case 4:
			t.CanSubscribe = v != 0
		case 5:
			t.SchemaID = string(raw)
		}
		return nil
	})
	return t, err
}

// ManagedFetchRequest opens a managed subscription (the first names it),
// asks for more events, or commits a position.
type ManagedFetchRequest struct {
	SubscriptionID string
	DeveloperName  string
	NumRequested   int32
	Commit         *CommitReplayRequest
}

// CommitReplayRequest asks Salesforce to keep a position: the last event
// processed, or the latest replay ID of an empty batch.
type CommitReplayRequest struct {
	CommitRequestID string
	ReplayID        []byte
}

// ManagedFetchResponse carries events, or none with the latest position,
// and may answer commits.
type ManagedFetchResponse struct {
	Events              []ConsumerEvent
	LatestReplayID      []byte
	RPCID               string
	PendingNumRequested int32
	Commit              *CommitReplayResponse
}

// CommitReplayResponse answers the latest of one or more commits.
type CommitReplayResponse struct {
	CommitRequestID string
	ReplayID        []byte
	// ErrorCode and ErrorMessage are set for a failed commit (code 2,
	// COMMIT, cannot be retried).
	ErrorCode    int
	ErrorMessage string
	ProcessTime  int64
}

// Failed reports whether the commit failed.
func (c CommitReplayResponse) Failed() bool { return c.ErrorCode != 0 || c.ErrorMessage != "" }

func (c CommitReplayRequest) encode() []byte {
	return appendBytes(appendString(nil, 1, c.CommitRequestID), 2, c.ReplayID)
}

func (r ManagedFetchRequest) Encode() Frame {
	var b []byte
	b = appendString(b, 1, r.SubscriptionID)
	b = appendString(b, 2, r.DeveloperName)
	b = appendVarint(b, 3, uint64(r.NumRequested))
	if r.Commit != nil {
		b = protowire.AppendTag(b, 5, protowire.BytesType)
		b = protowire.AppendBytes(b, r.Commit.encode())
	}
	return b
}

func DecodeManagedFetchRequest(b []byte) (ManagedFetchRequest, error) {
	var r ManagedFetchRequest
	err := fields(b, func(num protowire.Number, _ protowire.Type, raw []byte, v uint64) error {
		switch num {
		case 1:
			r.SubscriptionID = string(raw)
		case 2:
			r.DeveloperName = string(raw)
		case 3:
			r.NumRequested = int32(v)
		case 5:
			c := &CommitReplayRequest{}
			r.Commit = c
			return fields(raw, func(num protowire.Number, _ protowire.Type, raw []byte, _ uint64) error {
				switch num {
				case 1:
					c.CommitRequestID = string(raw)
				case 2:
					c.ReplayID = append([]byte{}, raw...)
				}
				return nil
			})
		}
		return nil
	})
	return r, err
}

func (c CommitReplayResponse) encode() []byte {
	b := appendString(nil, 1, c.CommitRequestID)
	b = appendBytes(b, 2, c.ReplayID)
	if c.Failed() {
		e := appendString(appendVarint(nil, 1, uint64(c.ErrorCode)), 2, c.ErrorMessage)
		b = protowire.AppendTag(b, 3, protowire.BytesType)
		b = protowire.AppendBytes(b, e)
	}
	return appendVarint(b, 4, uint64(c.ProcessTime))
}

func (r ManagedFetchResponse) Encode() Frame {
	b := appendEvents(nil, r.Events)
	b = appendBytes(b, 2, r.LatestReplayID)
	b = appendString(b, 3, r.RPCID)
	b = appendVarint(b, 4, uint64(r.PendingNumRequested))
	if r.Commit != nil {
		b = protowire.AppendTag(b, 5, protowire.BytesType)
		b = protowire.AppendBytes(b, r.Commit.encode())
	}
	return b
}

func DecodeManagedFetchResponse(b []byte) (ManagedFetchResponse, error) {
	var r ManagedFetchResponse
	err := fields(b, func(num protowire.Number, _ protowire.Type, raw []byte, v uint64) error {
		switch num {
		case 1:
			ce, err := decodeConsumerEvent(raw)
			if err != nil {
				return err
			}
			r.Events = append(r.Events, ce)
		case 2:
			r.LatestReplayID = append([]byte{}, raw...)
		case 3:
			r.RPCID = string(raw)
		case 4:
			r.PendingNumRequested = int32(v)
		case 5:
			c := &CommitReplayResponse{}
			r.Commit = c
			return fields(raw, func(num protowire.Number, _ protowire.Type, raw []byte, v uint64) error {
				switch num {
				case 1:
					c.CommitRequestID = string(raw)
				case 2:
					c.ReplayID = append([]byte{}, raw...)
				case 3:
					return fields(raw, func(num protowire.Number, _ protowire.Type, raw []byte, v uint64) error {
						switch num {
						case 1:
							c.ErrorCode = int(v)
						case 2:
							c.ErrorMessage = string(raw)
						}
						return nil
					})
				case 4:
					c.ProcessTime = int64(v)
				}
				return nil
			})
		}
		return nil
	})
	return r, err
}
