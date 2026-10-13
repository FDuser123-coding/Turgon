package salesforce

import (
	"io"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// endedStream is a stream the server ended as it opened: sends fail with
// io.EOF and the server's status arrives on receive.
type endedStream struct {
	grpc.ClientStream
	recv error
}

func (s endedStream) SendMsg(any) error { return io.EOF }
func (s endedStream) RecvMsg(any) error { return s.recv }

func TestSendMsgReturnsTheStatusOfAStreamTheServerEnded(t *testing.T) {
	err := sendMsg(endedStream{recv: status.Error(codes.Unauthenticated, "token expired")}, nil)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("err = %v", err)
	}
	// Ended cleanly: the send's io.EOF stands.
	if err := sendMsg(endedStream{recv: io.EOF}, nil); err != io.EOF {
		t.Fatalf("clean end: %v", err)
	}
}
