package rest

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
)

// A feed of changed records names each change: the poll and the webhook
// name an item alike, a record changed again gets a new ID, and
// versioned: false keeps the ID alone.
func TestChangedItemsAreNamedByTheirVersion(t *testing.T) {
	items := `{"things":[{"id":"a","updated":"2026-09-24T10:00:00Z"}]}`
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, items) }))
	defer api.Close()
	off := false
	hook := &WebhookConfig{Signature: "hmac-sha256", SecretRef: "openbao://x/hook", Header: "X-Signature", Items: "things"}
	cfg := Config{BaseURL: api.URL, Auth: Auth{Type: "none"}, Events: map[string]Event{
		"Changed": {Path: "/things", Items: "things", Position: "updated", PositionFormat: "rfc3339", Webhook: hook},
		"Plain":   {Path: "/things", Items: "things", Position: "updated", PositionFormat: "rfc3339", Versioned: &off},
	}}
	c := factory(t, cfg, connector.StaticSecrets{"openbao://x/hook": "s3cret"})
	ctx := context.Background()
	polled, err := c.Poll(ctx, "Changed", 0, 10)
	if err != nil || len(polled) != 1 || polled[0].ID != connector.VersionID("a", polled[0].Position) {
		t.Fatalf("polled %+v, %v", polled, err)
	}
	h, _ := c.Webhook("Changed")
	sign := func(b []byte) http.Header {
		m := hmac.New(sha256.New, []byte("s3cret"))
		m.Write(b)
		return http.Header{"X-Signature": {hex.EncodeToString(m.Sum(nil))}}
	}
	got, err := h.Receive(sign([]byte(items)), []byte(items), time.Now())
	if err != nil || len(got) != 1 || got[0].ID != polled[0].ID {
		t.Fatalf("delivered %+v, polled %s, %v", got, polled[0].ID, err)
	}
	later := []byte(strings.Replace(items, "10:00:00", "10:05:00", 1))
	if got, err := h.Receive(sign(later), later, time.Now()); err != nil || got[0].ID == polled[0].ID || connector.RecordID(got[0].ID) != "a" {
		t.Fatalf("a later change %+v, %v", got, err)
	}
	bare := []byte(`{"things":[{"id":"a"}]}`)
	if _, err := h.Receive(sign(bare), bare, time.Now()); err == nil || !strings.Contains(err.Error(), "names its version") {
		t.Fatalf("an item without its position: %v", err)
	}
	if evs, err := c.Poll(ctx, "Plain", 0, 10); err != nil || len(evs) != 1 || evs[0].ID != "a" {
		t.Errorf("versioned: false: %+v, %v", evs, err)
	}
}

// An API that answers at most maxItems is asked for no more, so a full
// answer is known to be cut short: the timestamp it ends in waits for the
// next poll instead of being split.
func TestMaxItemsCapsAPollAndKeepsTimestampsWhole(t *testing.T) {
	var asked string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked = r.URL.Query().Get("top")
		// The API stops at 2, as Exact Online stops at 60.
		fmt.Fprint(w, `{"d":{"results":[{"id":"a","m":"2026-09-24T10:00:00Z"},{"id":"b","m":"2026-09-24T10:00:01Z"}]}}`)
	}))
	defer api.Close()
	c, err := New(Config{BaseURL: api.URL, Auth: Auth{Type: "none"}, Events: map[string]Event{"Changed": {
		Path: "/x", Params: map[string]string{"top": "{{limit}}"}, Items: "d.results", Position: "m", PositionFormat: "rfc3339", MaxItems: 2,
	}}}, "", http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	evs, err := c.Poll(context.Background(), "Changed", 0, 100)
	if err != nil || asked != "2" || len(evs) != 1 || connector.RecordID(evs[0].ID) != "a" {
		t.Fatalf("asked for %s: %+v, %v", asked, evs, err)
	}
	if _, err := New(Config{BaseURL: api.URL, Auth: Auth{Type: "none"}, Events: map[string]Event{"Changed": {Path: "/x", MaxItems: -1}}}, "", http.DefaultClient); err == nil {
		t.Fatal("a negative maxItems was accepted")
	}
}
