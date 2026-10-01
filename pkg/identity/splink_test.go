package identity

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSplinkClient(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"a bearer token is required"}`))
			return
		}
		switch r.URL.Path {
		case "/healthz":
			_, _ = w.Write([]byte(`{"status":"ok","entities":{"Customer":{"records":10,"masters":6,"prior":0.05,"trained":["m","u"],"trainedAt":"2026-10-01T10:00:00Z"}}}`))
		case "/v1/match":
			_ = json.NewDecoder(r.Body).Decode(&got)
			switch got["entity"] {
			case "Customer":
				_, _ = w.Write([]byte(`{"entity":"Customer","model":{"records":10,"masters":6},"candidates":[
					{"master":"M2","probability":0.4,"matchWeight":-1,"reasons":null},
					{"master":"M1","probability":0.99996,"matchWeight":14.8,"reasons":["same email address"]}]}`))
			case "Supplier":
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"error":"no links for Supplier yet"}`))
			case "Broken":
				_, _ = w.Write([]byte(`{"candidates":[{"master":"","probability":2}]}`))
			default:
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error":"the models are not trained yet"}`))
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	ctx := context.Background()
	s := &Splink{URL: srv.URL + "/", Token: "tok"}

	out, model, err := s.Suggest(ctx, "Customer", Attributes{"email": "ada@acme.io"}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if got["entity"] != "Customer" || got["attributes"].(map[string]any)["email"] != "ada@acme.io" || got["limit"] != float64(3) {
		t.Fatalf("sent %v", got)
	}
	if len(out) != 2 || out[0].Master != "M1" || out[0].Score != 1 || out[0].Reasons[0] != "same email address" || out[1].Reasons == nil {
		t.Fatalf("got %+v", out)
	}
	if model.Records != 10 || model.Masters != 6 {
		t.Fatalf("model %+v", model)
	}
	if out, _, _ := s.Suggest(ctx, "Customer", Attributes{}, 1); len(out) != 1 {
		t.Fatalf("limit: %+v", out)
	}

	if _, _, err := s.Suggest(ctx, "Supplier", Attributes{}, 3); !errors.Is(err, ErrNoSplinkModel) {
		t.Fatalf("no model: %v", err)
	}
	var refused *SplinkError
	if _, _, err := s.Suggest(ctx, "Broken", Attributes{}, 3); !errors.As(err, &refused) {
		t.Fatalf("broken answer: %v", err)
	}
	if _, _, err := s.Suggest(ctx, "Other", Attributes{}, 3); err == nil || errors.As(err, &refused) {
		t.Fatalf("starting: want a retryable error, got %v", err)
	}
	if _, _, err := (&Splink{URL: srv.URL}).Suggest(ctx, "Customer", Attributes{}, 3); !errors.As(err, &refused) || refused.Status != 401 {
		t.Fatalf("no token: %v", err)
	}

	h, err := s.Health(ctx)
	if err != nil || h["Customer"].Masters != 6 || h["Customer"].Trained[1] != "u" {
		t.Fatalf("health %+v %v", h, err)
	}

	srv.Close()
	if _, _, err := s.Suggest(ctx, "Customer", Attributes{}, 3); err == nil || errors.As(err, &refused) {
		t.Fatalf("down: want a retryable error, got %v", err)
	}
}
