package engine

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
)

// MaxWebhookBody bounds a delivery (Stripe and Shopify payloads are far
// smaller).
const MaxWebhookBody = 4 << 20

// WebhookHandler receives webhook deliveries at
// POST /webhooks/{endpoint}/{event} for the runtime's webhook events. A
// delivery is verified, then stored in the inbox before it is
// acknowledged, so an acknowledged event is never lost; the dispatcher
// starts its run. Providers retry deliveries that are not acknowledged
// with a 2xx; the inbox drops the repeats. Delivered is called after new
// events are stored, to wake the dispatcher.
func WebhookHandler(rt *Runtime, inbox Inbox, delivered func()) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhooks/{endpoint}/{event}", func(w http.ResponseWriter, r *http.Request) {
		endpoint, event := r.PathValue("endpoint"), r.PathValue("event")
		hook, ok := rt.Webhooks[endpoint][event]
		if !ok {
			webhookReply(w, http.StatusNotFound, "no webhook event "+endpoint+"/"+event+" in this spec")
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxWebhookBody))
		if err != nil {
			webhookReply(w, http.StatusRequestEntityTooLarge, "delivery too large")
			return
		}
		events, err := hook.Receive(r.Header, body, time.Now())
		switch {
		case errors.Is(err, connector.ErrUnauthenticated):
			webhookReply(w, http.StatusUnauthorized, "signature is missing, invalid or expired")
			return
		case err != nil:
			webhookReply(w, http.StatusBadRequest, err.Error())
			return
		}
		n := 0
		if len(events) > 0 {
			// A provider that times out retries; finish storing regardless.
			ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 10*time.Second)
			defer cancel()
			if n, err = inbox.Deliver(ctx, InboxSource(endpoint, event), events); err != nil {
				// Not acknowledged, so the provider delivers it again.
				webhookReply(w, http.StatusServiceUnavailable, "could not store the delivery; retry")
				return
			}
		}
		if n > 0 && delivered != nil {
			delivered()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int{"received": len(events), "new": n})
	})
	return mux
}

func webhookReply(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
