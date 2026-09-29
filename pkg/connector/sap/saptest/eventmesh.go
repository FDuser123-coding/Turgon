package saptest

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Mesh is a fake SAP Event Mesh instance: the OAuth token endpoint of its
// service key (client credentials) and the queues of its REST messaging
// API. Like Event Mesh, a message consumed with QoS 1 stays on its queue,
// hidden, until it is acknowledged; if it is not within Redeliver, it is
// delivered again.
type Mesh struct {
	ClientID, ClientSecret string
	// Redeliver is how long a consumed message waits for its
	// acknowledgement (default 30s).
	Redeliver time.Duration

	mu     sync.Mutex
	queues map[string]*queue
	tokens map[string]bool
	next   int
	// Consumed and Acked count messages by queue.
	Consumed, Acked map[string]int
}

type queue struct {
	messages []*meshMessage
}

type meshMessage struct {
	id          string
	body        []byte
	contentType string
	lockedUntil time.Time
	deliveries  int
}

// NewMesh returns an instance with the given OAuth client.
func NewMesh(clientID, clientSecret string) *Mesh {
	return &Mesh{ClientID: clientID, ClientSecret: clientSecret, queues: map[string]*queue{}, tokens: map[string]bool{},
		Consumed: map[string]int{}, Acked: map[string]int{}}
}

// TokenPath is where the instance's token endpoint is served.
const TokenPath = "/oauth/token"

// CreateQueue makes a queue, as a person does in the Event Mesh
// application (subscribed to the S/4HANA topics).
func (m *Mesh) CreateQueue(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.queues[name] == nil {
		m.queues[name] = &queue{}
	}
}

// Publish puts a message on a queue.
func (m *Mesh) Publish(name string, body []byte, contentType string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	q := m.queues[name]
	if q == nil {
		q = &queue{}
		m.queues[name] = q
	}
	m.next++
	q.messages = append(q.messages, &meshMessage{id: "ID:" + strconv.Itoa(m.next) + ":" + random(), body: body, contentType: contentType})
}

// Publisher returns a function that publishes S/4HANA business events as
// CloudEvents (structured mode) to a queue, as S/4HANA's event channel does
// through the topics the queue subscribes to: fit for S4.Events.
func (m *Mesh) Publisher(queueName, source string) func(eventType string, data map[string]any) {
	return func(eventType string, data map[string]any) {
		b, _ := json.Marshal(map[string]any{
			"specversion": "1.0", "id": uuid(), "type": eventType, "source": source,
			"time": time.Now().UTC().Format(time.RFC3339Nano), "datacontenttype": "application/json", "data": data,
		})
		m.Publish(queueName, b, "application/json")
	}
}

// Depth returns how many messages a queue holds, acknowledged ones aside.
func (m *Mesh) Depth(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if q := m.queues[name]; q != nil {
		return len(q.messages)
	}
	return 0
}

func (m *Mesh) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == TokenPath {
		m.token(w, r)
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") || !m.tokens[strings.TrimPrefix(auth, "Bearer ")] {
		http.Error(w, `{"message":"Unauthorized"}`, http.StatusUnauthorized)
		return
	}
	// /messagingrest/v1/queues/<escaped name>/messages[/consumption | /<id>/acknowledgement]
	parts := strings.Split(strings.TrimPrefix(r.URL.EscapedPath(), "/messagingrest/v1/queues/"), "/")
	if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/messagingrest/v1/queues/") || len(parts) < 2 || parts[1] != "messages" {
		http.NotFound(w, r)
		return
	}
	name, err := url.PathUnescape(parts[0])
	q := m.queues[name]
	if err != nil || q == nil {
		http.Error(w, `{"message":"Queue not found"}`, http.StatusNotFound)
		return
	}
	switch {
	case len(parts) == 2:
		body := readAll(r)
		m.mu.Unlock()
		m.Publish(name, body, r.Header.Get("Content-Type"))
		m.mu.Lock()
		w.WriteHeader(http.StatusNoContent)
	case len(parts) == 3 && parts[2] == "consumption":
		qos := r.Header.Get("X-Qos")
		if qos != "0" && qos != "1" {
			http.Error(w, `{"message":"x-qos must be 0 or 1"}`, http.StatusBadRequest)
			return
		}
		now := time.Now()
		for i, msg := range q.messages {
			if now.Before(msg.lockedUntil) {
				continue
			}
			msg.deliveries++
			m.Consumed[name]++
			if qos == "0" {
				q.messages = append(q.messages[:i], q.messages[i+1:]...)
			} else {
				redeliver := m.Redeliver
				if redeliver <= 0 {
					redeliver = 30 * time.Second
				}
				msg.lockedUntil = now.Add(redeliver)
			}
			w.Header().Set("X-Message-Id", msg.id)
			w.Header().Set("Content-Type", msg.contentType)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(msg.body)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case len(parts) == 4 && parts[3] == "acknowledgement":
		id, _ := url.PathUnescape(parts[2])
		for i, msg := range q.messages {
			if msg.id == id && !msg.lockedUntil.IsZero() {
				q.messages = append(q.messages[:i], q.messages[i+1:]...)
				m.Acked[name]++
				w.WriteHeader(http.StatusOK)
				return
			}
		}
		http.Error(w, `{"message":"Message not found"}`, http.StatusNotFound)
	default:
		http.NotFound(w, r)
	}
}

// token issues tokens for the client credentials grant, the client
// authenticated in the form or by basic authentication. Like XSUAA it takes
// Basic credentials as they stand, without form-decoding them, so a client
// ID with "!" and "|" (sb-...!b12|xbem-service-broker!b3) that a client
// escaped is rejected.
func (m *Mesh) token(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil || r.Method != http.MethodPost || r.PostForm.Get("grant_type") != "client_credentials" {
		http.Error(w, `{"error":"unsupported_grant_type"}`, http.StatusBadRequest)
		return
	}
	id, secret, ok := r.BasicAuth()
	if !ok {
		id, secret = r.PostForm.Get("client_id"), r.PostForm.Get("client_secret")
	}
	if id != m.ClientID || secret != m.ClientSecret {
		http.Error(w, `{"error":"unauthorized","error_description":"Bad credentials"}`, http.StatusUnauthorized)
		return
	}
	tok := random() + random()
	m.mu.Lock()
	m.tokens[tok] = true
	m.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"access_token": tok, "token_type": "bearer", "expires_in": 43199})
}

// ExpireTokens rejects every token issued so far.
func (m *Mesh) ExpireTokens() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tokens = map[string]bool{}
}

func readAll(r *http.Request) []byte {
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	return b
}

func uuid() string {
	b := []byte(random() + random())
	s := string(b)
	for len(s) < 32 {
		s += "0"
	}
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:32]
}
