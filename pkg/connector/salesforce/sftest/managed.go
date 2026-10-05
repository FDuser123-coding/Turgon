package sftest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/fduser123-coding/turgon/pkg/connector/salesforce/pubsub"
)

// ManagedSubscription is a managed event subscription of the fake org: it
// keeps the replay ID its subscriber last committed.
type ManagedSubscription struct {
	ID, Name, Topic string
	// DefaultReplay (LATEST or EARLIEST) is where a subscription that never
	// committed starts; ErrorRecoveryReplay, where one whose committed
	// position expired restarts.
	DefaultReplay, ErrorRecoveryReplay string
	State                              string // RUN or STOP
	Committed                          []byte
	Commits                            int
}

type managedSubs struct {
	mu          sync.Mutex
	subs        map[string]*ManagedSubscription
	next        int
	failCommits bool
	open        int
}

func (s *Server) managed() *managedSubs {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ms == nil {
		s.ms = &managedSubs{subs: map[string]*ManagedSubscription{}}
	}
	return s.ms
}

// CreateManaged adds a running managed subscription, as an admin would
// with the Tooling API.
func (s *Server) CreateManaged(name, topic, defaultReplay string) {
	m := s.managed()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.next++
	m.subs[name] = &ManagedSubscription{ID: fmt.Sprintf("18x%015d", m.next), Name: name, Topic: topic,
		DefaultReplay: defaultReplay, ErrorRecoveryReplay: "LATEST", State: "RUN"}
}

// Managed returns a copy of a managed subscription.
func (s *Server) Managed(name string) (ManagedSubscription, bool) {
	m := s.managed()
	m.mu.Lock()
	defer m.mu.Unlock()
	sub, ok := m.subs[name]
	if !ok {
		return ManagedSubscription{}, false
	}
	return *sub, true
}

// SetManagedState runs or stops a managed subscription.
func (s *Server) SetManagedState(name, state string) {
	m := s.managed()
	m.mu.Lock()
	defer m.mu.Unlock()
	if sub, ok := m.subs[name]; ok {
		sub.State = state
	}
}

// FailCommits makes every commit fail (true) or succeed again.
func (s *Server) FailCommits(fail bool) {
	m := s.managed()
	m.mu.Lock()
	m.failCommits = fail
	m.mu.Unlock()
}

// ManagedSubscribers returns how many ManagedSubscribe streams are open.
func (s *Server) ManagedSubscribers() int {
	m := s.managed()
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.open
}

var developerNameQuery = regexp.MustCompile(`(?i)^SELECT .+ FROM ManagedEventSubscription(?: WHERE DeveloperName = '([A-Za-z0-9_]+)')?$`)

// tooling serves the Tooling API's ManagedEventSubscription: a query, and
// creating one.
func (s *Server) tooling(w http.ResponseWriter, r *http.Request, parts []string) {
	m := s.managed()
	switch {
	case len(parts) == 5 && parts[4] == "query" && r.Method == http.MethodGet:
		match := developerNameQuery.FindStringSubmatch(r.URL.Query().Get("q"))
		if match == nil {
			writeErr(w, http.StatusBadRequest, "MALFORMED_QUERY", "the fake answers SELECT ... FROM ManagedEventSubscription [WHERE DeveloperName = '...']")
			return
		}
		m.mu.Lock()
		records := []map[string]any{}
		for name, sub := range m.subs {
			if match[1] != "" && name != match[1] {
				continue
			}
			records = append(records, map[string]any{"Id": sub.ID, "DeveloperName": sub.Name, "Metadata": map[string]any{
				"label": sub.Name, "topicName": sub.Topic, "defaultReplay": sub.DefaultReplay,
				"errorRecoveryReplay": sub.ErrorRecoveryReplay, "state": sub.State}})
		}
		m.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"size": len(records), "totalSize": len(records), "done": true, "records": records})
	case len(parts) == 6 && parts[4] == "sobjects" && parts[5] == "ManagedEventSubscription" && r.Method == http.MethodPost:
		var in struct {
			FullName string `json:"FullName"`
			Metadata struct {
				TopicName           string `json:"topicName"`
				DefaultReplay       string `json:"defaultReplay"`
				ErrorRecoveryReplay string `json:"errorRecoveryReplay"`
				State               string `json:"state"`
			} `json:"Metadata"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.FullName == "" || in.Metadata.TopicName == "" {
			writeErr(w, http.StatusBadRequest, "INVALID_INPUT", "FullName and Metadata.topicName are required")
			return
		}
		if _, exists := s.Managed(in.FullName); exists {
			writeErr(w, http.StatusBadRequest, "DUPLICATE_DEVELOPER_NAME", "a managed subscription "+in.FullName+" exists")
			return
		}
		s.CreateManaged(in.FullName, in.Metadata.TopicName, or(in.Metadata.DefaultReplay, "LATEST"))
		m.mu.Lock()
		sub := m.subs[in.FullName]
		sub.ErrorRecoveryReplay, sub.State = or(in.Metadata.ErrorRecoveryReplay, "LATEST"), or(in.Metadata.State, "RUN")
		id := sub.ID
		m.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "success": true, "errors": []any{}})
	default:
		writeErr(w, http.StatusNotFound, "NOT_FOUND", "unknown tooling path")
	}
}

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// managedSubscribe serves ManagedSubscribe: events from the subscription's
// committed position, and commits as the subscriber sends them.
func (s *Server) managedSubscribe(st grpc.ServerStream) error {
	if err := s.authenticate(st.Context()); err != nil {
		return err
	}
	var in pubsub.Frame
	if err := st.RecvMsg(&in); err != nil {
		return err
	}
	first, err := pubsub.DecodeManagedFetchRequest(in)
	if err != nil {
		return err
	}
	m := s.managed()
	m.mu.Lock()
	var sub *ManagedSubscription
	for _, ms := range m.subs {
		if ms.Name == first.DeveloperName || (first.SubscriptionID != "" && ms.ID == first.SubscriptionID) {
			sub = ms
		}
	}
	if sub == nil {
		m.mu.Unlock()
		return status.Error(codes.NotFound, "Managed subscription not found: "+first.DeveloperName)
	}
	if sub.State != "RUN" {
		m.mu.Unlock()
		return status.Error(codes.FailedPrecondition, "Managed subscription "+sub.Name+" is not running")
	}
	committed, topic, def, recovery := sub.Committed, sub.Topic, sub.DefaultReplay, sub.ErrorRecoveryReplay
	m.open++
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.open--
		m.mu.Unlock()
	}()

	ps := s.ps
	ps.mu.Lock()
	evs := ps.topics[topic]
	var after []byte
	start := func(preset string) {
		after = nil
		if preset != "EARLIEST" && len(evs) > 0 {
			after = evs[len(evs)-1].replay
		}
	}
	switch {
	case committed == nil:
		start(def)
	case indexOf(evs, committed) < 0: // expired
		start(recovery)
	default:
		after = committed
	}
	ps.mu.Unlock()

	var credMu sync.Mutex
	credits := int(first.NumRequested)
	errc := make(chan error, 1)
	commits := make(chan pubsub.CommitReplayResponse, 16)
	go func() {
		for {
			var in pubsub.Frame
			if err := st.RecvMsg(&in); err != nil {
				errc <- err
				return
			}
			r, err := pubsub.DecodeManagedFetchRequest(in)
			if err != nil {
				errc <- err
				return
			}
			credMu.Lock()
			credits += int(r.NumRequested)
			credMu.Unlock()
			if c := r.Commit; c != nil {
				resp := pubsub.CommitReplayResponse{CommitRequestID: c.CommitRequestID, ReplayID: c.ReplayID, ProcessTime: time.Now().UnixMilli()}
				ps.mu.Lock()
				known := indexOf(ps.topics[topic], c.ReplayID) >= 0
				ps.mu.Unlock()
				m.mu.Lock()
				switch {
				case m.failCommits:
					resp.ErrorCode, resp.ErrorMessage = 2, "The commit could not be processed."
				case !known:
					resp.ErrorCode, resp.ErrorMessage = 2, "Invalid replay ID for this subscription."
				default:
					sub.Committed = append([]byte{}, c.ReplayID...)
					sub.Commits++
				}
				m.mu.Unlock()
				commits <- resp
			}
		}
	}()
	idle := time.Now()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		var resp pubsub.ManagedFetchResponse
		select {
		case <-errc:
			return nil
		case <-st.Context().Done():
			return nil
		case cr := <-commits:
			resp.Commit = &cr
		case <-tick.C:
			ps.mu.Lock()
			evs := ps.topics[topic]
			from := 0
			if after != nil {
				from = indexOf(evs, after) + 1 // 0 if it expired while subscribed: the oldest kept
			}
			credMu.Lock()
			n := min(len(evs)-from, credits, 3)
			credits -= max(n, 0)
			credMu.Unlock()
			if n > 0 {
				for _, e := range evs[from : from+n] {
					resp.Events = append(resp.Events, pubsub.ConsumerEvent{Event: e.event, ReplayID: e.replay})
				}
				after = evs[from+n-1].replay
				resp.LatestReplayID = after
			} else if time.Since(idle) >= KeepAlive && len(evs) > 0 {
				resp.LatestReplayID = evs[len(evs)-1].replay
				after = resp.LatestReplayID
			}
			ps.mu.Unlock()
			if resp.LatestReplayID == nil {
				continue
			}
			idle = time.Now()
		}
		out := resp.Encode()
		if err := st.SendMsg(&out); err != nil {
			return err
		}
	}
}

func indexOf(evs []psEvent, replay []byte) int {
	for i, e := range evs {
		if bytes.Equal(e.replay, replay) {
			return i
		}
	}
	return -1
}
