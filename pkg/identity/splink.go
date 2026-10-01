package identity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Splink asks a Splink service (deploy/splink) which master records a
// record may be. The service trains a model per entity on the links
// Turgon has confirmed; Turgon still decides what to link.
type Splink struct {
	// URL is the service's base URL, such as http://turgon-splink:8080.
	URL string
	// Token, if set, is sent as a bearer token.
	Token string
	// HTTP defaults to a client with a 20 second timeout.
	HTTP *http.Client
}

// ErrNoSplinkModel means the service has no links for the entity yet, so
// no model: a steward's first links train it.
var ErrNoSplinkModel = errors.New("the Splink service has no model for this entity yet")

// SplinkError is a request the service refused: retrying the same request
// will not help.
type SplinkError struct {
	Status  int
	Message string
}

func (e *SplinkError) Error() string {
	return fmt.Sprintf("the Splink service refused the request (%d): %s", e.Status, e.Message)
}

// SplinkModel describes the model that scored a record.
type SplinkModel struct {
	Records   int      `json:"records"`
	Masters   int      `json:"masters"`
	Prior     float64  `json:"prior"`
	Trained   []string `json:"trained"`
	TrainedAt string   `json:"trainedAt"`
}

type splinkCandidate struct {
	Master      string   `json:"master"`
	Probability float64  `json:"probability"`
	MatchWeight float64  `json:"matchWeight"`
	Reasons     []string `json:"reasons"`
}

func (s *Splink) do(ctx context.Context, method, path string, body any, out any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(s.URL, "/")+path, r)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if s.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.Token)
	}
	hc := s.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 20 * time.Second}
	}
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("the Splink service at %s: %w", s.URL, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("the Splink service at %s: %w", s.URL, err)
	}
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &e)
		switch {
		case resp.StatusCode == http.StatusNotFound && path == "/v1/match" && e.Error != "":
			return ErrNoSplinkModel
		case resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests:
			// Starting, or the database is away: worth retrying.
			return fmt.Errorf("the Splink service at %s answered %d: %s", s.URL, resp.StatusCode, e.Error)
		}
		return &SplinkError{Status: resp.StatusCode, Message: e.Error}
	}
	if err := json.Unmarshal(data, out); err != nil {
		return &SplinkError{Status: resp.StatusCode, Message: "the answer is not JSON: " + err.Error()}
	}
	return nil
}

// Suggest returns at most limit master records the record may be, best
// first, scored by the service's model for the entity.
func (s *Splink) Suggest(ctx context.Context, entity string, a Attributes, limit int) ([]Suggestion, SplinkModel, error) {
	var resp struct {
		Model      SplinkModel       `json:"model"`
		Candidates []splinkCandidate `json:"candidates"`
	}
	err := s.do(ctx, http.MethodPost, "/v1/match", map[string]any{"entity": entity, "attributes": a, "limit": limit}, &resp)
	if err != nil {
		return nil, SplinkModel{}, err
	}
	out := make([]Suggestion, 0, len(resp.Candidates))
	for _, c := range resp.Candidates {
		if c.Master == "" || c.Probability < 0 || c.Probability > 1 {
			return nil, SplinkModel{}, &SplinkError{Status: http.StatusOK, Message: "a candidate has no master or an impossible probability"}
		}
		reasons := c.Reasons
		if reasons == nil {
			reasons = []string{}
		}
		out = append(out, Suggestion{Master: c.Master, Score: math.Round(c.Probability*1000) / 1000, Reasons: reasons})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, resp.Model, nil
}

// Health returns the service's models per entity.
func (s *Splink) Health(ctx context.Context) (map[string]SplinkModel, error) {
	var resp struct {
		Entities map[string]SplinkModel `json:"entities"`
	}
	if err := s.do(ctx, http.MethodGet, "/healthz", nil, &resp); err != nil {
		return nil, err
	}
	return resp.Entities, nil
}
