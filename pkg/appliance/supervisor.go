// Package appliance runs Turgon on a single host without Kubernetes: a
// supervisor watches a directory of compiled runtime specs and keeps one
// worker process running per valid spec, the way the operator keeps a
// Deployment per Integration.
//
// A spec is checked before it runs: parsed strictly, its digest matched
// and, with trusted keys, its signature verified. One that fails is
// reported and never started, and the worker of the last valid version
// keeps running. A new version starts next to the old one, which is
// stopped only once the new worker is ready; a new version that never
// becomes ready is given up and the old one keeps serving. A worker that
// exits is restarted with backoff. Each version runs from an immutable
// copy of its spec, so editing the directory never changes what a running
// worker reads.
package appliance

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/signing"
)

// Supervisor keeps a worker per spec in Dir.
type Supervisor struct {
	// Dir holds the specs to run, one <name>.json each.
	Dir string
	// StateDir keeps the immutable copies workers run from.
	StateDir string
	// TrustedKeys, if set, admit only specs one of them signed.
	TrustedKeys []ed25519.PublicKey
	// Exe is the turgon binary; WorkerArgs are appended to its "run".
	Exe        string
	WorkerArgs []string
	// HealthBase is the first loopback port given to workers' health
	// endpoints (default 18100).
	HealthBase int
	// Interval between scans of Dir (default 5s); ReadyTimeout is how long
	// a new version may take to become ready (default 2m); StopTimeout how
	// long a worker may take to stop before it is killed (default 30s).
	Interval, ReadyTimeout, StopTimeout time.Duration
	// MinBackoff and MaxBackoff bound restarts of a worker that exits
	// (defaults 1s and 1m).
	MinBackoff, MaxBackoff time.Duration
	// Log receives the supervisor's and the workers' output.
	Log io.Writer

	mu    sync.Mutex
	units map[string]*unit
	logMu sync.Mutex
}

type unit struct {
	name    string
	current *proc
	next    *proc
	// failed is a digest whose worker never became ready; it is not tried
	// again until the file changes.
	failed       string
	failedReason string
	// invalid is why the file in Dir was refused, if it was.
	invalid string
	// fileDigest is the digest of the file last read, verified or not.
	fileDigest string
}

var nameRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

func (s *Supervisor) defaults() {
	if s.HealthBase == 0 {
		s.HealthBase = 18100
	}
	if s.Interval <= 0 {
		s.Interval = 5 * time.Second
	}
	if s.ReadyTimeout <= 0 {
		s.ReadyTimeout = 2 * time.Minute
	}
	if s.StopTimeout <= 0 {
		s.StopTimeout = 30 * time.Second
	}
	if s.MinBackoff <= 0 {
		s.MinBackoff = time.Second
	}
	if s.MaxBackoff <= 0 {
		s.MaxBackoff = time.Minute
	}
	if s.Log == nil {
		s.Log = io.Discard
	}
}

func (s *Supervisor) logf(format string, args ...any) {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	fmt.Fprintf(s.Log, "turgon appliance: "+format+"\n", args...)
}

// Run supervises until ctx ends, then stops every worker.
func (s *Supervisor) Run(ctx context.Context) error {
	s.defaults()
	if err := os.MkdirAll(filepath.Join(s.StateDir, "specs"), 0o750); err != nil {
		return err
	}
	s.mu.Lock()
	if s.units == nil {
		s.units = map[string]*unit{}
	}
	s.mu.Unlock()
	t := time.NewTicker(s.Interval)
	defer t.Stop()
	for {
		s.Reconcile(ctx)
		select {
		case <-ctx.Done():
			s.stopAll()
			return nil
		case <-t.C:
		}
	}
}

// verify reads and checks one spec file.
func (s *Supervisor) verify(path string) (*compiler.RuntimeSpec, []byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	spec, err := compiler.ParseSpec(data)
	if err != nil {
		return nil, data, err
	}
	if len(s.TrustedKeys) > 0 {
		if _, err := signing.Verify(spec, s.TrustedKeys); err != nil {
			return spec, data, err
		}
	}
	return spec, data, nil
}

// Reconcile makes one pass: it reads Dir, starts and swaps versions, and
// stops the workers of removed specs.
func (s *Supervisor) Reconcile(ctx context.Context) {
	s.defaults()
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		s.logf("read %s: %v", s.Dir, err)
		return
	}
	seen := map[string]bool{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".json")
		seen[name] = true
		s.reconcileOne(ctx, name, filepath.Join(s.Dir, e.Name()))
	}
	s.mu.Lock()
	var gone []*unit
	for name, u := range s.units {
		if !seen[name] {
			gone = append(gone, u)
			delete(s.units, name)
		}
	}
	s.mu.Unlock()
	for _, u := range gone {
		s.logf("%s: spec removed; stopping its worker", u.name)
		u.current.stop()
		u.next.stop()
	}
	s.pruneCopies()
}

func (s *Supervisor) unit(name string) *unit {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.units == nil {
		s.units = map[string]*unit{}
	}
	u, ok := s.units[name]
	if !ok {
		u = &unit{name: name}
		s.units[name] = u
	}
	return u
}

func (s *Supervisor) reconcileOne(ctx context.Context, name, path string) {
	u := s.unit(name)
	if !nameRE.MatchString(name) {
		s.setInvalid(u, "", fmt.Sprintf("%q is not a valid name: lower-case letters, digits and -", name))
		return
	}
	spec, data, err := s.verify(path)
	digest := ""
	if spec != nil {
		digest = spec.Metadata.Digest
	}
	if err != nil {
		s.setInvalid(u, digest, err.Error())
		return
	}
	s.mu.Lock()
	if u.invalid != "" {
		s.logf("%s: %s is valid", name, short(digest))
	}
	if u.fileDigest != digest {
		u.failed, u.failedReason = "", "" // a changed file is tried again
	}
	u.invalid, u.fileDigest = "", digest
	cur, nxt, failed := u.current, u.next, u.failed
	s.mu.Unlock()

	switch {
	case cur != nil && cur.digest == digest:
		// Running the file's version. A newer version being tried was
		// replaced in the file: drop it.
		if nxt != nil {
			s.logf("%s: %s is back in place; dropping %s", name, short(digest), short(nxt.digest))
			nxt.stop()
			s.mu.Lock()
			u.next = nil
			s.mu.Unlock()
		}
		return
	case digest == failed:
		return
	case nxt != nil && nxt.digest == digest:
		s.promote(u, nxt)
		return
	}
	// A new version: run it next to the current one.
	if nxt != nil {
		nxt.stop()
	}
	copyPath, err := s.writeCopy(name, digest, data)
	if err != nil {
		s.setInvalid(u, digest, "cannot keep a copy to run: "+err.Error())
		return
	}
	p := s.start(ctx, name, spec, copyPath)
	s.mu.Lock()
	u.next = p
	s.mu.Unlock()
	if cur == nil {
		s.logf("%s: starting %s (level %s)", name, short(digest), spec.Metadata.Level)
	} else {
		s.logf("%s: starting %s next to %s; switching once it is ready", name, short(digest), short(cur.digest))
	}
}

// promote swaps a ready new version in, or gives it up after ReadyTimeout.
func (s *Supervisor) promote(u *unit, nxt *proc) {
	if nxt.ready() {
		s.mu.Lock()
		old := u.current
		u.current, u.next = nxt, nil
		s.mu.Unlock()
		if old != nil {
			s.logf("%s: %s is ready; stopping %s", u.name, short(nxt.digest), short(old.digest))
			old.stop()
		} else {
			s.logf("%s: %s is ready", u.name, short(nxt.digest))
		}
		return
	}
	if time.Since(nxt.started) < s.ReadyTimeout {
		return
	}
	reason := nxt.lastError()
	s.mu.Lock()
	u.next, u.failed, u.failedReason = nil, nxt.digest, reason
	cur := u.current
	s.mu.Unlock()
	if cur != nil {
		s.logf("%s: %s did not become ready in %s (%s); %s keeps running", u.name, short(nxt.digest), s.ReadyTimeout, reason, short(cur.digest))
	} else {
		s.logf("%s: %s did not become ready in %s (%s)", u.name, short(nxt.digest), s.ReadyTimeout, reason)
	}
	nxt.stop()
}

func (s *Supervisor) setInvalid(u *unit, digest, reason string) {
	s.mu.Lock()
	changed := u.invalid != reason
	u.invalid, u.fileDigest = reason, digest
	running := u.current
	s.mu.Unlock()
	if changed {
		if running != nil {
			s.logf("%s: refused: %s; %s keeps running", u.name, reason, short(running.digest))
		} else {
			s.logf("%s: refused: %s", u.name, reason)
		}
	}
}

// writeCopy keeps an immutable copy of a verified spec to run from.
func (s *Supervisor) writeCopy(name, digest string, data []byte) (string, error) {
	path := filepath.Join(s.StateDir, "specs", name+"-"+short(digest)+".json")
	if _, err := os.Stat(path); err == nil {
		return path, nil
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o440); err != nil {
		return "", err
	}
	return path, os.Rename(tmp, path)
}

// pruneCopies deletes spec copies no worker runs from.
func (s *Supervisor) pruneCopies() {
	keep := map[string]bool{}
	s.mu.Lock()
	for _, u := range s.units {
		for _, p := range []*proc{u.current, u.next} {
			if p != nil {
				keep[p.specPath] = true
			}
		}
	}
	s.mu.Unlock()
	files, _ := filepath.Glob(filepath.Join(s.StateDir, "specs", "*.json"))
	for _, f := range files {
		if !keep[f] {
			_ = os.Remove(f)
		}
	}
}

func (s *Supervisor) stopAll() {
	s.mu.Lock()
	units := s.units
	s.units = map[string]*unit{}
	s.mu.Unlock()
	var wg sync.WaitGroup
	for _, u := range units {
		for _, p := range []*proc{u.current, u.next} {
			if p != nil {
				wg.Add(1)
				go func() { defer wg.Done(); p.stop() }()
			}
		}
	}
	wg.Wait()
}

// port returns the lowest health port no worker uses.
func (s *Supervisor) port() int {
	used := map[int]bool{}
	s.mu.Lock()
	for _, u := range s.units {
		for _, p := range []*proc{u.current, u.next} {
			if p != nil {
				used[p.port] = true
			}
		}
	}
	s.mu.Unlock()
	for p := s.HealthBase; ; p++ {
		if !used[p] {
			return p
		}
	}
}

func short(digest string) string {
	d := strings.TrimPrefix(digest, "sha256:")
	if len(d) > 12 {
		d = d[:12]
	}
	return d
}

// Status is a spec's state.
type Status struct {
	Name string `json:"name"`
	// State: running, starting, restarting, refused or stopped.
	State     string   `json:"state"`
	Digest    string   `json:"digest,omitempty"`
	Level     string   `json:"level,omitempty"`
	Workflows []string `json:"workflows,omitempty"`
	PID       int      `json:"pid,omitempty"`
	Restarts  int      `json:"restarts"`
	Since     string   `json:"since,omitempty"`
	HealthURL string   `json:"healthURL,omitempty"`
	// Pending is a new version starting next to the running one.
	Pending string `json:"pending,omitempty"`
	// Refused is why the spec file was not accepted; FailedDigest a
	// version that never became ready.
	Refused      string `json:"refused,omitempty"`
	FailedDigest string `json:"failedDigest,omitempty"`
	FailedReason string `json:"failedReason,omitempty"`
	LastError    string `json:"lastError,omitempty"`
}

// Statuses reports every spec, sorted by name.
func (s *Supervisor) Statuses() []Status {
	s.mu.Lock()
	units := make([]*unit, 0, len(s.units))
	for _, u := range s.units {
		units = append(units, u)
	}
	s.mu.Unlock()
	sort.Slice(units, func(i, j int) bool { return units[i].name < units[j].name })
	out := make([]Status, 0, len(units))
	for _, u := range units {
		s.mu.Lock()
		cur, nxt := u.current, u.next
		st := Status{Name: u.name, Refused: u.invalid, FailedDigest: u.failed, FailedReason: u.failedReason}
		s.mu.Unlock()
		p := cur
		if p == nil {
			p = nxt
		}
		switch {
		case p == nil && st.Refused != "":
			st.State = "refused"
		case p == nil:
			st.State = "stopped"
		default:
			info := p.info()
			st.Digest, st.Level, st.Workflows = p.digest, p.level, p.workflows
			st.PID, st.Restarts, st.LastError = info.pid, info.restarts, info.lastErr
			st.HealthURL = fmt.Sprintf("http://127.0.0.1:%d/readyz", p.port)
			if !info.since.IsZero() {
				st.Since = info.since.UTC().Format(time.RFC3339)
			}
			switch {
			case info.pid == 0:
				st.State = "restarting"
			case cur == nil || !cur.ready():
				st.State = "starting"
			default:
				st.State = "running"
			}
		}
		if cur != nil && nxt != nil {
			st.Pending = nxt.digest
		}
		out = append(out, st)
	}
	return out
}
