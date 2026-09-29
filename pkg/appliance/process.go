package appliance

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/fduser123-coding/turgon/pkg/compiler"
)

// proc is one version of a spec's worker, restarted when it exits.
type proc struct {
	name, digest, level, specPath string
	workflows                     []string
	port                          int
	started                       time.Time
	stopTimeout                   time.Duration

	cancel context.CancelFunc
	done   chan struct{}

	mu       sync.Mutex
	pid      int
	restarts int
	since    time.Time
	lastErr  string
}

type procInfo struct {
	pid, restarts int
	since         time.Time
	lastErr       string
}

func (p *proc) info() procInfo {
	p.mu.Lock()
	defer p.mu.Unlock()
	return procInfo{pid: p.pid, restarts: p.restarts, since: p.since, lastErr: p.lastErr}
}

func (p *proc) lastError() string {
	i := p.info()
	if i.lastErr == "" {
		return "not ready"
	}
	return i.lastErr
}

var readyClient = &http.Client{Timeout: 2 * time.Second}

// ready asks the worker's readiness endpoint.
func (p *proc) ready() bool {
	if p == nil || p.info().pid == 0 {
		return false
	}
	resp, err := readyClient.Get("http://127.0.0.1:" + strconv.Itoa(p.port) + "/readyz")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// stop terminates the worker (SIGTERM, then SIGKILL after the timeout) and
// waits for it. A nil proc is a no-op.
func (p *proc) stop() {
	if p == nil {
		return
	}
	p.cancel()
	<-p.done
}

// start runs a spec's worker and keeps it running until stopped.
func (s *Supervisor) start(parent context.Context, name string, spec *compiler.RuntimeSpec, specPath string) *proc {
	ctx, cancel := context.WithCancel(parent)
	p := &proc{name: name, digest: spec.Metadata.Digest, level: spec.Metadata.Level, specPath: specPath, port: s.port(),
		started: time.Now(), stopTimeout: s.StopTimeout, cancel: cancel, done: make(chan struct{})}
	for _, wf := range spec.Spec.Workflows {
		p.workflows = append(p.workflows, wf.Name)
	}
	args := append([]string{"run", "--spec=" + specPath, "--health-listen=127.0.0.1:" + strconv.Itoa(p.port)}, s.WorkerArgs...)
	go func() {
		defer close(p.done)
		backoff := s.MinBackoff
		for ctx.Err() == nil {
			began := time.Now()
			err := s.runOnce(ctx, p, args)
			if ctx.Err() != nil {
				return
			}
			p.mu.Lock()
			p.pid = 0
			p.restarts++
			p.lastErr = fmt.Sprintf("exited: %v", err)
			p.mu.Unlock()
			if time.Since(began) > 5*s.MaxBackoff {
				backoff = s.MinBackoff // it had been running fine
			}
			s.logf("%s: worker %s exited (%v); restarting in %s", name, short(p.digest), err, backoff)
			t := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
			backoff = min(2*backoff, s.MaxBackoff)
		}
	}()
	return p
}

func (s *Supervisor) runOnce(ctx context.Context, p *proc, args []string) error {
	cmd := exec.CommandContext(ctx, s.Exe, args...)
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = p.stopTimeout
	cmd.SysProcAttr = sysProcAttr()
	out, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return err
	}
	p.mu.Lock()
	p.pid, p.since = cmd.Process.Pid, time.Now()
	p.mu.Unlock()
	prefix := p.name + " " + short(p.digest) + ": "
	var last string
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sc := bufio.NewScanner(out)
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		for sc.Scan() {
			line := sc.Text()
			last = line
			s.logMu.Lock()
			fmt.Fprintln(s.Log, prefix+line)
			s.logMu.Unlock()
		}
		_, _ = io.Copy(io.Discard, out)
	}()
	wg.Wait()
	err = cmd.Wait()
	if err != nil && last != "" {
		err = fmt.Errorf("%w: %s", err, last)
	}
	return err
}
