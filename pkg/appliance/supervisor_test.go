package appliance

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/fduser123-coding/turgon/pkg/catalog"
	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/signing"
	"github.com/fduser123-coding/turgon/pkg/verifier"
)

// The test binary doubles as a fake worker: `run --spec --health-listen`,
// ready unless the control directory says <digest>.noready, crashing at
// once if it says <digest>.crash. It records starts and stops there.
func TestMain(m *testing.M) {
	if os.Getenv("TURGON_FAKE_WORKER") == "1" && len(os.Args) > 1 && os.Args[1] == "run" {
		fakeWorker()
		return
	}
	os.Exit(m.Run())
}

func fakeWorker() {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	spec := fs.String("spec", "", "")
	health := fs.String("health-listen", "", "")
	fs.String("poll", "", "")
	fs.String("trusted-keys", "", "")
	_ = fs.Parse(os.Args[2:])
	data, err := os.ReadFile(*spec)
	if err != nil {
		fmt.Println("cannot read spec:", err)
		os.Exit(2)
	}
	var s compiler.RuntimeSpec
	_ = json.Unmarshal(data, &s)
	d := short(s.Metadata.Digest)
	ctl := os.Getenv("TURGON_FAKE_CONTROL")
	record := func(what string) {
		f, _ := os.OpenFile(filepath.Join(ctl, "log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		fmt.Fprintf(f, "%s %s %d %s\n", what, d, os.Getpid(), strings.Join(os.Args[2:], " "))
		f.Close()
	}
	if _, err := os.Stat(filepath.Join(ctl, d+".crash")); err == nil {
		fmt.Println("connect erp-db: secret openbao://erp-db/dsn: set TURGON_SECRET_ERP_DB_DSN")
		os.Exit(3)
	}
	record("start")
	_, noready := os.Stat(filepath.Join(ctl, d+".noready"))
	srv := &http.Server{Addr: *health, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if noready == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})}
	go func() { _ = srv.ListenAndServe() }()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM)
	<-sig
	record("stop")
	os.Exit(0)
}

var (
	specOnce sync.Once
	specs    map[string][]byte
)

func compiled(t *testing.T, recipe string) []byte {
	t.Helper()
	specOnce.Do(func() {
		specs = map[string][]byte{}
		cat, err := catalog.Load("../../examples")
		if err != nil {
			panic(err)
		}
		for _, r := range []string{"shop-orders-to-erp", "stripe-payments-to-erp", "salesforce-won-deals-to-erp"} {
			obj, _ := cat.Find(r)
			s, _, err := compiler.Compile(cat, obj, verifier.Options{})
			if err != nil {
				panic(err)
			}
			specs[r], _ = json.Marshal(s)
		}
	})
	return specs[recipe]
}

func digestOf(t *testing.T, data []byte) string {
	t.Helper()
	var s compiler.RuntimeSpec
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatal(err)
	}
	return short(s.Metadata.Digest)
}

type harness struct {
	t    *testing.T
	sup  *Supervisor
	dir  string
	ctl  string
	stop func()
}

var healthBase = 19100

func newHarness(t *testing.T) *harness {
	t.Helper()
	ctl := t.TempDir()
	t.Setenv("TURGON_FAKE_WORKER", "1")
	t.Setenv("TURGON_FAKE_CONTROL", ctl)
	healthBase += 50
	h := &harness{t: t, dir: t.TempDir(), ctl: ctl}
	h.sup = &Supervisor{Dir: h.dir, StateDir: t.TempDir(), Exe: os.Args[0], WorkerArgs: []string{"--poll=1s"},
		HealthBase: healthBase, Interval: 30 * time.Millisecond, ReadyTimeout: 1500 * time.Millisecond,
		StopTimeout: 5 * time.Second, MinBackoff: 50 * time.Millisecond, MaxBackoff: 200 * time.Millisecond, Log: testLog{t}}
	return h
}

func (h *harness) run() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = h.sup.Run(ctx); close(done) }()
	h.stop = func() { cancel(); <-done }
	h.t.Cleanup(h.stop)
}

type testLog struct{ t *testing.T }

func (l testLog) Write(p []byte) (int, error) {
	l.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

func (h *harness) put(name string, data []byte) {
	h.t.Helper()
	tmp := filepath.Join(h.dir, "."+name+".tmp")
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		h.t.Fatal(err)
	}
	if err := os.Rename(tmp, filepath.Join(h.dir, name+".json")); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) control(digest, what string) {
	h.t.Helper()
	if err := os.WriteFile(filepath.Join(h.ctl, digest+"."+what), nil, 0o600); err != nil {
		h.t.Fatal(err)
	}
}

// events returns the fake workers' start and stop records.
func (h *harness) events(what, digest string) []string {
	data, _ := os.ReadFile(filepath.Join(h.ctl, "log"))
	var out []string
	for _, l := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(l, what+" "+digest+" ") {
			out = append(out, l)
		}
	}
	return out
}

func (h *harness) status(name string) Status {
	for _, s := range h.sup.Statuses() {
		if s.Name == name {
			return s
		}
	}
	return Status{Name: name, State: "absent"}
}

func (h *harness) eventually(what string, cond func() bool) {
	h.t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if cond() {
			return
		}
	}
	h.t.Fatalf("timed out: %s (status %+v)", what, h.sup.Statuses())
}

func TestSupervisorRunsVerifiedSpecs(t *testing.T) {
	h := newHarness(t)
	shop := compiled(t, "shop-orders-to-erp")
	d1 := digestOf(t, shop)
	h.put("shop", shop)
	h.run()
	h.eventually("shop running", func() bool { return h.status("shop").State == "running" })
	st := h.status("shop")
	if st.Level != "L1" || len(st.Workflows) != 1 || st.PID == 0 || !strings.Contains(st.HealthURL, "127.0.0.1:") {
		t.Fatalf("status: %+v", st)
	}
	if e := h.events("start", d1); len(e) != 1 || !strings.Contains(e[0], "--poll=1s") || !strings.Contains(e[0], "shop-"+d1+".json") {
		t.Fatalf("start: %v", e)
	}
	pid := st.PID

	// A tampered file is refused; the running worker is untouched.
	tampered := strings.Replace(string(shop), `"risk":"high"`, `"risk":"low"`, 1)
	h.put("shop", []byte(tampered))
	h.eventually("refusal", func() bool { return strings.Contains(h.status("shop").Refused, "digest mismatch") })
	if st := h.status("shop"); st.State != "running" || st.PID != pid {
		t.Fatalf("after refusal: %+v", st)
	}
	// So is a field the schema lacks, under an unchanged digest.
	h.put("shop", []byte(strings.Replace(string(shop), `"simulation":"rollback"`, `"simulation":"rollback","simulate":false`, 1)))
	h.eventually("unknown field refused", func() bool { return strings.Contains(h.status("shop").Refused, "unknown field") })

	// A new valid version starts next to the old one, which stops once the
	// new one is ready.
	stripe := compiled(t, "stripe-payments-to-erp")
	d2 := digestOf(t, stripe)
	h.put("shop", stripe)
	h.eventually("swap", func() bool { st := h.status("shop"); return st.State == "running" && short(st.Digest) == d2 })
	h.eventually("old stopped", func() bool { return len(h.events("stop", d1)) == 1 })
	if st := h.status("shop"); st.Refused != "" || st.Pending != "" {
		t.Fatalf("after swap: %+v", st)
	}
	// Only the running version's copy is kept.
	h.eventually("copies pruned", func() bool {
		files, _ := filepath.Glob(filepath.Join(h.sup.StateDir, "specs", "*.json"))
		return len(files) == 1 && strings.HasSuffix(files[0], d2+".json")
	})

	// A worker that exits is restarted.
	pid = h.status("shop").PID
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	h.eventually("restart", func() bool { st := h.status("shop"); return st.State == "running" && st.PID != pid && st.Restarts == 1 })

	// Removing the file stops the worker.
	if err := os.Remove(filepath.Join(h.dir, "shop.json")); err != nil {
		t.Fatal(err)
	}
	h.eventually("removed", func() bool { return h.status("shop").State == "absent" && len(h.events("stop", d2)) == 1 })
}

// A new version that never becomes ready is given up; the old one keeps
// serving, and the same version is not tried again until the file changes.
func TestSupervisorKeepsTheOldVersionWhenTheNewOneFails(t *testing.T) {
	h := newHarness(t)
	shop, stripe, sf := compiled(t, "shop-orders-to-erp"), compiled(t, "stripe-payments-to-erp"), compiled(t, "salesforce-won-deals-to-erp")
	d1, d2, d3 := digestOf(t, shop), digestOf(t, stripe), digestOf(t, sf)
	h.control(d2, "noready")
	h.control(d3, "crash")
	h.put("orders", shop)
	h.run()
	h.eventually("running", func() bool { return h.status("orders").State == "running" })
	pid := h.status("orders").PID

	h.put("orders", stripe)
	h.eventually("pending", func() bool { return short(h.status("orders").Pending) == d2 })
	h.eventually("given up", func() bool { return short(h.status("orders").FailedDigest) == d2 })
	if st := h.status("orders"); st.PID != pid || short(st.Digest) != d1 || st.Pending != "" {
		t.Fatalf("after the failed version: %+v", st)
	}
	time.Sleep(300 * time.Millisecond)
	if n := len(h.events("start", d2)); n != 1 {
		t.Fatalf("the failed version was started %d times", n)
	}
	if n := len(h.events("stop", d2)); n != 1 {
		t.Fatal("the failed version was not stopped")
	}

	// A version whose worker crashes at once: same, with the reason.
	h.put("orders", sf)
	h.eventually("crashing version given up", func() bool { return short(h.status("orders").FailedDigest) == d3 })
	if r := h.status("orders").FailedReason; !strings.Contains(r, "TURGON_SECRET_ERP_DB_DSN") {
		t.Fatalf("failed reason: %q", r)
	}
	if st := h.status("orders"); st.PID != pid {
		t.Fatalf("old worker replaced: %+v", st)
	}

	// Back to the running version's file: nothing to do.
	h.put("orders", shop)
	h.eventually("clear", func() bool { st := h.status("orders"); return st.Pending == "" && short(st.Digest) == d1 })
	if n := len(h.events("start", d1)); n != 1 {
		t.Fatalf("running version restarted: %d starts", n)
	}
}

func TestSupervisorWithTrustedKeys(t *testing.T) {
	h := newHarness(t)
	priv, pub, err := signing.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	h.sup.TrustedKeys, _ = signing.ParsePublicKeys(pub)
	shop := compiled(t, "shop-orders-to-erp")
	h.put("unsigned", shop)
	var s compiler.RuntimeSpec
	_ = json.Unmarshal(shop, &s)
	key, _ := signing.ParsePrivateKey(priv)
	if err := signing.Sign(&s, key); err != nil {
		t.Fatal(err)
	}
	signed, _ := json.Marshal(&s)
	h.put("signed", signed)
	h.put("Bad_Name", signed)
	h.run()
	h.eventually("signed running", func() bool { return h.status("signed").State == "running" })
	if st := h.status("unsigned"); st.State != "refused" || !strings.Contains(st.Refused, "not signed") {
		t.Fatalf("unsigned: %+v", st)
	}
	if st := h.status("Bad_Name"); st.State != "refused" || !strings.Contains(st.Refused, "not a valid name") {
		t.Fatalf("bad name: %+v", st)
	}
	// Stopping the supervisor stops its workers.
	h.stop()
	if n := len(h.events("stop", short(s.Metadata.Digest))); n != 1 {
		t.Fatalf("stops: %d", n)
	}
}
