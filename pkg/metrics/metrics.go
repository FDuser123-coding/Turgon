// Package metrics exposes Turgon's Prometheus metrics: what the write
// guard, the event dispatcher and the workflows do, and the Temporal SDK's
// own metrics through the same registry. Every process serves them at
// /metrics (workers on their health port, the console and the MCP servers
// with --metrics-listen), and the chart's PrometheusRule turns them into
// alerts on each recipe's SLO.
package metrics

import (
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.temporal.io/sdk/client"
)

// Registry holds every Turgon metric.
var Registry = prometheus.NewRegistry()

var latencyBuckets = []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300}

var (
	// Writes counts governed writes by outcome: committed, duplicate,
	// denied, rejected, failed, compensated.
	Writes = counter("turgon_writes_total", "Governed writes by target, operation and outcome.", "target", "operation", "status")
	// WriteDuration is how long target systems take to commit a write.
	WriteDuration = histogram("turgon_write_duration_seconds", "Time the target system takes to commit a write.", "target", "operation")
	// BreakerOpen is 1 while a target's circuit breaker refuses writes.
	BreakerOpen = gauge("turgon_breaker_open", "1 while a target's circuit breaker is open and refuses writes.", "target")
	// Events counts events handed to workflows, by how they arrived.
	Events = counter("turgon_events_total", "Events dispatched to workflows, by arrival (poll or webhook).", "workflow", "via")
	// PollErrors counts failed polls of a source.
	PollErrors = counter("turgon_poll_errors_total", "Failed polls of an event source.", "workflow")
	// LastPoll is when a workflow's source was last polled successfully.
	LastPoll = gauge("turgon_last_poll_success_timestamp_seconds", "When a workflow's event source was last polled successfully.", "workflow")
	// Webhooks counts webhook deliveries by result.
	// CDCRetainedWAL is how much write-ahead log a change capture slot
	// keeps the source database from recycling.
	CDCRetainedWAL = gauge("turgon_cdc_retained_wal_bytes", "Write-ahead log a change capture replication slot holds on the source database.", "slot")
	Webhooks       = counter("turgon_webhooks_total", "Webhook deliveries by result (accepted, ignored, unauthenticated, invalid, error).", "endpoint", "event", "result")
)

func init() {
	Registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
}

func counter(name, help string, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	Registry.MustRegister(c)
	return c
}

func gauge(name, help string, labels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
	Registry.MustRegister(g)
	return g
}

func histogram(name, help string, labels ...string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: latencyBuckets}, labels)
	Registry.MustRegister(h)
	return h
}

// Handler serves the registry in the Prometheus text format.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{Registry: Registry})
}

// Temporal returns a Temporal SDK metrics handler that records into the
// registry: the SDK's own metrics (task latencies, poller counts, workflow
// outcomes by task queue) and those workflows record with
// workflow.GetMetricsHandler, which the SDK suppresses during replay.
func Temporal() client.MetricsHandler { return temporalHandler{vecs: sdkVecs} }

// sdkVecs holds one vector per metric name. Prometheus needs a fixed label
// set per name; a name keeps the tag keys it was first used with, a missing
// tag records as "" and an extra one is dropped.
var sdkVecs = &vecs{byName: map[string]*vec{}}

type vecs struct {
	mu     sync.Mutex
	byName map[string]*vec
}

type vec struct {
	keys      []string
	counter   *prometheus.CounterVec
	gauge     *prometheus.GaugeVec
	histogram *prometheus.HistogramVec
}

var invalid = regexp.MustCompile(`[^a-zA-Z0-9_]`)

func sanitize(s string) string {
	s = invalid.ReplaceAllString(s, "_")
	if s == "" || (s[0] >= '0' && s[0] <= '9') {
		s = "_" + s
	}
	return s
}

func (v *vecs) get(kind, name string, tags map[string]string) (*vec, []string) {
	name = sanitize(name)
	v.mu.Lock()
	defer v.mu.Unlock()
	m, ok := v.byName[kind+" "+name]
	if !ok {
		keys := make([]string, 0, len(tags))
		for k := range tags {
			keys = append(keys, sanitize(k))
		}
		sort.Strings(keys)
		m = &vec{keys: keys}
		var c prometheus.Collector
		switch kind {
		case "counter":
			m.counter = prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: "Temporal SDK or workflow counter " + name + "."}, keys)
			c = m.counter
		case "gauge":
			m.gauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: "Temporal SDK or workflow gauge " + name + "."}, keys)
			c = m.gauge
		default:
			m.histogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name + "_seconds", Help: "Temporal SDK or workflow timer " + name + ".", Buckets: latencyBuckets}, keys)
			c = m.histogram
		}
		if err := Registry.Register(c); err != nil {
			// The name clashes with a metric of another kind: record nowhere.
			m = &vec{}
		}
		v.byName[kind+" "+name] = m
	}
	values := make([]string, len(m.keys))
	byKey := make(map[string]string, len(tags))
	for k, val := range tags {
		byKey[sanitize(k)] = val
	}
	for i, k := range m.keys {
		values[i] = byKey[k]
	}
	return m, values
}

type temporalHandler struct {
	vecs *vecs
	tags map[string]string
}

func (h temporalHandler) WithTags(tags map[string]string) client.MetricsHandler {
	merged := make(map[string]string, len(h.tags)+len(tags))
	for k, v := range h.tags {
		merged[k] = v
	}
	for k, v := range tags {
		merged[k] = v
	}
	return temporalHandler{vecs: h.vecs, tags: merged}
}

func (h temporalHandler) Counter(name string) client.MetricsCounter {
	m, values := h.vecs.get("counter", name, h.tags)
	if m.counter == nil {
		return nop{}
	}
	return counterOf{m.counter.WithLabelValues(values...)}
}

func (h temporalHandler) Gauge(name string) client.MetricsGauge {
	m, values := h.vecs.get("gauge", name, h.tags)
	if m.gauge == nil {
		return nop{}
	}
	return gaugeOf{m.gauge.WithLabelValues(values...)}
}

func (h temporalHandler) Timer(name string) client.MetricsTimer {
	m, values := h.vecs.get("timer", strings.TrimSuffix(name, "_seconds"), h.tags)
	if m.histogram == nil {
		return nop{}
	}
	return timerOf{m.histogram.WithLabelValues(values...)}
}

type counterOf struct{ c prometheus.Counter }

func (c counterOf) Inc(n int64) {
	if n > 0 {
		c.c.Add(float64(n))
	}
}

type gaugeOf struct{ g prometheus.Gauge }

func (g gaugeOf) Update(v float64) { g.g.Set(v) }

type timerOf struct{ h prometheus.Observer }

func (t timerOf) Record(d time.Duration) { t.h.Observe(d.Seconds()) }

type nop struct{}

func (nop) Inc(int64)              {}
func (nop) Update(float64)         {}
func (nop) Record(d time.Duration) {}
