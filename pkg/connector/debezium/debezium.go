// Package debezium is Turgon's connector for change capture from databases
// other than Postgres (SQL Server, Oracle, MySQL, Db2, and Postgres too) through
// Debezium: Debezium reads the database's log and writes each row change to
// a Kafka topic, and Turgon consumes the topic.
//
// Each change is stored in Turgon's inbox in one transaction with the Kafka
// offsets to resume after it, so a worker that crashes or restarts resumes
// exactly after what it stored: nothing lost, nothing read twice. Changes
// Debezium itself sends twice (after it restarts, it replays from its last
// committed position) keep their ID, taken from the database log position,
// and the inbox drops the second copy.
package debezium

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/fduser123-coding/turgon/pkg/compiler"
	"github.com/fduser123-coding/turgon/pkg/connector"
	"github.com/fduser123-coding/turgon/pkg/writeguard"
)

// Name is the connector name this package implements.
const Name = "debezium"

// Config is the connection-specific configuration.
type Config struct {
	// Brokers are the Kafka bootstrap servers.
	Brokers []string `json:"brokers"`
	// TLS connects with TLS (the system's roots). Required unless every
	// broker is on a loopback address.
	TLS bool `json:"tls,omitempty"`
	// SASL is plain, scram-sha-256 or scram-sha-512; the connection's
	// secret is then {"username", "password"}.
	SASL string `json:"sasl,omitempty"`
	// SchemaRegistry reads changes Debezium wrote with an Avro converter.
	SchemaRegistry *Registry        `json:"schemaRegistry,omitempty"`
	Events         map[string]Event `json:"events"`
}

// Event is a table's changes, as Debezium writes them to a topic
// (<topic.prefix>.<schema>.<table>, e.g. erp.dbo.Orders).
type Event struct {
	Topic string `json:"topic"`
	// Operations keeps only these changes: create, update, delete, read
	// (a row of Debezium's initial snapshot). Default create, update and
	// delete.
	Operations []string `json:"operations,omitempty"`
	// Match keeps only changes whose row has these column values, e.g.
	// {status: open}.
	Match map[string]any `json:"match,omitempty"`
	// Start is where a subscription without a stored position begins:
	// latest (default: changes from now on) or earliest (everything the
	// topic still holds, the snapshot included).
	Start string `json:"start,omitempty"`
}

var (
	topicRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)
	colRE   = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$#]*$`)
	opCodes = map[string]string{"create": "c", "update": "u", "delete": "d", "read": "r"}
)

func (c Config) validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("brokers are required")
	}
	loopback := true
	for _, b := range c.Brokers {
		host, _, err := net.SplitHostPort(b)
		if err != nil {
			return fmt.Errorf("broker %q must be host:port", b)
		}
		if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			loopback = false
		}
	}
	if !c.TLS && !loopback {
		return errors.New("tls is required for brokers that are not on this host: change data holds business records")
	}
	switch c.SASL {
	case "", "plain", "scram-sha-256", "scram-sha-512":
	default:
		return fmt.Errorf("sasl must be plain, scram-sha-256 or scram-sha-512, got %q", c.SASL)
	}
	if c.SASL == "plain" && !c.TLS {
		return errors.New("sasl plain sends the password as it is: it needs tls")
	}
	if c.SchemaRegistry != nil {
		if err := c.SchemaRegistry.validate(); err != nil {
			return err
		}
	}
	if len(c.Events) == 0 {
		return errors.New("events are required")
	}
	for name, e := range c.Events {
		if !topicRE.MatchString(e.Topic) {
			return fmt.Errorf("event %s: invalid topic %q", name, e.Topic)
		}
		for _, op := range e.Operations {
			if _, ok := opCodes[op]; !ok {
				return fmt.Errorf("event %s: operation must be create, update, delete or read, got %q", name, op)
			}
		}
		for col := range e.Match {
			if !colRE.MatchString(col) {
				return fmt.Errorf("event %s: invalid match column %q", name, col)
			}
		}
		switch e.Start {
		case "", "latest", "earliest":
		default:
			return fmt.Errorf("event %s: start must be latest or earliest", name)
		}
	}
	return nil
}

// Factory builds a connector from its compiled configuration.
func Factory(ctx context.Context, cfg compiler.ConnectorConfig, secrets connector.SecretResolver) (connector.Instance, error) {
	var c Config
	if err := json.Unmarshal(cfg.Config, &c); err != nil {
		return nil, fmt.Errorf("debezium %s: config: %w", cfg.Endpoint, err)
	}
	var secret string
	if c.SASL != "" || (c.SchemaRegistry != nil && c.SchemaRegistry.Auth == "basic") {
		s, err := secrets.Resolve(ctx, cfg.SecretRef)
		if err != nil {
			return nil, fmt.Errorf("debezium %s: %w", cfg.Endpoint, err)
		}
		secret = s
	}
	conn, err := New(c, secret)
	if err != nil {
		return nil, fmt.Errorf("debezium %s: %w", cfg.Endpoint, err)
	}
	return conn, nil
}

// Conn is a connector instance.
type Conn struct {
	cfg  Config
	opts []kgo.Opt
	// reg reads Avro changes; nil without a schema registry.
	reg *registry
}

var (
	_ connector.Instance = (*Conn)(nil)
	_ connector.Source   = (*Conn)(nil)
	_ connector.Streamer = (*Conn)(nil)
	_ connector.Checker  = (*Conn)(nil)
)

// New returns a connector; secret is {"username", "password"} with SASL,
// and holds {"registry": {"username", "password"}} for a registry with
// basic authentication.
func New(cfg Config, secret string) (*Conn, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	var regUser, regPass string
	if r := cfg.SchemaRegistry; r != nil && r.Auth == "basic" {
		var s struct {
			Registry struct{ Username, Password string } `json:"registry"`
		}
		if err := json.Unmarshal([]byte(secret), &s); err != nil || s.Registry.Username == "" || s.Registry.Password == "" {
			return nil, errors.New(`the secret must hold {"registry": {"username": "...", "password": "..."}} for the schema registry's basic auth`)
		}
		regUser, regPass = s.Registry.Username, s.Registry.Password
	}
	opts := []kgo.Opt{kgo.SeedBrokers(cfg.Brokers...), kgo.ClientID("turgon"), kgo.FetchMaxWait(time.Second)}
	if cfg.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	if cfg.SASL != "" {
		var creds struct{ Username, Password string }
		if err := json.Unmarshal([]byte(secret), &creds); err != nil || creds.Username == "" || creds.Password == "" {
			return nil, errors.New(`the secret must be {"username": "...", "password": "..."} for sasl`)
		}
		switch cfg.SASL {
		case "plain":
			opts = append(opts, kgo.SASL(plain.Auth{User: creds.Username, Pass: creds.Password}.AsMechanism()))
		case "scram-sha-256":
			opts = append(opts, kgo.SASL(scram.Auth{User: creds.Username, Pass: creds.Password}.AsSha256Mechanism()))
		case "scram-sha-512":
			opts = append(opts, kgo.SASL(scram.Auth{User: creds.Username, Pass: creds.Password}.AsSha512Mechanism()))
		}
	}
	return &Conn{cfg: cfg, opts: opts, reg: newRegistry(cfg.SchemaRegistry, regUser, regPass)}, nil
}

func (c *Conn) Close() {}

// Debezium only emits events; it writes nothing.

func (c *Conn) Simulate(context.Context, string, json.RawMessage) (json.RawMessage, error) {
	return nil, fmt.Errorf("%w: debezium connections only emit events", writeguard.ErrInvalid)
}

func (c *Conn) Commit(context.Context, string, string, json.RawMessage) (json.RawMessage, error) {
	return nil, fmt.Errorf("%w: debezium connections only emit events", writeguard.ErrInvalid)
}

// Poll is not how changes arrive: they are streamed.
func (c *Conn) Poll(context.Context, string, int64, int) ([]connector.Event, error) {
	return nil, errors.New("debezium: changes arrive over Kafka, not by polling")
}

// Streams reports whether event is one of the connection's tables.
func (c *Conn) Streams(event string) bool {
	_, ok := c.cfg.Events[event]
	return ok
}

// position is where each partition of a topic resumes: the next offset.
type position map[int32]int64

func (p position) encode() []byte {
	b, _ := json.Marshal(p)
	return b
}

// Stream consumes event's topic from resume (partition offsets; nil: from
// Start) and delivers each fetched batch of changes with the offsets to
// resume after it.
func (c *Conn) Stream(ctx context.Context, event string, resume []byte, deliver func([]connector.Event, []byte) error) error {
	ev, ok := c.cfg.Events[event]
	if !ok {
		return fmt.Errorf("debezium: event %q is not configured on this connection", event)
	}
	pos := position{}
	if len(resume) > 0 {
		if err := json.Unmarshal(resume, &pos); err != nil {
			return fmt.Errorf("debezium: stored position %q: %w", resume, err)
		}
	}
	// Partitions without a stored offset start at the topic's start or end,
	// resolved now and stored before anything is read: a worker that
	// restarts before the first change must not start at "latest" again.
	adm, err := kgo.NewClient(c.opts...)
	if err != nil {
		return err
	}
	starts, ends, err := offsets(ctx, kadm.NewClient(adm), ev.Topic)
	adm.Close()
	if err != nil {
		return err
	}
	fresh := false
	for p, end := range ends {
		off, known := pos[p]
		switch {
		case !known && ev.Start == "earliest":
			pos[p], fresh = starts[p], true
		case !known:
			pos[p], fresh = end, true
		case off < starts[p]:
			// Kafka deleted changes this subscription had not read (retention).
			return fmt.Errorf("debezium: %s partition %d: the stored offset %d is before the oldest change Kafka keeps (%d); changes were lost to retention. Re-run Debezium's snapshot or reset the position", ev.Topic, p, off, starts[p])
		}
	}
	if fresh {
		if err := deliver(nil, pos.encode()); err != nil {
			return err
		}
	}
	assign := map[int32]kgo.Offset{}
	for p, off := range pos {
		assign[p] = kgo.NewOffset().At(off)
	}
	cl, err := kgo.NewClient(append(c.opts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{ev.Topic: assign}))...)
	if err != nil {
		return err
	}
	defer cl.Close()
	ops := ev.Operations
	if len(ops) == 0 {
		ops = []string{"create", "update", "delete"}
	}
	keep := map[string]bool{}
	for _, op := range ops {
		keep[opCodes[op]] = true
	}
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var ferr error
		fetches.EachError(func(topic string, p int32, err error) {
			if ferr == nil {
				ferr = fmt.Errorf("debezium: %s partition %d: %w", topic, p, err)
			}
		})
		if ferr != nil {
			return ferr
		}
		var events []connector.Event
		var convErr error
		read := false
		fetches.EachRecord(func(r *kgo.Record) {
			if convErr != nil {
				return
			}
			read = true
			pos[r.Partition] = r.Offset + 1
			e, ok, err := c.convert(ctx, event, r.Key, r.Value, keep, ev.Match)
			if err != nil {
				convErr = fmt.Errorf("debezium: %s partition %d offset %d: %w", r.Topic, r.Partition, r.Offset, err)
				return
			}
			if ok {
				events = append(events, e)
			}
		})
		if convErr != nil {
			return convErr
		}
		if read {
			if err := deliver(events, pos.encode()); err != nil {
				return err
			}
		}
	}
}

// offsets returns each partition's oldest and next offset.
func offsets(ctx context.Context, adm *kadm.Client, topic string) (starts, ends position, err error) {
	s, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, nil, fmt.Errorf("debezium: %s: %w", topic, err)
	}
	e, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, nil, fmt.Errorf("debezium: %s: %w", topic, err)
	}
	starts, ends = position{}, position{}
	var lerr error
	collect := func(into position) func(kadm.ListedOffset) {
		return func(o kadm.ListedOffset) {
			if o.Err != nil {
				if lerr == nil {
					lerr = o.Err
				}
				return
			}
			into[o.Partition] = o.Offset
		}
	}
	s.Each(collect(starts))
	e.Each(collect(ends))
	if lerr != nil {
		return nil, nil, fmt.Errorf("debezium: topic %s: %w (is Debezium capturing the table, with this topic prefix?)", topic, lerr)
	}
	if len(ends) == 0 {
		return nil, nil, fmt.Errorf("debezium: topic %s does not exist (is Debezium capturing the table, with this topic prefix?)", topic)
	}
	return starts, ends, nil
}

// Check connects to the brokers and looks for each event's topic.
func (c *Conn) Check(ctx context.Context) []connector.CheckResult {
	cl, err := kgo.NewClient(c.opts...)
	if err != nil {
		return []connector.CheckResult{connector.Fail("kafka", err.Error(), "")}
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	names := make([]string, 0, len(c.cfg.Events))
	for n := range c.cfg.Events {
		names = append(names, n)
	}
	sort.Strings(names)
	topics := make([]string, 0, len(names))
	for _, n := range names {
		topics = append(topics, c.cfg.Events[n].Topic)
	}
	md, err := adm.Metadata(ctx, topics...)
	if err != nil {
		fix := connector.NetworkFix(err, strings.Join(c.cfg.Brokers, ", "))
		if strings.Contains(strings.ToLower(err.Error()), "sasl") {
			fix = "The brokers rejected the credentials: check sasl and the secret's username and password."
		}
		return []connector.CheckResult{connector.Fail("kafka", err.Error(), fix)}
	}
	out := []connector.CheckResult{connector.Pass("kafka", fmt.Sprintf("%d broker(s)", len(md.Brokers)))}
	if c.reg != nil {
		if n, err := c.reg.subjects(ctx); err != nil {
			fix := connector.NetworkFix(err, c.reg.url)
			if strings.Contains(err.Error(), "HTTP 401") || strings.Contains(err.Error(), "HTTP 403") {
				fix = `The registry rejected the credentials: put its API key and secret in the connection's secret as {"registry": {"username": ..., "password": ...}}.`
			}
			out = append(out, connector.Fail("schema registry", err.Error(), fix))
		} else {
			out = append(out, connector.Pass("schema registry", fmt.Sprintf("%s, %d subject(s)", c.reg.url, n)))
		}
	}
	for _, n := range names {
		t := c.cfg.Events[n].Topic
		d, ok := md.Topics[t]
		switch {
		case !ok || d.Err != nil:
			detail := "no such topic"
			if ok && d.Err != nil {
				detail = d.Err.Error()
			}
			out = append(out, connector.Fail("event "+n, t+": "+detail,
				"Debezium names topics <topic.prefix>.<schema>.<table>: check the prefix, and that table.include.list captures the table. The integration user needs Describe and Read on the topic."))
		default:
			out = append(out, connector.Pass("event "+n, fmt.Sprintf("%s, %d partition(s)", t, len(d.Partitions))))
		}
	}
	return out
}

// convert makes a Debezium change record an event. ok is false for records
// the event does not take (another operation, a tombstone, no match).
func (c *Conn) convert(ctx context.Context, event string, key, value []byte, keep map[string]bool, match map[string]any) (connector.Event, bool, error) {
	if len(value) == 0 {
		return connector.Event{}, false, nil // a tombstone, for log compaction
	}
	env, canonKey, err := c.decode(ctx, key, value)
	if err != nil {
		return connector.Event{}, false, err
	}
	if !keep[env.Op] {
		return connector.Event{}, false, nil
	}
	row := env.After
	if env.Op == "d" {
		row = env.Before
	}
	if row == nil {
		return connector.Event{}, false, fmt.Errorf("a %q change without its row (is the table's replica identity or supplemental logging full?)", env.Op)
	}
	for col, want := range match {
		a, _ := json.Marshal(row[col])
		b, _ := json.Marshal(want)
		if string(a) != string(b) {
			return connector.Event{}, false, nil
		}
	}
	payload := make(map[string]any, len(row)+1)
	for k, v := range row {
		payload[k] = v
	}
	meta := map[string]any{"op": opName(env.Op), "source": sourceSummary(env.Source)}
	if env.Op == "u" && env.Before != nil {
		meta["before"] = env.Before
	}
	if env.TsMs > 0 {
		meta["ts"] = time.UnixMilli(env.TsMs).UTC().Format(time.RFC3339Nano)
	}
	payload["debezium"] = meta
	b, err := json.Marshal(payload)
	if err != nil {
		return connector.Event{}, false, err
	}
	return connector.Event{ID: changeID(env, canonKey), Name: event, Payload: b}, true, nil
}

func opName(op string) string {
	for name, code := range opCodes {
		if code == op {
			return name
		}
	}
	return op
}

// sourceSummary keeps where the change comes from, without log positions.
func sourceSummary(src map[string]any) map[string]any {
	out := map[string]any{}
	for _, k := range []string{"connector", "name", "db", "schema", "table", "snapshot"} {
		if v, ok := src[k]; ok && v != nil {
			out[k] = v
		}
	}
	return out
}

// positionFields are, per Debezium connector, the source fields that place
// a change in the database's log. With the row's key and the operation they
// identify a change, the same when Debezium sends it again.
var positionFields = map[string][]string{
	"postgresql": {"lsn", "txId", "sequence"},
	"mysql":      {"server_id", "file", "pos", "row", "gtid"},
	"mariadb":    {"server_id", "file", "pos", "row", "gtid"},
	"sqlserver":  {"change_lsn", "commit_lsn", "event_serial_no"},
	"oracle":     {"scn", "commit_scn", "txId", "rs_id", "ssn", "redo_thread"},
	"db2":        {"change_lsn", "commit_lsn"},
	"mongodb":    {"ord", "lsid", "txnNumber"},
}

// decode reads a change's envelope, and its key as canonical JSON: Avro
// in the Confluent wire format with a schema registry, JSON otherwise. An
// Avro key gives the same JSON as the JSON converter's, so a change keeps
// its ID if the converter changes.
func (c *Conn) decode(ctx context.Context, key, value []byte) (envelope, []byte, error) {
	if _, _, avro := wireFormat(value); avro {
		if c.reg == nil {
			return envelope{}, nil, errors.New("this change is Avro in the Confluent wire format: set the connection's schemaRegistry")
		}
		_, m, err := c.reg.decodeAvro(ctx, value)
		if err != nil {
			return envelope{}, nil, err
		}
		env, err := avroEnvelope(m)
		if err != nil {
			return envelope{}, nil, err
		}
		canon := canonicalKey(key)
		if _, _, avroKey := wireFormat(key); avroKey {
			_, km, err := c.reg.decodeAvro(ctx, key)
			if err != nil {
				return envelope{}, nil, fmt.Errorf("key: %w", err)
			}
			canon, _ = json.Marshal(km) // map keys are sorted
		}
		return env, canon, nil
	}
	env, err := decodeEnvelope(value)
	return env, canonicalKey(key), err
}

// changeID derives the event ID from the change's log position, key (as
// canonical JSON) and operation.
func changeID(env envelope, key []byte) string {
	h := sha256.New()
	connector, _ := env.Source["connector"].(string)
	fmt.Fprintf(h, "%s\x00%s\x00", connector, env.Op)
	if fields, ok := positionFields[connector]; ok {
		for _, f := range fields {
			b, _ := json.Marshal(env.Source[f])
			h.Write(b)
			h.Write([]byte{0})
		}
	} else {
		// An unknown connector: its whole source block, less the parts that
		// are not positions.
		src := map[string]any{}
		for k, v := range env.Source {
			switch k {
			case "ts_ms", "ts_us", "ts_ns", "version", "snapshot":
			default:
				src[k] = v
			}
		}
		b, _ := json.Marshal(src) // map keys are sorted
		h.Write(b)
	}
	for _, k := range []string{"db", "schema", "table", "collection"} {
		fmt.Fprintf(h, "%v\x00", env.Source[k])
	}
	h.Write(key)
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// canonicalKey is the key's payload (with or without its schema), as JSON
// with sorted fields.
func canonicalKey(key []byte) []byte {
	var k any
	if json.Unmarshal(key, &k) != nil {
		return key
	}
	if m, ok := k.(map[string]any); ok {
		if p, has := m["payload"]; has && m["schema"] != nil {
			k = p
		}
	}
	b, _ := json.Marshal(k)
	return b
}
