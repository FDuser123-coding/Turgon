// Package audit implements Porter's append-only, hash-chained audit log
// (architecture §9). Every write, approval and policy decision is recorded;
// each entry commits to the one before it, so deleting or editing any entry
// breaks the chain and is detected by Verify.
package audit

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Genesis is the previous-hash value of the first entry.
const Genesis = "0000000000000000000000000000000000000000000000000000000000000000"

// ErrTampered reports a broken chain.
var ErrTampered = errors.New("audit log tampered")

// Entry is one audit record.
type Entry struct {
	Seq    uint64          `json:"seq"`
	Time   time.Time       `json:"time"`
	Actor  string          `json:"actor"`
	Action string          `json:"action"`
	Data   json.RawMessage `json:"data,omitempty"`
	Prev   string          `json:"prev"`
	Hash   string          `json:"hash"`
}

// computeHash hashes every field except Hash itself.
func computeHash(e Entry) string {
	e.Hash = ""
	b, err := json.Marshal(e)
	if err != nil {
		panic(fmt.Sprintf("audit: entry not serializable: %v", err))
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Recorder is the interface other packages depend on.
type Recorder interface {
	Record(actor, action string, data any) (Entry, error)
}

// Log appends entries as JSON lines to a writer.
type Log struct {
	mu   sync.Mutex
	w    io.Writer
	seq  uint64
	last string
	now  func() time.Time
}

// New starts a fresh chain on w.
func New(w io.Writer) *Log {
	return &Log{w: w, last: Genesis, now: func() time.Time { return time.Now().UTC() }}
}

// OpenFile opens (or creates) a log file, verifies the existing chain and
// continues it. It refuses to append to a tampered log.
func OpenFile(path string) (*Log, *os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return nil, nil, err
	}
	last, err := Verify(f)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	l := New(f)
	if last != nil {
		l.seq, l.last = last.Seq, last.Hash
	}
	return l, f, nil
}

// SetClock overrides the time source; for tests.
func (l *Log) SetClock(now func() time.Time) { l.now = now }

// Record appends an entry. data is JSON-encoded; it must never contain secrets.
func (l *Log) Record(actor, action string, data any) (Entry, error) {
	var raw json.RawMessage
	if data != nil {
		b, err := json.Marshal(data)
		if err != nil {
			return Entry{}, fmt.Errorf("audit: encode data: %w", err)
		}
		raw = b
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	e := Entry{Seq: l.seq + 1, Time: l.now(), Actor: actor, Action: action, Data: raw, Prev: l.last}
	e.Hash = computeHash(e)
	line, err := json.Marshal(e)
	if err != nil {
		return Entry{}, err
	}
	if _, err := l.w.Write(append(line, '\n')); err != nil {
		return Entry{}, fmt.Errorf("audit: write: %w", err)
	}
	l.seq, l.last = e.Seq, e.Hash
	return e, nil
}

// Head returns the sequence number and hash of the last entry.
func (l *Log) Head() (uint64, string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.seq, l.last
}

// Verify reads a log and checks every link. It returns the last entry, or
// nil for an empty log.
func Verify(r io.Reader) (*Entry, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	prev, seq := Genesis, uint64(0)
	var last *Entry
	for line := 1; sc.Scan(); line++ {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var e Entry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			return last, fmt.Errorf("%w: line %d is not a valid entry: %v", ErrTampered, line, err)
		}
		switch {
		case e.Seq != seq+1:
			return last, fmt.Errorf("%w: line %d has seq %d, want %d", ErrTampered, line, e.Seq, seq+1)
		case e.Prev != prev:
			return last, fmt.Errorf("%w: entry %d does not link to entry %d", ErrTampered, e.Seq, seq)
		case computeHash(e) != e.Hash:
			return last, fmt.Errorf("%w: entry %d content does not match its hash", ErrTampered, e.Seq)
		}
		prev, seq = e.Hash, e.Seq
		last = &e
	}
	return last, sc.Err()
}
