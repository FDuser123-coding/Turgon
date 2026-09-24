package audit

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func fixedClock() func() time.Time {
	t := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	return func() time.Time { t = t.Add(time.Second); return t }
}

func writeLog(t *testing.T, n int) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	l := New(&buf)
	l.SetClock(fixedClock())
	for i := 0; i < n; i++ {
		if _, err := l.Record("agent-7 for alice", "writeback.committed", map[string]any{"doc": 4500000 + i}); err != nil {
			t.Fatal(err)
		}
	}
	return &buf
}

func TestChainVerifies(t *testing.T) {
	buf := writeLog(t, 5)
	last, err := Verify(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if last.Seq != 5 {
		t.Fatalf("last seq = %d", last.Seq)
	}
}

func TestTamperingIsDetected(t *testing.T) {
	lines := strings.Split(strings.TrimSpace(writeLog(t, 5).String()), "\n")
	cases := map[string][]string{
		"edited":         append(append([]string{}, lines[:2]...), append([]string{strings.Replace(lines[2], "4500002", "4500999", 1)}, lines[3:]...)...),
		"deleted":        append(append([]string{}, lines[:2]...), lines[3:]...),
		"reordered":      {lines[0], lines[2], lines[1], lines[3], lines[4]},
		"truncated head": lines[1:],
	}
	for name, ls := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Verify(strings.NewReader(strings.Join(ls, "\n")))
			if !errors.Is(err, ErrTampered) {
				t.Fatalf("err = %v, want ErrTampered", err)
			}
		})
	}
}

func TestOpenFileContinuesChain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	for round := 0; round < 3; round++ {
		l, f, err := OpenFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := l.Record("porter", "test", round); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	f, _ := os.Open(path)
	defer f.Close()
	last, err := Verify(f)
	if err != nil || last.Seq != 3 {
		t.Fatalf("last=%+v err=%v", last, err)
	}

	// A tampered file is never appended to.
	data, _ := os.ReadFile(path)
	os.WriteFile(path, bytes.Replace(data, []byte(`"data":1`), []byte(`"data":7`), 1), 0o600)
	if _, _, err := OpenFile(path); !errors.Is(err, ErrTampered) {
		t.Fatalf("err = %v, want ErrTampered", err)
	}
}
