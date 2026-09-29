package postgres

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// A decoder for the messages of PostgreSQL's pgoutput logical decoding
// plugin, protocol version 1, as returned by
// pg_logical_slot_peek_binary_changes. Only what change capture needs is
// kept; message types it does not use are skipped.

type column struct {
	name string
	oid  uint32
}

type relation struct {
	id        uint32
	namespace string
	name      string
	columns   []column
}

// tupleValue is one column of a row: kind 'n' is null, 'u' an unchanged
// TOASTed value the plugin does not send, 't' text.
type tupleValue struct {
	kind byte
	data []byte
}

type message struct {
	kind byte
	// finalLSN is where the transaction's commit record starts (Begin).
	finalLSN uint64
	// endLSN is where the commit record ends (Commit).
	endLSN uint64
	rel    *relation    // Relation
	relID  uint32       // Insert, Update, Delete
	newRow []tupleValue // Insert, Update
	oldRow []tupleValue // Update (with replica identity FULL), Delete
}

type reader struct {
	b   []byte
	err error
}

var errShort = errors.New("pgoutput: message too short")

func (r *reader) take(n int) []byte {
	if r.err != nil || len(r.b) < n {
		r.err = errShort
		return make([]byte, n)
	}
	out := r.b[:n]
	r.b = r.b[n:]
	return out
}

func (r *reader) u8() byte    { return r.take(1)[0] }
func (r *reader) u16() uint16 { return binary.BigEndian.Uint16(r.take(2)) }
func (r *reader) u32() uint32 { return binary.BigEndian.Uint32(r.take(4)) }
func (r *reader) u64() uint64 { return binary.BigEndian.Uint64(r.take(8)) }

func (r *reader) str() string {
	if r.err != nil {
		return ""
	}
	for i, c := range r.b {
		if c == 0 {
			s := string(r.b[:i])
			r.b = r.b[i+1:]
			return s
		}
	}
	r.err = errShort
	return ""
}

func (r *reader) tuple() []tupleValue {
	n := int(r.u16())
	out := make([]tupleValue, 0, n)
	for i := 0; i < n && r.err == nil; i++ {
		v := tupleValue{kind: r.u8()}
		switch v.kind {
		case 'n', 'u':
		case 't', 'b':
			v.data = r.take(int(r.u32()))
		default:
			r.err = fmt.Errorf("pgoutput: unknown column kind %q", v.kind)
		}
		out = append(out, v)
	}
	return out
}

func decode(data []byte) (message, error) {
	if len(data) == 0 {
		return message{}, errShort
	}
	r := &reader{b: data[1:]}
	m := message{kind: data[0]}
	switch m.kind {
	case 'B':
		m.finalLSN = r.u64()
	case 'C':
		r.u8()  // flags
		r.u64() // commit LSN
		m.endLSN = r.u64()
	case 'R':
		rel := &relation{id: r.u32(), namespace: r.str(), name: r.str()}
		r.u8() // replica identity setting
		n := int(r.u16())
		for i := 0; i < n && r.err == nil; i++ {
			r.u8() // flags: part of the key
			c := column{name: r.str(), oid: r.u32()}
			r.u32() // type modifier
			rel.columns = append(rel.columns, c)
		}
		m.rel = rel
	case 'I':
		m.relID = r.u32()
		if k := r.u8(); k != 'N' {
			return m, fmt.Errorf("pgoutput: insert: unexpected tuple kind %q", k)
		}
		m.newRow = r.tuple()
	case 'U':
		m.relID = r.u32()
		k := r.u8()
		if k == 'K' || k == 'O' {
			m.oldRow = r.tuple()
			k = r.u8()
		}
		if k != 'N' {
			return m, fmt.Errorf("pgoutput: update: unexpected tuple kind %q", k)
		}
		m.newRow = r.tuple()
	case 'D':
		m.relID = r.u32()
		if k := r.u8(); k != 'K' && k != 'O' {
			return m, fmt.Errorf("pgoutput: delete: unexpected tuple kind %q", k)
		}
		m.oldRow = r.tuple()
	default:
		// Origin, Type, Truncate and logical messages: not events.
	}
	return m, r.err
}
