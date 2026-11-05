// Package mapping evaluates JSONata field mappings (architecture §7.4).
// Expressions are compiled once and applied to each source document; the
// result is a new document keyed by target field.
package mapping

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	jsonata "github.com/blues/jsonata-go"
)

// Mapper applies a compiled set of field expressions.
type Mapper struct {
	fields []field
}

type field struct {
	target string
	expr   *jsonata.Expr
	// src is compiled again for every document when the expression applies
	// a function with ~> (see fresh).
	src   string
	fresh bool
}

// evalMu serializes evaluations: jsonata-go (v1.5.4) names the built-in
// function it calls by writing to the one callable every expression shares
// (callableName.SetName), so two documents mapped at once, as a worker's
// concurrent activities do, race on it. An evaluation takes microseconds.
var evalMu sync.Mutex

// fresh reports expressions jsonata-go (v1.5.4) cannot evaluate twice:
// applying a function with ~> inserts the left-hand side into the compiled
// call's arguments in place, so the second document gets one argument too
// many ("$substring takes 3 arguments, got 4"), and concurrent evaluations
// race on the tree. Those are compiled again for each document.
func fresh(src string) bool { return strings.Contains(src, "~>") }

// Compile compiles every expression, reporting all failures at once.
func Compile(fields map[string]string) (*Mapper, error) {
	targets := make([]string, 0, len(fields))
	for t := range fields {
		targets = append(targets, t)
	}
	sort.Strings(targets)
	m := &Mapper{}
	var errs []error
	for _, t := range targets {
		e, err := jsonata.Compile(fields[t])
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", t, err))
			continue
		}
		m.fields = append(m.fields, field{target: t, expr: e, src: fields[t], fresh: fresh(fields[t])})
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return m, nil
}

// Check reports whether a single expression compiles.
func Check(expr string) error {
	_, err := jsonata.Compile(expr)
	return err
}

// FieldError is a field whose expression failed on a document. Its error
// can quote the document's values.
type FieldError struct {
	Field string
	Err   error
}

func (e *FieldError) Error() string { return fmt.Sprintf("mapping field %s: %v", e.Field, e.Err) }
func (e *FieldError) Unwrap() error { return e.Err }

// Apply evaluates every field against doc. Fields whose expression yields
// no result (JSONata "undefined") are omitted, not set to null.
func (m *Mapper) Apply(doc any) (map[string]any, error) {
	evalMu.Lock()
	defer evalMu.Unlock()
	out := make(map[string]any, len(m.fields))
	for _, f := range m.fields {
		expr := f.expr
		if f.fresh {
			e, err := jsonata.Compile(f.src)
			if err != nil {
				return nil, &FieldError{Field: f.target, Err: err}
			}
			expr = e
		}
		v, err := expr.Eval(doc)
		if err != nil {
			if isUndefined(err) {
				continue
			}
			return nil, &FieldError{Field: f.target, Err: err}
		}
		out[f.target] = v
	}
	return out, nil
}

func isUndefined(err error) bool {
	return errors.Is(err, jsonata.ErrUndefined) || strings.Contains(err.Error(), "no results found")
}
