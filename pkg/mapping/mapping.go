// Package mapping evaluates JSONata field mappings (architecture §7.4).
// Expressions are compiled once and applied to each source document; the
// result is a new document keyed by target field.
package mapping

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	jsonata "github.com/blues/jsonata-go"
)

// Mapper applies a compiled set of field expressions.
type Mapper struct {
	fields []field
}

type field struct {
	target string
	expr   *jsonata.Expr
}

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
		m.fields = append(m.fields, field{target: t, expr: e})
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

// Apply evaluates every field against doc. Fields whose expression yields
// no result (JSONata "undefined") are omitted, not set to null.
func (m *Mapper) Apply(doc any) (map[string]any, error) {
	out := make(map[string]any, len(m.fields))
	for _, f := range m.fields {
		v, err := f.expr.Eval(doc)
		if err != nil {
			if isUndefined(err) {
				continue
			}
			return nil, fmt.Errorf("mapping field %s: %w", f.target, err)
		}
		out[f.target] = v
	}
	return out, nil
}

func isUndefined(err error) bool {
	return errors.Is(err, jsonata.ErrUndefined) || strings.Contains(err.Error(), "no results found")
}
