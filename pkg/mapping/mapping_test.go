package mapping

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestApply(t *testing.T) {
	m, err := Compile(map[string]string{
		"externalId": "Id",
		"orderDate":  "$substring(CloseDate, 0, 10)",
		"netValue":   "$number(Amount)",
		"shipping":   "shipping = 'express' ? '01' : '02'",
		"lines":      "Items.{ 'material': Code, 'quantity': Qty }[]",
		"missing":    "NotThere",
	})
	if err != nil {
		t.Fatal(err)
	}
	var doc any
	_ = json.Unmarshal([]byte(`{"Id":"006","CloseDate":"2026-09-24T10:00:00Z","Amount":"1200.5","shipping":"express","Items":[{"Code":"M-1","Qty":2}]}`), &doc)
	got, err := m.Apply(doc)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"externalId": "006",
		"orderDate":  "2026-09-24",
		"netValue":   1200.5,
		"shipping":   "01",
		"lines":      []any{map[string]any{"material": "M-1", "quantity": float64(2)}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %#v\nwant %#v", got, want)
	}
}

func TestCompileReportsBadExpressions(t *testing.T) {
	if _, err := Compile(map[string]string{"a": "Id", "b": "$substring(", "c": "(("}); err == nil {
		t.Fatal("bad expressions compiled")
	}
	if Check("Id & '-x'") != nil {
		t.Fatal("valid expression rejected")
	}
}

// Upstream jsonata-go rewrote a compiled ~> application in place, so a
// mapping that pipes into a function failed from its second document on.
// The patched copy (third_party/jsonata-go) maps every document alike,
// also concurrently.
func TestFunctionApplicationMapsEveryDocument(t *testing.T) {
	m, err := Compile(map[string]string{"doc": "'SHOP' & $string(id) ~> $substring(0, 6)"})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out, err := m.Apply(map[string]any{"id": 1000 + i})
			if err != nil || out["doc"] != "SHOP10" {
				t.Errorf("document %d: %v, %v", i, out, err)
			}
		}(i)
	}
	wg.Wait()
}

// ~> inside a path or $map runs once per element: with upstream jsonata-go
// the second element already got an argument too many. Built-in functions
// that take their context (here $string() on each line's sku) give every
// concurrent evaluation its own.
func TestFunctionApplicationPerElementAndConcurrently(t *testing.T) {
	m, err := Compile(map[string]string{
		"skus":  "lines.(sku ~> $uppercase())",
		"short": "$map(lines, function($l) { $l.sku ~> $substring(0, 3) })",
		"ctx":   "lines.sku.$string()",
	})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			a, b := fmt.Sprintf("abc-%d", i), fmt.Sprintf("xyz-%d", i)
			out, err := m.Apply(map[string]any{"lines": []any{map[string]any{"sku": a}, map[string]any{"sku": b}, map[string]any{"sku": "def"}}})
			want := map[string]any{
				"skus":  []any{strings.ToUpper(a), strings.ToUpper(b), "DEF"},
				"short": []any{"abc", "xyz", "def"},
				"ctx":   []any{a, b, "def"},
			}
			if err != nil || !reflect.DeepEqual(out, want) {
				t.Errorf("document %d: %v, %v", i, out, err)
			}
		}(i)
	}
	wg.Wait()
}
