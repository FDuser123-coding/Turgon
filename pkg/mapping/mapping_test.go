package mapping

import (
	"encoding/json"
	"reflect"
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
