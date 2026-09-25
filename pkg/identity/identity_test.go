package identity

import (
	"math"
	"strings"
	"testing"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
)

var fields = []v1alpha1.MatchField{
	{Field: "customerRef", Kind: v1alpha1.MatchEmail},
	{Field: "customerRef", Kind: v1alpha1.MatchDomain},
	{Field: "customerName", Kind: v1alpha1.MatchName},
	{Field: "vatId", Kind: v1alpha1.MatchExact},
}

func attrs(email, name string) Attributes {
	return Extract(map[string]any{"customerRef": email, "customerName": name}, fields)
}

func TestExtractNormalizes(t *testing.T) {
	a := Extract(map[string]any{"customerRef": " Grace@Lovelace-GmbH.example ", "customerName": "Hopper,  Grace ", "vatId": "at u 123 456"}, fields)
	if a["email"] != "grace@lovelace-gmbh.example" || a["domain"] != "lovelace-gmbh.example" || a["name"] != "hopper grace" || a["exact:vatId"] != "ATU123456" {
		t.Fatalf("%v", a)
	}
	// A free mail domain says nothing about the company.
	if b := attrs("someone@gmail.com", ""); b["domain"] != "" || b["email"] == "" {
		t.Fatalf("%v", b)
	}
	if b := attrs("not an email", ""); len(b) != 0 {
		t.Fatalf("%v", b)
	}
}

func TestCompare(t *testing.T) {
	known := attrs("ada@lovelace-gmbh.example", "Ada Lovelace")
	cases := []struct {
		name     string
		other    Attributes
		min, max float64
	}{
		{"same email", attrs("ada@lovelace-gmbh.example", ""), 0.99, 1},
		{"same company domain and name", attrs("a.lovelace@lovelace-gmbh.example", "Ada Lovelace"), 0.99, 1},
		{"colleague: same domain, other name", attrs("grace@lovelace-gmbh.example", "Grace Hopper"), 0.1, 0.5},
		{"stranger", attrs("bob@elsewhere.example", "Bob Smith"), 0, 0.001},
		{"namesake at a free mail provider", attrs("ada.l@gmail.com", "Ada Lovelaice"), 0.2, 0.6},
	}
	for _, c := range cases {
		p, reasons := Compare(c.other, known)
		if p < c.min || p > c.max {
			t.Errorf("%s: %.4f not in [%v, %v] (%v)", c.name, p, c.min, c.max, reasons)
		}
	}
	if _, reasons := Compare(attrs("a.lovelace@lovelace-gmbh.example", "Ada Lovelace"), known); strings.Join(reasons, ",") != "same company email domain,same name" {
		t.Errorf("reasons %v", reasons)
	}
}

func TestSuggestAndDecide(t *testing.T) {
	cands := []Candidate{
		{Master: "C-100", Attributes: attrs("ada@lovelace-gmbh.example", "Ada Lovelace")},
		{Master: "C-100", Attributes: attrs("grace@lovelace-gmbh.example", "Grace Hopper")},
		{Master: "C-200", Attributes: attrs("alan@turing-ltd.example", "Alan Turing")},
	}
	// A known address links automatically.
	s := Suggest(attrs("grace@lovelace-gmbh.example", ""), cands, 3)
	if m, ok := Decide(s, 0.95); !ok || m != "C-100" || len(s) != 1 {
		t.Fatalf("known email: %+v", s)
	}
	// A new colleague at a known company is suggested, not linked.
	s = Suggest(attrs("edsger@lovelace-gmbh.example", "Edsger Dijkstra"), cands, 3)
	if _, ok := Decide(s, 0.95); ok || len(s) != 1 || s[0].Master != "C-100" || s[0].Reasons[0] != "same company email domain" {
		t.Fatalf("colleague: %+v", s)
	}
	// Nothing in common: no suggestions.
	if s := Suggest(attrs("x@nowhere.example", "Nobody"), cands, 3); len(s) != 0 {
		t.Fatalf("stranger: %+v", s)
	}
	// Two masters both likely: ambiguous, so a person decides.
	amb := []Suggestion{{Master: "C-1", Score: 0.97}, {Master: "C-2", Score: 0.6}}
	if _, ok := Decide(amb, 0.95); ok {
		t.Fatal("ambiguous match decided")
	}
}

func TestJaroWinkler(t *testing.T) {
	for _, c := range []struct {
		a, b string
		want float64
	}{
		{"martha", "marhta", 0.961}, {"dwayne", "duane", 0.84}, {"dixon", "dicksonx", 0.813}, {"abc", "abc", 1}, {"abc", "", 0},
	} {
		if got := JaroWinkler(c.a, c.b); math.Abs(got-c.want) > 0.001 {
			t.Errorf("%s/%s = %.3f, want %.3f", c.a, c.b, got, c.want)
		}
	}
}
