// Package identity matches a source record to master records it may be
// (architecture §7.3), the way record-linkage tools such as Splink do: a
// Fellegi-Sunter model scores how much each compared field supports a
// match, and the evidence is combined into a probability.
//
// The records it compares against are the ones already linked: every
// cross-reference keeps the identifying attributes of its source record,
// so each link a data steward confirms improves later matches.
package identity

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode"

	"github.com/fduser123-coding/turgon/apis/v1alpha1"
)

// Attributes are a record's normalized identifying values, keyed by kind
// and field: "email", "domain", "name", or "exact:<field>".
type Attributes map[string]string

// Extract reads and normalizes the match fields from a document.
func Extract(doc map[string]any, fields []v1alpha1.MatchField) Attributes {
	a := Attributes{}
	for _, f := range fields {
		v, ok := doc[f.Field].(string)
		if !ok {
			if n, isNum := doc[f.Field].(float64); isNum {
				v = fmt.Sprint(n)
			}
		}
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		switch f.Kind {
		case v1alpha1.MatchEmail, v1alpha1.MatchDomain:
			email := strings.ToLower(v)
			at := strings.LastIndex(email, "@")
			if at <= 0 || at == len(email)-1 {
				continue
			}
			if f.Kind == v1alpha1.MatchEmail {
				a["email"] = email
			} else if d := email[at+1:]; !freeMail[d] {
				a["domain"] = d
			}
		case v1alpha1.MatchName:
			if n := normalizeName(v); n != "" {
				a["name"] = n
			}
		case v1alpha1.MatchExact:
			a["exact:"+f.Field] = strings.ToUpper(strings.Join(strings.Fields(v), ""))
		}
	}
	return a
}

// freeMail domains say nothing about who a person works for.
var freeMail = map[string]bool{}

func init() {
	for _, d := range strings.Fields(`gmail.com googlemail.com outlook.com hotmail.com live.com msn.com
		yahoo.com yahoo.de yahoo.co.uk icloud.com me.com mac.com aol.com gmx.de gmx.net gmx.at web.de
		t-online.de proton.me protonmail.com mail.com yandex.com zoho.com libero.it virgilio.it orange.fr
		free.fr laposte.net wanadoo.fr hey.com fastmail.com`) {
		freeMail[d] = true
	}
}

func normalizeName(s string) string {
	var b strings.Builder
	space := false
	for _, r := range strings.ToLower(s) {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			if space && b.Len() > 0 {
				b.WriteByte(' ')
			}
			b.WriteRune(r)
			space = false
		default:
			space = true
		}
	}
	return b.String()
}

// A comparison's m probability is how often it agrees for records of the
// same entity, u how often by chance for different ones. Their ratio is
// the evidence it gives. The values are conservative defaults for
// customer data; they are not trained per deployment.
type level struct {
	m, u   float64
	reason string
}

var (
	emailSame = level{0.95, 0.0001, "same email address"}
	// People use several addresses, so a different one is weak evidence.
	emailDiffers = level{0.30, 0.9999, ""}
	domainSame   = level{0.90, 0.002, "same company email domain"}
	domainDiff   = level{0.10, 0.998, ""}
	nameSame     = level{0.85, 0.02, "same name"}
	nameClose    = level{0.10, 0.05, "similar name"}
	nameDiffers  = level{0.05, 0.93, ""}
	exactSame    = level{0.95, 0.001, "same %s"}
	exactDiffers = level{0.05, 0.999, ""}
)

// Prior is the probability, before any comparison, that two records are
// the same entity.
const Prior = 0.05

// Candidate is a record already linked to a master record.
type Candidate struct {
	Master     string
	Attributes Attributes
}

// Suggestion is a master record a source record may be.
type Suggestion struct {
	Master  string   `json:"master"`
	Score   float64  `json:"score"`
	Reasons []string `json:"reasons"`
}

// Compare returns the probability that two records are the same entity,
// and the evidence for it. Fields either record lacks are not compared.
func Compare(a, b Attributes) (float64, []string) {
	logBF := 0.0
	var reasons []string
	use := func(l level) {
		logBF += math.Log(l.m / l.u)
		if l.reason != "" {
			reasons = append(reasons, l.reason)
		}
	}
	emailMatched := false
	if x, y := a["email"], b["email"]; x != "" && y != "" {
		if x == y {
			use(emailSame)
			emailMatched = true
		} else {
			use(emailDiffers)
		}
	}
	// The domain is evidence of its own only when the addresses differ.
	if x, y := a["domain"], b["domain"]; x != "" && y != "" && !emailMatched {
		if x == y {
			use(domainSame)
		} else {
			use(domainDiff)
		}
	}
	if x, y := a["name"], b["name"]; x != "" && y != "" {
		switch s := JaroWinkler(x, y); {
		case s >= 0.94:
			use(nameSame)
		case s >= 0.84:
			use(nameClose)
		default:
			use(nameDiffers)
		}
	}
	keys := make([]string, 0, len(a))
	for k := range a {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !strings.HasPrefix(k, "exact:") || b[k] == "" {
			continue
		}
		if a[k] == b[k] {
			l := exactSame
			l.reason = fmt.Sprintf(l.reason, strings.TrimPrefix(k, "exact:"))
			use(l)
		} else {
			use(exactDiffers)
		}
	}
	odds := Prior / (1 - Prior) * math.Exp(logBF)
	return odds / (1 + odds), reasons
}

// Suggest scores a record against candidates and returns at most limit
// master records, best first, keeping each master's best-matching record.
func Suggest(a Attributes, candidates []Candidate, limit int) []Suggestion {
	best := map[string]Suggestion{}
	for _, c := range candidates {
		score, reasons := Compare(a, c.Attributes)
		if len(reasons) == 0 {
			continue // nothing in common: no suggestion
		}
		if s, ok := best[c.Master]; !ok || score > s.Score {
			best[c.Master] = Suggestion{Master: c.Master, Score: math.Round(score*1000) / 1000, Reasons: reasons}
		}
	}
	out := make([]Suggestion, 0, len(best))
	for _, s := range best {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].Master < out[j].Master
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

// Decide returns the master record to link automatically, if the best
// suggestion reaches the threshold and no other master comes close
// (a runner-up at 0.5 or more makes the match ambiguous).
func Decide(s []Suggestion, threshold float64) (string, bool) {
	if len(s) == 0 || s[0].Score < threshold {
		return "", false
	}
	if len(s) > 1 && s[1].Score >= 0.5 {
		return "", false
	}
	return s[0].Master, true
}

// JaroWinkler returns the Jaro-Winkler similarity of two strings, in [0, 1].
func JaroWinkler(a, b string) float64 {
	s, t := []rune(a), []rune(b)
	if len(s) == 0 && len(t) == 0 {
		return 1
	}
	if len(s) == 0 || len(t) == 0 {
		return 0
	}
	window := max(len(s), len(t))/2 - 1
	if window < 0 {
		window = 0
	}
	sm, tm := make([]bool, len(s)), make([]bool, len(t))
	matches := 0
	for i := range s {
		lo, hi := max(0, i-window), min(len(t), i+window+1)
		for j := lo; j < hi; j++ {
			if !tm[j] && s[i] == t[j] {
				sm[i], tm[j] = true, true
				matches++
				break
			}
		}
	}
	if matches == 0 {
		return 0
	}
	transpositions, k := 0, 0
	for i := range s {
		if !sm[i] {
			continue
		}
		for !tm[k] {
			k++
		}
		if s[i] != t[k] {
			transpositions++
		}
		k++
	}
	m := float64(matches)
	jaro := (m/float64(len(s)) + m/float64(len(t)) + (m-float64(transpositions)/2)/m) / 3
	prefix := 0
	for i := 0; i < min(4, len(s), len(t)) && s[i] == t[i]; i++ {
		prefix++
	}
	return jaro + float64(prefix)*0.1*(1-jaro)
}
