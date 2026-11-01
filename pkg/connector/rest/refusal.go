package rest

import (
	"encoding/json"
	"net/http"
	"regexp"
	"sort"
	"strings"

	"github.com/fduser123-coding/turgon/pkg/meta"
)

// LearnRefusals marks, in the objects the connection's writes send, the
// fields refused writes were missing. Without an OpenAPI description the
// records are sampled, and a sample cannot show that a field became
// required; the API's refusal of a write that lacks it can, and every
// other operation writing the same object is then reported by the drift.
func (c *Conn) LearnRefusals(cat *meta.Catalog, refusals []meta.Refusal) {
	for _, r := range refusals {
		op, ok := c.cfg.Operations[r.Operation]
		if !ok || op.Method == http.MethodGet || op.Restore {
			continue
		}
		fields := requiredFields(r.Error)
		if len(fields) == 0 {
			continue
		}
		for _, u := range cat.Uses {
			if u.By != "operation "+r.Operation && u.By != "operation "+r.Operation+" (writes)" {
				continue
			}
			for _, f := range fields {
				if f != topField(op.Wrap) { // Shopify's "order" missing is the wrapper, not a field
					meta.Learn(cat, u.Object, f, r)
				}
			}
			break
		}
	}
}

var (
	statusRE = regexp.MustCompile(`HTTP (\d{3}): `)
	// What APIs say about a field a request must send.
	requiredRE = regexp.MustCompile(`(?i)\b(required|missing|mandatory|can'?t be blank|cannot be blank|must be present|must be provided|must not be (empty|null|blank)|may not be (empty|null|blank))\b`)
	// The same, in a message naming the field.
	textRE = []*regexp.Regexp{
		regexp.MustCompile(`(?i)missing required (?:param(?:eter)?|field|property|attribute)s?:?\s*['"]?([A-Za-z_][\w.\[\]]*)`),
		regexp.MustCompile(`(?i)required (?:param(?:eter)?|field|property|attribute) ['"]?([A-Za-z_][\w.\[\]]*)['"]? (?:is )?missing`),
		regexp.MustCompile(`(?i)['"]([A-Za-z_][\w.\[\]]*)['"] (?:is|are) (?:a )?required`),
		regexp.MustCompile(`(?i)\b([A-Za-z_][\w.\[\]]*) (?:is|are) (?:a )?required (?:field|property|param(?:eter)?|attribute)`),
	}
)

// requiredFields reads the fields an API's refusal says a request lacked,
// from the error its write failed with ("... HTTP 422: <body>"). It knows
// the common shapes: problem details' invalid-params (RFC 9457), Stripe's
// error.param, a field-to-messages map ({"errors": {"email": ["can't be
// blank"]}}, as Rails APIs such as Shopify's answer), lists of errors
// naming their field, and messages such as "Missing required param: email".
// Only 400 and 422 answers count; the fields are top-level (a[b] and a.b
// are a).
func requiredFields(errText string) []string {
	m := statusRE.FindStringSubmatchIndex(errText)
	if m == nil {
		return nil
	}
	if s := errText[m[2]:m[3]]; s != "400" && s != "422" {
		return nil
	}
	body := errText[m[1]:]
	found := map[string]bool{}
	add := func(name string) {
		if i := strings.IndexAny(name, "[."); i > 0 {
			name = name[:i]
		}
		name = strings.TrimPrefix(name, "/") // a JSON pointer's first segment
		if i := strings.Index(name, "/"); i > 0 {
			name = name[:i]
		}
		if name != "" && name != "body" && name != "data" && name != "attributes" {
			found[name] = true
		}
	}
	var doc any
	if json.Unmarshal([]byte(body), &doc) == nil {
		walk(doc, add)
	}
	for _, re := range textRE {
		for _, sm := range re.FindAllStringSubmatch(body, -1) {
			add(sm[1])
		}
	}
	out := make([]string, 0, len(found))
	for f := range found {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

// walk finds errors naming a field and saying it is required, anywhere in
// an error body.
func walk(v any, add func(string)) {
	switch v := v.(type) {
	case []any:
		for _, e := range v {
			walk(e, add)
		}
	case map[string]any:
		if name := fieldOf(v); name != "" && requiredRE.MatchString(messages(v)) {
			add(name)
		}
		for k, e := range v {
			// {"errors": {"email": ["can't be blank"]}}
			if (k == "errors" || k == "error") && isFieldMap(e) {
				for field, msgs := range e.(map[string]any) {
					if requiredRE.MatchString(text(msgs)) {
						add(field)
					}
				}
				continue
			}
			walk(e, add)
		}
	}
}

// fieldOf is the field an error object names.
func fieldOf(e map[string]any) string {
	for _, k := range []string{"param", "field", "property", "propertyName", "path", "attribute"} {
		if s, ok := e[k].(string); ok && s != "" {
			return s
		}
	}
	// "name" may name the error ({"name": "ValidationError"}): it names a
	// field in problem details' invalid-params, which give a reason, or
	// when the message mentions it.
	if s, ok := e["name"].(string); ok && s != "" {
		if _, ok := e["reason"]; ok || strings.Contains(messages(e), s) {
			return s
		}
	}
	if src, ok := e["source"].(map[string]any); ok { // JSON:API
		if s, ok := src["pointer"].(string); ok {
			return strings.TrimPrefix(strings.TrimPrefix(s, "/data/attributes"), "/")
		}
	}
	return ""
}

// messages is what an error object says.
func messages(e map[string]any) string {
	var b strings.Builder
	for _, k := range []string{"message", "reason", "detail", "code", "type", "description", "error", "title"} {
		if s, ok := e[k].(string); ok {
			b.WriteString(strings.ReplaceAll(s, "_", " "))
			b.WriteByte(' ')
		}
	}
	return b.String()
}

// isFieldMap reports a map of field names to messages.
func isFieldMap(v any) bool {
	m, ok := v.(map[string]any)
	if !ok || len(m) == 0 {
		return false
	}
	for _, k := range []string{"message", "type", "code", "detail", "title", "status"} {
		if _, ok := m[k]; ok {
			return false // an error object, not fields
		}
	}
	for _, e := range m {
		switch e.(type) {
		case string, []any:
		default:
			return false
		}
	}
	return true
}

func text(v any) string {
	switch v := v.(type) {
	case string:
		return v
	case []any:
		var parts []string
		for _, e := range v {
			parts = append(parts, text(e))
		}
		return strings.Join(parts, " ")
	}
	return ""
}
