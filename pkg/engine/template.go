package engine

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var placeholderRE = regexp.MustCompile(`\{\{\s*(source|doc)\.([A-Za-z0-9_.]+)\s*\}\}`)

// Render expands {{ source.path }} and {{ doc.path }} placeholders, as used
// by idempotency keys. Every placeholder must resolve to a scalar; an empty
// key would let retries create duplicates.
func Render(tmpl string, source, doc map[string]any) (string, error) {
	var errs []string
	out := placeholderRE.ReplaceAllStringFunc(tmpl, func(m string) string {
		parts := placeholderRE.FindStringSubmatch(m)
		root := source
		if parts[1] == "doc" {
			root = doc
		}
		v, ok := lookup(root, parts[2])
		if !ok {
			errs = append(errs, parts[1]+"."+parts[2])
			return ""
		}
		switch x := v.(type) {
		case string:
			return x
		case float64:
			return strconv.FormatFloat(x, 'f', -1, 64)
		case bool:
			return strconv.FormatBool(x)
		default:
			errs = append(errs, parts[1]+"."+parts[2]+" (not a scalar)")
			return ""
		}
	})
	if len(errs) > 0 {
		return "", fmt.Errorf("template %q: unresolved %s", tmpl, strings.Join(errs, ", "))
	}
	if strings.TrimSpace(out) == "" {
		return "", fmt.Errorf("template %q rendered empty", tmpl)
	}
	return out, nil
}

func lookup(root map[string]any, path string) (any, bool) {
	var cur any = root
	for _, p := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = m[p]; !ok || cur == nil {
			return nil, false
		}
	}
	return cur, true
}
