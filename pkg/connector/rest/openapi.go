package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/fduser123-coding/turgon/pkg/meta"
)

// maxSpec bounds an OpenAPI document (Stripe's is about 8 MB).
const maxSpec = 64 << 20

// spec is an OpenAPI 3 document, kept as decoded JSON: schemas are walked
// as they are, $refs resolved on the way.
type spec struct {
	doc     map[string]any
	servers []string // the servers' URL paths, e.g. /admin/api/2026-07
}

// fetchSpec reads the OpenAPI document (JSON or YAML) at u.
func fetchSpec(ctx context.Context, u string) (*spec, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json, application/yaml;q=0.9, */*;q=0.5")
	resp, err := (&http.Client{Timeout: time.Minute}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("openapi: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("openapi: %s: HTTP %d", u, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSpec+1))
	if err != nil {
		return nil, fmt.Errorf("openapi: %w", err)
	}
	if len(body) > maxSpec {
		return nil, fmt.Errorf("openapi: %s is larger than %d MB", u, maxSpec>>20)
	}
	return parseSpec(body)
}

func parseSpec(body []byte) (*spec, error) {
	if t := strings.TrimSpace(string(body)); !strings.HasPrefix(t, "{") {
		j, err := yaml.YAMLToJSON(body)
		if err != nil {
			return nil, fmt.Errorf("openapi: neither JSON nor YAML: %w", err)
		}
		body = j
	}
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("openapi: %w", err)
	}
	v, _ := doc["openapi"].(string)
	if !strings.HasPrefix(v, "3.") {
		return nil, fmt.Errorf("openapi: version %q: Turgon reads OpenAPI 3 documents", v)
	}
	s := &spec{doc: doc}
	servers, _ := doc["servers"].([]any)
	for _, sv := range servers {
		if m, ok := sv.(map[string]any); ok {
			if raw, _ := m["url"].(string); raw != "" {
				if u, err := url.Parse(raw); err == nil {
					s.servers = append(s.servers, strings.TrimRight(u.Path, "/"))
				}
			}
		}
	}
	return s, nil
}

var (
	specParamRE  = regexp.MustCompile(`\{[^{}/]+\}`)
	turgonVarRE  = regexp.MustCompile(`\{\{[^{}/]+\}\}`)
	slashesRE    = regexp.MustCompile(`/+`)
	refSchemasRE = regexp.MustCompile(`^#/components/schemas/(.+)$`)
)

// templ turns a path template into a comparable form: parameters become {}.
func templ(p string, re *regexp.Regexp) string {
	return strings.TrimRight(slashesRE.ReplaceAllString(re.ReplaceAllString(p, "{}"), "/"), "/")
}

// operation finds the OpenAPI operation for a request a connection makes.
// base is the path of the connection's baseURL, which may include the
// server's path or not.
func (s *spec) operation(method, base, path string) (string, map[string]any) {
	want := map[string]bool{templ(path, turgonVarRE): true, templ(base+path, turgonVarRE): true}
	paths, _ := s.doc["paths"].(map[string]any)
	names := make([]string, 0, len(paths))
	for p := range paths {
		names = append(names, p)
	}
	sort.Strings(names)
	for _, p := range names {
		cands := []string{templ(p, specParamRE)}
		for _, sv := range s.servers {
			cands = append(cands, templ(sv+p, specParamRE))
		}
		for _, c := range cands {
			if want[c] {
				item, _ := paths[p].(map[string]any)
				if op, ok := item[strings.ToLower(method)].(map[string]any); ok {
					return strings.ToUpper(method) + " " + p, op
				}
			}
		}
	}
	return "", nil
}

// resolve follows $refs, returning the schema and the name of the last
// component schema it went through.
func (s *spec) resolve(node any, name string) (map[string]any, string) {
	for i := 0; i < 32; i++ {
		m, ok := node.(map[string]any)
		if !ok {
			return nil, name
		}
		ref, _ := m["$ref"].(string)
		if ref == "" {
			return m, name
		}
		if g := refSchemasRE.FindStringSubmatch(ref); g != nil {
			name = g[1]
		}
		node = s.pointer(ref)
	}
	return nil, name // a cycle of $refs
}

// pointer resolves a local JSON pointer (#/components/schemas/x).
func (s *spec) pointer(ref string) any {
	if !strings.HasPrefix(ref, "#/") {
		return nil // external documents are not followed
	}
	var cur any = s.doc
	for _, part := range strings.Split(ref[2:], "/") {
		part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[part]
	}
	return cur
}

// object is a schema's properties, flattened through allOf, anyOf and
// oneOf (a property only some variants have is not required).
type object struct {
	props    map[string]any
	required map[string]bool
}

func (s *spec) object(node any, depth int) object {
	o := object{props: map[string]any{}, required: map[string]bool{}}
	m, _ := s.resolve(node, "")
	if m == nil || depth > 16 {
		return o
	}
	if props, ok := m["properties"].(map[string]any); ok {
		for k, v := range props {
			o.props[k] = v
		}
	}
	if req, ok := m["required"].([]any); ok {
		for _, r := range req {
			if n, ok := r.(string); ok {
				o.required[n] = true
			}
		}
	}
	if all, ok := m["allOf"].([]any); ok {
		for _, sub := range all {
			so := s.object(sub, depth+1)
			for k, v := range so.props {
				o.props[k] = v
			}
			for k := range so.required {
				o.required[k] = true
			}
		}
	}
	for _, key := range []string{"anyOf", "oneOf"} {
		if alts, ok := m[key].([]any); ok {
			for _, sub := range alts {
				for k, v := range s.object(sub, depth+1).props {
					if _, has := o.props[k]; !has {
						o.props[k] = v
					}
				}
			}
		}
	}
	return o
}

// typeOf describes a property's type: a JSON type with its format, a
// component's name, or the alternatives of an anyOf.
func (s *spec) typeOf(node any, depth int) string {
	m, name := s.resolve(node, "")
	if m == nil || depth > 8 {
		return ""
	}
	if name != "" {
		return name
	}
	for _, key := range []string{"anyOf", "oneOf"} {
		if alts, ok := m[key].([]any); ok {
			seen := map[string]bool{}
			var out []string
			for _, a := range alts {
				if t := s.typeOf(a, depth+1); t != "" && !seen[t] {
					seen[t] = true
					out = append(out, t)
				}
			}
			sort.Strings(out)
			return strings.Join(out, "|")
		}
	}
	t, _ := m["type"].(string)
	if t == "array" {
		return "array<" + s.typeOf(m["items"], depth+1) + ">"
	}
	if f, _ := m["format"].(string); f != "" {
		return t + "(" + f + ")"
	}
	if t == "" && m["properties"] != nil {
		return "object"
	}
	return t
}

// fields describes an object schema's properties. For a request body,
// a required property the API marks read-only is not required of a writer.
func (s *spec) fields(node any) []meta.Field {
	o := s.object(node, 0)
	names := make([]string, 0, len(o.props))
	for n := range o.props {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]meta.Field, 0, len(names))
	for _, n := range names {
		p, _ := s.resolve(o.props[n], "")
		f := meta.Field{Name: n, Type: s.typeOf(o.props[n], 0)}
		if p != nil {
			f.ReadOnly, _ = p["readOnly"].(bool)
			if ml, ok := p["maxLength"].(float64); ok {
				f.Length = int(ml)
			}
			f.Label, _ = p["description"].(string)
			if len(f.Label) > 200 {
				f.Label = f.Label[:200] + "…"
			}
		}
		f.Required = o.required[n] && !f.ReadOnly
		out = append(out, f)
	}
	return out
}

// property descends a dotted path of properties (items: data, result:
// order) from a schema.
func (s *spec) property(node any, path string) any {
	if path == "" {
		return node
	}
	for _, part := range strings.Split(path, ".") {
		node = s.object(node, 0).props[part]
		if node == nil {
			return nil
		}
	}
	return node
}

// content is an operation's request body or 2xx response schema.
func (s *spec) content(op map[string]any, request bool) any {
	var holder map[string]any
	if request {
		holder, _ = s.resolve(op["requestBody"], "")
	} else {
		responses, _ := op["responses"].(map[string]any)
		codes := make([]string, 0, len(responses))
		for c := range responses {
			if strings.HasPrefix(c, "2") {
				codes = append(codes, c)
			}
		}
		sort.Strings(codes)
		if len(codes) == 0 {
			return nil
		}
		holder, _ = s.resolve(responses[codes[0]], "")
	}
	if holder == nil {
		return nil
	}
	content, _ := holder["content"].(map[string]any)
	for _, ct := range []string{"application/json", "application/x-www-form-urlencoded", "multipart/form-data"} {
		if c, ok := content[ct].(map[string]any); ok {
			return c["schema"]
		}
	}
	for ct, c := range content { // a vendor JSON type, such as application/vnd.api+json
		if strings.Contains(ct, "json") {
			if cm, ok := c.(map[string]any); ok {
				return cm["schema"]
			}
		}
	}
	return nil
}

// describe makes an object of a schema: named after its component, or
// after the request it belongs to. Only a request body's required
// properties are required: in a response, required means always present.
func (s *spec) describe(node any, fallback, kind string, request bool) (meta.Object, bool) {
	if node == nil {
		return meta.Object{}, false
	}
	m, name := s.resolve(node, "")
	if m == nil {
		return meta.Object{}, false
	}
	o := meta.Object{Name: fallback, Kind: kind, Fields: s.fields(node)}
	if !request {
		for i := range o.Fields {
			o.Fields[i].Required = false
		}
	}
	if name != "" {
		o.Name, o.Kind = name, "schema"
	}
	o.Label, _ = m["description"].(string)
	if len(o.Label) > 200 {
		o.Label = o.Label[:200] + "…"
	}
	return o, len(o.Fields) > 0
}

// topField is the first segment of a field path (metadata.erp_id is
// metadata); "\." is a literal dot.
func topField(path string) string {
	var b strings.Builder
	for i := 0; i < len(path); i++ {
		switch {
		case path[i] == '\\' && i+1 < len(path) && path[i+1] == '.':
			b.WriteByte('.')
			i++
		case path[i] == '.':
			return b.String()
		default:
			b.WriteByte(path[i])
		}
	}
	return b.String()
}
