package secrets

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
)

// A cloud secret manager (AWS Secrets Manager, Azure Key Vault, Google
// Secret Manager) holds a connection's secret as one secret whose value is
// a JSON object: "openbao://shop-db/dsn" is the field "dsn" of the secret
// named "shop-db" (after a prefix, and in the characters the manager
// allows). The scheme of a reference is only a label: catalogs keep their
// references whichever manager a deployment uses.

// fetcher reads secrets from one manager.
type fetcher interface {
	// kind names the manager, e.g. "AWS Secrets Manager".
	kind() string
	// secretName maps a reference's path to a secret name the manager
	// accepts.
	secretName(path string) (string, error)
	// fetch returns a secret's current value.
	fetch(ctx context.Context, name string) (string, error)
	// fix says how to resolve a failed fetch.
	fix(err error, name string) string
}

// Cloud resolves references against a cloud secret manager.
type Cloud struct {
	f      fetcher
	prefix string
	ttl    time.Duration
	now    func() time.Time

	mu    sync.Mutex
	cache map[string]cachedValue
}

type cachedValue struct {
	fields  map[string]any
	fetched time.Time
}

var _ connector.SecretResolver = (*Cloud)(nil)

func newCloud(f fetcher, prefix string, ttl time.Duration) *Cloud {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &Cloud{f: f, prefix: prefix, ttl: ttl, now: time.Now, cache: map[string]cachedValue{}}
}

var anyRefRE = regexp.MustCompile(`^[a-z][a-z0-9+.-]*://`)

// ParseRef splits "<scheme>://a/b/key" into the path "a/b" and the key.
func ParseRef(ref string) (path, key string, err error) {
	loc := anyRefRE.FindStringIndex(ref)
	if loc == nil {
		return "", "", fmt.Errorf("secret %s: a reference is <scheme>://<path>/<key>", ref)
	}
	rest := ref[loc[1]:]
	i := strings.LastIndexByte(rest, '/')
	if i <= 0 || i == len(rest)-1 || !validPath(rest) {
		return "", "", fmt.Errorf("secret %s: a reference is <scheme>://<path>/<key>", ref)
	}
	return rest[:i], rest[i+1:], nil
}

// Where says where a reference is read from, for `turgon secrets`.
func (c *Cloud) Where(ref string) string {
	path, key, err := ParseRef(ref)
	if err != nil {
		return err.Error()
	}
	name, err := c.f.secretName(c.prefix + path)
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("%s secret %s, field %s", c.f.kind(), name, key)
}

// Forget empties the cache, so the next reads get the current values.
func (c *Cloud) Forget() {
	c.mu.Lock()
	c.cache = map[string]cachedValue{}
	c.mu.Unlock()
}

// Resolve returns a reference's value.
func (c *Cloud) Resolve(ctx context.Context, ref string) (string, error) {
	path, key, err := ParseRef(ref)
	if err != nil {
		return "", err
	}
	name, err := c.f.secretName(c.prefix + path)
	if err != nil {
		return "", fmt.Errorf("secret %s: %w", ref, err)
	}
	fields, err := c.read(ctx, name)
	if err != nil {
		return "", fmt.Errorf("secret %s: %w", ref, err)
	}
	v, ok := fields[key]
	if !ok {
		keys := make([]string, 0, len(fields))
		for k := range fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		return "", fmt.Errorf("secret %s: %s secret %s has no field %q (it has %s)", ref, c.f.kind(), name, key, strings.Join(keys, ", "))
	}
	switch v := v.(type) {
	case string:
		return v, nil
	case nil:
		return "", fmt.Errorf("secret %s: the field is null", ref)
	default:
		b, err := json.Marshal(v)
		return string(b), err
	}
}

func (c *Cloud) read(ctx context.Context, name string) (map[string]any, error) {
	c.mu.Lock()
	cv, ok := c.cache[name]
	c.mu.Unlock()
	if ok && c.now().Sub(cv.fetched) < c.ttl {
		return cv.fields, nil
	}
	raw, err := c.f.fetch(ctx, name)
	if err != nil {
		return nil, err
	}
	var fields map[string]any
	dec := json.NewDecoder(bytes.NewReader([]byte(raw)))
	dec.UseNumber()
	if err := dec.Decode(&fields); err != nil || fields == nil {
		return nil, fmt.Errorf("%s secret %s holds a plain value; store a JSON object of fields, e.g. {\"dsn\": \"...\"}", c.f.kind(), name)
	}
	c.mu.Lock()
	c.cache[name] = cachedValue{fields: fields, fetched: c.now()}
	c.mu.Unlock()
	return fields, nil
}

// Check reads each reference, for `turgon check`. It reports which secrets
// it could read, never their values.
func (c *Cloud) Check(ctx context.Context, refs []string) []connector.CheckResult {
	var out []connector.CheckResult
	for _, ref := range refs {
		if _, err := c.Resolve(ctx, ref); err != nil {
			fix := ""
			if path, _, perr := ParseRef(ref); perr == nil {
				if name, nerr := c.f.secretName(c.prefix + path); nerr == nil {
					fix = c.f.fix(err, name)
				}
			}
			out = append(out, connector.Fail("secret "+ref, err.Error(), fix))
		} else {
			out = append(out, connector.Pass("secret "+ref, c.Where(ref)))
		}
	}
	return out
}

// mapName applies a manager's naming rules: separators it does not allow
// become "--" (for "/") or "-", and the result must match allowed.
func mapName(kind, path string, replacer *strings.Replacer, allowed *regexp.Regexp) (string, error) {
	name := path
	if replacer != nil {
		name = replacer.Replace(path)
	}
	if !allowed.MatchString(name) {
		return "", fmt.Errorf("%q is not a valid %s secret name", name, kind)
	}
	return name, nil
}
