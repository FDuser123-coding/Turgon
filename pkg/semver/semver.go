// Package semver implements the small subset of semantic versioning Porter
// needs to pin connectors, mappings and plugins: parsing, ordering and
// constraints of the forms "*", "1", "1.2", "1.2.3", "^1.2", "~1.2" and
// ">=1.2.0".
package semver

import (
	"fmt"
	"strconv"
	"strings"
)

// Version is a parsed semantic version. Build metadata is not supported.
type Version struct {
	Major, Minor, Patch int
	Pre                 string
}

// Parse parses "1.2.3", "v1.2.3" or "1.2.3-rc.1".
func Parse(s string) (Version, error) {
	v, n, err := parsePartial(s)
	if err != nil {
		return Version{}, err
	}
	if n != 3 {
		return Version{}, fmt.Errorf("semver: %q must have major, minor and patch", s)
	}
	return v, nil
}

// MustParse is Parse that panics; for tests and constants.
func MustParse(s string) Version {
	v, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return v
}

// parsePartial parses up to three numeric components and returns how many
// were present, so "1.2" yields (1.2.0, 2).
func parsePartial(s string) (Version, int, error) {
	orig := s
	s = strings.TrimPrefix(strings.TrimSpace(s), "v")
	var v Version
	if i := strings.IndexByte(s, '-'); i >= 0 {
		v.Pre = s[i+1:]
		s = s[:i]
		if v.Pre == "" {
			return Version{}, 0, fmt.Errorf("semver: empty pre-release in %q", orig)
		}
	}
	parts := strings.Split(s, ".")
	if len(parts) == 0 || len(parts) > 3 {
		return Version{}, 0, fmt.Errorf("semver: invalid version %q", orig)
	}
	nums := [3]int{}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 || (len(p) > 1 && p[0] == '0') {
			return Version{}, 0, fmt.Errorf("semver: invalid component %q in %q", p, orig)
		}
		nums[i] = n
	}
	v.Major, v.Minor, v.Patch = nums[0], nums[1], nums[2]
	if v.Pre != "" && len(parts) != 3 {
		return Version{}, 0, fmt.Errorf("semver: pre-release requires a full version in %q", orig)
	}
	return v, len(parts), nil
}

func (v Version) String() string {
	s := fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
	if v.Pre != "" {
		s += "-" + v.Pre
	}
	return s
}

// Compare returns -1, 0 or 1. Pre-releases sort before their release and are
// compared lexically, which is sufficient for Porter's catalog.
func (v Version) Compare(o Version) int {
	for _, d := range [3]int{v.Major - o.Major, v.Minor - o.Minor, v.Patch - o.Patch} {
		if d < 0 {
			return -1
		}
		if d > 0 {
			return 1
		}
	}
	switch {
	case v.Pre == o.Pre:
		return 0
	case v.Pre == "":
		return 1
	case o.Pre == "":
		return -1
	case v.Pre < o.Pre:
		return -1
	default:
		return 1
	}
}

// Constraint is a half-open range [min, max). A zero max means unbounded.
type Constraint struct {
	raw      string
	min, max Version
	bounded  bool
	exact    bool
}

// ParseConstraint parses a constraint string. The empty string and "*" match
// any release version.
func ParseConstraint(s string) (Constraint, error) {
	s = strings.TrimSpace(s)
	c := Constraint{raw: s}
	switch {
	case s == "" || s == "*":
		return c, nil
	case strings.HasPrefix(s, ">="):
		v, _, err := parsePartial(s[2:])
		if err != nil {
			return c, err
		}
		c.min = v
		return c, nil
	case strings.HasPrefix(s, "^"):
		v, _, err := parsePartial(s[1:])
		if err != nil {
			return c, err
		}
		c.min, c.bounded = v, true
		switch {
		case v.Major > 0:
			c.max = Version{Major: v.Major + 1}
		case v.Minor > 0:
			c.max = Version{Minor: v.Minor + 1}
		default:
			c.max = Version{Patch: v.Patch + 1}
		}
		return c, nil
	case strings.HasPrefix(s, "~"):
		v, n, err := parsePartial(s[1:])
		if err != nil {
			return c, err
		}
		c.min, c.bounded = v, true
		if n == 1 {
			c.max = Version{Major: v.Major + 1}
		} else {
			c.max = Version{Major: v.Major, Minor: v.Minor + 1}
		}
		return c, nil
	default:
		v, n, err := parsePartial(s)
		if err != nil {
			return c, err
		}
		c.min, c.bounded = v, true
		switch n {
		case 1:
			c.max = Version{Major: v.Major + 1}
		case 2:
			c.max = Version{Major: v.Major, Minor: v.Minor + 1}
		default:
			c.max = Version{Major: v.Major, Minor: v.Minor, Patch: v.Patch + 1}
			c.exact = v.Pre != "" // an exact pre-release pin matches only itself
		}
		return c, nil
	}
}

// Check reports whether v satisfies the constraint. Pre-release versions only
// match constraints whose lower bound is itself a pre-release.
func (c Constraint) Check(v Version) bool {
	if c.exact {
		return v.Compare(c.min) == 0
	}
	if v.Pre != "" && c.min.Pre == "" {
		return false
	}
	if v.Compare(c.min) < 0 {
		return false
	}
	if c.bounded {
		maxv := c.max
		if maxv.Pre == "" && v.Pre != "" && v.Major == maxv.Major && v.Minor == maxv.Minor && v.Patch == maxv.Patch {
			return false // 2.0.0-rc.1 is not below ^1's bound for our purposes
		}
		return v.Compare(maxv) < 0
	}
	return true
}

func (c Constraint) String() string {
	if c.raw == "" {
		return "*"
	}
	return c.raw
}
