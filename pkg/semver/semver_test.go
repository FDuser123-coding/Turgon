package semver

import "testing"

func TestConstraints(t *testing.T) {
	cases := []struct {
		constraint, version string
		want                bool
	}{
		{"", "1.2.3", true},
		{"*", "0.0.1", true},
		{"^3", "3.9.0", true},
		{"^3", "4.0.0", false},
		{"^3", "2.9.9", false},
		{"^0.4", "0.4.7", true},
		{"^0.4", "0.5.0", false},
		{"^0.0.3", "0.0.4", false},
		{"~1.2", "1.2.9", true},
		{"~1.2", "1.3.0", false},
		{"1.2", "1.2.5", true},
		{"1.2", "1.3.0", false},
		{"3", "3.0.0", true},
		{"1.2.3", "1.2.3", true},
		{"1.2.3", "1.2.4", false},
		{">=1.2.0", "7.0.0", true},
		{">=1.2.0", "1.1.9", false},
		{"^1", "1.5.0-rc.1", false},
		{"^1.5.0-rc.1", "1.5.0-rc.2", true},
		{"1.5.0-rc.1", "1.5.0-rc.1", true},
		{"1.5.0-rc.1", "1.5.0", false},
	}
	for _, c := range cases {
		con, err := ParseConstraint(c.constraint)
		if err != nil {
			t.Fatalf("ParseConstraint(%q): %v", c.constraint, err)
		}
		if got := con.Check(MustParse(c.version)); got != c.want {
			t.Errorf("%q.Check(%s) = %v, want %v", c.constraint, c.version, got, c.want)
		}
	}
}

func TestParseRejectsInvalid(t *testing.T) {
	for _, s := range []string{"", "1", "1.2", "01.2.3", "1.2.3.4", "a.b.c", "1.2.3-"} {
		if _, err := Parse(s); err == nil {
			t.Errorf("Parse(%q) succeeded, want error", s)
		}
	}
}

func TestCompareOrdersPreReleases(t *testing.T) {
	order := []string{"0.9.9", "1.0.0-alpha", "1.0.0-beta", "1.0.0", "1.0.1", "1.1.0", "2.0.0"}
	for i := 1; i < len(order); i++ {
		if MustParse(order[i-1]).Compare(MustParse(order[i])) >= 0 {
			t.Errorf("%s should sort before %s", order[i-1], order[i])
		}
	}
}
