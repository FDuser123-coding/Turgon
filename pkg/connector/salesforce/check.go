package salesforce

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/fduser123-coding/turgon/pkg/connector"
)

var _ connector.Checker = (*Conn)(nil)

// Check verifies the login and that every configured sObject and field
// exists, and that fields the connector writes are updateable by the
// integration user.
func (c *Conn) Check(ctx context.Context) []connector.CheckResult {
	var out []connector.CheckResult
	_, instance, err := c.sess.current(ctx)
	if err != nil {
		return append(out, connector.Fail("login", err.Error(), loginFix(err, c.sess.creds)))
	}
	out = append(out, connector.Pass("login", fmt.Sprintf("%s as %s", instance, c.sess.creds.Username)))

	type want struct{ read, write []string }
	need := map[string]*want{}
	get := func(s string) *want {
		if need[s] == nil {
			need[s] = &want{}
		}
		return need[s]
	}
	for _, q := range c.cfg.Events {
		for _, f := range q.Fields {
			if f = strings.TrimSpace(f); identRE.MatchString(f) { // skip subqueries and relationship paths
				get(q.SObject).read = append(get(q.SObject).read, f)
			}
		}
	}
	for _, op := range c.cfg.Operations {
		for f := range op.Fields {
			get(op.SObject).write = append(get(op.SObject).write, f)
		}
		get(op.SObject)
	}
	objects := make([]string, 0, len(need))
	for s := range need {
		objects = append(objects, s)
	}
	sort.Strings(objects)
	for _, s := range objects {
		var d struct {
			Fields []struct {
				Name       string `json:"name"`
				Updateable bool   `json:"updateable"`
			} `json:"fields"`
		}
		name := "sobject " + s
		if err := c.do(ctx, http.MethodGet, fmt.Sprintf("%s/sobjects/%s/describe", c.base(), s), nil, &d); err != nil {
			fix := "Check the sObject name, and that the integration user's profile or permission set can read it."
			var api *APIError
			if errors.As(err, &api) && api.Status == http.StatusForbidden {
				fix = "The integration user may not access " + s + ". Grant it in a permission set."
			}
			out = append(out, connector.Fail(name, err.Error(), fix))
			continue
		}
		fields := map[string]bool{}
		for _, f := range d.Fields {
			fields[f.Name] = f.Updateable
		}
		var problems []string
		for _, f := range need[s].read {
			if _, ok := fields[f]; !ok {
				problems = append(problems, f+" is not visible")
			}
		}
		for _, f := range need[s].write {
			if upd, ok := fields[f]; !ok {
				problems = append(problems, f+" is not visible")
			} else if !upd {
				problems = append(problems, f+" is read-only for this user")
			}
		}
		if len(problems) > 0 {
			sort.Strings(problems)
			out = append(out, connector.Fail(name, strings.Join(problems, "; "),
				"Create the fields, or give the integration user field-level access (read for events, edit for writes) in a permission set."))
			continue
		}
		out = append(out, connector.Pass(name, fmt.Sprintf("%d fields checked", len(need[s].read)+len(need[s].write))))
	}
	return out
}

func loginFix(err error, c Credentials) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "invalid_grant") && strings.Contains(msg, "approved"):
		return fmt.Sprintf("Pre-authorize %s for the connected app: set it to admin-approved users and add the user's profile or a permission set.", c.Username)
	case strings.Contains(msg, "invalid_grant") && strings.Contains(msg, "signature"):
		return "The connected app does not hold the certificate matching this private key. Upload the key's certificate to the connected app."
	case strings.Contains(msg, "invalid_grant") && strings.Contains(msg, "audience"):
		return "Use https://login.salesforce.com (production) or https://test.salesforce.com (sandbox) as loginUrl, or set audience."
	case strings.Contains(msg, "invalid_client"):
		return "The client ID or secret is wrong, or the client credentials flow is not enabled for the connected app."
	}
	if fix := connector.NetworkFix(err, c.LoginURL); fix != "" {
		return fix
	}
	return "Check loginUrl, clientId and the integration user in the connection's secret."
}
