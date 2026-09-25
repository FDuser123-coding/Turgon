package rest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"

	"github.com/fduser123-coding/turgon/pkg/connector"
)

var _ connector.Checker = (*Conn)(nil)

// Check proves the credential works and every event's list request
// answers with items where the configuration expects them.
func (c *Conn) Check(ctx context.Context) []connector.CheckResult {
	var out []connector.CheckResult
	host := c.cfg.BaseURL
	if u, err := url.Parse(c.cfg.BaseURL); err == nil {
		host = u.Host
	}
	if c.cfg.Auth.Type == "oauth2" {
		if _, err := c.auth.current(ctx); err != nil {
			return append(out, connector.Fail("oauth2 token", err.Error(), fix(err, host,
				"Check the clientId and clientSecret in the connection's secret, and that the client may use the client-credentials grant.")))
		}
		out = append(out, connector.Pass("oauth2 token", "access token issued by "+c.cfg.Auth.TokenURL))
	}
	if c.cfg.CheckPath != "" {
		if err := c.do(ctx, http.MethodGet, c.cfg.CheckPath, nil, nil, nil, nil); err != nil {
			return append(out, connector.Fail("reach "+host, err.Error(), fix(err, host, "")))
		}
		out = append(out, connector.Pass("reach "+host, "GET "+c.cfg.CheckPath+" answered with the configured credential"))
	}
	names := make([]string, 0, len(c.cfg.Events))
	for n := range c.cfg.Events {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		events, err := c.Poll(ctx, n, 0, 1)
		if err != nil {
			out = append(out, connector.Fail("event "+n, err.Error(), fix(err, host,
				"Check the event's path, params and items in the connection's config against the API's list response.")))
			continue
		}
		e := c.cfg.Events[n]
		out = append(out, connector.Pass("event "+n, fmt.Sprintf("%s %s answers (%d item(s) in a first page of 1)", or(e.Method, http.MethodGet), e.Path, len(events))))
	}
	return out
}

func fix(err error, host, otherwise string) string {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		switch apiErr.Status {
		case http.StatusUnauthorized:
			return "The API rejected the credential. Put a valid one in the connection's secret (see the connection's auth type)."
		case http.StatusForbidden:
			return "The credential is valid but lacks access. Grant the integration user or app the scopes this connection's events and operations need."
		case http.StatusNotFound:
			return "The path does not exist. Check baseURL (including the API version) and the configured paths."
		}
		return otherwise
	}
	if f := connector.NetworkFix(err, host); f != "" {
		return f
	}
	return otherwise
}
