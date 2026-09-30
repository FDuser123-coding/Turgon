package salesforce

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/fduser123-coding/turgon/pkg/connector"
)

// Export is a set of records read at once with the Bulk API 2.0 (a query
// job), for loads such as linking every account to its master record at
// go-live. A job reads millions of records in a few API calls, where the
// REST API needs one call per 2,000.
type Export struct {
	SObject string `json:"sobject"`
	// Fields to read; relationship fields (Owner.Email) are allowed,
	// subqueries are not (the Bulk API does not run them).
	Fields []string `json:"fields"`
	// Where is a SOQL condition.
	Where string `json:"where,omitempty"`
	// All includes deleted and archived records (queryAll).
	All bool `json:"all,omitempty"`
}

var fieldPathRE = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*(\.[A-Za-z][A-Za-z0-9_]*){0,4}$`)

func (e Export) validate(name string) error {
	if !identRE.MatchString(e.SObject) || len(e.Fields) == 0 {
		return fmt.Errorf("export %s: sobject and fields are required", name)
	}
	for _, f := range e.Fields {
		if !fieldPathRE.MatchString(f) {
			return fmt.Errorf("export %s: field %q must be a field or relationship path (the Bulk API runs no subqueries)", name, f)
		}
	}
	if strings.Contains(e.Where, ";") {
		return fmt.Errorf("export %s: SOQL fragments must not contain ';'", name)
	}
	return nil
}

func (e Export) soql() string {
	q := fmt.Sprintf("SELECT %s FROM %s", strings.Join(e.Fields, ", "), e.SObject)
	if e.Where != "" {
		q += " WHERE " + e.Where
	}
	return q
}

var _ connector.Exporter = (*Conn)(nil)

// bulkJob is a query job's state.
type bulkJob struct {
	ID                     string `json:"id"`
	State                  string `json:"state"`
	ErrorMessage           string `json:"errorMessage"`
	NumberRecordsProcessed int    `json:"numberRecordsProcessed"`
}

// Export runs the named export as a Bulk API 2.0 query job, waits for it,
// and passes each record to each (field name as in the export, "" for an
// empty value). It checks that it read as many records as the job
// processed, and deletes the job when done.
func (c *Conn) Export(ctx context.Context, name string, each func(map[string]string) error) (int, error) {
	e, ok := c.cfg.Exports[name]
	if !ok {
		return 0, fmt.Errorf("salesforce: no export %q on this connection", name)
	}
	op := "query"
	if e.All {
		op = "queryAll"
	}
	var job bulkJob
	if err := c.do(ctx, http.MethodPost, c.base()+"/jobs/query", map[string]string{"operation": op, "query": e.soql()}, &job); err != nil {
		return 0, fmt.Errorf("salesforce bulk %s: %w", name, err)
	}
	if job.ID == "" {
		return 0, fmt.Errorf("salesforce bulk %s: the job was not created", name)
	}
	jobPath := c.base() + "/jobs/query/" + url.PathEscape(job.ID)
	done := false // the job ended: delete it; otherwise abort it
	defer func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if done {
			_ = c.do(ctx, http.MethodDelete, jobPath, nil, nil) // results are kept 7 days otherwise
		} else {
			_ = c.do(ctx, http.MethodPatch, jobPath, map[string]string{"state": "Aborted"}, nil)
		}
	}()

	wait := c.BulkPoll
	if wait <= 0 {
		wait = time.Second
	}
	for job.State != "JobComplete" {
		switch job.State {
		case "Failed", "Aborted":
			done = true
			return 0, fmt.Errorf("salesforce bulk %s: job %s %s: %s", name, job.ID, strings.ToLower(job.State), job.ErrorMessage)
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return 0, ctx.Err()
		case <-t.C:
		}
		if wait < 10*time.Second {
			wait = min(wait*2, 10*time.Second)
		}
		if err := c.do(ctx, http.MethodGet, jobPath, nil, &job); err != nil {
			return 0, fmt.Errorf("salesforce bulk %s: %w", name, err)
		}
	}
	done = true

	per := c.BulkPageSize
	if per <= 0 {
		per = 50_000
	}
	n, locator := 0, ""
	for {
		q := url.Values{"maxRecords": {strconv.Itoa(per)}}
		if locator != "" {
			q.Set("locator", locator)
		}
		header, body, err := c.send(ctx, http.MethodGet, jobPath+"/results?"+q.Encode(), nil, "text/csv")
		if err != nil {
			return n, fmt.Errorf("salesforce bulk %s: results: %w", name, err)
		}
		got, err := readCSV(body, each)
		n += got
		if err != nil {
			return n, fmt.Errorf("salesforce bulk %s: results: %w", name, err)
		}
		locator = header.Get("Sforce-Locator")
		if locator == "" || locator == "null" {
			break
		}
	}
	if n != job.NumberRecordsProcessed {
		return n, fmt.Errorf("salesforce bulk %s: read %d records, the job processed %d", name, n, job.NumberRecordsProcessed)
	}
	return n, nil
}

// readCSV reads one page of results: a header row, then records.
func readCSV(body []byte, each func(map[string]string) error) (int, error) {
	r := csv.NewReader(bytes.NewReader(body))
	r.ReuseRecord = false
	header, err := r.Read()
	if errors.Is(err, io.EOF) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n := 0
	for {
		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			return n, nil
		}
		if err != nil {
			return n, err
		}
		rec := make(map[string]string, len(header))
		for i, h := range header {
			rec[h] = row[i]
		}
		if err := each(rec); err != nil {
			return n, err
		}
		n++
	}
}
