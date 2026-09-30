package sftest

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// The Bulk API 2.0 query jobs the fake serves: SELECT with fields and
// relationship fields (Owner.Email), FROM one sObject, and a WHERE of
// conditions joined by AND: Field = 'v', Field != 'v', Field = null,
// Field != null. Records with IsDeleted true are only read by queryAll. A
// job completes after JobPolls status checks; results come as CSV pages.

type bulkJob struct {
	id, state, errorMessage string
	rows                    [][]string // header first
	checks                  int
	locators                map[string]int
}

var (
	bulkRE = regexp.MustCompile(`^SELECT (.+) FROM (\w+)(?: WHERE (.+))?$`)
	condRE = regexp.MustCompile(`^([A-Za-z][\w.]*) (=|!=) (null|'[^']*')$`)
)

// BulkJobs counts query jobs created; BulkDeleted those deleted.
func (s *Server) BulkJobs() (created, deleted int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bulkCreated, s.bulkDeleted
}

func (s *Server) bulk(w http.ResponseWriter, r *http.Request, parts []string) {
	// services/data/vXX.X/jobs/query[/<id>[/results]]
	switch {
	case len(parts) == 5 && r.Method == http.MethodPost:
		var in struct{ Operation, Query string }
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || (in.Operation != "query" && in.Operation != "queryAll") {
			writeErr(w, http.StatusBadRequest, "INVALIDJOB", "operation must be query or queryAll")
			return
		}
		rows, err := s.bulkQuery(in.Query, in.Operation == "queryAll")
		if err != nil {
			writeErr(w, http.StatusBadRequest, "INVALIDJOB", err.Error())
			return
		}
		s.mu.Lock()
		s.nextID++
		s.bulkCreated++
		job := &bulkJob{id: fmt.Sprintf("750%015d", s.nextID), state: "UploadComplete", rows: rows, locators: map[string]int{}}
		if s.jobs == nil {
			s.jobs = map[string]*bulkJob{}
		}
		s.jobs[job.id] = job
		s.mu.Unlock()
		s.writeJob(w, job)
	case len(parts) >= 6:
		s.mu.Lock()
		job := s.jobs[parts[5]]
		s.mu.Unlock()
		if job == nil {
			writeErr(w, http.StatusNotFound, "NOT_FOUND", "The requested resource does not exist")
			return
		}
		switch {
		case len(parts) == 6 && r.Method == http.MethodGet:
			s.mu.Lock()
			job.checks++
			if job.state != "Aborted" && job.checks >= s.JobPolls {
				job.state = "JobComplete"
				if s.FailJobs != "" {
					job.state, job.errorMessage = "Failed", s.FailJobs
				}
			} else if job.state == "UploadComplete" {
				job.state = "InProgress"
			}
			s.mu.Unlock()
			s.writeJob(w, job)
		case len(parts) == 6 && r.Method == http.MethodPatch:
			s.mu.Lock()
			if job.state == "JobComplete" || job.state == "Failed" {
				s.mu.Unlock()
				writeErr(w, http.StatusBadRequest, "INVALIDJOBSTATE", "the job is already complete")
				return
			}
			job.state = "Aborted"
			s.mu.Unlock()
			s.writeJob(w, job)
		case len(parts) == 6 && r.Method == http.MethodDelete:
			s.mu.Lock()
			delete(s.jobs, job.id)
			s.bulkDeleted++
			s.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		case len(parts) == 7 && parts[6] == "results" && r.Method == http.MethodGet:
			s.results(w, r, job)
		default:
			writeErr(w, http.StatusNotFound, "NOT_FOUND", "unknown path")
		}
	default:
		writeErr(w, http.StatusNotFound, "NOT_FOUND", "unknown path")
	}
}

func (s *Server) writeJob(w http.ResponseWriter, job *bulkJob) {
	s.mu.Lock()
	out := map[string]any{"id": job.id, "operation": "query", "object": "", "state": job.state}
	if job.state == "JobComplete" {
		out["numberRecordsProcessed"] = len(job.rows) - 1
	}
	if job.errorMessage != "" {
		out["errorMessage"] = job.errorMessage
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) results(w http.ResponseWriter, r *http.Request, job *bulkJob) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job.state != "JobComplete" {
		writeErr(w, http.StatusBadRequest, "INVALIDJOBSTATE", "the job is not complete")
		return
	}
	per, err := strconv.Atoi(r.URL.Query().Get("maxRecords"))
	if err != nil || per <= 0 {
		per = 1000
	}
	start := 0
	if loc := r.URL.Query().Get("locator"); loc != "" {
		var ok bool
		if start, ok = job.locators[loc]; !ok {
			writeErr(w, http.StatusBadRequest, "INVALIDLOCATOR", "invalid locator")
			return
		}
	}
	data := job.rows[1:]
	end := min(start+per, len(data))
	var buf bytes.Buffer
	cw := csv.NewWriter(&buf)
	_ = cw.Write(job.rows[0])
	_ = cw.WriteAll(data[start:end])
	next := "null"
	if end < len(data) {
		next = fmt.Sprintf("MTAw%d", end)
		job.locators[next] = end
	}
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Sforce-Locator", next)
	w.Header().Set("Sforce-NumberOfRecords", strconv.Itoa(end-start))
	_, _ = w.Write(buf.Bytes())
}

// bulkQuery evaluates a query job's SOQL into CSV rows.
func (s *Server) bulkQuery(soql string, all bool) ([][]string, error) {
	if strings.Contains(soql, "(SELECT") {
		return nil, fmt.Errorf("FeatureNotEnabled: Cannot use nested queries in Bulk API 2.0")
	}
	m := bulkRE.FindStringSubmatch(soql)
	if m == nil {
		return nil, fmt.Errorf("MALFORMED_QUERY: unsupported query: %s", soql)
	}
	fields := strings.Split(m[1], ", ")
	type cond struct{ field, op, value string }
	var conds []cond
	if m[3] != "" {
		for _, part := range strings.Split(m[3], " AND ") {
			c := condRE.FindStringSubmatch(strings.TrimSpace(part))
			if c == nil {
				return nil, fmt.Errorf("MALFORMED_QUERY: unsupported condition %q", part)
			}
			conds = append(conds, cond{c[1], c[2], c[3]})
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.records[m[2]]))
	for id := range s.records[m[2]] {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	rows := [][]string{fields}
	for _, id := range ids {
		rec := s.records[m[2]][id]
		if deleted, _ := rec["IsDeleted"].(bool); deleted && !all {
			continue
		}
		keep := true
		for _, c := range conds {
			v, ok := path(rec, c.field)
			var eq bool
			if c.value == "null" {
				eq = !ok || v == nil
			} else {
				eq = ok && v != nil && fmt.Sprint(v) == strings.Trim(c.value, "'")
			}
			if (c.op == "=") != eq {
				keep = false
			}
		}
		if !keep {
			continue
		}
		row := make([]string, len(fields))
		for i, f := range fields {
			if v, ok := path(rec, f); ok && v != nil {
				row[i] = fmt.Sprint(v)
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// path reads a field or a relationship field (Owner.Email).
func path(rec map[string]any, field string) (any, bool) {
	var v any = rec
	for _, part := range strings.Split(field, ".") {
		m, ok := v.(map[string]any)
		if !ok {
			return nil, false
		}
		if v, ok = m[part]; !ok {
			return nil, false
		}
	}
	return v, true
}
