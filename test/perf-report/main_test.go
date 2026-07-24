//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeProm serves canned instant-query responses keyed by an exact-or-substring
// match on the query string, so tests can drive the client deterministically.
func fakeProm(t *testing.T, scalars map[string]string, vectors map[string]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		for sub, val := range scalars {
			if strings.Contains(q, sub) {
				fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[0,"%s"]}]}}`, val)
				return
			}
		}
		for sub, body := range vectors {
			if strings.Contains(q, sub) {
				fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":%s}}`, body)
				return
			}
		}
		// default: empty vector (metric absent)
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
	}))
}

func TestQueryScalar(t *testing.T) {
	tests := []struct {
		name    string
		scalars map[string]string
		query   string
		wantVal float64
		wantOK  bool
	}{
		{"present", map[string]string{"queries_durations_ms_bucket": "12.5"}, "histogram_quantile(0.95, queries_durations_ms_bucket)", 12.5, true},
		{"absent", map[string]string{}, "missing_metric", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := fakeProm(t, tt.scalars, nil)
			defer srv.Close()
			c := &promClient{base: srv.URL, http: srv.Client()}
			v, ok, err := c.queryScalar(tt.query)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if ok != tt.wantOK || (ok && v != tt.wantVal) {
				t.Fatalf("got (%v,%v) want (%v,%v)", v, ok, tt.wantVal, tt.wantOK)
			}
		})
	}
}

func TestQueryScalarRejectsNaN(t *testing.T) {
	srv := fakeProm(t, map[string]string{"q": "NaN"}, nil)
	defer srv.Close()
	c := &promClient{base: srv.URL, http: srv.Client()}
	_, ok, err := c.queryScalar("q")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ok {
		t.Fatalf("NaN should be treated as absent")
	}
}

func TestQueryByLabel(t *testing.T) {
	body := `[{"metric":{"test_pkg":"grpc"},"value":[0,"3.1"]},{"metric":{"test_pkg":"schema"},"value":[0,"7.2"]}]`
	srv := fakeProm(t, nil, map[string]string{"test_pkg": body})
	defer srv.Close()
	c := &promClient{base: srv.URL, http: srv.Client()}
	got, err := c.queryByLabel("... by (test_pkg) ...", "test_pkg")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got["grpc"] != 3.1 || got["schema"] != 7.2 {
		t.Fatalf("unexpected map: %v", got)
	}
}

func TestDeltaCell(t *testing.T) {
	tests := []struct {
		name          string
		cur, base     metricValue
		hasBase       bool
		wantRegressed bool
		wantContains  string
	}{
		{
			name:          "latency regression flagged",
			cur:           metricValue{Value: 120, LowerIsBetter: true, Present: true},
			base:          metricValue{Value: 100, LowerIsBetter: true, Present: true},
			hasBase:       true,
			wantRegressed: true,
			wantContains:  "+20.0%",
		},
		{
			name:          "latency improvement not flagged",
			cur:           metricValue{Value: 80, LowerIsBetter: true, Present: true},
			base:          metricValue{Value: 100, LowerIsBetter: true, Present: true},
			hasBase:       true,
			wantRegressed: false,
			wantContains:  "-20.0%",
		},
		{
			name:          "small change within threshold",
			cur:           metricValue{Value: 103, LowerIsBetter: true, Present: true},
			base:          metricValue{Value: 100, LowerIsBetter: true, Present: true},
			hasBase:       true,
			wantRegressed: false,
			wantContains:  "+3.0%",
		},
		{
			name:         "missing baseline",
			cur:          metricValue{Value: 100, LowerIsBetter: true, Present: true},
			hasBase:      false,
			wantContains: "n/a",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cell, regressed := deltaCell(tt.cur, tt.base, tt.hasBase, 10)
			if regressed != tt.wantRegressed {
				t.Errorf("regressed=%v want %v", regressed, tt.wantRegressed)
			}
			if !strings.Contains(cell, tt.wantContains) {
				t.Errorf("cell %q missing %q", cell, tt.wantContains)
			}
		})
	}
}

func TestRenderMarkdownWithBaseline(t *testing.T) {
	cur := report{GitSHA: "abc", GitBranch: "pr", Metrics: map[string]metricValue{
		"query_p95": {Value: 130, Unit: "ms", LowerIsBetter: true, Present: true},
	}}
	base := &report{GitSHA: "def", GitBranch: "main", Metrics: map[string]metricValue{
		"query_p95": {Value: 100, Unit: "ms", LowerIsBetter: true, Present: true},
	}}
	out := renderMarkdown("Perf", 10, cur, base, map[string]float64{"grpc": 5})
	for _, want := range []string{"Query latency p95", "Baseline", "regressed", "```mermaid", "by package", "grpc"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n---\n%s", want, out)
		}
	}
}

func TestRenderMarkdownNoBaseline(t *testing.T) {
	cur := report{GitSHA: "abc", GitBranch: "pr", Metrics: map[string]metricValue{
		"query_p95": {Value: 130, Unit: "ms", LowerIsBetter: true, Present: true},
	}}
	out := renderMarkdown("Perf", 10, cur, nil, nil)
	if !strings.Contains(out, "No baseline available") {
		t.Errorf("expected current-only note, got:\n%s", out)
	}
	if strings.Contains(out, "Δ") {
		t.Errorf("should not render delta column without baseline:\n%s", out)
	}
}
