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

// Command perf-report queries the acceptance-test Prometheus, snapshots the key
// performance metrics, compares them against a baseline (typically captured on
// main), and renders a Markdown dashboard for the GitHub Actions run summary.
//
// It is intentionally tolerant: a missing metric (no traffic of that kind in the
// suite) renders as "n/a" rather than failing, and a missing/empty baseline
// degrades to a current-only report with no delta column.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// metricSpec is a single headline number to extract from Prometheus.
//
// PromQL queries the raw cumulative histogram buckets/counters (no rate): every
// Weaviate container is fresh for the test, so the cumulative value at job end
// is the statistic "over the whole run". histogram_quantile over sum-by-le of
// the buckets yields the quantile across every node/package at once.
type metricSpec struct {
	Key           string // stable identifier, also the JSON key
	Title         string // human label in the report
	Group         string // section heading
	Unit          string // ms, req, obj, …
	PromQL        string
	LowerIsBetter bool
	Chart         bool // include a baseline-vs-current Mermaid bar chart
}

var specs = []metricSpec{
	// --- Query latency (histogram, ms) ---
	{"query_p50", "Query latency p50", "Query latency", "ms", `histogram_quantile(0.50, sum by (le) (queries_durations_ms_bucket))`, true, false},
	{"query_p95", "Query latency p95", "Query latency", "ms", `histogram_quantile(0.95, sum by (le) (queries_durations_ms_bucket))`, true, true},
	{"query_p99", "Query latency p99", "Query latency", "ms", `histogram_quantile(0.99, sum by (le) (queries_durations_ms_bucket))`, true, false},
	{"query_avg", "Query latency avg", "Query latency", "ms", `sum(queries_durations_ms_sum) / sum(queries_durations_ms_count)`, true, false},

	// --- Batch / import latency (histogram, ms) ---
	{"batch_p95", "Batch op p95", "Import / batch", "ms", `histogram_quantile(0.95, sum by (le) (batch_durations_ms_bucket))`, true, true},
	{"batch_avg", "Batch op avg", "Import / batch", "ms", `sum(batch_durations_ms_sum) / sum(batch_durations_ms_count)`, true, false},
	{"batch_objects", "Batch objects processed", "Import / batch", "obj", `sum(batch_objects_processed_total)`, false, false},

	// --- API latency (histograms are in seconds → ×1000 for ms) ---
	{"http_p95", "HTTP request p95", "API latency", "ms", `1000 * histogram_quantile(0.95, sum by (le) (http_request_duration_seconds_bucket))`, true, true},
	{"grpc_p95", "gRPC request p95", "API latency", "ms", `1000 * histogram_quantile(0.95, sum by (le) (grpc_server_request_duration_seconds_bucket))`, true, false},

	// --- Volume (context, not pass/fail) ---
	{"total_queries", "Total queries", "Volume", "req", `sum(queries_durations_ms_count)`, false, false},
	{"total_http", "Total HTTP requests", "Volume", "req", `sum(http_request_duration_seconds_count)`, false, false},
}

// perPackageQuery extracts query p95 broken down by the test_pkg label so
// per-area regressions stay legible even when the aggregate is noisy.
const perPackageQuery = `histogram_quantile(0.95, sum by (le, test_pkg) (queries_durations_ms_bucket))`

type metricValue struct {
	Value         float64 `json:"value"`
	Unit          string  `json:"unit"`
	LowerIsBetter bool    `json:"lower_is_better"`
	Present       bool    `json:"present"`
}

type report struct {
	GitSHA    string                 `json:"git_sha"`
	GitBranch string                 `json:"git_branch"`
	Timestamp string                 `json:"timestamp"`
	Metrics   map[string]metricValue `json:"metrics"`
}

func main() {
	var promURL, outPath, baselinePath, title string
	var regressPct float64
	flag.StringVar(&promURL, "prom", "http://localhost:9090", "Prometheus base URL")
	flag.StringVar(&outPath, "out", "", "Write the captured metrics JSON to this path (optional)")
	flag.StringVar(&baselinePath, "baseline", "", "Baseline metrics JSON to compare against (optional)")
	flag.StringVar(&title, "title", "Acceptance performance metrics", "Report title")
	flag.Float64Var(&regressPct, "regress-pct", 10, "Flag a regression when a lower-is-better metric worsens by more than this percent")
	flag.Parse()

	cur := report{
		GitSHA:    getenv("GIT_SHA", "unknown"),
		GitBranch: getenv("GIT_BRANCH", "unknown"),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Metrics:   map[string]metricValue{},
	}

	c := &promClient{base: strings.TrimRight(promURL, "/"), http: &http.Client{Timeout: 30 * time.Second}}
	for _, s := range specs {
		v, ok, err := c.queryScalar(s.PromQL)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warn: query %q failed: %v\n", s.Key, err)
		}
		cur.Metrics[s.Key] = metricValue{Value: v, Unit: s.Unit, LowerIsBetter: s.LowerIsBetter, Present: ok}
	}

	if outPath != "" {
		if err := writeJSON(outPath, cur); err != nil {
			fmt.Fprintf(os.Stderr, "warn: write %q: %v\n", outPath, err)
		}
	}

	var base *report
	if baselinePath != "" {
		if b, err := readBaseline(baselinePath); err != nil {
			fmt.Fprintf(os.Stderr, "info: no usable baseline (%v); rendering current-only\n", err)
		} else {
			base = b
		}
	}

	perPkg, err := c.queryByLabel(perPackageQuery, "test_pkg")
	if err != nil {
		fmt.Fprintf(os.Stderr, "warn: per-package query failed: %v\n", err)
	}

	fmt.Print(renderMarkdown(title, regressPct, cur, base, perPkg))
}

// ---------------------------------------------------------------------------
// Prometheus client
// ---------------------------------------------------------------------------

type promClient struct {
	base string
	http *http.Client
}

type promResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string          `json:"resultType"`
		Result     json.RawMessage `json:"result"`
	} `json:"data"`
	Error string `json:"error"`
}

type promSample struct {
	Metric map[string]string `json:"metric"`
	Value  [2]interface{}    `json:"value"`
}

func (c *promClient) rawQuery(q string) (*promResponse, error) {
	u := fmt.Sprintf("%s/api/v1/query?query=%s", c.base, url.QueryEscape(q))
	resp, err := c.http.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var pr promResponse
	if err := json.Unmarshal(body, &pr); err != nil {
		return nil, err
	}
	if pr.Status != "success" {
		return nil, fmt.Errorf("prometheus error: %s", pr.Error)
	}
	return &pr, nil
}

// queryScalar returns a single number: a scalar result directly, or the sum of
// a vector's finite samples. ok=false means no (finite) data was returned.
func (c *promClient) queryScalar(q string) (float64, bool, error) {
	pr, err := c.rawQuery(q)
	if err != nil {
		return 0, false, err
	}
	switch pr.Data.ResultType {
	case "scalar":
		var raw [2]interface{}
		if err := json.Unmarshal(pr.Data.Result, &raw); err != nil {
			return 0, false, err
		}
		f, ok := parseSampleValue(raw[1])
		return f, ok, nil
	case "vector":
		var samples []promSample
		if err := json.Unmarshal(pr.Data.Result, &samples); err != nil {
			return 0, false, err
		}
		var sum float64
		var any bool
		for _, s := range samples {
			if f, ok := parseSampleValue(s.Value[1]); ok {
				sum += f
				any = true
			}
		}
		return sum, any, nil
	default:
		return 0, false, nil
	}
}

// queryByLabel returns finite vector samples keyed by the given label value.
func (c *promClient) queryByLabel(q, label string) (map[string]float64, error) {
	pr, err := c.rawQuery(q)
	if err != nil {
		return nil, err
	}
	out := map[string]float64{}
	if pr.Data.ResultType != "vector" {
		return out, nil
	}
	var samples []promSample
	if err := json.Unmarshal(pr.Data.Result, &samples); err != nil {
		return nil, err
	}
	for _, s := range samples {
		key := s.Metric[label]
		if key == "" {
			key = "(unlabeled)"
		}
		if f, ok := parseSampleValue(s.Value[1]); ok {
			out[key] = f
		}
	}
	return out, nil
}

// parseSampleValue parses a Prometheus sample value ("1.23"), rejecting NaN/Inf.
func parseSampleValue(v interface{}) (float64, bool) {
	s, ok := v.(string)
	if !ok {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	return f, true
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

func renderMarkdown(title string, regressPct float64, cur report, base *report, perPkg map[string]float64) string {
	var b strings.Builder
	fmt.Fprintf(&b, "## 📊 %s\n\n", title)
	fmt.Fprintf(&b, "Commit `%s` on `%s` · captured %s\n\n", cur.GitSHA, cur.GitBranch, cur.Timestamp)
	if base != nil {
		fmt.Fprintf(&b, "Baseline: commit `%s` on `%s`\n\n", base.GitSHA, base.GitBranch)
	} else {
		b.WriteString("_No baseline available — showing current values only._\n\n")
	}

	// Group specs in declaration order, preserving first-seen group order.
	var groupOrder []string
	byGroup := map[string][]metricSpec{}
	for _, s := range specs {
		if _, seen := byGroup[s.Group]; !seen {
			groupOrder = append(groupOrder, s.Group)
		}
		byGroup[s.Group] = append(byGroup[s.Group], s)
	}

	regressions := 0
	for _, g := range groupOrder {
		fmt.Fprintf(&b, "### %s\n\n", g)
		if base != nil {
			b.WriteString("| Metric | Baseline | Current | Δ |\n|---|---:|---:|---:|\n")
		} else {
			b.WriteString("| Metric | Current |\n|---|---:|\n")
		}
		for _, s := range byGroup[g] {
			cv := cur.Metrics[s.Key]
			if base == nil {
				fmt.Fprintf(&b, "| %s | %s |\n", s.Title, fmtVal(cv))
				continue
			}
			bv, hasBase := base.Metrics[s.Key]
			delta, regressed := deltaCell(cv, bv, hasBase, regressPct)
			if regressed {
				regressions++
			}
			baseCell := "n/a"
			if hasBase {
				baseCell = fmtVal(bv)
			}
			fmt.Fprintf(&b, "| %s | %s | %s | %s |\n", s.Title, baseCell, fmtVal(cv), delta)
		}
		b.WriteString("\n")
	}

	// Headline charts: baseline vs current for the flagged metrics.
	if base != nil {
		for _, s := range specs {
			if !s.Chart {
				continue
			}
			cv := cur.Metrics[s.Key]
			bv, hasBase := base.Metrics[s.Key]
			if !cv.Present || !hasBase || !bv.Present {
				continue
			}
			fmt.Fprint(&b, mermaidBar(s.Title+" ("+s.Unit+")", s.Unit, bv.Value, cv.Value))
		}
	}

	// Per-package query latency (top offenders first).
	if len(perPkg) > 0 {
		b.WriteString("### Query latency p95 by package\n\n")
		b.WriteString("| Package | p95 (ms) |\n|---|---:|\n")
		type kv struct {
			k string
			v float64
		}
		rows := make([]kv, 0, len(perPkg))
		for k, v := range perPkg {
			rows = append(rows, kv{k, v})
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].v > rows[j].v })
		for _, r := range rows {
			fmt.Fprintf(&b, "| `%s` | %s |\n", r.k, fmtNum(r.v))
		}
		b.WriteString("\n")
	}

	if base != nil {
		if regressions > 0 {
			fmt.Fprintf(&b, "> ⚠️ **%d metric(s) regressed by more than %.0f%%.**\n", regressions, regressPct)
		} else {
			b.WriteString("> ✅ No regressions beyond threshold.\n")
		}
	}
	return b.String()
}

// deltaCell formats the change vs baseline and reports whether it is a
// regression worse than the threshold (only meaningful for latency metrics).
func deltaCell(cur, base metricValue, hasBase bool, regressPct float64) (string, bool) {
	if !hasBase || !base.Present || !cur.Present {
		return "n/a", false
	}
	if base.Value == 0 {
		return "n/a", false
	}
	pct := (cur.Value - base.Value) / math.Abs(base.Value) * 100
	arrow := "➡️"
	regressed := false
	switch {
	case pct > 0.5:
		arrow = "📈"
		if cur.LowerIsBetter && pct > regressPct {
			arrow = "🔴"
			regressed = true
		}
	case pct < -0.5:
		arrow = "📉"
		if !cur.LowerIsBetter && -pct > regressPct {
			arrow = "🔴"
			regressed = true
		}
	}
	return fmt.Sprintf("%s %+.1f%%", arrow, pct), regressed
}

// mermaidBar renders a two-bar baseline-vs-current chart. GitHub renders Mermaid
// in job summaries; xychart-beta draws the comparison.
func mermaidBar(title, unit string, baseline, current float64) string {
	return fmt.Sprintf("```mermaid\nxychart-beta\n    title \"%s\"\n    x-axis [baseline, current]\n    y-axis \"%s\"\n    bar [%.3f, %.3f]\n```\n\n",
		title, unit, baseline, current)
}

func fmtVal(v metricValue) string {
	if !v.Present {
		return "n/a"
	}
	return fmt.Sprintf("%s %s", fmtNum(v.Value), v.Unit)
}

func fmtNum(f float64) string {
	if f >= 1000 || f == math.Trunc(f) {
		return strconv.FormatFloat(f, 'f', 0, 64)
	}
	return strconv.FormatFloat(f, 'f', 2, 64)
}

// ---------------------------------------------------------------------------
// IO helpers
// ---------------------------------------------------------------------------

func readBaseline(path string) (*report, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil, fmt.Errorf("empty baseline file")
	}
	var r report
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	if len(r.Metrics) == 0 {
		return nil, fmt.Errorf("baseline has no metrics")
	}
	return &r, nil
}

func writeJSON(path string, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
