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

//go:build postingedgebench

// Turns the result files of the benchmark into the tables and charts of the
// benchmark report:
//
//	go test -tags postingedgebench -run TestBenchmarkReport \
//	    ./adapters/repos/db/vector/postingedge/ \
//	    -bench.report.postingedge postingedge.json -bench.report.hnsw hnsw.json \
//	    -bench.report.postingedge.work postingedge-work.json \
//	    -bench.report.hnsw.work hnsw-work.json \
//	    -bench.report.out testdata/bench
//
// The first two files come from TestBenchmark, one per arm. The others are
// optional: the work files (TestBenchmark with -bench.mode work for the
// posting-edge index, TestSearchWork of the hnsw package for HNSW), an earlier
// posting-edge result to compare against (-bench.report.before), a source
// margin sweep (-bench.report.margin), a probe sweep (-bench.report.probes)
// and work results to compare at equal ef (-bench.report.probecheck).

package postingedge

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	reportPostingEdge     = flag.String("bench.report.postingedge", "", "posting-edge result file of TestBenchmark")
	reportHNSW            = flag.String("bench.report.hnsw", "", "hnsw result file of TestBenchmark")
	reportPostingEdgeWork = flag.String("bench.report.postingedge.work", "", "posting-edge result file of TestBenchmark -bench.mode work")
	reportHNSWWork        = flag.String("bench.report.hnsw.work", "", "result file of TestSearchWork of the hnsw package")
	reportOut             = flag.String("bench.report.out", "", "directory to write tables.md and the charts to")
	reportBefore          = flag.String("bench.report.before", "", "an earlier posting-edge result file of TestBenchmark, to compare against")
	reportBeforeWork      = flag.String("bench.report.before.work", "", "the work result file that goes with -bench.report.before")
	reportMargin          = flag.String("bench.report.margin", "", "posting-edge work result file of a source margin sweep")
	reportProbes          = flag.String("bench.report.probes", "", "posting-edge result file of TestBenchmark -bench.mode probes")
	reportProbeCheck      = flag.String("bench.report.probecheck", "", "posting-edge work result files to compare at equal ef, as label=path pairs separated by commas")
)

// marginTolerance is how much recall a source margin may cost at any ef, and
// marginSaving how much of the endpoint scoring it has to save to be worth
// having.
const (
	marginTolerance = 0.002
	marginSaving    = 0.40
)

// isoRecallTargets are the recall levels the two indexes are compared at.
var isoRecallTargets = []float64{0.90, 0.95, 0.99}

// profileRecall is the recall level at which the search time is broken down
// by phase.
const profileRecall = 0.95

// hnswWork mirrors the result file of TestSearchWork of the hnsw package.
type hnswWork struct {
	Vectors int `json:"vectors"`
	Levels  []struct {
		Level       int `json:"level"`
		Nodes       int `json:"nodes"`
		Connections int `json:"connections"`
	} `json:"levels"`
	PackedConnectionBytes int64 `json:"packedConnectionBytes"`
	Points                []struct {
		EF              int     `json:"ef"`
		RescoreLimit    int     `json:"rescoreLimit"`
		Recall          float64 `json:"recall"`
		CodeDistances   float64 `json:"codeDistances"`
		VectorDistances float64 `json:"vectorDistances"`
	} `json:"points"`
}

func TestBenchmarkReport(t *testing.T) {
	if *reportPostingEdge == "" || *reportHNSW == "" || *reportOut == "" {
		t.Skip("needs -bench.report.postingedge, -bench.report.hnsw and -bench.report.out")
	}
	var ours, theirs benchResult
	readJSON(t, *reportPostingEdge, &ours)
	readJSON(t, *reportHNSW, &theirs)
	var oursWork *benchResult
	if *reportPostingEdgeWork != "" {
		oursWork = &benchResult{}
		readJSON(t, *reportPostingEdgeWork, oursWork)
	}
	var theirsWork *hnswWork
	if *reportHNSWWork != "" {
		theirsWork = &hnswWork{}
		readJSON(t, *reportHNSWWork, theirsWork)
	}
	require.NoError(t, os.MkdirAll(*reportOut, 0o755))

	var md strings.Builder
	if *reportBefore != "" {
		var before benchResult
		readJSON(t, *reportBefore, &before)
		var beforeWork *benchResult
		if *reportBeforeWork != "" {
			beforeWork = &benchResult{}
			readJSON(t, *reportBeforeWork, beforeWork)
		}
		writeBeforeAfterTable(&md, &before, &ours, &theirs)
		writeProfileComparison(&md, &before, beforeWork, &ours, oursWork)
	}
	if *reportMargin != "" {
		var margin benchResult
		readJSON(t, *reportMargin, &margin)
		writeMarginTables(&md, &margin)
	}
	if *reportProbes != "" {
		var probes benchResult
		readJSON(t, *reportProbes, &probes)
		writeProbeTable(&md, &probes)
	}
	if *reportProbeCheck != "" {
		var labels []string
		var results []*benchResult
		for _, pair := range strings.Split(*reportProbeCheck, ",") {
			label, path, found := strings.Cut(pair, "=")
			require.Truef(t, found, "-bench.report.probecheck entry %q is not label=path", pair)
			result := &benchResult{}
			readJSON(t, path, result)
			labels, results = append(labels, label), append(results, result)
		}
		writeEqualEFTable(&md, labels, results)
	}
	writeMarginFrontierTable(&md, &ours)
	writeIsoRecallTable(&md, &ours, &theirs)
	writeBuildTable(&md, &ours, &theirs)
	writeMemoryTables(&md, &ours, oursWork, &theirs, theirsWork)
	writeStructureTable(&md, &ours)
	writeWorkTables(&md, &ours, oursWork, theirsWork)
	writeProfileTable(&md, &ours, oursWork)
	writePostingEdgePoints(&md, &ours)
	writeHNSWPoints(&md, &theirs)
	require.NoError(t, os.WriteFile(filepath.Join(*reportOut, "tables.md"), []byte(md.String()), 0o644))

	for _, threads := range threadCounts(&ours, &theirs) {
		chart := recallQPSChart(&ours, &theirs, threads)
		name := fmt.Sprintf("recall-qps-threads%d.svg", threads)
		require.NoError(t, os.WriteFile(filepath.Join(*reportOut, name), []byte(chart), 0o644))
	}
}

func readJSON(t *testing.T, path string, into any) {
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, into))
}

func threadCounts(results ...*benchResult) []int {
	var out []int
	for _, result := range results {
		for _, point := range result.Points {
			if !slices.Contains(out, point.Threads) {
				out = append(out, point.Threads)
			}
		}
	}
	slices.Sort(out)
	return out
}

// pick returns the points of a result measured with the given thread count
// that the filter accepts.
func pick(result *benchResult, threads int, accept func(benchPoint) bool) []benchPoint {
	var out []benchPoint
	for _, point := range result.Points {
		if point.Threads == threads && (accept == nil || accept(point)) {
			out = append(out, point)
		}
	}
	return out
}

// frontier reduces a set of operating points to its Pareto frontier: the
// points no other point beats on both recall and QPS, by ascending recall.
func frontier(points []benchPoint) []benchPoint {
	sorted := slices.Clone(points)
	slices.SortFunc(sorted, func(a, b benchPoint) int {
		switch {
		case a.Recall > b.Recall:
			return -1
		case a.Recall < b.Recall:
			return 1
		case a.QPS > b.QPS:
			return -1
		case a.QPS < b.QPS:
			return 1
		}
		return 0
	})
	// From the highest recall down, a point is on the frontier if it is
	// faster than everything with at least its recall.
	var out []benchPoint
	best := math.Inf(-1)
	for _, point := range sorted {
		if point.QPS > best {
			out = append(out, point)
			best = point.QPS
		}
	}
	slices.Reverse(out)
	return out
}

// qpsAtRecall reads a frontier at a recall level, interpolating between the
// two neighboring points linearly in recall and in log QPS. A target below
// the frontier takes its fastest point, since any point above the target
// qualifies. ok is false if the frontier never reaches the target.
func qpsAtRecall(front []benchPoint, target float64) (qps float64, ok bool) {
	if len(front) == 0 || front[len(front)-1].Recall < target {
		return 0, false
	}
	if front[0].Recall >= target {
		return front[0].QPS, true
	}
	for i := 1; i < len(front); i++ {
		low, high := front[i-1], front[i]
		if high.Recall < target {
			continue
		}
		share := (target - low.Recall) / (high.Recall - low.Recall)
		return math.Exp(math.Log(low.QPS) + share*(math.Log(high.QPS)-math.Log(low.QPS))), true
	}
	return 0, false
}

// writeIsoRecallTable compares the QPS of the two indexes at equal recall,
// first with both at the same rescore limit and the default load batch, then
// with each index on the best operating points it has.
func writeIsoRecallTable(md *strings.Builder, ours, theirs *benchResult) {
	protocol := 0
	for _, point := range theirs.Points {
		protocol = max(protocol, point.RescoreLimit)
	}
	views := []struct {
		name   string
		ours   func(benchPoint) bool
		theirs func(benchPoint) bool
	}{
		{
			name: fmt.Sprintf("same rescore limit (%d), load batch %d", protocol, DefaultLoadBatch),
			ours: func(p benchPoint) bool {
				return p.RescoreLimit == protocol && p.LoadBatch == DefaultLoadBatch
			},
			theirs: func(p benchPoint) bool { return p.RescoreLimit == protocol },
		},
		{name: "best measured points of each index"},
	}

	fmt.Fprintf(md, "## QPS at equal recall\n\n")
	fmt.Fprintf(md, "Read off each index's Pareto frontier, interpolated linearly in recall and log QPS.\n\n")
	fmt.Fprintf(md, "| comparison | threads | recall | posting-edge QPS | HNSW QPS | HNSW / posting-edge |\n|---|---|---|---|---|---|\n")
	for _, view := range views {
		for _, threads := range threadCounts(ours, theirs) {
			oursFront := frontier(pick(ours, threads, view.ours))
			theirsFront := frontier(pick(theirs, threads, view.theirs))
			for _, target := range isoRecallTargets {
				oursQPS, oursOK := qpsAtRecall(oursFront, target)
				theirsQPS, theirsOK := qpsAtRecall(theirsFront, target)
				ratio := "—"
				if oursOK && theirsOK {
					ratio = fmt.Sprintf("%.2fx", theirsQPS/oursQPS)
				}
				fmt.Fprintf(md, "| %s | %d | %.2f | %s | %s | %s |\n", view.name, threads, target,
					qpsCell(oursQPS, oursOK), qpsCell(theirsQPS, theirsOK), ratio)
			}
		}
	}
	fmt.Fprintf(md, "\n")
}

// writeBeforeAfterTable compares an earlier posting-edge result with the
// current one at equal recall, each on its best measured points, and both
// against HNSW.
func writeBeforeAfterTable(md *strings.Builder, before, after, theirs *benchResult) {
	fmt.Fprintf(md, "## QPS at equal recall, before and after\n\n")
	fmt.Fprintf(md, "Each index on its best measured points, read off its Pareto frontier.\n\n")
	fmt.Fprintf(md, "| threads | recall | before QPS | after QPS | after / before | HNSW QPS | HNSW / before | HNSW / after |\n|---|---|---|---|---|---|---|---|\n")
	for _, threads := range threadCounts(before, after, theirs) {
		beforeFront := frontier(pick(before, threads, nil))
		afterFront := frontier(pick(after, threads, nil))
		theirsFront := frontier(pick(theirs, threads, nil))
		for _, target := range isoRecallTargets {
			b, bOK := qpsAtRecall(beforeFront, target)
			a, aOK := qpsAtRecall(afterFront, target)
			h, hOK := qpsAtRecall(theirsFront, target)
			ratio := func(x, y float64, ok bool) string {
				if !ok {
					return "—"
				}
				return fmt.Sprintf("%.2fx", x/y)
			}
			fmt.Fprintf(md, "| %d | %.2f | %s | %s | %s | %s | %s | %s |\n", threads, target,
				qpsCell(b, bOK), qpsCell(a, aOK), ratio(a, b, aOK && bOK),
				qpsCell(h, hOK), ratio(h, b, hOK && bOK), ratio(h, a, hOK && aOK))
		}
	}
	fmt.Fprintf(md, "\n")
}

// profileAt returns the single-thread point of a configuration at the lowest
// ef that reaches the profile recall, with its work stats.
func profileAt(points []benchPoint, accept func(benchPoint) bool) *benchPoint {
	var chosen *benchPoint
	for i := range points {
		p := &points[i]
		if p.Stats == nil || p.Recall < profileRecall || !accept(*p) {
			continue
		}
		if chosen == nil || p.EF < chosen.EF {
			chosen = p
		}
	}
	return chosen
}

// writeProfileComparison breaks the search time down by phase before and
// after, for the default load batch and each index at its lowest rescore
// limit: the configuration either would be run with.
func writeProfileComparison(md *strings.Builder, before, beforeWork, after, afterWork *benchResult) {
	lowestRescore := func(points []benchPoint) int {
		lowest := math.MaxInt
		for _, p := range points {
			lowest = min(lowest, p.RescoreLimit)
		}
		return lowest
	}
	rows := []struct {
		name   string
		points []benchPoint
	}{{"before", workPoints(before, beforeWork)}, {"after", workPoints(after, afterWork)}}

	fmt.Fprintf(md, "## Where the time goes at recall %.2f, before and after\n\n", profileRecall)
	fmt.Fprintf(md, "Mean microseconds per query on one thread, load batch %d, at the lowest ef that reaches the recall.\n\n", DefaultLoadBatch)
	fmt.Fprintf(md, "| | ef | rescore | margin | recall | centroid stage | posting scan | endpoint scoring | round selection | rescore | other | total | endpoints scored |\n")
	fmt.Fprintf(md, "|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, row := range rows {
		rescore := lowestRescore(row.points)
		// Of the margins measured, the one that scores the fewest
		// endpoints is the one the index would be run with.
		var chosen *benchPoint
		for _, margin := range sourceMargins(row.points) {
			p := profileAt(row.points, func(p benchPoint) bool {
				return p.RescoreLimit == rescore && p.LoadBatch == DefaultLoadBatch && p.SourceMargin == margin
			})
			if p != nil && (chosen == nil || p.Stats.ExpandUs < chosen.Stats.ExpandUs) {
				chosen = p
			}
		}
		if chosen == nil {
			continue
		}
		s := chosen.Stats
		phases := s.CentroidUs + s.ScanUs + s.ExpandUs + s.SelectUs + s.RescoreUs
		total := s.TotalUs
		if total == 0 {
			total = chosen.MeanMs * 1000
		}
		cell := func(us float64) string { return fmt.Sprintf("%.0f (%.0f%%)", us, 100*us/total) }
		fmt.Fprintf(md, "| %s | %d | %d | %.2f | %.4f | %s | %s | %s | %s | %s | %s | %.0f | %.0f |\n",
			row.name, chosen.EF, chosen.RescoreLimit, chosen.SourceMargin, chosen.Recall,
			cell(s.CentroidUs), cell(s.ScanUs), cell(s.ExpandUs), cell(s.SelectUs), cell(s.RescoreUs),
			cell(total-phases), total, s.EndpointsMean)
	}
	fmt.Fprintf(md, "\n")
}

// fastEndRecall is the recall floor of the fast end of a frontier: the best
// QPS an index reaches without dropping below it.
const fastEndRecall = 0.92

// writeMarginFrontierTable compares the recall/QPS frontier of each source
// margin with the frontier without one, at the lowest rescore limit and the
// default load batch: the QPS at the iso-recall targets and at the fast end.
// A margin dominates if it is at least as fast as no margin at every one of
// them, for every thread count.
func writeMarginFrontierTable(md *strings.Builder, ours *benchResult) {
	margins := sourceMargins(ours.Points)
	if len(margins) < 2 {
		return
	}
	rescore := math.MaxInt
	for _, p := range ours.Points {
		rescore = min(rescore, p.RescoreLimit)
	}
	// readings are, per margin, the QPS at each recall target and then at
	// the fast end; a negative value is a target the frontier never reaches.
	read := func(threads int, margin float64) []float64 {
		points := pick(ours, threads, func(p benchPoint) bool {
			return p.RescoreLimit == rescore && p.LoadBatch == DefaultLoadBatch && p.SourceMargin == margin
		})
		front := frontier(points)
		out := make([]float64, 0, len(isoRecallTargets)+1)
		for _, target := range isoRecallTargets {
			qps, ok := qpsAtRecall(front, target)
			if !ok {
				qps = -1
			}
			out = append(out, qps)
		}
		fast := -1.0
		for _, p := range points {
			if p.Recall >= fastEndRecall {
				fast = max(fast, p.QPS)
			}
		}
		return append(out, fast)
	}

	fmt.Fprintf(md, "## Source margin: frontier against no margin\n\n")
	fmt.Fprintf(md, "Rescore %d, load batch %d. QPS read off the Pareto frontier of each margin; the fast end is the best QPS with recall of at least %.2f. In brackets, against no margin.\n\n",
		rescore, DefaultLoadBatch, fastEndRecall)
	fmt.Fprintf(md, "| threads | margin |")
	for _, target := range isoRecallTargets {
		fmt.Fprintf(md, " QPS at %.2f |", target)
	}
	fmt.Fprintf(md, " fast end QPS | against no margin |\n|---|---|")
	for range isoRecallTargets {
		fmt.Fprintf(md, "---|")
	}
	fmt.Fprintf(md, "---|---|\n")

	dominates := map[float64]bool{}
	for _, margin := range margins {
		dominates[margin] = margin != 0
	}
	for _, threads := range threadCounts(ours) {
		baseline := read(threads, 0)
		for _, margin := range margins {
			readings := read(threads, margin)
			fmt.Fprintf(md, "| %d | %s |", threads, marginLabel(margin))
			worst := math.Inf(1)
			for i, qps := range readings {
				switch {
				case qps < 0:
					fmt.Fprintf(md, " not reached |")
					worst = math.Inf(-1)
				case margin == 0 || baseline[i] < 0:
					fmt.Fprintf(md, " %.0f |", qps)
				default:
					gain := qps/baseline[i] - 1
					worst = min(worst, gain)
					fmt.Fprintf(md, " %.0f (%+.1f%%) |", qps, 100*gain)
				}
			}
			verdict := ""
			if margin != 0 {
				verdict = "at least as fast everywhere"
				if worst < 0 {
					verdict = "slower somewhere"
					dominates[margin] = false
				}
			}
			fmt.Fprintf(md, " %s |\n", verdict)
		}
	}
	fmt.Fprintf(md, "\n")
	for _, margin := range margins {
		if margin == 0 {
			continue
		}
		outcome := "does not dominate"
		if dominates[margin] {
			outcome = "dominates"
		}
		fmt.Fprintf(md, "- Margin %.2f %s no margin over the measured range.\n", margin, outcome)
	}
	fmt.Fprintf(md, "\n")
}

func sourceMargins(points []benchPoint) []float64 {
	var out []float64
	for _, p := range points {
		if !slices.Contains(out, p.SourceMargin) {
			out = append(out, p.SourceMargin)
		}
	}
	slices.Sort(out)
	return out
}

// writeMarginTables shows what each source margin of a sweep costs in recall
// and saves in endpoint scoring, at every ef, against the points without a
// margin, and names the margin the calibration rule picks: the smallest one
// that costs at most marginTolerance of recall at every ef.
func writeMarginTables(md *strings.Builder, result *benchResult) {
	type key struct {
		margin float64
		ef     int
	}
	points := map[key]benchPoint{}
	var efs []int
	for _, p := range result.Points {
		points[key{p.SourceMargin, p.EF}] = p
		if !slices.Contains(efs, p.EF) {
			efs = append(efs, p.EF)
		}
	}
	slices.Sort(efs)
	margins := sourceMargins(result.Points)

	header := func(title, unit string) {
		fmt.Fprintf(md, "### %s\n\n| margin |", title)
		for _, ef := range efs {
			fmt.Fprintf(md, " ef=%d |", ef)
		}
		fmt.Fprintf(md, " %s |\n|---|", unit)
		for range efs {
			fmt.Fprintf(md, "---|")
		}
		fmt.Fprintf(md, "---|\n")
	}

	fmt.Fprintf(md, "## Source margin calibration\n\n")
	fmt.Fprintf(md, "Rescore %d, load batch %d, %d queries. Every margin against no margin at the same ef.\n\n",
		result.Points[0].RescoreLimit, result.Points[0].LoadBatch, result.Queries)

	header("Recall", "")
	for _, margin := range margins {
		fmt.Fprintf(md, "| %s |", marginLabel(margin))
		for _, ef := range efs {
			fmt.Fprintf(md, " %.4f |", points[key{margin, ef}].Recall)
		}
		fmt.Fprintf(md, " |\n")
	}
	fmt.Fprintf(md, "\n")

	chosen, found := 0.0, false
	header("Recall against no margin", "worst")
	for _, margin := range margins {
		if margin == 0 {
			continue
		}
		worst := 0.0
		fmt.Fprintf(md, "| %s |", marginLabel(margin))
		for _, ef := range efs {
			delta := points[key{margin, ef}].Recall - points[key{0, ef}].Recall
			if math.Abs(delta) > math.Abs(worst) {
				worst = delta
			}
			fmt.Fprintf(md, " %+.4f |", delta)
		}
		verdict := "within tolerance"
		if math.Abs(worst) > marginTolerance {
			verdict = "too much"
		} else if !found {
			chosen, found = margin, true
		}
		fmt.Fprintf(md, " %+.4f, %s |\n", worst, verdict)
	}
	fmt.Fprintf(md, "\n")

	header("Endpoints scored per query", "mean share of no margin")
	for _, margin := range margins {
		share := 0.0
		fmt.Fprintf(md, "| %s |", marginLabel(margin))
		for _, ef := range efs {
			scored := points[key{margin, ef}].Stats.EndpointsMean
			baseline := points[key{0, ef}].Stats.EndpointsMean
			share += scored / baseline / float64(len(efs))
			if margin == 0 {
				fmt.Fprintf(md, " %.0f |", scored)
			} else {
				fmt.Fprintf(md, " %.0f (%.0f%%) |", scored, 100*scored/baseline)
			}
		}
		fmt.Fprintf(md, " %.0f%% |\n", 100*share)
	}
	fmt.Fprintf(md, "\n")

	header("Postings loaded per query", "")
	for _, margin := range margins {
		fmt.Fprintf(md, "| %s |", marginLabel(margin))
		for _, ef := range efs {
			fmt.Fprintf(md, " %.1f |", points[key{margin, ef}].Stats.LoadsMean)
		}
		fmt.Fprintf(md, " |\n")
	}
	fmt.Fprintf(md, "\n")

	if !found {
		fmt.Fprintf(md, "No margin of the sweep stays within %.3f of recall at every ef.\n\n", marginTolerance)
		return
	}
	saved := 0.0
	for _, ef := range efs {
		saved += (1 - points[key{chosen, ef}].Stats.EndpointsMean/points[key{0, ef}].Stats.EndpointsMean) / float64(len(efs))
	}
	verdict := "pays"
	if saved < marginSaving {
		verdict = "does not pay"
	}
	fmt.Fprintf(md, "Smallest margin within %.3f of recall at every ef: **%.2f**. It saves %.0f%% of the endpoint scoring on average over the ef sweep, against a bar of %.0f%%: it %s.\n\n",
		marginTolerance, chosen, 100*saved, 100*marginSaving, verdict)
}

func marginLabel(margin float64) string {
	if margin == 0 {
		return "none"
	}
	return fmt.Sprintf("%.2f", margin)
}

func writeProbeTable(md *strings.Builder, result *benchResult) {
	fmt.Fprintf(md, "## Build probe sweep\n\n")
	fmt.Fprintf(md, "The edge builder at each probe width over one partition (%.0f s) and assignment (%.0f s).\n\n",
		result.BuildStage["partition"], result.BuildStage["assign"])
	fmt.Fprintf(md, "| probe | edge time s | candidates per vector | edges per vector | edge recall |\n|---|---|---|---|---|\n")
	for _, p := range result.ProbeSweep {
		fmt.Fprintf(md, "| %d | %.0f | %.0f | %.2f | %.4f |\n", p.Probe, p.EdgeSec, p.CandidatesPerVector, p.EdgesPerVector, p.EdgeRecall)
	}
	fmt.Fprintf(md, "\n")
}

// writeEqualEFTable compares the recall and the work of several work results
// point by point, against the first one.
func writeEqualEFTable(md *strings.Builder, labels []string, results []*benchResult) {
	type key struct {
		ef, rescore, batch int
		margin             float64
	}
	keyOf := func(p benchPoint) key { return key{p.EF, p.RescoreLimit, p.LoadBatch, p.SourceMargin} }
	indexed := make([]map[key]benchPoint, len(results))
	for i, result := range results {
		indexed[i] = map[key]benchPoint{}
		for _, p := range result.Points {
			indexed[i][keyOf(p)] = p
		}
	}

	fmt.Fprintf(md, "## Recall at equal ef\n\n| ef | rescore | batch | margin |")
	for _, label := range labels {
		fmt.Fprintf(md, " %s recall | %s endpoints |", label, label)
	}
	fmt.Fprintf(md, " recall against %s |\n|---|---|---|---|", labels[0])
	for range labels {
		fmt.Fprintf(md, "---|---|")
	}
	fmt.Fprintf(md, "---|\n")
	for _, p := range results[0].Points {
		k := keyOf(p)
		complete := true
		for i := range results {
			if _, ok := indexed[i][k]; !ok {
				complete = false
			}
		}
		if !complete {
			continue
		}
		fmt.Fprintf(md, "| %d | %d | %d | %s |", k.ef, k.rescore, k.batch, marginLabel(k.margin))
		var deltas []string
		for i := range results {
			q := indexed[i][k]
			fmt.Fprintf(md, " %.4f | %.0f |", q.Recall, q.Stats.EndpointsMean)
			if i > 0 {
				deltas = append(deltas, fmt.Sprintf("%+.4f", q.Recall-p.Recall))
			}
		}
		fmt.Fprintf(md, " %s |\n", strings.Join(deltas, " / "))
	}
	fmt.Fprintf(md, "\n")
}

func qpsCell(qps float64, ok bool) string {
	if !ok {
		return "not reached"
	}
	return fmt.Sprintf("%.0f", qps)
}

func writeBuildTable(md *strings.Builder, ours, theirs *benchResult) {
	fmt.Fprintf(md, "## Build time\n\n")
	fmt.Fprintf(md, "Both builds run on %d threads (GOMAXPROCS).\n\n", ours.GOMAXPROCS)
	fmt.Fprintf(md, "| index | stage | seconds | share |\n|---|---|---|---|\n")
	for _, arm := range []struct {
		name   string
		result *benchResult
	}{{"posting-edge", ours}, {"HNSW", theirs}} {
		for _, stage := range sortedByValue(arm.result.BuildStage) {
			fmt.Fprintf(md, "| %s | %s | %.1f | %.0f%% |\n", arm.name, stage,
				arm.result.BuildStage[stage], 100*arm.result.BuildStage[stage]/arm.result.BuildSec)
		}
		fmt.Fprintf(md, "| %s | **total** | **%.1f** | |\n", arm.name, arm.result.BuildSec)
	}
	fmt.Fprintf(md, "\n")
}

// sortedByValue returns the keys of a map, largest value first.
func sortedByValue(values map[string]float64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b string) int {
		switch {
		case values[a] > values[b]:
			return -1
		case values[a] < values[b]:
			return 1
		}
		return strings.Compare(a, b)
	})
	return keys
}

// writeMemoryTables lists the memory of both indexes by component, codes
// apart from structure, and the process memory after the build and after
// the first queries.
func writeMemoryTables(md *strings.Builder, ours, oursWork, theirs *benchResult, theirsWork *hnswWork) {
	const mb = 1 << 20
	perVector := func(megabytes float64, vectors int) float64 { return megabytes * mb / float64(vectors) }
	row := func(index, component, kind string, megabytes float64, vectors int) {
		fmt.Fprintf(md, "| %s | %s | %s | %.1f | %.1f |\n", index, component, kind, megabytes, perVector(megabytes, vectors))
	}

	fmt.Fprintf(md, "## Memory by component\n\n")
	fmt.Fprintf(md, "| index | component | kind | MB | bytes/vector |\n|---|---|---|---|---|\n")
	oc := ours.Components
	oursStructure := oc["postings"] + oc["edges"] + oc["centroidVectors"] + oc["otherMeasured"]
	row("posting-edge", "codes", "codes", oc["codes"], ours.Vectors)
	row("posting-edge", "postings: doc ids and directory", "structure", oc["postings"], ours.Vectors)
	row("posting-edge", "edges", "structure", oc["edges"], ours.Vectors)
	row("posting-edge", "centroids: vectors", "structure", oc["centroidVectors"], ours.Vectors)
	row("posting-edge", "centroids: graph, and the rest of the heap (measured)", "structure", oc["otherMeasured"], ours.Vectors)
	row("posting-edge", "**structure total**", "", oursStructure, ours.Vectors)
	row("posting-edge", "**index total (measured heap)**", "", ours.IndexHeapMB, ours.Vectors)
	row("posting-edge", "full-precision vectors (shared array)", "f32", oc["f32"], ours.Vectors)

	tc := theirs.Components
	row("HNSW", "codes", "codes", tc["codes"], theirs.Vectors)
	row("HNSW", "graph, and the rest of the heap (measured)", "structure", tc["graphMeasured"], theirs.Vectors)
	row("HNSW", "**index total (measured heap)**", "", theirs.IndexHeapMB, theirs.Vectors)
	row("HNSW", "full-precision vectors (shared array)", "f32", tc["f32"], theirs.Vectors)
	fmt.Fprintf(md, "\n")

	if theirsWork != nil && len(theirsWork.Levels) > 0 {
		totalConnections := 0
		for _, level := range theirsWork.Levels {
			totalConnections += level.Connections
		}
		packedMB := float64(theirsWork.PackedConnectionBytes) / mb
		fmt.Fprintf(md, "### HNSW graph by level\n\n")
		fmt.Fprintf(md, "From a second build of the same configuration. Packed bytes are the serialized connection lists, shared out by connection count.\n\n")
		fmt.Fprintf(md, "| level | nodes | connections | mean degree | packed MB |\n|---|---|---|---|---|\n")
		for _, level := range theirsWork.Levels {
			fmt.Fprintf(md, "| %d | %d | %d | %.1f | %.2f |\n", level.Level, level.Nodes, level.Connections,
				float64(level.Connections)/float64(max(level.Nodes, 1)),
				packedMB*float64(level.Connections)/float64(max(totalConnections, 1)))
		}
		fmt.Fprintf(md, "| **all** | | %d | | %.1f (%.1f bytes/vector) |\n\n", totalConnections, packedMB,
			perVector(packedMB, theirsWork.Vectors))
	}

	fmt.Fprintf(md, "### Process memory\n\n")
	fmt.Fprintf(md, "RSS as ps reports it; on macOS it still counts the pages the Go runtime has released as reusable, which the physical footprint does not.\n\n")
	fmt.Fprintf(md, "| index | when | RSS MB | footprint MB | live heap MB |\n|---|---|---|---|---|\n")
	process := func(index string, result *benchResult) {
		if result == nil {
			return
		}
		snapshot := func(when string, s memSnapshot) {
			footprint := "—"
			if s.FootprintMB > 0 {
				footprint = fmt.Sprintf("%.0f", s.FootprintMB)
			}
			fmt.Fprintf(md, "| %s | %s | %.0f | %s | %.0f |\n", index, when, s.RSSMB, footprint, s.HeapAllocMB)
		}
		snapshot("dataset loaded", result.Baseline)
		snapshot("after the build", result.AfterBuild)
		if result.SettleQueries > 0 {
			snapshot(fmt.Sprintf("after %d queries", result.SettleQueries), result.AfterQueries)
		}
		fmt.Fprintf(md, "| %s | peak during the build | %.0f | — | — |\n", index, result.PeakRSSMB)
	}
	// The timed posting-edge run predates the after-queries reading, so
	// the process readings of that arm come from the work run if there is
	// one.
	if oursWork != nil {
		process("posting-edge", oursWork)
	} else {
		process("posting-edge", ours)
	}
	process("HNSW", theirs)
	fmt.Fprintf(md, "\n")
}

func writeStructureTable(md *strings.Builder, ours *benchResult) {
	if len(ours.Diagnostics) == 0 {
		return
	}
	fmt.Fprintf(md, "## Posting-edge structure\n\n| | |\n|---|---|\n")
	keys := make([]string, 0, len(ours.Diagnostics))
	for key := range ours.Diagnostics {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		fmt.Fprintf(md, "| %s | %.4g |\n", key, ours.Diagnostics[key])
	}
	fmt.Fprintf(md, "\n")
}

// workPoints returns the single-thread points of the work run, or of the
// timed run if there is no work run: the work of a search does not depend on
// the thread count.
func workPoints(ours, oursWork *benchResult) []benchPoint {
	source := ours
	if oursWork != nil {
		source = oursWork
	}
	var out []benchPoint
	for _, p := range source.Points {
		if p.Threads == source.Points[0].Threads && p.Stats != nil {
			out = append(out, p)
		}
	}
	return out
}

func writeWorkTables(md *strings.Builder, ours, oursWork *benchResult, theirsWork *hnswWork) {
	fmt.Fprintf(md, "## Work per query\n\n### Posting-edge\n\n")
	fmt.Fprintf(md, "Per query. Code distances are the vectors scanned plus the endpoints scored.\n\n")
	fmt.Fprintf(md, "| ef | rescore | batch | margin | recall | postings loaded mean / p50 / p95 | rounds mean / p50 / p95 / max | vectors scanned | sources cut | edges followed | into loaded postings | repeated endpoints | endpoints scored | code distances | full-precision distances |\n")
	fmt.Fprintf(md, "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, p := range workPoints(ours, oursWork) {
		s := p.Stats
		edges := "—"
		inLoaded, repeated := "—", "—"
		if s.EdgesMean > 0 {
			edges = fmt.Sprintf("%.0f", s.EdgesMean)
			inLoaded = fmt.Sprintf("%.0f (%.0f%%)", s.InLoadedMean, 100*s.InLoadedMean/s.EdgesMean)
			repeated = fmt.Sprintf("%.0f (%.0f%%)", s.RepeatedMean, 100*s.RepeatedMean/s.EdgesMean)
		}
		loads := fmt.Sprintf("%.1f / — / %.0f", s.LoadsMean, s.LoadsP95)
		if s.LoadsP50 > 0 {
			loads = fmt.Sprintf("%.1f / %.0f / %.0f", s.LoadsMean, s.LoadsP50, s.LoadsP95)
		}
		fmt.Fprintf(md, "| %d | %d | %d | %s | %.4f | %s | %.1f / %.0f / %.0f / %.0f | %.0f | %.0f | %s | %s | %s | %.0f | %.0f | %.0f |\n",
			p.EF, p.RescoreLimit, p.LoadBatch, marginLabel(p.SourceMargin), p.Recall, loads,
			s.RoundsMean, s.RoundsP50, s.RoundsP95, s.RoundsMax, s.ScannedMean, s.SourcesCut,
			edges, inLoaded, repeated, s.EndpointsMean, s.ScannedMean+s.EndpointsMean, s.RescoredMean)
	}
	fmt.Fprintf(md, "\n")

	if theirsWork == nil {
		return
	}
	fmt.Fprintf(md, "### HNSW\n\n")
	fmt.Fprintf(md, "Per query, counted on a second build of the same configuration.\n\n")
	fmt.Fprintf(md, "| ef | rescore | recall | code distances | full-precision distances |\n|---|---|---|---|---|\n")
	for _, p := range theirsWork.Points {
		fmt.Fprintf(md, "| %d | %d | %.4f | %.0f | %.0f |\n", p.EF, p.RescoreLimit, p.Recall, p.CodeDistances, p.VectorDistances)
	}
	fmt.Fprintf(md, "\n")
}

// writeProfileTable breaks the search time down by phase at the operating
// points nearest above the profile recall, one per rescore limit and batch.
func writeProfileTable(md *strings.Builder, ours, oursWork *benchResult) {
	points := workPoints(ours, oursWork)
	type config struct {
		rescore, batch int
		margin         float64
	}
	var configs []config
	for _, p := range points {
		if c := (config{p.RescoreLimit, p.LoadBatch, p.SourceMargin}); !slices.Contains(configs, c) {
			configs = append(configs, c)
		}
	}

	fmt.Fprintf(md, "## Where the time goes at recall %.2f\n\n", profileRecall)
	fmt.Fprintf(md, "Mean microseconds per query on one thread, at the first operating point of each configuration that reaches the recall. \"Other\" is what the phases do not cover: preparing the query, borrowing scratch, assembling the result.\n\n")
	fmt.Fprintf(md, "| rescore | batch | margin | ef | recall | centroid stage | posting scan | endpoint scoring | round selection | rescore | other | total |\n")
	fmt.Fprintf(md, "|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, c := range configs {
		var chosen *benchPoint
		for i := range points {
			p := &points[i]
			if p.RescoreLimit != c.rescore || p.LoadBatch != c.batch || p.SourceMargin != c.margin ||
				p.Recall < profileRecall {
				continue
			}
			if chosen == nil || p.EF < chosen.EF {
				chosen = p
			}
		}
		if chosen == nil {
			continue
		}
		s := chosen.Stats
		phases := s.CentroidUs + s.ScanUs + s.ExpandUs + s.SelectUs + s.RescoreUs
		// Without the total of the stats pass, fall back to the mean
		// latency of the timed pass.
		total := s.TotalUs
		if total == 0 {
			total = chosen.MeanMs * 1000
		}
		cell := func(us float64) string { return fmt.Sprintf("%.0f (%.0f%%)", us, 100*us/total) }
		fmt.Fprintf(md, "| %d | %d | %s | %d | %.4f | %s | %s | %s | %s | %s | %s | %.0f |\n",
			c.rescore, c.batch, marginLabel(c.margin), chosen.EF, chosen.Recall, cell(s.CentroidUs), cell(s.ScanUs), cell(s.ExpandUs),
			cell(s.SelectUs), cell(s.RescoreUs), cell(total-phases), total)
	}
	fmt.Fprintf(md, "\n")
}

func writePostingEdgePoints(md *strings.Builder, result *benchResult) {
	fmt.Fprintf(md, "## Posting-edge operating points\n\n")
	fmt.Fprintf(md, "| ef | rescore | batch | margin | threads | recall@%d | QPS | QPS of the runs | mean ms | p50 ms | p95 ms | p99 ms |\n", result.K)
	fmt.Fprintf(md, "|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, p := range result.Points {
		fmt.Fprintf(md, "| %d | %d | %d | %s | %d | %.4f | %.0f | %s | %.2f | %.2f | %.2f | %.2f |\n",
			p.EF, p.RescoreLimit, p.LoadBatch, marginLabel(p.SourceMargin), p.Threads, p.Recall, p.QPS, runList(p.QPSRuns),
			p.MeanMs, p.P50Ms, p.P95Ms, p.P99Ms)
	}
	fmt.Fprintf(md, "\n")
}

func writeHNSWPoints(md *strings.Builder, result *benchResult) {
	fmt.Fprintf(md, "## HNSW operating points\n\n")
	fmt.Fprintf(md, "| ef | rescore | threads | recall@%d | QPS | QPS of the runs | mean ms | p50 ms | p95 ms | p99 ms |\n", result.K)
	fmt.Fprintf(md, "|---|---|---|---|---|---|---|---|---|---|\n")
	for _, p := range result.Points {
		fmt.Fprintf(md, "| %d | %d | %d | %.4f | %.0f | %s | %.2f | %.2f | %.2f | %.2f |\n",
			p.EF, p.RescoreLimit, p.Threads, p.Recall, p.QPS, runList(p.QPSRuns),
			p.MeanMs, p.P50Ms, p.P95Ms, p.P99Ms)
	}
	fmt.Fprintf(md, "\n")
}

func runList(runs []float64) string {
	parts := make([]string, len(runs))
	for i, qps := range runs {
		parts[i] = fmt.Sprintf("%.0f", qps)
	}
	return strings.Join(parts, " / ")
}

// chartSeries is one line of the recall/QPS chart. Color identifies the
// index, the dash pattern the configuration.
type chartSeries struct {
	label  string
	color  string
	dash   string
	points []benchPoint
}

const (
	chartSurface   = "#fcfcfb"
	chartInk       = "#0b0b0b"
	chartInkMuted  = "#52514e"
	chartGrid      = "#e7e6e1"
	chartPostEdge  = "#eb6834"
	chartHNSW      = "#2a78d6"
	chartWidth     = 860.0
	chartHeight    = 520.0
	chartLeft      = 76.0
	chartRight     = 26.0
	chartTop       = 96.0
	chartBottom    = 62.0
	chartFontStack = "-apple-system, 'Segoe UI', Helvetica, Arial, sans-serif"
)

// recallQPSChart draws recall against QPS for one thread count. Recall runs
// on a -log10(1-recall) scale, which spreads the high-recall end where the
// operating points crowd together, and QPS on a log scale.
func recallQPSChart(ours, theirs *benchResult, threads int) string {
	var series []chartSeries
	add := func(result *benchResult, label, color, dash string, accept func(benchPoint) bool) bool {
		points := pick(result, threads, accept)
		if len(points) == 0 {
			return false
		}
		slices.SortFunc(points, func(a, b benchPoint) int { return a.EF - b.EF })
		series = append(series, chartSeries{label: label, color: color, dash: dash, points: points})
		return true
	}
	dashes := []string{"", "7 5", "2 4"}
	for i, rescore := range rescoreLimits(theirs) {
		add(theirs, fmt.Sprintf("HNSW, rescore %d", rescore), chartHNSW, dashes[min(i, len(dashes)-1)],
			func(p benchPoint) bool { return p.RescoreLimit == rescore })
	}
	style := 0
	for _, margin := range sourceMargins(ours.Points) {
		for _, rescore := range rescoreLimits(ours) {
			for _, batch := range loadBatches(ours) {
				label := fmt.Sprintf("posting-edge, rescore %d, batch %d", rescore, batch)
				if margin != 0 {
					label += fmt.Sprintf(", margin %.2f", margin)
				}
				added := add(ours, label, chartPostEdge, dashes[min(style, len(dashes)-1)],
					func(p benchPoint) bool {
						return p.RescoreLimit == rescore && p.LoadBatch == batch && p.SourceMargin == margin
					})
				if added {
					style++
				}
			}
		}
	}

	xOf := func(recall float64) float64 { return -math.Log10(1 - min(recall, 0.99995)) }
	minX, maxX := math.Inf(1), math.Inf(-1)
	minQPS, maxQPS := math.Inf(1), math.Inf(-1)
	for _, s := range series {
		for _, p := range s.points {
			minX, maxX = min(minX, xOf(p.Recall)), max(maxX, xOf(p.Recall))
			minQPS, maxQPS = min(minQPS, p.QPS), max(maxQPS, p.QPS)
		}
	}
	minX, maxX = math.Floor(minX*4)/4, math.Ceil(maxX*4)/4
	// A little air below the slowest point, for the label under it.
	minY, maxY := math.Floor((math.Log10(minQPS)-0.08)*4)/4, math.Ceil(math.Log10(maxQPS)*4)/4

	plotW, plotH := chartWidth-chartLeft-chartRight, chartHeight-chartTop-chartBottom
	px := func(recall float64) float64 { return chartLeft + (xOf(recall)-minX)/(maxX-minX)*plotW }
	py := func(qps float64) float64 { return chartTop + (maxY-math.Log10(qps))/(maxY-minY)*plotH }

	var svg strings.Builder
	fmt.Fprintf(&svg, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %.0f %.0f" font-family="%s">`+"\n",
		chartWidth, chartHeight, chartFontStack)
	fmt.Fprintf(&svg, `<rect width="%.0f" height="%.0f" fill="%s"/>`+"\n", chartWidth, chartHeight, chartSurface)
	fmt.Fprintf(&svg, `<text x="%.0f" y="26" font-size="16" font-weight="600" fill="%s">Recall@%d vs QPS, %d query thread%s</text>`+"\n",
		chartLeft, chartInk, ours.K, threads, plural(threads))
	fmt.Fprintf(&svg, `<text x="%.0f" y="45" font-size="12" fill="%s">%s, %d vectors x %d dimensions, %d queries. Up and to the right is better.</text>`+"\n",
		chartLeft, chartInkMuted, ours.Dataset, ours.Vectors, ours.Dims, ours.Queries)

	// legend: line keys above the plot, three to a row
	legendX := chartLeft
	for i, s := range series {
		if i%3 == 0 {
			legendX = chartLeft
		}
		y := 64.0 + 16*float64(i/3)
		fmt.Fprintf(&svg, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="%s" stroke-width="2"%s/>`+"\n",
			legendX, y, legendX+26, y, s.color, dashAttr(s.dash))
		fmt.Fprintf(&svg, `<text x="%.1f" y="%.1f" font-size="12" fill="%s">%s</text>`+"\n",
			legendX+32, y+4, chartInk, s.label)
		legendX += 250
	}

	// grid and axes
	for _, recall := range []float64{0.5, 0.7, 0.8, 0.9, 0.95, 0.98, 0.99, 0.995, 0.998, 0.999, 0.9995} {
		if xOf(recall) < minX-1e-9 || xOf(recall) > maxX+1e-9 {
			continue
		}
		x := px(recall)
		fmt.Fprintf(&svg, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="%s" stroke-width="1"/>`+"\n",
			x, chartTop, x, chartTop+plotH, chartGrid)
		fmt.Fprintf(&svg, `<text x="%.1f" y="%.1f" font-size="12" fill="%s" text-anchor="middle">%s</text>`+"\n",
			x, chartTop+plotH+18, chartInkMuted, strings.TrimRight(fmt.Sprintf("%.4f", recall), "0"))
	}
	for exponent := math.Floor(minY); exponent <= maxY; exponent++ {
		for _, mantissa := range []float64{1, 2, 5} {
			qps := mantissa * math.Pow(10, exponent)
			if math.Log10(qps) < minY-1e-9 || math.Log10(qps) > maxY+1e-9 {
				continue
			}
			y := py(qps)
			fmt.Fprintf(&svg, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="%s" stroke-width="1"/>`+"\n",
				chartLeft, y, chartLeft+plotW, y, chartGrid)
			fmt.Fprintf(&svg, `<text x="%.1f" y="%.1f" font-size="12" fill="%s" text-anchor="end">%.0f</text>`+"\n",
				chartLeft-8, y+4, chartInkMuted, qps)
		}
	}
	fmt.Fprintf(&svg, `<text x="%.1f" y="%.1f" font-size="12" fill="%s" text-anchor="middle">recall@%d</text>`+"\n",
		chartLeft+plotW/2, chartHeight-16, chartInkMuted, ours.K)
	fmt.Fprintf(&svg, `<text x="18" y="%.1f" font-size="12" fill="%s" text-anchor="middle" transform="rotate(-90 18 %.1f)">queries per second</text>`+"\n",
		chartTop+plotH/2, chartInkMuted, chartTop+plotH/2)

	// lines first, then the markers on top with a surface ring
	for _, s := range series {
		var path strings.Builder
		for i, p := range s.points {
			command := "L"
			if i == 0 {
				command = "M"
			}
			fmt.Fprintf(&path, "%s%.1f %.1f ", command, px(p.Recall), py(p.QPS))
		}
		fmt.Fprintf(&svg, `<path d="%s" fill="none" stroke="%s" stroke-width="2" stroke-linejoin="round"%s/>`+"\n",
			strings.TrimSpace(path.String()), s.color, dashAttr(s.dash))
	}
	for _, s := range series {
		for _, p := range s.points {
			fmt.Fprintf(&svg, `<circle cx="%.1f" cy="%.1f" r="4" fill="%s" stroke="%s" stroke-width="2"><title>%s, ef %d: recall %.4f, %.0f QPS</title></circle>`+"\n",
				px(p.Recall), py(p.QPS), s.color, chartSurface, s.label, p.EF, p.Recall, p.QPS)
		}
	}
	// the ef of every point of the first series of each index, so that a
	// curve can be read back to a configuration
	labeled := map[string]bool{}
	for _, s := range series {
		if labeled[s.color] {
			continue
		}
		labeled[s.color] = true
		offset := -10.0
		if s.color == chartPostEdge {
			offset = 18
		}
		for _, p := range s.points {
			fmt.Fprintf(&svg, `<text x="%.1f" y="%.1f" font-size="10" fill="%s" text-anchor="middle">%d</text>`+"\n",
				px(p.Recall), py(p.QPS)+offset, chartInkMuted, p.EF)
		}
	}
	fmt.Fprintf(&svg, `<text x="%.1f" y="%.1f" font-size="10" fill="%s" text-anchor="end">point labels: ef</text>`+"\n",
		chartLeft+plotW, chartHeight-16, chartInkMuted)
	svg.WriteString("</svg>\n")
	return svg.String()
}

func rescoreLimits(result *benchResult) []int {
	var out []int
	for _, p := range result.Points {
		if !slices.Contains(out, p.RescoreLimit) {
			out = append(out, p.RescoreLimit)
		}
	}
	return out
}

func loadBatches(result *benchResult) []int {
	var out []int
	for _, p := range result.Points {
		if !slices.Contains(out, p.LoadBatch) {
			out = append(out, p.LoadBatch)
		}
	}
	return out
}

func dashAttr(dash string) string {
	if dash == "" {
		return ""
	}
	return fmt.Sprintf(` stroke-dasharray="%s"`, dash)
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
