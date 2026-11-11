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

// Compares builds of the posting-edge index that differ in one build
// parameter, a cell each: the code width, the posting size. Every cell is a
// result file of TestBenchmark; the first one given is the control.
//
//	go test -tags postingedgebench -run TestCellReport \
//	    ./adapters/repos/db/vector/postingedge/ \
//	    -bench.report.cells full=full.json,768=768.json,512=512.json \
//	    -bench.report.hnsw hnsw.json -bench.report.cells.out cells.md

package postingedge

import (
	"flag"
	"fmt"
	"math"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	reportCells = flag.String("bench.report.cells", "", "posting-edge result files of TestBenchmark to compare cell by cell, "+
		"as label=path pairs separated by commas; the first is the control")
	reportCellsOut = flag.String("bench.report.cells.out", "", "file to write the cell tables to")
)

// reachRecall is the recall every default configuration has to reach by
// raising ef alone.
const reachRecall = 0.99

type cell struct {
	label  string
	result *benchResult
}

func TestCellReport(t *testing.T) {
	if *reportCells == "" || *reportCellsOut == "" {
		t.Skip("needs -bench.report.cells and -bench.report.cells.out")
	}
	var cells []cell
	for _, pair := range strings.Split(*reportCells, ",") {
		label, path, found := strings.Cut(pair, "=")
		require.Truef(t, found, "-bench.report.cells entry %q is not label=path", pair)
		result := &benchResult{}
		readJSON(t, path, result)
		cells = append(cells, cell{label: label, result: result})
	}
	var theirs *benchResult
	if *reportHNSW != "" {
		theirs = &benchResult{}
		readJSON(t, *reportHNSW, theirs)
	}

	var md strings.Builder
	writeCellBuilds(&md, cells)
	writeCellFrontiers(&md, cells, theirs)
	writeCellReach(&md, cells)
	writeCellWork(&md, cells)
	writeCellCalibration(&md, cells)
	writeCellNoise(&md, cells)
	writeCellPoints(&md, cells)
	require.NoError(t, os.WriteFile(*reportCellsOut, []byte(md.String()), 0o644))
}

// configNumber reads a number of the build configuration of a result.
func configNumber(result *benchResult, key string) float64 {
	value, _ := result.Config[key].(float64)
	return value
}

// cellMargin is the source margin the timed sweeps of a cell ran with at a
// rescore limit: the calibrated one, or the one of its first timed point.
func cellMargin(result *benchResult, rescore int) float64 {
	if margin, found := result.AutoMargins[rescore]; found {
		return margin
	}
	if result.AutoMargin != nil {
		return *result.AutoMargin
	}
	for _, point := range result.Points {
		if !point.Untimed && point.RescoreLimit == rescore {
			return point.SourceMargin
		}
	}
	return 0
}

// cellMargins lists the source margin of a cell at each of its rescore
// limits, as one label if they agree.
func cellMargins(result *benchResult) string {
	var labels []string
	for _, rescore := range timedRescoreLimits(result) {
		labels = append(labels, marginLabel(cellMargin(result, rescore)))
	}
	if len(slices.Compact(slices.Clone(labels))) == 1 {
		return labels[0]
	}
	return strings.Join(labels, " / ")
}

// timedCurve returns the timed points of one configuration of a cell, by
// ascending ef.
func timedCurve(result *benchResult, threads, rescore int, margin float64) []benchPoint {
	points := pick(result, threads, func(p benchPoint) bool {
		return !p.Untimed && p.RescoreLimit == rescore && p.SourceMargin == margin
	})
	slices.SortFunc(points, func(a, b benchPoint) int { return a.EF - b.EF })
	return points
}

// timedPoints returns the timed points of a cell at a thread count.
func timedPoints(result *benchResult, threads int) []benchPoint {
	return pick(result, threads, func(p benchPoint) bool { return !p.Untimed })
}

// timedMargins lists the source margins a cell was timed with at a rescore
// limit, in ascending order. A cell has more than one where the margin its
// calibration took is set against others on the frontier.
func timedMargins(result *benchResult, rescore int) []float64 {
	var out []float64
	for _, point := range result.Points {
		if !point.Untimed && point.RescoreLimit == rescore && !slices.Contains(out, point.SourceMargin) {
			out = append(out, point.SourceMargin)
		}
	}
	slices.Sort(out)
	return out
}

// cellConfig is one timed configuration of a cell: an ef curve.
type cellConfig struct {
	rescore int
	margin  float64
}

// timedConfigs lists the timed configurations of a cell, by rescore limit and
// then by source margin.
func timedConfigs(result *benchResult) []cellConfig {
	var out []cellConfig
	for _, rescore := range timedRescoreLimits(result) {
		for _, margin := range timedMargins(result, rescore) {
			out = append(out, cellConfig{rescore: rescore, margin: margin})
		}
	}
	return out
}

func timedRescoreLimits(result *benchResult) []int {
	var out []int
	for _, point := range result.Points {
		if !point.Untimed && !slices.Contains(out, point.RescoreLimit) {
			out = append(out, point.RescoreLimit)
		}
	}
	slices.Sort(out)
	return out
}

func writeCellBuilds(md *strings.Builder, cells []cell) {
	fmt.Fprintf(md, "## Cells: build and memory\n\n")
	fmt.Fprintf(md, "Code bytes include the header word. Structure is the index heap without the codes. "+
		"The margin is the one the calibration of the cell took, or the one it was timed with if it was not calibrated, "+
		"one per rescore limit where they differ.\n\n")
	fmt.Fprintf(md, "| cell | code B/vector | postings | mean posting | P | R | candidates/vector | edge recall | edges/vector | margin | index heap MB | codes MB | structure MB | build s | assign s | edges s |\n")
	fmt.Fprintf(md, "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, c := range cells {
		r := c.result
		codes := r.Components["codes"]
		fmt.Fprintf(md, "| %s | %.0f | %.0f | %.0f | %.0f | %.0f | %.0f | %.4f | %.2f | %s | %.1f | %.1f | %.1f | %.0f | %.0f | %.0f |\n",
			c.label, codes*(1<<20)/float64(r.Vectors), r.Diagnostics["postings"], r.Diagnostics["postingSizeMean"],
			configNumber(r, "buildProbe"), configNumber(r, "rescueLimit"), r.Diagnostics["candidatesPerVector"],
			r.Diagnostics["edgeRecall"], r.Diagnostics["edgesPerVector"], cellMargins(r),
			r.IndexHeapMB, codes, r.IndexHeapMB-codes, r.BuildSec, r.BuildStage["assign"], r.BuildStage["edges"])
	}
	fmt.Fprintf(md, "\n")

	fmt.Fprintf(md, "Memory by component and build by stage. The rest of the index heap is what the components do not account for.\n\n")
	fmt.Fprintf(md, "| cell | centroid vectors MB | codes MB | edges MB | postings MB | rest MB | index heap MB | partition s | assign s | encode s | edges s | centroid graph s | build s |\n")
	fmt.Fprintf(md, "|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, c := range cells {
		r := c.result
		named := r.Components["centroidVectors"] + r.Components["codes"] + r.Components["edges"] + r.Components["postings"]
		fmt.Fprintf(md, "| %s | %.1f | %.1f | %.1f | %.1f | %.1f | %.1f | %.0f | %.0f | %.0f | %.0f | %.1f | %.0f |\n",
			c.label, r.Components["centroidVectors"], r.Components["codes"], r.Components["edges"], r.Components["postings"],
			r.IndexHeapMB-named, r.IndexHeapMB, r.BuildStage["partition"], r.BuildStage["assign"], r.BuildStage["encode"],
			r.BuildStage["edges"], r.BuildStage["centroidGraph"], r.BuildSec)
	}
	fmt.Fprintf(md, "\n")
}

// writeCellFrontiers reads the frontier of every cell and rescore limit at
// the recall levels, and compares it with the control and with HNSW, each of
// those on its best measured points.
func writeCellFrontiers(md *strings.Builder, cells []cell, theirs *benchResult) {
	control := cells[0]
	ratio := func(x float64, xOK bool, y float64, yOK bool) string {
		if !xOK || !yOK {
			return "—"
		}
		return fmt.Sprintf("%.2fx", x/y)
	}

	fmt.Fprintf(md, "## QPS at equal recall\n\n")
	fmt.Fprintf(md, "Read off the ef curve of each cell and rescore limit, interpolated linearly in recall and log QPS. ")
	fmt.Fprintf(md, "The fast end is the best QPS of a measured point with recall of at least %.2f. ", fastEndRecall)
	fmt.Fprintf(md, "The ratios compare with the best configuration of the control (%s) and of HNSW at the same recall.\n\n", control.label)
	fmt.Fprintf(md, "| threads | cell | rescore | margin | QPS@0.90 | QPS@0.95 | QPS@0.99 | fast end | cell / control @0.95 | cell / control @0.99 | HNSW / cell @0.95 | HNSW / cell @0.99 |\n")
	fmt.Fprintf(md, "|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	results := make([]*benchResult, len(cells))
	for i, c := range cells {
		results[i] = c.result
	}
	for _, threads := range threadCounts(results...) {
		controlPoints := timedPoints(control.result, threads)
		var theirsPoints []benchPoint
		if theirs != nil {
			theirsPoints = pick(theirs, threads, nil)
		}
		for _, c := range cells {
			for _, config := range timedConfigs(c.result) {
				curve := timedCurve(c.result, threads, config.rescore, config.margin)
				if len(curve) == 0 {
					continue
				}
				front := frontier(curve)
				fmt.Fprintf(md, "| %d | %s | %d | %s |", threads, c.label, config.rescore, marginLabel(config.margin))
				var at [3]float64
				var ok [3]bool
				for i, target := range isoRecallTargets {
					at[i], ok[i] = qpsAtRecall(front, target)
					fmt.Fprintf(md, " %s |", qpsCell(at[i], ok[i]))
				}
				fast, fastOK := 0.0, false
				for _, p := range curve {
					if p.Recall >= fastEndRecall && p.QPS > fast {
						fast, fastOK = p.QPS, true
					}
				}
				fmt.Fprintf(md, " %s |", qpsCell(fast, fastOK))
				for _, i := range []int{1, 2} {
					q, qOK := bestQPSAtRecall(controlPoints, isoRecallTargets[i])
					fmt.Fprintf(md, " %s |", ratio(at[i], ok[i], q, qOK))
				}
				for _, i := range []int{1, 2} {
					q, qOK := bestQPSAtRecall(theirsPoints, isoRecallTargets[i])
					fmt.Fprintf(md, " %s |", ratio(q, qOK, at[i], ok[i]))
				}
				fmt.Fprintf(md, "\n")
			}
		}
	}
	fmt.Fprintf(md, "\n")
}

// writeCellReach tells how far the recall of every cell and rescore limit
// goes by raising ef alone, and whether the source margin is what limits it.
func writeCellReach(md *strings.Builder, cells []cell) {
	fmt.Fprintf(md, "## Recall reached by raising ef\n\n")
	fmt.Fprintf(md, "With the source margin of the cell. A configuration that stays below %.2f over its sweep gets untimed passes "+
		"that keep doubling ef until it gets there or a doubling adds less than %.4f. The ceiling is the best recall measured, "+
		"and it saturates at the smallest ef within %.4f of it. Without margin is the recall at the largest ef measured, "+
		"with no source margin.\n\n", reachRecall, ceilingMinGain, ceilingMinGain)
	fmt.Fprintf(md, "| cell | rescore | margin | first ef with recall ≥ %.2f | recall there | ceiling | saturates at ef | largest ef | recall there | without margin |\n", reachRecall)
	fmt.Fprintf(md, "|---|---|---|---|---|---|---|---|---|---|\n")
	for _, c := range cells {
		for _, config := range timedConfigs(c.result) {
			rescore, margin := config.rescore, config.margin
			// timed and untimed points alike, one recall per ef
			recallAt := func(m float64) map[int]float64 {
				out := map[int]float64{}
				for _, p := range c.result.Points {
					if p.RescoreLimit == rescore && p.SourceMargin == m && p.LoadBatch == DefaultLoadBatch {
						out[p.EF] = p.Recall
					}
				}
				for _, p := range c.result.Calibration {
					if p.RescoreLimit == rescore && p.SourceMargin == m && p.LoadBatch == DefaultLoadBatch {
						if _, found := out[p.EF]; !found {
							out[p.EF] = p.Recall
						}
					}
				}
				return out
			}
			with, without := recallAt(margin), recallAt(0)
			efs := make([]int, 0, len(with))
			for ef := range with {
				efs = append(efs, ef)
			}
			slices.Sort(efs)
			if len(efs) == 0 {
				continue
			}
			first, firstRecall := "never", "—"
			for _, ef := range efs {
				if with[ef] >= reachRecall {
					first, firstRecall = fmt.Sprint(ef), fmt.Sprintf("%.4f", with[ef])
					break
				}
			}
			ceiling, saturates := 0.0, 0
			for _, ef := range efs {
				ceiling = max(ceiling, with[ef])
			}
			for _, ef := range efs {
				if with[ef] >= ceiling-ceilingMinGain {
					saturates = ef
					break
				}
			}
			last := efs[len(efs)-1]
			bare := "—"
			if recall, found := without[last]; found {
				bare = fmt.Sprintf("%.4f", recall)
			}
			fmt.Fprintf(md, "| %s | %d | %s | %s | %s | %.4f | %d | %d | %.4f | %s |\n",
				c.label, rescore, marginLabel(margin), first, firstRecall, ceiling, saturates, last, with[last], bare)
		}
	}
	fmt.Fprintf(md, "\n")
}

// statAtRecall reads a statistic along an ef curve at a recall level, the way
// qpsAtRecall reads the QPS: between the two points around the level,
// linearly in recall and in the logarithm of the statistic. The first point
// stands for any level below it. ok is false if the curve never reaches the
// level.
func statAtRecall(curve []benchPoint, target float64, stat func(benchPoint) float64) (value float64, ok bool) {
	for i, high := range curve {
		if high.Recall < target {
			continue
		}
		if i == 0 {
			return stat(high), true
		}
		low := curve[i-1]
		share := (target - low.Recall) / (high.Recall - low.Recall)
		a, b := stat(low), stat(high)
		if a <= 0 || b <= 0 {
			return a + share*(b-a), true
		}
		return math.Exp(math.Log(a) + share*(math.Log(b)-math.Log(a))), true
	}
	return 0, false
}

// controlCurve is the ef curve of the control, with one thread and the margin
// of its calibration, that a cell at a rescore limit is set against: the one
// at that limit, or at the largest limit of the control if it was not timed
// there.
func controlCurve(control *benchResult, rescore int) []benchPoint {
	limits := timedRescoreLimits(control)
	if len(limits) == 0 {
		return nil
	}
	if !slices.Contains(limits, rescore) {
		rescore = limits[len(limits)-1]
	}
	return timedCurve(control, 1, rescore, cellMargin(control, rescore))
}

// writeCellWork breaks down what a search does and where its time goes at
// equal recall, per cell and rescore limit, with one thread.
func writeCellWork(md *strings.Builder, cells []cell) {
	stats := []struct {
		name string
		stat func(benchPoint) float64
	}{
		{"ef", func(p benchPoint) float64 { return float64(p.EF) }},
		{"postings loaded", func(p benchPoint) float64 { return p.Stats.LoadsMean }},
		{"rounds", func(p benchPoint) float64 { return p.Stats.RoundsMean }},
		{"vectors scanned", func(p benchPoint) float64 { return p.Stats.ScannedMean }},
		{"endpoints scored", func(p benchPoint) float64 { return p.Stats.EndpointsMean }},
		{"centroid µs", func(p benchPoint) float64 { return p.Stats.CentroidUs }},
		{"scan µs", func(p benchPoint) float64 { return p.Stats.ScanUs }},
		{"endpoints µs", func(p benchPoint) float64 { return p.Stats.ExpandUs }},
		{"select µs", func(p benchPoint) float64 { return p.Stats.SelectUs }},
		{"rescore µs", func(p benchPoint) float64 { return p.Stats.RescoreUs }},
		{"total µs", func(p benchPoint) float64 { return p.Stats.TotalUs }},
	}
	loads := stats[1].stat

	fmt.Fprintf(md, "## Work and time per query at equal recall\n\n")
	fmt.Fprintf(md, "One thread, read along the ef curve of each cell and rescore limit like the QPS. The times come from the "+
		"instrumented pass, which is slower than the timed ones. Postings loaded are also given against the control at "+
		"the same rescore limit, or at its largest one where the control was not timed at that limit.\n\n")
	for _, target := range isoRecallTargets {
		fmt.Fprintf(md, "### Recall %.2f\n\n", target)
		fmt.Fprintf(md, "| cell | rescore | margin |")
		for _, s := range stats {
			fmt.Fprintf(md, " %s |", s.name)
		}
		fmt.Fprintf(md, " loads / control |\n|---|---|---|")
		for range stats {
			fmt.Fprintf(md, "---|")
		}
		fmt.Fprintf(md, "---|\n")
		for _, c := range cells {
			for _, config := range timedConfigs(c.result) {
				rescore := config.rescore
				curve := timedCurve(c.result, 1, rescore, config.margin)
				if _, ok := statAtRecall(curve, target, loads); !ok {
					fmt.Fprintf(md, "| %s | %d | %s | not reached |", c.label, rescore, marginLabel(config.margin))
					for range len(stats) - 1 {
						fmt.Fprintf(md, " — |")
					}
					fmt.Fprintf(md, " — |\n")
					continue
				}
				fmt.Fprintf(md, "| %s | %d | %s |", c.label, rescore, marginLabel(config.margin))
				for _, s := range stats {
					value, _ := statAtRecall(curve, target, s.stat)
					fmt.Fprintf(md, " %.0f |", value)
				}
				against := "—"
				if base, ok := statAtRecall(controlCurve(cells[0].result, rescore), target, loads); ok {
					own, _ := statAtRecall(curve, target, loads)
					against = fmt.Sprintf("%.2fx", own/base)
				}
				fmt.Fprintf(md, " %s |\n", against)
			}
		}
		fmt.Fprintf(md, "\n")
	}
}

// writeCellCalibration shows what every source margin tried for a cell costs
// in recall at equal ef, against no margin, and which one the cell took.
func writeCellCalibration(md *strings.Builder, cells []cell) {
	fmt.Fprintf(md, "## Source margin of each cell\n\n")
	fmt.Fprintf(md, "The recall each margin costs at equal ef, against no margin: the worst ef and the ef it happens at. "+
		"A cell takes the smallest margin within its tolerance, for each rescore limit it was calibrated at.\n\n")
	fmt.Fprintf(md, "| cell | rescore | margin | worst recall change | at ef | endpoints scored at that ef, against no margin | taken |\n|---|---|---|---|---|---|---|\n")
	for _, c := range cells {
		type config struct {
			rescore int
			margin  float64
		}
		baseline := map[[2]int]benchPoint{} // by rescore limit and ef
		var configs []config
		for _, p := range c.result.Calibration {
			key := config{rescore: p.RescoreLimit, margin: p.SourceMargin}
			if p.SourceMargin == 0 {
				baseline[[2]int{p.RescoreLimit, p.EF}] = p
			} else if !slices.Contains(configs, key) {
				configs = append(configs, key)
			}
		}
		for _, key := range configs {
			worst, worstEF, scored := 0.0, 0, 0.0
			for _, p := range c.result.Calibration {
				base, found := baseline[[2]int{p.RescoreLimit, p.EF}]
				if p.RescoreLimit != key.rescore || p.SourceMargin != key.margin || !found {
					continue
				}
				if change := p.Recall - base.Recall; worstEF == 0 || change < worst {
					worst, worstEF = change, p.EF
					scored = p.Stats.EndpointsMean / base.Stats.EndpointsMean
				}
			}
			taken := ""
			if cellMargin(c.result, key.rescore) == key.margin {
				taken = "yes"
			}
			fmt.Fprintf(md, "| %s | %d | %s | %+.4f | %d | %.2fx | %s |\n",
				c.label, key.rescore, marginLabel(key.margin), worst, worstEF, scored, taken)
		}
	}
	fmt.Fprintf(md, "\n")
}

// noisySpread is how far apart the measured passes of a timed point may be,
// as a share of their median, before the point is listed as noisy.
const noisySpread = 0.10

// writeCellNoise lists the timed points whose measured passes disagree, the
// sign that something else ran on the machine while they were taken.
func writeCellNoise(md *strings.Builder, cells []cell) {
	fmt.Fprintf(md, "## Noise\n\n")
	fmt.Fprintf(md, "Timed points whose passes are more than %.0f%% of their median apart.\n\n", 100*noisySpread)
	fmt.Fprintf(md, "| cell | timed points | noisy | which (threads, rescore, margin, ef: QPS of each pass) |\n|---|---|---|---|\n")
	for _, c := range cells {
		var timed int
		var noisy []string
		for _, p := range c.result.Points {
			if p.Untimed || len(p.QPSRuns) == 0 {
				continue
			}
			timed++
			if (slices.Max(p.QPSRuns)-slices.Min(p.QPSRuns))/p.QPS > noisySpread {
				noisy = append(noisy, fmt.Sprintf("%d threads, rescore %d, margin %s, ef %d: %s",
					p.Threads, p.RescoreLimit, marginLabel(p.SourceMargin), p.EF, runList(p.QPSRuns)))
			}
		}
		which := "—"
		if len(noisy) > 0 {
			which = strings.Join(noisy, "; ")
		}
		fmt.Fprintf(md, "| %s | %d | %d | %s |\n", c.label, timed, len(noisy), which)
	}
	fmt.Fprintf(md, "\n")
}

func writeCellPoints(md *strings.Builder, cells []cell) {
	fmt.Fprintf(md, "## Operating points\n\n")
	fmt.Fprintf(md, "| cell | ef | rescore | margin | threads | recall@10 | QPS | p50 ms | p95 ms | postings loaded | rounds |\n")
	fmt.Fprintf(md, "|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, c := range cells {
		for _, p := range c.result.Points {
			qps, p50, p95 := "untimed", "—", "—"
			if !p.Untimed {
				qps, p50, p95 = fmt.Sprintf("%.0f", p.QPS), fmt.Sprintf("%.2f", p.P50Ms), fmt.Sprintf("%.2f", p.P95Ms)
			}
			fmt.Fprintf(md, "| %s | %d | %d | %s | %d | %.4f | %s | %s | %s | %.1f | %.1f |\n",
				c.label, p.EF, p.RescoreLimit, marginLabel(p.SourceMargin), p.Threads, p.Recall, qps, p50, p95,
				p.Stats.LoadsMean, p.Stats.RoundsMean)
		}
	}
	fmt.Fprintf(md, "\n")
}
