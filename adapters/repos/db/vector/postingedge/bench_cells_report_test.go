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
//
// -bench.report.cells.recalls reads the cells at more recall levels than the
// three of the comparison with HNSW.

package postingedge

import (
	"cmp"
	"flag"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	reportCells = flag.String("bench.report.cells", "", "posting-edge result files of TestBenchmark to compare cell by cell, "+
		"as label=path pairs separated by commas; the first is the control")
	reportCellsOut     = flag.String("bench.report.cells.out", "", "file to write the cell tables to")
	reportCellsRecalls = flag.String("bench.report.cells.recalls", "", "recall levels to read the QPS and the work of the cells at, "+
		"separated by commas, on top of 0.90, 0.95 and 0.99")
)

// reachRecall is the recall every default configuration has to reach by
// raising ef alone.
const reachRecall = 0.99

// floorRecallGap is how far below the fastest point of a curve a recall level
// has to be for what is read there to be told apart as that point: the curve
// has nothing cheaper, and the point has more recall than was asked for.
const floorRecallGap = 0.0005

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

	levels := cellRecalls(t)

	var md strings.Builder
	writeCellSummary(&md, cells, theirs)
	if len(levels) > len(isoRecallTargets) {
		writeCellLevels(&md, cells, theirs, levels)
	}
	writeCellBuilds(&md, cells)
	writeCellFrontiers(&md, cells, theirs, levels)
	writeCellReach(&md, cells)
	writeCellWork(&md, cells, levels)
	writeCellUntimedWork(&md, cells)
	writeCellCalibration(&md, cells)
	writeCellNoise(&md, cells)
	writeCellPoints(&md, cells)
	require.NoError(t, os.WriteFile(*reportCellsOut, []byte(md.String()), 0o644))
}

// cellRecalls are the recall levels the cells are read at: those of the
// comparison with HNSW and the ones of -bench.report.cells.recalls, ascending.
func cellRecalls(t *testing.T) []float64 {
	levels := slices.Clone(isoRecallTargets)
	for _, field := range strings.Split(*reportCellsRecalls, ",") {
		if field = strings.TrimSpace(field); field == "" {
			continue
		}
		level, err := strconv.ParseFloat(field, 64)
		require.NoErrorf(t, err, "-bench.report.cells.recalls: %q is not a number", field)
		require.Truef(t, level > 0 && level <= 1, "-bench.report.cells.recalls: %v is not a recall", level)
		if !slices.Contains(levels, level) {
			levels = append(levels, level)
		}
	}
	slices.Sort(levels)
	return levels
}

// recallLabel writes a recall level with the digits it was given with, and at
// least two decimals.
func recallLabel(level float64) string {
	label := strconv.FormatFloat(level, 'f', -1, 64)
	if len(label) < len("0.00") {
		return fmt.Sprintf("%.2f", level)
	}
	return label
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

// cellConfig is one configuration of a cell, an ef curve: points that differ
// in nothing but ef.
type cellConfig struct {
	rescore int
	batch   int
	margin  float64
}

func configOf(p benchPoint) cellConfig {
	return cellConfig{rescore: p.RescoreLimit, batch: p.LoadBatch, margin: p.SourceMargin}
}

func compareConfigs(a, b cellConfig) int {
	return cmp.Or(cmp.Compare(a.rescore, b.rescore), cmp.Compare(a.batch, b.batch), cmp.Compare(a.margin, b.margin))
}

// label names a configuration next to a number read off its curve: rescore
// limit and source margin, and the load batch where it is not the default.
func (c cellConfig) label() string {
	if c.batch != DefaultLoadBatch {
		return fmt.Sprintf("%d, %s, batch %d", c.rescore, marginLabel(c.margin), c.batch)
	}
	return fmt.Sprintf("%d, %s", c.rescore, marginLabel(c.margin))
}

// timedCurve returns the timed points of one configuration of a cell at a
// thread count, by ascending ef.
func timedCurve(result *benchResult, threads int, config cellConfig) []benchPoint {
	points := pick(result, threads, func(p benchPoint) bool { return !p.Untimed && configOf(p) == config })
	slices.SortFunc(points, func(a, b benchPoint) int { return a.EF - b.EF })
	return points
}

// timedPoints returns the timed points of a cell at a thread count.
func timedPoints(result *benchResult, threads int) []benchPoint {
	return pick(result, threads, func(p benchPoint) bool { return !p.Untimed })
}

// timedConfigs lists the timed configurations of a cell, by rescore limit,
// load batch and source margin. A cell has more than one margin at a rescore
// limit where the one its calibration took is set against others on the
// frontier, and more than one load batch where the size of a round is what is
// being measured.
func timedConfigs(result *benchResult) []cellConfig {
	var out []cellConfig
	for _, point := range result.Points {
		if config := configOf(point); !point.Untimed && !slices.Contains(out, config) {
			out = append(out, config)
		}
	}
	slices.SortFunc(out, compareConfigs)
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

// cellCodesMB is the memory of the codes of a cell: the codes of the sidecar,
// and those of the postings where they have codes of their own for the scan.
func cellCodesMB(result *benchResult) float64 {
	return result.Components["codes"] + result.Components["scanCodes"]
}

func writeCellBuilds(md *strings.Builder, cells []cell) {
	fmt.Fprintf(md, "## Cells: build and memory\n\n")
	fmt.Fprintf(md, "Code bytes include the header word. Structure is the index heap without the codes. "+
		"The margin is the one the calibration of the cell took, or the one it was timed with if it was not calibrated, "+
		"one per rescore limit where they differ. A cell whose postings have scan codes of their own holds two codes "+
		"per vector, the one of the sidecar and the shorter one the scan reads: its code columns are both together.\n\n")
	fmt.Fprintf(md, "| cell | code B/vector | postings | mean posting | P | R | candidates/vector | edge recall | edges/vector | margin | index heap MB | codes MB | structure MB | build s | assign s | edges s |\n")
	fmt.Fprintf(md, "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, c := range cells {
		r := c.result
		codes := cellCodesMB(r)
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
		codes := cellCodesMB(r)
		named := r.Components["centroidVectors"] + codes + r.Components["edges"] + r.Components["postings"]
		fmt.Fprintf(md, "| %s | %.1f | %.1f | %.1f | %.1f | %.1f | %.1f | %.0f | %.0f | %.0f | %.0f | %.1f | %.0f |\n",
			c.label, r.Components["centroidVectors"], codes, r.Components["edges"], r.Components["postings"],
			r.IndexHeapMB-named, r.IndexHeapMB, r.BuildStage["partition"], r.BuildStage["assign"], r.BuildStage["encode"],
			r.BuildStage["edges"], r.BuildStage["centroidGraph"], r.BuildSec)
	}
	fmt.Fprintf(md, "\n")
}

// timedCurveConfigs lists the configurations of a cell that were timed at a
// thread count.
func timedCurveConfigs(result *benchResult, threads int) []cellConfig {
	var out []cellConfig
	for _, config := range timedConfigs(result) {
		if len(timedCurve(result, threads, config)) > 0 {
			out = append(out, config)
		}
	}
	return out
}

// bestConfigAtRecall finds the timed configuration of a cell that gives the
// most QPS at a recall level, read off the ef curve of each. ok is false if
// none of them reaches the level.
func bestConfigAtRecall(result *benchResult, threads int, target float64) (qps float64, best cellConfig, ok bool) {
	for _, config := range timedConfigs(result) {
		curve := timedCurve(result, threads, config)
		if read, reached := qpsAtRecall(frontier(curve), target); reached && read > qps {
			qps, best, ok = read, config, true
		}
	}
	return qps, best, ok
}

// writeCellSummary puts every cell on one row per thread count: what it costs
// in memory, and its QPS at each recall level with the best of its timed
// configurations, against the control and against HNSW.
func writeCellSummary(md *strings.Builder, cells []cell, theirs *benchResult) {
	control := cells[0]
	fmt.Fprintf(md, "## Cells at their best configuration\n\n")
	fmt.Fprintf(md, "Each cell at the timed configuration that gives it the most QPS at each recall, read off its ef curve "+
		"as in the frontier table and given with its rescore limit and source margin, and with its load batch where that "+
		"is not the default. The ratios compare with the control (%s) and with HNSW, each at its own best.\n\n", control.label)
	fmt.Fprintf(md, "| threads | cell | code B/vector | index heap MB |")
	for _, target := range isoRecallTargets {
		fmt.Fprintf(md, " QPS@%.2f |", target)
	}
	for _, target := range isoRecallTargets {
		fmt.Fprintf(md, " cell / control @%.2f |", target)
	}
	for _, target := range isoRecallTargets {
		fmt.Fprintf(md, " HNSW / cell @%.2f |", target)
	}
	fmt.Fprintf(md, "\n|---|---|---|---|")
	for range 3 * len(isoRecallTargets) {
		fmt.Fprintf(md, "---|")
	}
	fmt.Fprintf(md, "\n")

	results := make([]*benchResult, len(cells))
	for i, c := range cells {
		results[i] = c.result
	}
	for _, threads := range threadCounts(results...) {
		var theirsPoints []benchPoint
		if theirs != nil {
			theirsPoints = pick(theirs, threads, nil)
		}
		for _, c := range cells {
			r := c.result
			if len(timedCurveConfigs(r, threads)) == 0 {
				continue
			}
			fmt.Fprintf(md, "| %d | %s | %.0f | %.1f |", threads, c.label, cellCodesMB(r)*(1<<20)/float64(r.Vectors), r.IndexHeapMB)
			var own, base, hnsw strings.Builder
			for _, target := range isoRecallTargets {
				qps, config, ok := bestConfigAtRecall(r, threads, target)
				if !ok {
					fmt.Fprintf(&own, " not reached |")
					fmt.Fprintf(&base, " — |")
					fmt.Fprintf(&hnsw, " — |")
					continue
				}
				fmt.Fprintf(&own, " %.0f (%s) |", qps, config.label())
				if q, _, found := bestConfigAtRecall(control.result, threads, target); found {
					fmt.Fprintf(&base, " %.2fx |", qps/q)
				} else {
					fmt.Fprintf(&base, " — |")
				}
				if q, found := bestQPSAtRecall(theirsPoints, target); found {
					fmt.Fprintf(&hnsw, " %.2fx |", q/qps)
				} else {
					fmt.Fprintf(&hnsw, " — |")
				}
			}
			fmt.Fprintf(md, "%s%s%s\n", own.String(), base.String(), hnsw.String())
		}
		if theirs != nil {
			fmt.Fprintf(md, "| %d | HNSW | — | %.1f |", threads, theirs.IndexHeapMB)
			for _, target := range isoRecallTargets {
				q, ok := bestQPSAtRecall(theirsPoints, target)
				fmt.Fprintf(md, " %s |", qpsCell(q, ok))
			}
			for range 2 * len(isoRecallTargets) {
				fmt.Fprintf(md, " — |")
			}
			fmt.Fprintf(md, "\n")
		}
	}
	fmt.Fprintf(md, "\n")
}

// writeCellLevels reads every cell at each recall level, at the timed
// configuration that gives it the most QPS there, against the control: the
// summary over more levels, which shows where two cells meet and where they
// part.
func writeCellLevels(md *strings.Builder, cells []cell, theirs *benchResult, levels []float64) {
	control := cells[0]
	fmt.Fprintf(md, "## QPS at each recall level\n\n")
	fmt.Fprintf(md, "Each cell at the timed configuration that gives it the most QPS at the level, read off its ef curve, "+
		"with its ratio to the control (%s). A QPS given with a recall is that of the fastest point of the curve, which has "+
		"that recall and not the one of the level: the cell has no cheaper configuration.\n\n", control.label)

	results := make([]*benchResult, len(cells))
	for i, c := range cells {
		results[i] = c.result
	}
	for _, threads := range threadCounts(results...) {
		var theirsPoints []benchPoint
		if theirs != nil {
			theirsPoints = pick(theirs, threads, nil)
		}
		fmt.Fprintf(md, "Threads: %d\n\n| recall |", threads)
		for _, c := range cells {
			fmt.Fprintf(md, " %s |", c.label)
		}
		if theirs != nil {
			fmt.Fprintf(md, " HNSW |")
		}
		fmt.Fprintf(md, "\n|---|")
		for range cells {
			fmt.Fprintf(md, "---|")
		}
		if theirs != nil {
			fmt.Fprintf(md, "---|")
		}
		fmt.Fprintf(md, "\n")
		for _, level := range levels {
			base, _, baseOK := bestConfigAtRecall(control.result, threads, level)
			fmt.Fprintf(md, "| %s |", recallLabel(level))
			for i, c := range cells {
				if len(timedCurveConfigs(c.result, threads)) == 0 {
					fmt.Fprintf(md, " — |")
					continue
				}
				qps, config, ok := bestConfigAtRecall(c.result, threads, level)
				if !ok {
					fmt.Fprintf(md, " not reached |")
					continue
				}
				fmt.Fprintf(md, " %.0f", qps)
				if fastest := frontier(timedCurve(c.result, threads, config))[0]; fastest.Recall-level >= floorRecallGap {
					fmt.Fprintf(md, " at %.4f", fastest.Recall)
				}
				if i > 0 && baseOK {
					fmt.Fprintf(md, " (%.2fx)", qps/base)
				}
				fmt.Fprintf(md, " |")
			}
			if theirs != nil {
				q, ok := bestQPSAtRecall(theirsPoints, level)
				fmt.Fprintf(md, " %s |", qpsCell(q, ok))
			}
			fmt.Fprintf(md, "\n")
		}
		fmt.Fprintf(md, "\n")
	}
}

// writeCellFrontiers reads the frontier of every configuration of each cell
// at the recall levels, and compares it with the control and with HNSW, each
// of those on its best measured points.
func writeCellFrontiers(md *strings.Builder, cells []cell, theirs *benchResult, levels []float64) {
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
	// the ratios stay at the two levels the verdicts are given at
	ratioLevels := []float64{0.95, 0.99}
	fmt.Fprintf(md, "| threads | cell | rescore | batch | margin |")
	for _, level := range levels {
		fmt.Fprintf(md, " QPS@%s |", recallLabel(level))
	}
	fmt.Fprintf(md, " fast end |")
	for _, level := range ratioLevels {
		fmt.Fprintf(md, " cell / control @%s |", recallLabel(level))
	}
	for _, level := range ratioLevels {
		fmt.Fprintf(md, " HNSW / cell @%s |", recallLabel(level))
	}
	fmt.Fprintf(md, "\n|---|---|---|---|---|---|")
	for range len(levels) + 2*len(ratioLevels) {
		fmt.Fprintf(md, "---|")
	}
	fmt.Fprintf(md, "\n")
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
				curve := timedCurve(c.result, threads, config)
				if len(curve) == 0 {
					continue
				}
				front := frontier(curve)
				fmt.Fprintf(md, "| %d | %s | %d | %d | %s |", threads, c.label, config.rescore, config.batch, marginLabel(config.margin))
				for _, level := range levels {
					qps, ok := qpsAtRecall(front, level)
					fmt.Fprintf(md, " %s |", qpsCell(qps, ok))
				}
				fast, fastOK := 0.0, false
				for _, p := range curve {
					if p.Recall >= fastEndRecall && p.QPS > fast {
						fast, fastOK = p.QPS, true
					}
				}
				fmt.Fprintf(md, " %s |", qpsCell(fast, fastOK))
				for _, level := range ratioLevels {
					own, ok := qpsAtRecall(front, level)
					q, qOK := bestQPSAtRecall(controlPoints, level)
					fmt.Fprintf(md, " %s |", ratio(own, ok, q, qOK))
				}
				for _, level := range ratioLevels {
					own, ok := qpsAtRecall(front, level)
					q, qOK := bestQPSAtRecall(theirsPoints, level)
					fmt.Fprintf(md, " %s |", ratio(q, qOK, own, ok))
				}
				fmt.Fprintf(md, "\n")
			}
		}
		if theirs != nil {
			// HNSW itself, at the best of its measured points at each recall.
			fmt.Fprintf(md, "| %d | HNSW | best | — | — |", threads)
			for _, level := range levels {
				q, ok := bestQPSAtRecall(theirsPoints, level)
				fmt.Fprintf(md, " %s |", qpsCell(q, ok))
			}
			fmt.Fprintf(md, "%s\n", strings.Repeat(" — |", 1+2*len(ratioLevels)))
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
	fmt.Fprintf(md, "| cell | rescore | batch | margin | first ef with recall ≥ %.2f | recall there | ceiling | saturates at ef | largest ef | recall there | without margin |\n", reachRecall)
	fmt.Fprintf(md, "|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, c := range cells {
		for _, config := range timedConfigs(c.result) {
			// timed and untimed points alike, one recall per ef
			recallAt := func(margin float64) map[int]float64 {
				at := cellConfig{rescore: config.rescore, batch: config.batch, margin: margin}
				out := map[int]float64{}
				for _, p := range c.result.Points {
					if configOf(p) == at {
						out[p.EF] = p.Recall
					}
				}
				for _, p := range c.result.Calibration {
					if _, found := out[p.EF]; !found && configOf(p) == at {
						out[p.EF] = p.Recall
					}
				}
				return out
			}
			with, without := recallAt(config.margin), recallAt(0)
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
			fmt.Fprintf(md, "| %s | %d | %d | %s | %s | %s | %.4f | %d | %d | %.4f | %s |\n",
				c.label, config.rescore, config.batch, marginLabel(config.margin), first, firstRecall, ceiling, saturates,
				last, with[last], bare)
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

// controlCurve is the ef curve of the control, with one thread, the default
// load batch and the margin of its calibration, that a cell at a rescore limit
// is set against: the one at that limit, or at the largest limit of the
// control if it was not timed there.
func controlCurve(control *benchResult, rescore int) []benchPoint {
	limits := timedRescoreLimits(control)
	if len(limits) == 0 {
		return nil
	}
	if !slices.Contains(limits, rescore) {
		rescore = limits[len(limits)-1]
	}
	return timedCurve(control, 1, cellConfig{rescore: rescore, batch: DefaultLoadBatch, margin: cellMargin(control, rescore)})
}

// writeCellWork breaks down what a search does and where its time goes at
// equal recall, per configuration of each cell, with one thread.
func writeCellWork(md *strings.Builder, cells []cell, levels []float64) {
	stats := []struct {
		name string
		stat func(benchPoint) float64
	}{
		{"ef", func(p benchPoint) float64 { return float64(p.EF) }},
		{"postings loaded", func(p benchPoint) float64 { return p.Stats.LoadsMean }},
		{"rounds", func(p benchPoint) float64 { return p.Stats.RoundsMean }},
		{"vectors scanned", func(p benchPoint) float64 { return p.Stats.ScannedMean }},
		{"sources expanded", func(p benchPoint) float64 { return p.Stats.ScannedMean - p.Stats.SourcesCut }},
		{"edges followed", func(p benchPoint) float64 { return p.Stats.EdgesMean }},
		{"endpoints scored", func(p benchPoint) float64 { return p.Stats.EndpointsMean }},
		{"centroid µs", func(p benchPoint) float64 { return p.Stats.CentroidUs }},
		{"scan µs", func(p benchPoint) float64 { return p.Stats.ScanUs }},
		{"endpoints µs", func(p benchPoint) float64 { return p.Stats.ExpandUs }},
		{"select µs", func(p benchPoint) float64 { return p.Stats.SelectUs }},
		{"rescore µs", func(p benchPoint) float64 { return p.Stats.RescoreUs }},
		{"rest µs", func(p benchPoint) float64 {
			s := p.Stats
			return s.TotalUs - s.CentroidUs - s.ScanUs - s.ExpandUs - s.SelectUs - s.RescoreUs
		}},
		{"total µs", func(p benchPoint) float64 { return p.Stats.TotalUs }},
	}
	loads := stats[1].stat

	fmt.Fprintf(md, "## Work and time per query at equal recall\n\n")
	fmt.Fprintf(md, "One thread, read along the ef curve of each configuration like the QPS. The recall column is the level, "+
		"or the recall of the first point of the curve where the level is below it: the row is then that point, the cheapest "+
		"search of the configuration. Sources expanded are the scanned vectors within the source margin, the ones whose "+
		"edges are followed. The times come from the instrumented pass, which is slower than the timed ones, and the rest "+
		"is the time no phase accounts for. Postings loaded are also given against the control at the same rescore limit, "+
		"or at its largest one where the control was not timed at that limit.\n\n")
	for _, target := range levels {
		fmt.Fprintf(md, "### Recall %s\n\n", recallLabel(target))
		fmt.Fprintf(md, "| cell | rescore | batch | margin | recall |")
		for _, s := range stats {
			fmt.Fprintf(md, " %s |", s.name)
		}
		fmt.Fprintf(md, " loads / control |\n|---|---|---|---|---|")
		for range stats {
			fmt.Fprintf(md, "---|")
		}
		fmt.Fprintf(md, "---|\n")
		for _, c := range cells {
			for _, config := range timedConfigs(c.result) {
				curve := timedCurve(c.result, 1, config)
				if len(curve) == 0 {
					continue
				}
				fmt.Fprintf(md, "| %s | %d | %d | %s |", c.label, config.rescore, config.batch, marginLabel(config.margin))
				if _, ok := statAtRecall(curve, target, loads); !ok {
					fmt.Fprintf(md, " not reached |")
					for range stats {
						fmt.Fprintf(md, " — |")
					}
					fmt.Fprintf(md, " — |\n")
					continue
				}
				read := recallLabel(target)
				if curve[0].Recall-target >= floorRecallGap {
					read = fmt.Sprintf("%.4f", curve[0].Recall)
				}
				fmt.Fprintf(md, " %s |", read)
				for _, s := range stats {
					value, _ := statAtRecall(curve, target, s.stat)
					fmt.Fprintf(md, " %.0f |", value)
				}
				against := "—"
				if base, ok := statAtRecall(controlCurve(cells[0].result, config.rescore), target, loads); ok {
					own, _ := statAtRecall(curve, target, loads)
					against = fmt.Sprintf("%.2fx", own/base)
				}
				fmt.Fprintf(md, " %s |\n", against)
			}
		}
		fmt.Fprintf(md, "\n")
	}
}

// minUntimedCurve is the number of ef values an untimed configuration needs
// for its points to be read as a curve.
const minUntimedCurve = 3

// untimedCurve returns the untimed points of one configuration of a cell, by
// ascending ef, whatever the thread count of the sweep that asked for them.
func untimedCurve(result *benchResult, config cellConfig) []benchPoint {
	var points []benchPoint
	for _, p := range result.Points {
		if p.Untimed && configOf(p) == config &&
			!slices.ContainsFunc(points, func(q benchPoint) bool { return q.EF == p.EF }) {
			points = append(points, p)
		}
	}
	slices.SortFunc(points, func(a, b benchPoint) int { return a.EF - b.EF })
	return points
}

// untimedConfigs lists the configurations of a cell that have no timed point
// and an untimed curve to read: they give recall and work at each ef, and no
// QPS. A curve has enough ef values and starts at the smallest ef of the
// cell. The passes that only probe the top of the range, to find a ceiling,
// are not one: read at a recall below their first point they would give the
// work of that point.
func untimedConfigs(result *benchResult) []cellConfig {
	timed := timedConfigs(result)
	lowest := math.MaxInt
	for _, p := range result.Points {
		lowest = min(lowest, p.EF)
	}
	var out []cellConfig
	for _, p := range result.Points {
		config := configOf(p)
		if !p.Untimed || slices.Contains(timed, config) || slices.Contains(out, config) {
			continue
		}
		curve := untimedCurve(result, config)
		if len(curve) >= minUntimedCurve && curve[0].EF == lowest {
			out = append(out, config)
		}
	}
	slices.SortFunc(out, compareConfigs)
	return out
}

// writeCellUntimedWork gives the postings loaded at equal recall by the
// configurations that were not timed: what a search loads with a rescore
// limit wide enough to take the final cut out of the way, or with no margin.
func writeCellUntimedWork(md *strings.Builder, cells []cell) {
	if !slices.ContainsFunc(cells, func(c cell) bool { return len(untimedConfigs(c.result)) > 0 }) {
		return
	}
	loads := func(p benchPoint) float64 { return p.Stats.LoadsMean }

	fmt.Fprintf(md, "## Postings loaded at equal recall, untimed configurations\n\n")
	fmt.Fprintf(md, "Configurations measured over a sweep of ef without timing: recall and work at each ef, no QPS. "+
		"Read along the ef curve like the timed ones, and set against the control the same way. "+
		"The last columns are the largest ef measured and the recall there.\n\n")
	fmt.Fprintf(md, "| cell | rescore | batch | margin |")
	for _, target := range isoRecallTargets {
		fmt.Fprintf(md, " loads @%.2f | loads / control @%.2f |", target, target)
	}
	fmt.Fprintf(md, " largest ef | recall there |\n|---|---|---|---|")
	for range isoRecallTargets {
		fmt.Fprintf(md, "---|---|")
	}
	fmt.Fprintf(md, "---|---|\n")
	for _, c := range cells {
		for _, config := range untimedConfigs(c.result) {
			curve := untimedCurve(c.result, config)
			fmt.Fprintf(md, "| %s | %d | %d | %s |", c.label, config.rescore, config.batch, marginLabel(config.margin))
			for _, target := range isoRecallTargets {
				own, ok := statAtRecall(curve, target, loads)
				if !ok {
					fmt.Fprintf(md, " not reached | — |")
					continue
				}
				against := "—"
				if base, ok := statAtRecall(controlCurve(cells[0].result, config.rescore), target, loads); ok {
					against = fmt.Sprintf("%.2fx", own/base)
				}
				fmt.Fprintf(md, " %.0f | %s |", own, against)
			}
			last := curve[len(curve)-1]
			fmt.Fprintf(md, " %d | %.4f |\n", last.EF, last.Recall)
		}
	}
	fmt.Fprintf(md, "\n")
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
	fmt.Fprintf(md, "| cell | timed points | noisy | which (threads, rescore, batch, margin, ef: QPS of each pass) |\n|---|---|---|---|\n")
	for _, c := range cells {
		var timed int
		var noisy []string
		for _, p := range c.result.Points {
			if p.Untimed || len(p.QPSRuns) == 0 {
				continue
			}
			timed++
			if (slices.Max(p.QPSRuns)-slices.Min(p.QPSRuns))/p.QPS > noisySpread {
				noisy = append(noisy, fmt.Sprintf("%d threads, rescore %d, batch %d, margin %s, ef %d: %s",
					p.Threads, p.RescoreLimit, p.LoadBatch, marginLabel(p.SourceMargin), p.EF, runList(p.QPSRuns)))
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
	fmt.Fprintf(md, "| cell | ef | rescore | batch | margin | threads | recall@10 | QPS | p50 ms | p95 ms | postings loaded | rounds |\n")
	fmt.Fprintf(md, "|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, c := range cells {
		for _, p := range c.result.Points {
			qps, p50, p95 := "untimed", "—", "—"
			if !p.Untimed {
				qps, p50, p95 = fmt.Sprintf("%.0f", p.QPS), fmt.Sprintf("%.2f", p.P50Ms), fmt.Sprintf("%.2f", p.P95Ms)
			}
			fmt.Fprintf(md, "| %s | %d | %d | %d | %s | %d | %.4f | %s | %s | %s | %.1f | %.1f |\n",
				c.label, p.EF, p.RescoreLimit, p.LoadBatch, marginLabel(p.SourceMargin), p.Threads, p.Recall, qps, p50, p95,
				p.Stats.LoadsMean, p.Stats.RoundsMean)
		}
	}
	fmt.Fprintf(md, "\n")
}
