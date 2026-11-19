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

package postingedge

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/concurrency"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

var (
	benchDeleteStages = flag.String("bench.deletes", "", "the deletes experiment of v6.0 (TestStreamDeletes): its stages, "+
		"separated by commas and run in the order given, of random10, random30, random50 (that share of the train set "+
		"deleted at random), clustered30 (whole regions of neighboring postings) and churn (deletes and inserts in "+
		"rounds, with the best cell of random30, which must come before it)")
	benchDeleteMin = flag.Int("bench.deletemin", 118, "with -bench.deletes: the live members under which the cleanup "+
		"merges a posting into its nearest one")
	benchDeleteOver = flag.Float64("bench.deleteover", 0.30, "with -bench.deletes: the share of deleted members past "+
		"which the cleanup writes a posting again without them")
	benchDeleteRegion = flag.Int("bench.deleteregion", 64, "with -bench.deletes: the postings of a region of clustered30")
	benchDeleteUntil  = flag.String("bench.deleteuntil", "", "with -bench.deletes: the time of day, as 15:04, from which "+
		"no stage starts. What is left is reported as not run")
	benchDeleteCell = flag.Int("bench.deletecell", 0, "with -bench.deletes: the cell churn runs with, when random30 is "+
		"not among the stages to choose it")
	benchDeleteSources = flag.Bool("bench.deletesources", false, "with -bench.deletes: cell 1b is measured at every "+
		"point, after cell 1: a deleted member that is still in its posting is a routing source")
	benchDeleteFill = flag.Bool("bench.deletefill", false, "with -bench.deletes: every point builds besides a batch "+
		"index of the vectors that are left with the mean live size of the postings of its cells as its posting size, "+
		"one for the cells before the cleanup and one for those after it, and sets every cell against its own")
)

// deleteCellNames are the cells of v6.0, each on top of the one before.
var deleteCellNames = []string{
	"", "1: logical deletes, deleted endpoints scored", "2: deleted endpoints not scored",
	"3: postings written again and merged", "4: edges refreshed",
}

// The noise floor of v2.0, and the criterion of v6.0.
const (
	deleteRecallSlack = 0.005
	deleteLoads95     = 1.15
	deleteLoads99     = 1.10
	deleteNoise99     = 0.03
)

// deletePoint is the search of every query at one ef: the recall, and the
// work of a query, on average.
type deletePoint struct {
	EF        int     `json:"ef"`
	Recall    float64 `json:"recall"`
	Loads     float64 `json:"loads"`
	Rounds    float64 `json:"rounds"`
	Scanned   float64 `json:"scanned"`
	Endpoints float64 `json:"endpoints"`
}

// deleteCurve is an index searched over the grid, with its loads read at the
// two recalls as v2.x reads them: between the two ef around the recall,
// linear in the recall and in the logarithm of the loads.
type deleteCurve struct {
	Points    []deletePoint `json:"points"`
	Reached95 bool          `json:"reached95"`
	Loads95   float64       `json:"loads95"`
	Reached99 bool          `json:"reached99"`
	Loads99   float64       `json:"loads99"`
	// Crossing is the two ef 0.99 is crossed between.
	Crossing [2]int `json:"crossing99"`
}

func (c *deleteCurve) read() {
	c.Loads95, c.Reached95, _ = deleteLoadsAt(c.Points, 0.95)
	c.Loads99, c.Reached99, c.Crossing = deleteLoadsAt(c.Points, 0.99)
}

func deleteLoadsAt(points []deletePoint, target float64) (loads float64, reached bool, between [2]int) {
	for i, high := range points {
		if high.Recall < target {
			continue
		}
		if i == 0 {
			return high.Loads, true, [2]int{high.EF, high.EF}
		}
		low := points[i-1]
		share := (target - low.Recall) / (high.Recall - low.Recall)
		return math.Exp(math.Log(low.Loads) + share*(math.Log(high.Loads)-math.Log(low.Loads))), true, [2]int{low.EF, high.EF}
	}
	return 0, false, between
}

// deleteReference is the batch index of the vectors that are left.
type deleteReference struct {
	// TargetSize is the posting size it was built for: that of the index,
	// or the mean live size of the postings of the cells it is the fill
	// reference of.
	TargetSize   int     `json:"targetSize"`
	Vectors      int     `json:"vectors"`
	Postings     int     `json:"postings"`
	SizeMean     float64 `json:"sizeMean"`
	BuildSeconds float64 `json:"buildSeconds"`
	deleteCurve
}

// deleteVerdict sets a cell against its reference, by the criterion.
type deleteVerdict struct {
	// WorstRecall is the lowest difference of recall with the reference
	// over the ef of the grid, and WorstEF where.
	WorstRecall float64 `json:"worstRecallDifference"`
	WorstEF     int     `json:"worstRecallEF"`
	Loads95     float64 `json:"loads95OverReference"`
	Loads99     float64 `json:"loads99OverReference"`
	Meets       bool    `json:"meets"`
}

// deleteFill sets a cell against the batch index built with the live size
// of its postings: whether its loads are within the bars of the criterion
// there, which tells a failure of fill from one of edges.
type deleteFill struct {
	TargetSize  int     `json:"targetSize"`
	WorstRecall float64 `json:"worstRecallDifference"`
	Loads95     float64 `json:"loads95OverReference"`
	Loads99     float64 `json:"loads99OverReference"`
	Within      bool    `json:"loadsWithin"`
}

// deleteCell is one cell at one point. Variant is "b" for cell 1b.
type deleteCell struct {
	Cell     int           `json:"cell"`
	Variant  string        `json:"variant,omitempty"`
	Name     string        `json:"name"`
	Counts   deleteCounts  `json:"counts"`
	Work     deleteStats   `json:"work"`
	Refresh  *refreshStats `json:"refresh,omitempty"`
	Splits   int           `json:"splits"`
	Coverage string        `json:"coverage"`
	deleteCurve
	Verdict deleteVerdict `json:"verdict"`
	Fill    *deleteFill   `json:"fill,omitempty"`
}

// deleteRound is one round of the churn.
type deleteRound struct {
	Round    int          `json:"round"`
	Deleted  int          `json:"deleted"`
	Inserted int          `json:"inserted"`
	Counts   deleteCounts `json:"counts"`
	Work     deleteStats  `json:"work"`
	Inserts  streamStats  `json:"inserts"`
	Refresh  refreshStats `json:"refresh"`
	Coverage string       `json:"coverage"`
}

// deleteStage is one point of the protocol.
type deleteStage struct {
	Name string `json:"name"`
	// Corpus is the vectors of the index before the deletes, Deleted those
	// deleted, and Regions, for the clustered deletes, the regions and the
	// postings they took.
	Corpus         int             `json:"corpus"`
	Deleted        int             `json:"deleted"`
	Inserted       int             `json:"inserted,omitempty"`
	Regions        int             `json:"regions,omitempty"`
	RegionPostings int             `json:"regionPostings,omitempty"`
	TruthFallbacks int             `json:"truthFallbacks"`
	Reference      deleteReference `json:"reference"`
	// Fills are the batch indexes of the vectors that are left built with
	// the live sizes of the postings of the cells.
	Fills   []deleteReference `json:"fills,omitempty"`
	Cells   []deleteCell      `json:"cells"`
	Rounds  []deleteRound     `json:"rounds,omitempty"`
	Stopped string            `json:"stopped,omitempty"`
	Seconds float64           `json:"seconds"`
}

type deleteResult struct {
	Dataset     string         `json:"dataset"`
	Vectors     int            `json:"vectors"`
	Queries     int            `json:"queries"`
	K           int            `json:"k"`
	Config      map[string]any `json:"config"`
	Grid        []int          `json:"grid"`
	Threshold   int            `json:"threshold"`
	InsertEF    int            `json:"insertEF"`
	MinLive     int            `json:"minLive"`
	RewriteOver float64        `json:"rewriteOver"`
	// Build is the batch index of the whole train set every point of
	// deletes starts from.
	Build *buildWork `json:"build,omitempty"`
	// Best is the cell the churn runs with, and why.
	Best    int           `json:"best"`
	BestWhy string        `json:"bestWhy"`
	Stages  []deleteStage `json:"stages"`
	// NotRun are the stages that did not start: it was past the time of
	// -bench.deleteuntil.
	NotRun []string `json:"notRun,omitempty"`
}

// deleteSweep searches every query at every ef of the grid and counts its
// work. A deleted vector among the results stops it: the string tells which.
func deleteSweep(ctx context.Context, t *testing.T, logger logrus.FieldLogger, index *Index, uc UserConfig,
	queries [][]float32, truth [][]uint64, k int, deleted []bool,
) (deleteCurve, string) {
	var curve deleteCurve
	searchCtx := concurrency.CtxWithBudget(ctx, 1)
	for _, ef := range streamGrid {
		searchUC := uc
		searchUC.EF = ef
		require.NoError(t, index.UpdateUserConfig(searchUC, func() {}))
		var next, hits, loads, rounds, scanned, endpoints atomic.Int64
		var mu sync.Mutex
		stopped := ""
		eg := enterrors.NewErrorGroupWrapper(logger)
		for range 8 {
			eg.Go(func() error {
				for {
					q := int(next.Add(1)) - 1
					if q >= len(queries) {
						return nil
					}
					var work SearchStats
					ids, _, err := index.search(searchCtx, queries[q], k, nil, &work)
					if err != nil {
						return err
					}
					found := 0
					for _, id := range ids {
						if deleted != nil && deleted[id] {
							mu.Lock()
							if stopped == "" {
								stopped = fmt.Sprintf("query %d at ef %d returned doc %d, which is deleted", q, ef, id)
							}
							mu.Unlock()
						}
						if slices.Contains(truth[q], id) {
							found++
						}
					}
					hits.Add(int64(found))
					loads.Add(int64(work.PostingsLoaded))
					rounds.Add(int64(work.Rounds))
					scanned.Add(int64(work.VectorsScanned))
					endpoints.Add(int64(work.EndpointsScored))
				}
			})
		}
		require.NoError(t, eg.Wait())
		if stopped != "" {
			return curve, stopped
		}
		n := float64(len(queries))
		curve.Points = append(curve.Points, deletePoint{
			EF: ef, Recall: float64(hits.Load()) / (n * float64(k)), Loads: float64(loads.Load()) / n,
			Rounds: float64(rounds.Load()) / n, Scanned: float64(scanned.Load()) / n, Endpoints: float64(endpoints.Load()) / n,
		})
	}
	curve.read()
	return curve, ""
}

// deleteJudge sets the curve of a cell against that of its reference.
func deleteJudge(cell, reference *deleteCurve) deleteVerdict {
	verdict := deleteVerdict{WorstRecall: math.Inf(1)}
	for i, point := range cell.Points {
		if diff := point.Recall - reference.Points[i].Recall; diff < verdict.WorstRecall {
			verdict.WorstRecall, verdict.WorstEF = diff, point.EF
		}
	}
	if cell.Reached95 && reference.Reached95 {
		verdict.Loads95 = cell.Loads95 / reference.Loads95
	}
	if cell.Reached99 && reference.Reached99 {
		verdict.Loads99 = cell.Loads99 / reference.Loads99
	}
	verdict.Meets = verdict.WorstRecall >= -deleteRecallSlack && cell.Reached95 && cell.Reached99 &&
		verdict.Loads95 <= deleteLoads95 && verdict.Loads99 <= deleteLoads99
	return verdict
}

// deleteBest chooses the cell the churn runs with: the simplest one that
// meets the criterion; if none does, the one with the fewest loads at 0.99
// over the reference, a simpler one standing unless another is under it by
// more than the noise floor.
func deleteBest(all []deleteCell) (int, string) {
	// Among the four cells: cell 1b is not one the churn runs with.
	var cells []deleteCell
	for _, cell := range all {
		if cell.Variant == "" {
			cells = append(cells, cell)
		}
	}
	for _, cell := range cells {
		if cell.Verdict.Meets {
			return cell.Cell, "the simplest cell that meets the criterion with 30 % deleted at random"
		}
	}
	best := -1
	for i, cell := range cells {
		if !cell.Reached99 || cell.Verdict.Loads99 == 0 {
			continue
		}
		if best < 0 || cell.Verdict.Loads99 < cells[best].Verdict.Loads99-deleteNoise99 {
			best = i
		}
	}
	if best < 0 {
		return cells[len(cells)-1].Cell, "no cell reaches 0.99 with 30 % deleted at random: the last one"
	}
	return cells[best].Cell, "no cell meets the criterion with 30 % deleted at random: the one with the fewest loads at 0.99 " +
		"over its reference, the simpler one where two are within the noise floor"
}

// TestStreamDeletes runs the deletes experiment of v6.0 (bench-report.md,
// "v6.0: deletes en memoria") on the dataset of -bench.dataset, in counts:
// nothing is timed. One batch index of the whole train set; for every point,
// a copy of it with its share of vectors deleted, searched as each of the
// four cells, one on top of the other, next to a batch index of the vectors
// that are left; and a churn of deletes and inserts with the best cell. A
// deleted vector returned by a search, or a live vector that is not in
// exactly one posting, stops the stage.
func TestStreamDeletes(t *testing.T) {
	if *benchDatasetDir == "" || *benchDeleteStages == "" {
		t.Skip("no -bench.dataset and -bench.deletes given")
	}
	require.NotEmpty(t, *benchOut, "-bench.deletes writes its results to -bench.out")
	require.Positive(t, *benchStreamThreshold, "-bench.deletes needs -bench.streamthreshold")
	ctx := context.Background()
	data := loadBenchData(t, *benchDatasetDir, *benchLimit, *benchQueries, *benchK, false)
	if *benchLimit > 0 {
		data.neighbors = bruteNeighbors(data, streamTruthDepth)
	}
	require.NotNil(t, data.neighbors, "the live ground truth is drawn from the shipped one")
	n, k := len(data.train), *benchK
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)
	stages := strings.Split(*benchDeleteStages, ",")
	until := time.Time{}
	if *benchDeleteUntil != "" {
		clock, err := time.ParseInLocation("15:04", *benchDeleteUntil, time.Local)
		require.NoError(t, err, "-bench.deleteuntil")
		now := time.Now()
		until = time.Date(now.Year(), now.Month(), now.Day(), clock.Hour(), clock.Minute(), 0, 0, time.Local)
	}

	uc := NewDefaultUserConfig()
	uc.TargetPostingSize = *benchPosting
	uc.CodeBits = *benchCodeBits
	uc.BuildProbe = *benchProbe
	uc.RescueLimit = *benchRescue
	uc.TrainingLimit = *benchTraining
	insertUC := uc
	insertUC.EF = *benchStreamInsertEF
	cfg := streamConfig{threshold: *benchStreamThreshold, inverse: true, seed: 1}
	result := &deleteResult{
		Dataset: data.name, Vectors: n, Queries: len(data.queries), K: k, Grid: streamGrid, Threshold: cfg.threshold,
		InsertEF: *benchStreamInsertEF, MinLive: *benchDeleteMin, RewriteOver: *benchDeleteOver, Best: *benchDeleteCell,
		Config: map[string]any{
			"targetPostingSize": uc.TargetPostingSize, "buildProbe": uc.BuildProbe, "rescueLimit": uc.RescueLimit,
			"edges": uc.Edges, "centroidEF": centroidEF, "rescoreLimit": uc.RescoreLimit, "loadBatch": DefaultLoadBatch,
		},
	}
	if result.Best != 0 {
		result.BestWhy = "given with -bench.deletecell"
	}
	write := func() {
		raw, err := json.MarshalIndent(result, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(*benchOut, raw, 0o644))
	}
	_, tempVectorForID := benchThunks(data)
	vectorOf := func(id uint64) []float32 { return data.train[id] }
	// build builds the given vectors in batch, in the order of their ids,
	// for the given posting size.
	build := func(ids []uint64, target int) (*Index, time.Duration) {
		ids = slices.Clone(ids)
		slices.Sort(ids)
		begin := time.Now()
		sized := uc
		sized.TargetPostingSize = target
		index, err := New(Config{
			ID:                           "deletes",
			RootPath:                     t.TempDir(),
			Logger:                       logger,
			DistanceProvider:             distancer.NewCosineDistanceProvider(),
			TempVectorForIDWithViewThunk: tempVectorForID,
			GetViewThunk:                 func() common.BucketView { return noopBucketView{} },
			Seed:                         1,
		}, sized)
		require.NoError(t, err)
		const chunk = 10000
		for from := 0; from < len(ids); from += chunk {
			to := min(from+chunk, len(ids))
			vectors := make([][]float32, to-from)
			for i, id := range ids[from:to] {
				vectors[i] = data.train[id]
			}
			require.NoError(t, index.AddBatch(ctx, ids[from:to], vectors))
		}
		require.NoError(t, index.Build(ctx))
		return index, time.Since(begin)
	}
	// reference builds the vectors that are left in batch, for the given
	// posting size, and searches them.
	reference := func(live []bool, truth [][]uint64, target int) deleteReference {
		var ids []uint64
		for id, isLive := range live {
			if isLive {
				ids = append(ids, uint64(id))
			}
		}
		index, took := build(ids, target)
		defer func() { require.NoError(t, index.Shutdown(ctx)) }()
		st := index.state.Load()
		ref := deleteReference{
			TargetSize: target, Vectors: len(ids), Postings: st.postings.postings(), BuildSeconds: took.Seconds(),
			SizeMean: float64(len(ids)) / float64(st.postings.postings()),
		}
		// Searched with the configuration it was built with.
		sized := uc
		sized.TargetPostingSize = target
		var stopped string
		ref.deleteCurve, stopped = deleteSweep(ctx, t, logger, index, sized, data.queries, truth, k, nil)
		require.Empty(t, stopped)
		t.Logf("reference with postings of %d: %d vectors in %d postings, built in %s | loads at 0.95 %.2f, at 0.99 %.2f "+
			"(reached %t, between ef %d and %d)", target, ref.Vectors, ref.Postings, took.Round(time.Second), ref.Loads95,
			ref.Loads99, ref.Reached99, ref.Crossing[0], ref.Crossing[1])
		return ref
	}
	// fill builds the batch indexes of the vectors that are left with the
	// live sizes of the postings of the cells of a stage, one per size, and
	// sets every cell against its own: its loads within the bars of the
	// criterion there, or not.
	fill := func(stage *deleteStage, live []bool, truth [][]uint64) {
		if !*benchDeleteFill || stage.Stopped != "" {
			return
		}
		bySize := map[int]int{}
		for i := range stage.Cells {
			cell := &stage.Cells[i]
			if len(cell.Points) == 0 || cell.Counts.Postings == 0 {
				continue
			}
			size := int(math.Round(float64(cell.Counts.LiveMembers) / float64(cell.Counts.Postings)))
			at, built := bySize[size]
			if !built {
				stage.Fills = append(stage.Fills, reference(live, truth, size))
				at = len(stage.Fills) - 1
				bySize[size] = at
			}
			against := deleteJudge(&cell.deleteCurve, &stage.Fills[at].deleteCurve)
			cell.Fill = &deleteFill{
				TargetSize: size, WorstRecall: against.WorstRecall, Loads95: against.Loads95, Loads99: against.Loads99,
				Within: cell.Reached95 && cell.Reached99 && against.Loads95 > 0 && against.Loads99 > 0 &&
					against.Loads95 <= deleteLoads95 && against.Loads99 <= deleteLoads99,
			}
			t.Logf("%s, cell %d%s against the batch index with postings of %d: loads at 0.95 %.3f of it, at 0.99 %.3f | within "+
				"the bars %t | worst recall at equal ef %+.4f", stage.Name, cell.Cell, cell.Variant, size, against.Loads95,
				against.Loads99, cell.Fill.Within, against.WorstRecall)
			write()
		}
	}
	// measure searches a cell, counts it and sets it against the reference.
	// It returns false if the stage has to stop.
	measure := func(stage *deleteStage, cell int, variant string, index *Index, d *deleteIndex, truth [][]uint64,
		refresh *refreshStats,
	) bool {
		out := deleteCell{
			Cell: cell, Variant: variant, Name: deleteCellNames[cell], Counts: d.counts(uc.TargetPostingSize), Work: d.stats,
			Refresh: refresh, Splits: d.s.stats.Splits, Coverage: "complete",
		}
		if variant == "b" {
			out.Name = "1b: logical deletes, deleted members as sources"
		}
		if err := d.checkCoverage(); err != nil {
			out.Coverage = err.Error()
			stage.Stopped = fmt.Sprintf("coverage, cell %d%s: %v", cell, variant, err)
		} else {
			var stopped string
			out.deleteCurve, stopped = deleteSweep(ctx, t, logger, index, uc, data.queries, truth, k, d.deleted)
			if stopped != "" {
				stage.Stopped = fmt.Sprintf("cell %d%s: %s", cell, variant, stopped)
			} else {
				out.Verdict = deleteJudge(&out.deleteCurve, &stage.Reference.deleteCurve)
			}
		}
		stage.Cells = append(stage.Cells, out)
		write()
		if stage.Stopped != "" {
			t.Logf("STOP, %s: %s", stage.Name, stage.Stopped)
			return false
		}
		c := out.Counts
		t.Logf("%s, cell %d%s: worst recall at equal ef %+.4f (ef %d) | loads at 0.95 %.2f, %.3f of the reference; at 0.99 %.2f, "+
			"%.3f (reached %t, between ef %d and %d) | meets %t | %d postings of %.1f live members and %.1f in all, %d under "+
			"%d | dead edges %.3f, %d vectors with half or more | rewritten %d, merges %d, empty %d, splits %d", stage.Name,
			cell, variant, out.Verdict.WorstRecall, out.Verdict.WorstEF, out.Loads95, out.Verdict.Loads95, out.Loads99,
			out.Verdict.Loads99, out.Reached99, out.Crossing[0], out.Crossing[1], out.Verdict.Meets, c.Postings,
			float64(c.LiveMembers)/float64(max(c.Postings, 1)), float64(c.Members)/float64(max(c.Postings, 1)), c.UnderMin,
			d.minLive, float64(c.DeadEdges)/float64(max(c.Edges, 1)), c.HalfDead, out.Work.Rewritten, out.Work.Merges,
			out.Work.EmptyRetired, out.Work.MergeSplits)
		return true
	}

	var whole *Index
	var built *state
	needWhole := slices.ContainsFunc(stages, func(stage string) bool { return stage != "churn" })
	if needWhole {
		all := make([]uint64, n)
		for i := range all {
			all[i] = uint64(i)
		}
		var took time.Duration
		whole, took = build(all, uc.TargetPostingSize)
		built = whole.state.Load()
		result.Build = workOfBuild(t, whole, 0)
		t.Logf("the batch index of the whole train set: %d vectors in %d postings, built in %s", n, built.postings.postings(),
			took.Round(time.Second))
		write()
	}
	// random is one order of the train set: a share deleted at random is
	// its first vectors, so that a larger share holds a smaller one.
	random := rand.New(rand.NewPCG(4, 4)).Perm(n)

	// point runs the four cells over a copy of the whole index with the
	// given vectors deleted.
	point := func(stage *deleteStage, gone []uint64) {
		live := make([]bool, n)
		for i := range live {
			live[i] = true
		}
		for _, id := range gone {
			live[id] = false
		}
		stage.Corpus, stage.Deleted = n, len(gone)
		var truth [][]uint64
		truth, stage.TruthFallbacks = liveTruthOf(t, data, live, k)
		stage.Reference = reference(live, truth, uc.TargetPostingSize)
		write()

		// A copy of the whole index, with a centroid graph of its own: the
		// cleanup takes centroids out of it and adds others.
		whole.state.Store(built)
		s, err := newStreamIndex(whole, n, cfg, vectorOf)
		require.NoError(t, err)
		graph, err := newCentroidIndex(ctx, &whole.cfg, t.TempDir(), slices.Clone(built.centroids.vectors))
		require.NoError(t, err)
		s.st.centroids = graph
		defer func() {
			whole.state.Store(built)
			require.NoError(t, graph.shutdown(ctx))
		}()
		d := newDeleteIndex(s, *benchDeleteOver, *benchDeleteMin)
		for _, id := range gone {
			require.NoError(t, d.remove(id))
		}

		d.scoreDeletedEndpoints(true)
		if !measure(stage, 1, "", whole, d, truth, nil) {
			return
		}
		if *benchDeleteSources {
			// Cell 1b: the same index, its deleted members as sources.
			d.deletedAreSources(true)
			ok := measure(stage, 1, "b", whole, d, truth, nil)
			d.deletedAreSources(false)
			if !ok {
				return
			}
		}
		d.scoreDeletedEndpoints(false)
		if !measure(stage, 2, "", whole, d, truth, nil) {
			return
		}
		if err := d.cleanup(ctx); err != nil {
			stage.Stopped = fmt.Sprintf("cleanup: %v", err)
			write()
			return
		}
		if err := s.cleanCentroidGraph(); err != nil {
			t.Logf("%s: cleaning the centroid graph failed, measured with its tombstones: %v", stage.Name, err)
		}
		if !measure(stage, 3, "", whole, d, truth, nil) {
			return
		}
		require.NoError(t, whole.UpdateUserConfig(insertUC, func() {}))
		refreshed, err := d.refresh(ctx)
		if err != nil {
			stage.Stopped = fmt.Sprintf("refresh: %v", err)
			write()
			return
		}
		t.Logf("%s: %d vectors refreshed in %s: %d loads, %d endpoints scored, %d inverse edges", stage.Name,
			refreshed.Refreshed, refreshed.Time.Round(time.Second), refreshed.Work.SearchLoads, refreshed.Work.SearchEndpoints,
			refreshed.Work.InverseEdges)
		if measure(stage, 4, "", whole, d, truth, &refreshed) {
			fill(stage, live, truth)
		}
	}

	// clustered lists the vectors of whole regions of neighboring postings,
	// up to the share.
	clustered := func(stage *deleteStage, share float64) []uint64 {
		store, centroids := built.postings, built.centroids.vectors
		postings := store.postings()
		taken := make([]bool, postings)
		rng := rand.New(rand.NewPCG(5, 5))
		l2 := distancer.NewL2SquaredProvider()
		var gone []uint64
		target := int(share * float64(n))
		type near struct {
			p    int
			dist float32
		}
		for len(gone) < target {
			seed := rng.IntN(postings)
			for taken[seed] {
				seed = rng.IntN(postings)
			}
			var region []near
			for p := range postings {
				if taken[p] {
					continue
				}
				dist, err := l2.SingleDist(centroids[seed], centroids[p])
				require.NoError(t, err)
				region = append(region, near{p, dist})
			}
			slices.SortFunc(region, func(a, b near) int {
				if a.dist != b.dist {
					if a.dist < b.dist {
						return -1
					}
					return 1
				}
				return a.p - b.p
			})
			stage.Regions++
			for _, member := range region[:min(*benchDeleteRegion, len(region))] {
				if len(gone) >= target {
					break
				}
				taken[member.p] = true
				stage.RegionPostings++
				gone = append(gone, store.docIDs[store.start[member.p]:store.start[member.p+1]]...)
			}
		}
		return gone
	}

	// churn deletes a share of a base index and inserts as many new vectors,
	// in rounds, with the passes of the given cell after each.
	churn := func(stage *deleteStage, cell int) {
		order := rand.New(rand.NewPCG(6, 6)).Perm(n)
		baseN := n * 760 / 990
		each := baseN * 3 / 100
		const rounds = 10
		base, fresh := make([]uint64, baseN), make([]uint64, 0, n-baseN)
		for i, id := range order {
			if i < baseN {
				base[i] = uint64(id)
			} else {
				fresh = append(fresh, uint64(id))
			}
		}
		require.GreaterOrEqual(t, len(fresh), rounds*each, "new vectors to insert")
		index, took := build(base, uc.TargetPostingSize)
		defer func() { require.NoError(t, index.Shutdown(ctx)) }()
		t.Logf("churn: the base, %d vectors, built in %s", baseN, took.Round(time.Second))
		s, err := newStreamIndex(index, n, cfg, vectorOf)
		require.NoError(t, err)
		d := newDeleteIndex(s, *benchDeleteOver, *benchDeleteMin)
		d.scoreDeletedEndpoints(cell == 1)
		if cell >= 3 {
			d.dropAtSplit()
		}
		stage.Corpus = baseN
		// The base is deleted in the order of another shuffle of it.
		leaving := slices.Clone(base)
		rand.New(rand.NewPCG(7, 7)).Shuffle(len(leaving), func(i, j int) { leaving[i], leaving[j] = leaving[j], leaving[i] })
		require.NoError(t, index.UpdateUserConfig(insertUC, func() {}))
		for round := range rounds {
			out := deleteRound{Round: round + 1, Coverage: "complete"}
			for _, id := range leaving[round*each : (round+1)*each] {
				require.NoError(t, d.remove(id))
			}
			inserts := s.stats.clone()
			for _, id := range fresh[round*each : (round+1)*each] {
				if err := s.insert(ctx, id, data.train[id]); err != nil {
					stage.Stopped = fmt.Sprintf("round %d, insert of doc %d: %v", round+1, id, err)
					write()
					return
				}
			}
			stage.Deleted += each
			stage.Inserted += each
			out.Deleted, out.Inserted = stage.Deleted, stage.Inserted
			if cell >= 3 {
				if err := d.cleanup(ctx); err != nil {
					stage.Stopped = fmt.Sprintf("round %d, cleanup: %v", round+1, err)
					write()
					return
				}
			}
			if cell >= 4 {
				if out.Refresh, err = d.refresh(ctx); err != nil {
					stage.Stopped = fmt.Sprintf("round %d, refresh: %v", round+1, err)
					write()
					return
				}
			}
			if err := s.cleanCentroidGraph(); err != nil {
				t.Logf("churn, round %d: cleaning the centroid graph failed: %v", round+1, err)
			}
			if err := d.checkCoverage(); err != nil {
				out.Coverage = err.Error()
				stage.Stopped = fmt.Sprintf("round %d, coverage: %v", round+1, err)
			}
			out.Counts, out.Work = d.counts(uc.TargetPostingSize), d.stats
			out.Inserts = s.stats.clone()
			out.Inserts.Inserted -= inserts.Inserted
			out.Inserts.Splits -= inserts.Splits
			out.Inserts.SearchLoads -= inserts.SearchLoads
			out.Inserts.InverseEdges -= inserts.InverseEdges
			stage.Rounds = append(stage.Rounds, out)
			write()
			if stage.Stopped != "" {
				t.Logf("STOP, churn: %s", stage.Stopped)
				return
			}
			c := out.Counts
			t.Logf("churn, round %d: %d deleted and %d inserted so far | %d live in %d postings of %.1f live members | "+
				"dead edges %.3f | this round: %d splits, %.1f loads an insert; so far %d rewritten, %d merges, %d refreshed",
				round+1, out.Deleted, out.Inserted, c.Live, c.Postings, float64(c.LiveMembers)/float64(max(c.Postings, 1)),
				float64(c.DeadEdges)/float64(max(c.Edges, 1)), out.Inserts.Splits,
				float64(out.Inserts.SearchLoads)/float64(max(out.Inserts.Inserted, 1)), out.Work.Rewritten, out.Work.Merges,
				out.Refresh.Refreshed)
		}
		live := make([]bool, n)
		copy(live, s.live)
		var truth [][]uint64
		truth, stage.TruthFallbacks = liveTruthOf(t, data, live, k)
		stage.Reference = reference(live, truth, uc.TargetPostingSize)
		write()
		if measure(stage, cell, "", index, d, truth, nil) {
			fill(stage, live, truth)
		}
	}

	for _, name := range stages {
		name = strings.TrimSpace(name)
		if !until.IsZero() && !time.Now().Before(until) {
			result.NotRun = append(result.NotRun, name)
			t.Logf("%s does not start: it is past %s", name, *benchDeleteUntil)
			write()
			continue
		}
		begin := time.Now()
		result.Stages = append(result.Stages, deleteStage{Name: name})
		stage := &result.Stages[len(result.Stages)-1]
		t.Logf("== %s starts at %s", name, begin.Format("15:04"))
		switch name {
		case "random10", "random30", "random50":
			share := map[string]int{"random10": 10, "random30": 30, "random50": 50}[name]
			gone := make([]uint64, n*share/100)
			for i := range gone {
				gone[i] = uint64(random[i])
			}
			point(stage, gone)
			if name == "random30" && stage.Stopped == "" && result.Best == 0 {
				result.Best, result.BestWhy = deleteBest(stage.Cells)
				t.Logf("the cell of the churn: %d, %s", result.Best, result.BestWhy)
			}
		case "clustered30":
			point(stage, clustered(stage, 0.30))
		case "churn":
			require.NotZero(t, result.Best, "churn runs with the best cell of random30, or with that of -bench.deletecell")
			churn(stage, result.Best)
		default:
			t.Fatalf("unknown stage %q of -bench.deletes", name)
		}
		stage.Seconds = time.Since(begin).Seconds()
		write()
		t.Logf("== %s ends, in %s", name, time.Since(begin).Round(time.Second))
	}
	if whole != nil {
		whole.state.Store(built)
		require.NoError(t, whole.Shutdown(ctx))
	}
}
