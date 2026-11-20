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
		"rounds, with the best cell of random30, which must come before it). And those of v6.1: repair30 (the maintenance "+
		"pass in two phases, repair and eliminate, in its cells, over 30 % deleted at random) and repairchurn (the churn "+
		"with the best of them). And those of v6.2: memory30 (the pass of v6.1, B and C, each over its own copy with "+
		"30 % deleted at random) and memorychurn (the churn with the pass of v6.1, A, B and C, each over its own base). "+
		"And those of v6.2b: memorylong (that churn over more rounds, measured after some of them) and "+
		"memoryclustered (the cells of v6.2 over whole regions deleted, 30 % of the train set)")
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
	benchRepairCell = flag.String("bench.repaircell", "", "with -bench.deletes: the cell repairchurn runs with, by its "+
		"key, when repair30 is not among the stages to choose it")
	benchMemoryRounds = flag.Int("bench.memoryrounds", 30, "with the stage memorylong of -bench.deletes (v6.2b): the "+
		"rounds of its churn, each deleting 3 % of the base and inserting as many new vectors")
	benchMemoryMeasure = flag.String("bench.memorymeasure", "10,20,30", "with the stage memorylong: the rounds after "+
		"which every cell is searched and set against a batch index of the vectors live then, separated by commas")
	benchMemoryBase = flag.Int("bench.memorybase", 520, "with the stage memorylong: the vectors of the base of its "+
		"churn, of every 990 of the train set. The new vectors of all its rounds have to fit in the rest")
	benchMemoryCells = flag.String("bench.memorycells", "", "with the stages memorylong, memoryclustered and memory30: "+
		"the cells of v6.2 they run, by their keys, separated by commas. Empty: all of them")
	benchDeleteFill = flag.Bool("bench.deletefill", false, "with -bench.deletes: every point builds besides a batch "+
		"index of the vectors that are left with the mean live size of the postings of its cells as its posting size, "+
		"one for the cells before the cleanup and one for those after it, and sets every cell against its own")
)

// deleteCellNames are the cells of v6.0, each on top of the one before.
var deleteCellNames = []string{
	"", "1: logical deletes, deleted endpoints scored", "2: deleted endpoints not scored",
	"3: postings written again and merged", "4: edges refreshed",
	"5: repaired with the deleted navigable, then eliminated and merged",
}

// repairSpec is a cell of v6.1: the share of dead edges from which a vector
// is repaired (0/1 is one dead edge or more) and the merges of the second
// phase. legacy is the cell 4 of v6.0 as it was, the reference; mine a cell
// the order did not ask for.
type repairSpec struct {
	key        string
	share      string
	num, den   int
	mergeUnder int
	sumWithin  int
	legacy     bool
	mine       bool
}

// repairCost is the order of cost of the four cells of the order, the
// cheapest first: fewer repair searches, and with the same share the merge
// that moves fewer members.
var repairCost = []string{"r25-m118", "r25-m177", "r1-m118", "r1-m177"}

// repairBest chooses the cell the churn of v6.1 runs with: the cheapest of
// the four that meets the criterion; if none does, the one with the fewest
// loads at 0.99 over the reference, a cheaper one standing unless another is
// under it by more than the noise floor.
func repairBest(cells []deleteCell) (string, string) {
	byKey := map[string]*deleteCell{}
	for i := range cells {
		byKey[cells[i].Label] = &cells[i]
	}
	for _, key := range repairCost {
		if cell := byKey[key]; cell != nil && cell.Verdict.Meets {
			return key, "the cheapest cell that meets the criterion with 30 % deleted at random"
		}
	}
	best := ""
	for _, key := range repairCost {
		cell := byKey[key]
		if cell == nil || !cell.Reached99 || cell.Verdict.Loads99 == 0 {
			continue
		}
		if best == "" || cell.Verdict.Loads99 < byKey[best].Verdict.Loads99-deleteNoise99 {
			best = key
		}
	}
	if best == "" {
		return repairCost[len(repairCost)-1], "no cell reaches 0.99 with 30 % deleted at random: the last one"
	}
	return best, "no cell meets the criterion with 30 % deleted at random: the one with the fewest loads at 0.99 over " +
		"its reference, the cheaper one where two are within the noise floor"
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
	// Edges is the edge recall of the sample of a stage of v6.2.
	Edges *memoryEdges `json:"edgeRecall,omitempty"`
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
	Cell    int    `json:"cell"`
	Variant string `json:"variant,omitempty"`
	Name    string `json:"name"`
	// Label, RepairShare, MergeUnder, MergeWithin and Mine are those of a
	// cell of v6.1.
	Label       string        `json:"label,omitempty"`
	RepairShare string        `json:"repairShare,omitempty"`
	MergeUnder  int           `json:"mergeUnder,omitempty"`
	MergeWithin int           `json:"mergeWithin,omitempty"`
	Mine        bool          `json:"mine,omitempty"`
	Counts      deleteCounts  `json:"counts"`
	Work        deleteStats   `json:"work"`
	Refresh     *refreshStats `json:"refresh,omitempty"`
	Splits      int           `json:"splits"`
	Coverage    string        `json:"coverage"`
	deleteCurve
	Verdict deleteVerdict `json:"verdict"`
	Fill    *deleteFill   `json:"fill,omitempty"`
	// Memory is what a cell of v6.2 adds.
	Memory *memoryCell `json:"memory,omitempty"`
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
	Fills  []deleteReference `json:"fills,omitempty"`
	Cells  []deleteCell      `json:"cells"`
	Rounds []deleteRound     `json:"rounds,omitempty"`
	// Checkpoints are the rounds of the long churn of v6.2b after which its
	// cells are measured, each with its own reference: the batch index of
	// the vectors live then.
	Checkpoints []deleteStage `json:"checkpoints,omitempty"`
	Stopped     string        `json:"stopped,omitempty"`
	Seconds     float64       `json:"seconds"`
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
	Best    int    `json:"best"`
	BestWhy string `json:"bestWhy"`
	// BestRepair is the cell of v6.1 its churn runs with, by its key.
	BestRepair    string        `json:"bestRepair,omitempty"`
	BestRepairWhy string        `json:"bestRepairWhy,omitempty"`
	Stages        []deleteStage `json:"stages"`
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
		BestRepair: *benchRepairCell,
		Config: map[string]any{
			"targetPostingSize": uc.TargetPostingSize, "buildProbe": uc.BuildProbe, "rescueLimit": uc.RescueLimit,
			"edges": uc.Edges, "centroidEF": centroidEF, "rescoreLimit": uc.RescoreLimit, "loadBatch": DefaultLoadBatch,
		},
	}
	if result.Best != 0 {
		result.BestWhy = "given with -bench.deletecell"
	}
	if result.BestRepair != "" {
		result.BestRepairWhy = "given with -bench.repaircell"
	}
	// The cells of v6.1, in the order they run: the reference, mine, and
	// the four of the order from the fewest repairs to the most.
	fillUnder := uc.TargetPostingSize * 3 / 4
	repairSpecs := []repairSpec{
		{key: "v60-4", share: "half or more, after the cleanup and without going through the deleted", num: 1, den: 2, mergeUnder: *benchDeleteMin, legacy: true},
		{key: "r50-m118", share: "half or more", num: 1, den: 2, mergeUnder: *benchDeleteMin, mine: true},
		{key: "r25-m118", share: "a quarter or more", num: 1, den: 4, mergeUnder: *benchDeleteMin},
		{key: "r25-m177", share: "a quarter or more", num: 1, den: 4, mergeUnder: fillUnder, sumWithin: cfg.threshold},
		{key: "r1-m118", share: "one or more", num: 0, den: 1, mergeUnder: *benchDeleteMin},
		{key: "r1-m177", share: "one or more", num: 0, den: 1, mergeUnder: fillUnder, sumWithin: cfg.threshold},
	}
	repairSpecOf := func(key string) *repairSpec {
		for i := range repairSpecs {
			if repairSpecs[i].key == key {
				return &repairSpecs[i]
			}
		}
		return nil
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
	// sampled, which a stage of v6.2 sets, counts the edge recall of its
	// sample in a batch index of the vectors that are left.
	var sampled func(index *Index) *memoryEdges
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
		if sampled != nil {
			ref.Edges = sampled(index)
		}
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
	needWhole := slices.ContainsFunc(stages, func(stage string) bool {
		return stage != "churn" && stage != "repairchurn" && stage != "memorychurn" && stage != "memorylong"
	})
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

	// repairPoint runs the cells of v6.1 over copies of the whole index with
	// the given vectors deleted: each cell on a copy of its own. The repair
	// of a share runs once: it changes no posting, so the second cell of
	// the share takes the lists it left.
	repairPoint := func(stage *deleteStage, gone []uint64) {
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

		type repaired struct {
			snap  edgeSnapshot
			stats refreshStats
		}
		done := map[[2]int]*repaired{}
		cell := func(spec repairSpec) bool {
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
			stop := func(what string, err error) bool {
				stage.Stopped = fmt.Sprintf("%s, %s: %v", spec.key, what, err)
				write()
				t.Logf("STOP, %s: %s", stage.Name, stage.Stopped)
				return false
			}
			var refreshed refreshStats
			number := 5
			if spec.legacy {
				// The cell 4 of v6.0, as it ran there.
				number = 4
				d.scoreDeletedEndpoints(false)
				if err := d.cleanup(ctx); err != nil {
					return stop("cleanup", err)
				}
				if err := s.cleanCentroidGraph(); err != nil {
					t.Logf("%s, %s: cleaning the centroid graph failed, measured with its tombstones: %v", stage.Name, spec.key, err)
				}
				require.NoError(t, whole.UpdateUserConfig(insertUC, func() {}))
				if refreshed, err = d.refresh(ctx); err != nil {
					return stop("refresh", err)
				}
			} else {
				share := [2]int{spec.num, spec.den}
				if prior := done[share]; prior != nil {
					require.NoError(t, s.restoreEdges(prior.snap))
					refreshed = prior.stats
				} else {
					require.NoError(t, whole.UpdateUserConfig(insertUC, func() {}))
					if refreshed, err = d.repair(ctx, spec.num, spec.den); err != nil {
						return stop("repair", err)
					}
					done[share] = &repaired{snap: s.snapshotEdges(), stats: refreshed}
				}
				if err := d.eliminate(ctx, spec.mergeUnder, spec.sumWithin); err != nil {
					return stop("eliminate", err)
				}
				if err := s.cleanCentroidGraph(); err != nil {
					t.Logf("%s, %s: cleaning the centroid graph failed, measured with its tombstones: %v", stage.Name, spec.key, err)
				}
			}
			t.Logf("%s, %s: %d vectors repaired (%s) in %s: %d loads, %d endpoints scored, %d exact distances, %d inverse "+
				"edges | %d edges dropped, %d merges, %d not made for the size", stage.Name, spec.key, refreshed.Refreshed,
				spec.share, refreshed.Time.Round(time.Second), refreshed.Work.SearchLoads, refreshed.Work.SearchEndpoints,
				refreshed.Work.SearchRescored, refreshed.Work.InverseEdges, d.stats.DroppedEdges, d.stats.Merges,
				d.stats.MergesSkipped)
			ok := measure(stage, number, "", whole, d, truth, &refreshed)
			out := &stage.Cells[len(stage.Cells)-1]
			out.Label, out.RepairShare, out.MergeUnder, out.MergeWithin, out.Mine = spec.key, spec.share, spec.mergeUnder,
				spec.sumWithin, spec.mine
			write()
			return ok
		}
		for _, spec := range repairSpecs {
			if !cell(spec) {
				return
			}
		}
		fill(stage, live, truth)
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
	churn := func(stage *deleteStage, cell int, repair *repairSpec) {
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
		if cell >= 3 && repair == nil {
			d.dropAtSplit()
		}
		if repair != nil {
			// Until a pass takes them out, the deleted vectors are
			// navigable, for the searches of the inserts too. A posting
			// that an insert leaves past the threshold is split with them.
			d.scoreDeletedEndpoints(true)
			d.deletedAreSources(true)
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
			if repair != nil {
				if out.Refresh, err = d.repair(ctx, repair.num, repair.den); err != nil {
					stage.Stopped = fmt.Sprintf("round %d, repair: %v", round+1, err)
					write()
					return
				}
				if err := d.eliminate(ctx, repair.mergeUnder, repair.sumWithin); err != nil {
					stage.Stopped = fmt.Sprintf("round %d, eliminate: %v", round+1, err)
					write()
					return
				}
			}
			if cell >= 3 && repair == nil {
				if err := d.cleanup(ctx); err != nil {
					stage.Stopped = fmt.Sprintf("round %d, cleanup: %v", round+1, err)
					write()
					return
				}
			}
			if cell >= 4 && repair == nil {
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
			// Of the inserts of a round, these four counts are of the round.
			// Every other count of out.Inserts, and all of out.Work, are of
			// the rounds so far. out.Refresh is of the round.
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
				"dead edges %.3f | this round: %d splits, %.1f loads an insert, %d lists built again; so far %d rewritten, %d merges",
				round+1, out.Deleted, out.Inserted, c.Live, c.Postings, float64(c.LiveMembers)/float64(max(c.Postings, 1)),
				float64(c.DeadEdges)/float64(max(c.Edges, 1)), out.Inserts.Splits,
				float64(out.Inserts.SearchLoads)/float64(max(out.Inserts.Inserted, 1)), out.Refresh.Refreshed,
				out.Work.Rewritten, out.Work.Merges)
		}
		live := make([]bool, n)
		copy(live, s.live)
		var truth [][]uint64
		truth, stage.TruthFallbacks = liveTruthOf(t, data, live, k)
		stage.Reference = reference(live, truth, uc.TargetPostingSize)
		write()
		ok := measure(stage, cell, "", index, d, truth, nil)
		if repair != nil {
			out := &stage.Cells[len(stage.Cells)-1]
			out.Label, out.RepairShare, out.MergeUnder, out.MergeWithin = repair.key, repair.share, repair.mergeUnder,
				repair.sumWithin
			write()
		}
		if ok {
			fill(stage, live, truth)
		}
	}

	// memoryWanted tells whether a stage runs the cell of v6.2 with the
	// given key: all of them, unless -bench.memorycells names some.
	memoryWanted := func(key string) bool {
		if *benchMemoryCells == "" {
			return true
		}
		for _, wanted := range strings.Split(*benchMemoryCells, ",") {
			if strings.TrimSpace(wanted) == key {
				return true
			}
		}
		return false
	}
	// memoryMeasure counts a cell of v6.2 when its passes end and searches
	// it. Its verdict waits for the reference of its stage, which is built
	// after the cells (memoryJudge). It returns false if the stage has to
	// stop.
	memoryMeasure := func(stage *deleteStage, spec memorySpec, index *Index, d *deleteIndex, truth [][]uint64,
		memory *memoryCell, sample []uint64, nearest [][]scored, inserted []bool,
	) bool {
		out := deleteCell{
			Cell: 6, Name: spec.name, Label: spec.key, RepairShare: "a quarter or more", MergeUnder: fillUnder,
			MergeWithin: cfg.threshold, Mine: spec.mine, Counts: d.counts(uc.TargetPostingSize), Work: d.stats,
			Splits: d.s.stats.Splits, Coverage: "complete", Memory: memory,
		}
		if err := d.checkCoverage(); err != nil {
			out.Coverage = err.Error()
			stage.Stopped = fmt.Sprintf("%s, coverage: %v", spec.key, err)
		} else if edges, err := d.edgesOfSample(sample, nearest, inserted); err != nil {
			stage.Stopped = fmt.Sprintf("%s, the edge recall: %v", spec.key, err)
		} else {
			memory.Edges = &edges
			var stopped string
			out.deleteCurve, stopped = deleteSweep(ctx, t, logger, index, uc, data.queries, truth, k, d.deleted)
			if stopped != "" {
				stage.Stopped = fmt.Sprintf("%s: %s", spec.key, stopped)
			}
		}
		stage.Cells = append(stage.Cells, out)
		write()
		if stage.Stopped != "" {
			t.Logf("STOP, %s: %s", stage.Name, stage.Stopped)
			return false
		}
		c, e := out.Counts, memory.Edges.All
		deleted := float64(max(memory.Deleted, 1))
		t.Logf("%s, %s: loads at 0.95 %.2f, at 0.99 %.2f (reached %t, between ef %d and %d) | %d postings of %.1f live "+
			"members | %.2f edges a live vector | edge recall %.4f on %d vectors, of postings %.4f, %d substitutes of which "+
			"%d are true edges | a deleted vector: %.1f search loads, %.2f postings of the second phase, %.1f exact "+
			"distances, %.1f code distances and %.1f of the rehearsals", stage.Name, spec.key, out.Loads95, out.Loads99,
			out.Reached99, out.Crossing[0], out.Crossing[1], c.Postings, float64(c.LiveMembers)/float64(max(c.Postings, 1)),
			float64(c.Edges)/float64(max(c.Live, 1)), e.Recall, e.Vectors, e.PostingRecall, e.Substitutes, e.SubstitutesTrue,
			float64(memory.SearchLoads)/deleted, float64(memory.SecondPhasePostings)/deleted,
			float64(memory.SearchRescored+memory.InverseDistances)/deleted, float64(memory.CodeDistances)/deleted,
			float64(memory.RehearsalCodeDistances)/deleted)
		return true
	}
	// memoryJudge sets every cell of a stage of v6.2 against its reference.
	memoryJudge := func(stage *deleteStage) {
		for i := range stage.Cells {
			cell := &stage.Cells[i]
			cell.Verdict = deleteJudge(&cell.deleteCurve, &stage.Reference.deleteCurve)
			t.Logf("%s, %s: worst recall at equal ef %+.4f (ef %d) | loads at 0.95 %.3f of the reference, at 0.99 %.3f | "+
				"meets %t", stage.Name, cell.Label, cell.Verdict.WorstRecall, cell.Verdict.WorstEF, cell.Verdict.Loads95,
				cell.Verdict.Loads99, cell.Verdict.Meets)
		}
		if ref := stage.Reference.Edges; ref != nil {
			t.Logf("%s, the reference: edge recall %.4f on %d vectors, of postings %.4f, %.2f edges a vector", stage.Name,
				ref.All.Recall, ref.All.Vectors, ref.All.PostingRecall, ref.All.Degree)
		}
		write()
	}
	// memoryLog logs one pass of a cell of v6.2.
	memoryLog := func(stage *deleteStage, spec memorySpec, round int, pass *memoryPass, counts deleteCounts) {
		t.Logf("%s, %s, pass %d: %d lists built again with %d loads and %d exact distances | %d postings in the second "+
			"phase, %d merges | %d edges substituted with %d code distances, %d taken out with none, %d code distances in "+
			"the rehearsal | %.2f edges a live vector, %d of them substitutes | %d edges lost in the count, of %d vectors",
			stage.Name, spec.key, round, pass.Repair.Refreshed, pass.Repair.Work.SearchLoads,
			pass.Repair.Work.SearchRescored+pass.Repair.Work.InverseDistances, pass.SecondPhasePostings, pass.Merges,
			pass.Substitution.Substituted, pass.Substitution.CodeDistances, pass.Dropped, pass.Rehearsal.CodeDistances,
			float64(counts.Edges)/float64(max(counts.Live, 1)), pass.Memory.SubstituteEdges, pass.Memory.Lost,
			pass.Memory.WithLost)
	}

	// memoryPoint runs the cells of v6.2 over copies of the whole index with
	// the given vectors deleted at once, each on a copy of its own: the pass
	// of v6.1, B and C. With every count at zero A is the pass of v6.1. The
	// references are built after the cells.
	memoryPoint := func(stage *deleteStage, gone []uint64) {
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
		sample := memorySample(live, memorySampleSize)
		nearest, err := memoryNearest(ctx, logger, whole.provider, vectorOf, live, sample, uc.Edges)
		require.NoError(t, err)
		sampled = func(index *Index) *memoryEdges {
			edges, err := edgesOfBatch(index.state.Load(), n, sample, nearest)
			require.NoError(t, err)
			return &edges
		}
		defer func() { sampled = nil }()

		cell := func(spec memorySpec) bool {
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
			d.remember()
			for _, id := range gone {
				require.NoError(t, d.remove(id))
			}
			require.NoError(t, whole.UpdateUserConfig(insertUC, func() {}))
			memory := &memoryCell{Deleted: len(gone)}
			pass, err := d.pass(ctx, spec, fillUnder, cfg.threshold)
			if err != nil {
				stage.Stopped = fmt.Sprintf("%s: %v", spec.key, err)
				write()
				t.Logf("STOP, %s: %s", stage.Name, stage.Stopped)
				return false
			}
			if err := s.cleanCentroidGraph(); err != nil {
				t.Logf("%s, %s: cleaning the centroid graph failed, measured with its tombstones: %v", stage.Name, spec.key, err)
			}
			memory.add(pass)
			memoryLog(stage, spec, 1, &pass, d.counts(uc.TargetPostingSize))
			return memoryMeasure(stage, spec, whole, d, truth, memory, sample, nearest, nil)
		}
		for _, spec := range memorySpecs {
			if spec.key == "A" || !memoryWanted(spec.key) {
				continue
			}
			if !cell(spec) {
				return
			}
		}
		stage.Reference = reference(live, truth, uc.TargetPostingSize)
		write()
		memoryJudge(stage)
		fill(stage, live, truth)
	}

	// memoryChurn runs the churn of v6.1 with every cell of v6.2, each over a
	// base index of its own, built the same, with the same deletes and the
	// same inserts: ten rounds that delete 3 % of the base and insert as many
	// new vectors, and the pass of the cell after each. Until a pass takes
	// them out the deleted vectors are navigable, for the searches of the
	// inserts too. The references are built after the cells.
	memoryChurn := func(stage *deleteStage) {
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
		// The base is deleted in the order of another shuffle of it.
		leaving := slices.Clone(base)
		rand.New(rand.NewPCG(7, 7)).Shuffle(len(leaving), func(i, j int) { leaving[i], leaving[j] = leaving[j], leaving[i] })
		// The live vectors when the rounds end, the same in every cell.
		live, inserted := make([]bool, n), make([]bool, n)
		for _, id := range base {
			live[id] = true
		}
		for _, id := range leaving[:rounds*each] {
			live[id] = false
		}
		for _, id := range fresh[:rounds*each] {
			live[id], inserted[id] = true, true
		}
		stage.Corpus, stage.Deleted, stage.Inserted = baseN, rounds*each, rounds*each
		var truth [][]uint64
		truth, stage.TruthFallbacks = liveTruthOf(t, data, live, k)
		sample := memorySample(live, memorySampleSize)
		var nearest [][]scored
		defer func() { sampled = nil }()

		cell := func(spec memorySpec) bool {
			index, took := build(base, uc.TargetPostingSize)
			defer func() { require.NoError(t, index.Shutdown(ctx)) }()
			t.Logf("%s, %s: the base, %d vectors, built in %s", stage.Name, spec.key, baseN, took.Round(time.Second))
			if nearest == nil {
				var err error
				nearest, err = memoryNearest(ctx, logger, index.provider, vectorOf, live, sample, uc.Edges)
				require.NoError(t, err)
				sampled = func(index *Index) *memoryEdges {
					edges, err := edgesOfBatch(index.state.Load(), n, sample, nearest)
					require.NoError(t, err)
					return &edges
				}
			}
			s, err := newStreamIndex(index, n, cfg, vectorOf)
			require.NoError(t, err)
			d := newDeleteIndex(s, *benchDeleteOver, *benchDeleteMin)
			d.remember()
			d.scoreDeletedEndpoints(true)
			d.deletedAreSources(true)
			require.NoError(t, index.UpdateUserConfig(insertUC, func() {}))
			memory := &memoryCell{Deleted: rounds * each}
			stop := func(format string, args ...any) bool {
				stage.Stopped = spec.key + ", " + fmt.Sprintf(format, args...)
				write()
				t.Logf("STOP, %s: %s", stage.Name, stage.Stopped)
				return false
			}
			for round := range rounds {
				out := deleteRound{Round: round + 1, Coverage: "complete"}
				for _, id := range leaving[round*each : (round+1)*each] {
					require.NoError(t, d.remove(id))
				}
				inserts := s.stats.clone()
				for _, id := range fresh[round*each : (round+1)*each] {
					if err := s.insert(ctx, id, data.train[id]); err != nil {
						return stop("round %d, insert of doc %d: %v", round+1, id, err)
					}
				}
				out.Deleted, out.Inserted = (round+1)*each, (round+1)*each
				pass, err := d.pass(ctx, spec, fillUnder, cfg.threshold)
				if err != nil {
					return stop("round %d: %v", round+1, err)
				}
				if err := s.cleanCentroidGraph(); err != nil {
					t.Logf("%s, %s, round %d: cleaning the centroid graph failed: %v", stage.Name, spec.key, round+1, err)
				}
				if err := d.checkCoverage(); err != nil {
					return stop("round %d, coverage: %v", round+1, err)
				}
				out.Counts, out.Work, out.Refresh = d.counts(uc.TargetPostingSize), d.stats, pass.Repair
				// Of the inserts of a round, these four counts are of the
				// round. Every other count of out.Inserts, and all of
				// out.Work, are of the rounds so far.
				out.Inserts = s.stats.clone()
				out.Inserts.Inserted -= inserts.Inserted
				out.Inserts.Splits -= inserts.Splits
				out.Inserts.SearchLoads -= inserts.SearchLoads
				out.Inserts.InverseEdges -= inserts.InverseEdges
				memory.add(pass)
				memory.Rounds = append(memory.Rounds, out)
				memoryLog(stage, spec, round+1, &pass, out.Counts)
			}
			if !slices.Equal(live, s.live) {
				return stop("the live vectors when the rounds end are not those the sample was drawn from")
			}
			return memoryMeasure(stage, spec, index, d, truth, memory, sample, nearest, inserted)
		}
		for _, spec := range memorySpecs {
			if !cell(spec) {
				return
			}
		}
		stage.Reference = reference(live, truth, uc.TargetPostingSize)
		write()
		memoryJudge(stage)
		fill(stage, live, truth)
	}

	// memoryLong is the long churn of v6.2b: the churn of memoryChurn over
	// -bench.memoryrounds rounds, for the cells of -bench.memorycells, each
	// over a base of its own. After every round of -bench.memorymeasure a
	// cell is searched and set against the batch index of the vectors that
	// are live then, which is built the first time a cell gets there and is
	// the same for all: so are the deletes and the inserts. The base is
	// smaller than that of memoryChurn so that every insert of every round
	// is a vector the index never held.
	memoryLong := func(stage *deleteStage) {
		rounds := *benchMemoryRounds
		var measureAt []int
		for _, field := range strings.Split(*benchMemoryMeasure, ",") {
			var round int
			_, err := fmt.Sscanf(strings.TrimSpace(field), "%d", &round)
			require.NoError(t, err, "-bench.memorymeasure")
			require.True(t, round >= 1 && round <= rounds, "-bench.memorymeasure: round %d of %d", round, rounds)
			measureAt = append(measureAt, round)
		}
		require.True(t, slices.IsSorted(measureAt), "-bench.memorymeasure: in order")
		order := rand.New(rand.NewPCG(6, 6)).Perm(n)
		baseN := n * *benchMemoryBase / 990
		each := baseN * 3 / 100
		base, fresh := make([]uint64, baseN), make([]uint64, 0, n-baseN)
		for i, id := range order {
			if i < baseN {
				base[i] = uint64(id)
			} else {
				fresh = append(fresh, uint64(id))
			}
		}
		require.GreaterOrEqual(t, len(fresh), rounds*each, "new vectors to insert")
		require.GreaterOrEqual(t, len(base), rounds*each, "vectors of the base to delete")
		// The base is deleted in the order of another shuffle of it.
		leaving := slices.Clone(base)
		rand.New(rand.NewPCG(7, 7)).Shuffle(len(leaving), func(i, j int) { leaving[i], leaving[j] = leaving[j], leaving[i] })
		stage.Corpus, stage.Deleted, stage.Inserted = baseN, rounds*each, rounds*each

		// What every round of measure is measured with: the vectors live
		// after it, those of them the churn inserted, the truth of the
		// queries among them and the sample of the edge recall.
		type measured struct {
			live, inserted []bool
			truth          [][]uint64
			sample         []uint64
			nearest        [][]scored
		}
		points := make([]measured, len(measureAt))
		stage.Checkpoints = make([]deleteStage, len(measureAt))
		for at, round := range measureAt {
			pt, cp := &points[at], &stage.Checkpoints[at]
			pt.live, pt.inserted = make([]bool, n), make([]bool, n)
			for _, id := range base {
				pt.live[id] = true
			}
			for _, id := range leaving[:round*each] {
				pt.live[id] = false
			}
			for _, id := range fresh[:round*each] {
				pt.live[id], pt.inserted[id] = true, true
			}
			cp.Name = fmt.Sprintf("round %d", round)
			cp.Corpus, cp.Deleted, cp.Inserted = baseN, round*each, round*each
			pt.truth, cp.TruthFallbacks = liveTruthOf(t, data, pt.live, k)
			pt.sample = memorySample(pt.live, memorySampleSize)
		}
		defer func() { sampled = nil }()

		cell := func(spec memorySpec) bool {
			index, took := build(base, uc.TargetPostingSize)
			defer func() { require.NoError(t, index.Shutdown(ctx)) }()
			t.Logf("%s, %s: the base, %d vectors, built in %s", stage.Name, spec.key, baseN, took.Round(time.Second))
			s, err := newStreamIndex(index, n, cfg, vectorOf)
			require.NoError(t, err)
			d := newDeleteIndex(s, *benchDeleteOver, *benchDeleteMin)
			d.remember()
			d.scoreDeletedEndpoints(true)
			d.deletedAreSources(true)
			require.NoError(t, index.UpdateUserConfig(insertUC, func() {}))
			memory := &memoryCell{}
			stop := func(format string, args ...any) bool {
				stage.Stopped = spec.key + ", " + fmt.Sprintf(format, args...)
				write()
				t.Logf("STOP, %s: %s", stage.Name, stage.Stopped)
				return false
			}
			for round := range rounds {
				out := deleteRound{Round: round + 1, Coverage: "complete"}
				for _, id := range leaving[round*each : (round+1)*each] {
					require.NoError(t, d.remove(id))
				}
				inserts := s.stats.clone()
				for _, id := range fresh[round*each : (round+1)*each] {
					if err := s.insert(ctx, id, data.train[id]); err != nil {
						return stop("round %d, insert of doc %d: %v", round+1, id, err)
					}
				}
				out.Deleted, out.Inserted = (round+1)*each, (round+1)*each
				pass, err := d.pass(ctx, spec, fillUnder, cfg.threshold)
				if err != nil {
					return stop("round %d: %v", round+1, err)
				}
				if err := s.cleanCentroidGraph(); err != nil {
					t.Logf("%s, %s, round %d: cleaning the centroid graph failed: %v", stage.Name, spec.key, round+1, err)
				}
				if err := d.checkCoverage(); err != nil {
					return stop("round %d, coverage: %v", round+1, err)
				}
				out.Counts, out.Work, out.Refresh = d.counts(uc.TargetPostingSize), d.stats, pass.Repair
				// Of the inserts of a round, these four counts are of the
				// round. Every other count of out.Inserts, and all of
				// out.Work, are of the rounds so far.
				out.Inserts = s.stats.clone()
				out.Inserts.Inserted -= inserts.Inserted
				out.Inserts.Splits -= inserts.Splits
				out.Inserts.SearchLoads -= inserts.SearchLoads
				out.Inserts.InverseEdges -= inserts.InverseEdges
				memory.add(pass)
				memory.Rounds = append(memory.Rounds, out)
				memoryLog(stage, spec, round+1, &pass, out.Counts)

				at := slices.Index(measureAt, round+1)
				if at < 0 {
					continue
				}
				pt, cp := &points[at], &stage.Checkpoints[at]
				if !slices.Equal(pt.live, s.live) {
					return stop("round %d: the live vectors are not those its measure was prepared for", round+1)
				}
				if pt.nearest == nil {
					pt.nearest, err = memoryNearest(ctx, logger, index.provider, vectorOf, pt.live, pt.sample, uc.Edges)
					require.NoError(t, err)
				}
				// What the cell has done so far, as it is after this round.
				so := *memory
				so.Deleted = (round + 1) * each
				so.Passes, so.Rounds = slices.Clone(memory.Passes), slices.Clone(memory.Rounds)
				if !memoryMeasure(cp, spec, index, d, pt.truth, &so, pt.sample, pt.nearest, pt.inserted) {
					stage.Stopped = fmt.Sprintf("round %d, %s", round+1, cp.Stopped)
					write()
					return false
				}
				if len(cp.Reference.Points) == 0 {
					sampled = func(index *Index) *memoryEdges {
						edges, err := edgesOfBatch(index.state.Load(), n, pt.sample, pt.nearest)
						require.NoError(t, err)
						return &edges
					}
					cp.Reference = reference(pt.live, pt.truth, uc.TargetPostingSize)
					sampled = nil
				}
				memoryJudge(cp)
				// The searches of the next inserts go on with their ef.
				require.NoError(t, index.UpdateUserConfig(insertUC, func() {}))
			}
			return true
		}
		for _, spec := range memorySpecs {
			if !memoryWanted(spec.key) {
				continue
			}
			if !cell(spec) {
				return
			}
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
			churn(stage, result.Best, nil)
		case "repair30":
			gone := make([]uint64, n*30/100)
			for i := range gone {
				gone[i] = uint64(random[i])
			}
			repairPoint(stage, gone)
			if stage.Stopped == "" && result.BestRepair == "" {
				result.BestRepair, result.BestRepairWhy = repairBest(stage.Cells)
				t.Logf("the cell of the churn: %s, %s", result.BestRepair, result.BestRepairWhy)
			}
		case "repairchurn":
			spec := repairSpecOf(result.BestRepair)
			require.NotNilf(t, spec, "repairchurn runs with the best cell of repair30, or with that of -bench.repaircell: %q",
				result.BestRepair)
			churn(stage, 5, spec)
		case "memory30":
			gone := make([]uint64, n*30/100)
			for i := range gone {
				gone[i] = uint64(random[i])
			}
			memoryPoint(stage, gone)
		case "memorychurn":
			memoryChurn(stage)
		case "memorylong":
			memoryLong(stage)
		case "memoryclustered":
			memoryPoint(stage, clustered(stage, 0.30))
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
