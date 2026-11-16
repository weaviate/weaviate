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
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

var (
	benchStream = flag.String("bench.stream", "", "the streaming experiment of v2.0 (TestStream): calibrate, or a cell: "+
		"principal, h1 (inverse edges), h2 (edge repair) or both")
	benchStreamOrder = flag.String("bench.streamorder", "shuffled", "with -bench.stream: the order the train set is "+
		"built and inserted in, shuffled (seed 1) or natural")
	benchStreamThreshold  = flag.Int("bench.streamthreshold", 0, "with a -bench.stream cell: the size past which a posting splits")
	benchStreamThresholds = flag.String("bench.streamthresholds", "280,320,360,400,472", "with -bench.stream calibrate: "+
		"the thresholds to try")
	benchStreamInsertEF    = flag.Int("bench.streaminsertef", 320, "with -bench.stream: the ef of the search for a new vector's edges")
	benchStreamCheckpoints = flag.String("bench.streamcheckpoints", "60,70,80,90,100", "with -bench.stream: the shares of "+
		"the train set, in percent, measured after the batch half")
	benchStreamSample        = flag.Int("bench.streamsample", 2000, "with -bench.stream: vectors sampled for each edge recall")
	benchStreamMinEdgeRecall = flag.Float64("bench.streamminedgerecall", 0.90, "with a -bench.stream cell: the edge recall "+
		"at insertion below which the cell stops at a checkpoint, the guard of the edge builder (the pre-registered 0.90 "+
		"of the amendment; lower only for a smoke)")
	benchStreamReference = flag.Bool("bench.streamreference", false, "with a -bench.stream cell: build the batch "+
		"reference over the whole train set first, measure it, and time its frontier and the cell's at the end with "+
		"the plan of -bench.sweep")
)

// streamTruthDepth is how deep the ground truth of a truncated corpus goes,
// that of the shipped one of dbpedia-openai.
const streamTruthDepth = 100

// streamGrid is the untimed grid measured at every checkpoint.
var streamGrid = []int{10, 20, 40, 80, 160, 320, 640, 1024}

// streamCheckpoint is the state of a cell at one share of the train set.
type streamCheckpoint struct {
	Percent  int `json:"percent"`
	Live     int `json:"live"`
	Postings int `json:"postings"`
	// The live postings' sizes.
	SizeMean float64 `json:"sizeMean"`
	SizeP50  int     `json:"sizeP50"`
	SizeP95  int     `json:"sizeP95"`
	SizeMax  int     `json:"sizeMax"`
	// The edges of a sample of the inserts of the stretch since the last
	// checkpoint, counted when each insert wrote them against the live
	// vectors of that moment: the guard of the edge builder.
	AtInsert edgeCounts `json:"atInsert"`
	// The edges of a sample of the inserted vectors and of one of the
	// vectors of the batch half, counted now against the live vectors now:
	// how stale they are.
	Inserted edgeCounts `json:"inserted"`
	Batch    edgeCounts `json:"batch"`
	Coverage string     `json:"coverage"`
	// TruthFallbacks counts the queries whose shipped top neighbors held too
	// few live ones, and whose ground truth was brute-forced.
	TruthFallbacks int          `json:"truthFallbacks"`
	Stats          streamStats  `json:"stats"`
	InsertSec      float64      `json:"insertSec"`
	Points         []benchPoint `json:"points"`
}

type streamResult struct {
	Cell        string             `json:"cell"`
	Order       string             `json:"order"`
	Threshold   int                `json:"threshold"`
	InsertEF    int                `json:"insertEF"`
	Dataset     string             `json:"dataset"`
	Vectors     int                `json:"vectors"`
	Queries     int                `json:"queries"`
	Config      map[string]any     `json:"config"`
	Checkpoints []streamCheckpoint `json:"checkpoints"`
	Stopped     string             `json:"stopped,omitempty"`
}

// TestStream runs the streaming experiment of v2.0 (bench-report.md, "v2.0:
// streaming en memoria") on the dataset of -bench.dataset: a batch build over
// half of the train set, in the order of -bench.streamorder, and the other
// half inserted by the incremental path, measured untimed at every
// checkpoint. -bench.stream calibrate only assigns and splits, for every
// threshold of -bench.streamthresholds, and reports the posting sizes it
// ends with.
func TestStream(t *testing.T) {
	if *benchDatasetDir == "" || *benchStream == "" {
		t.Skip("no -bench.dataset and -bench.stream given")
	}
	require.NotEmpty(t, *benchOut, "-bench.stream writes its results next to -bench.out")
	ctx := context.Background()
	data := loadBenchData(t, *benchDatasetDir, *benchLimit, *benchQueries, *benchK, false)
	if *benchLimit > 0 {
		// The shipped ground truth ranks the whole corpus; a truncated one
		// gets its own, as deep.
		data.neighbors = bruteNeighbors(data, streamTruthDepth)
	}
	require.NotNil(t, data.neighbors, "the live ground truth is drawn from the shipped one")
	n := len(data.train)
	order := make([]uint64, n)
	for i := range order {
		order[i] = uint64(i)
	}
	switch *benchStreamOrder {
	case "natural":
	case "shuffled":
		rand.New(rand.NewPCG(1, 1)).Shuffle(n, func(i, j int) { order[i], order[j] = order[j], order[i] })
	default:
		t.Fatalf("unknown -bench.streamorder %q", *benchStreamOrder)
	}
	half := n / 2
	base := strings.TrimSuffix(*benchOut, ".json")
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	uc := NewDefaultUserConfig()
	uc.TargetPostingSize = *benchPosting
	uc.CodeBits = *benchCodeBits
	uc.BuildProbe = *benchProbe
	uc.RescueLimit = *benchRescue
	uc.TrainingLimit = *benchTraining
	config := map[string]any{
		"targetPostingSize": uc.TargetPostingSize, "buildProbe": uc.BuildProbe, "rescueLimit": uc.RescueLimit,
		"edges": uc.Edges, "centroidEF": centroidEF, "rescoreLimit": uc.RescoreLimit,
	}
	_, tempVectorForID := benchThunks(data)
	build := func(ids []uint64) *Index {
		begin := time.Now()
		index, err := New(Config{
			ID:                           "stream",
			RootPath:                     t.TempDir(),
			Logger:                       logger,
			DistanceProvider:             distancer.NewCosineDistanceProvider(),
			TempVectorForIDWithViewThunk: tempVectorForID,
			GetViewThunk:                 func() common.BucketView { return noopBucketView{} },
			Seed:                         1,
		}, uc)
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
		t.Logf("built %d vectors in %s", len(ids), time.Since(begin).Round(time.Second))
		return index
	}
	vectorOf := func(id uint64) []float32 { return data.train[id] }

	if *benchStream == "calibrate" {
		calibrateStream(t, ctx, data, order, half, build, vectorOf, base)
		return
	}

	cfg := streamConfig{threshold: *benchStreamThreshold, seed: 1}
	switch *benchStream {
	case "principal":
	case "h1":
		cfg.inverse = true
	case "h2":
		cfg.repair = true
	case "both":
		cfg.inverse, cfg.repair = true, true
	default:
		t.Fatalf("unknown -bench.stream %q", *benchStream)
	}
	require.Positive(t, cfg.threshold, "a cell needs -bench.streamthreshold")
	result := &streamResult{
		Cell: *benchStream, Order: *benchStreamOrder, Threshold: cfg.threshold, InsertEF: *benchStreamInsertEF,
		Dataset: data.name, Vectors: n, Queries: len(data.queries), Config: config,
	}
	write := func() {
		raw, err := json.MarshalIndent(result, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(base+".json", raw, 0o644))
	}
	grid := []sweep{{
		efs: streamGrid, threads: []int{1}, rescores: []int{uc.RescoreLimit}, batches: []int{DefaultLoadBatch},
		margins: []float64{DefaultCosineSourceMargin}, untimed: true,
	}}
	gridUC := uc

	var reference *Index
	if *benchStreamReference {
		all := slices.Clone(order)
		slices.Sort(all)
		reference = build(all)
		data.truth = liveTruth(t, data, nil, *benchK)
		refResult := &benchResult{Arm: "postingedge", Dataset: data.name, Vectors: n, Queries: len(data.queries), K: *benchK, Config: config}
		runPostingEdgePlan(t, reference, data, gridUC, grid, refResult, base+"-reference.json")
	}

	s, err := newStreamIndex(build(order[:half]), n, cfg, vectorOf)
	require.NoError(t, err)
	insertUC := uc
	insertUC.EF = *benchStreamInsertEF
	rng := rand.New(rand.NewPCG(2, 2))
	batchSample := sampleOf(rng, order[:half], *benchStreamSample)

	measure := func(percent, upto int, insertSec float64, atInsert edgeCounts) {
		cp := streamCheckpoint{Percent: percent, Live: s.liveCount, Stats: s.stats, InsertSec: insertSec, AtInsert: atInsert}
		if err := s.cleanCentroidGraph(); err != nil {
			t.Logf("checkpoint %d%%: cleaning the centroid graph failed, measured with its tombstones: %v", percent, err)
		}
		cp.Coverage = "complete"
		if err := s.checkCoverage(); err != nil {
			cp.Coverage = err.Error()
			result.Checkpoints = append(result.Checkpoints, cp)
			result.Stopped = fmt.Sprintf("coverage at %d%%: %v", percent, err)
			write()
			t.Fatalf("stop and report: %s", result.Stopped)
		}
		sizes := s.postingSizes()
		slices.Sort(sizes)
		cp.Postings, cp.SizeMax = len(sizes), sizes[len(sizes)-1]
		cp.SizeP50, cp.SizeP95 = sizes[len(sizes)/2], sizes[len(sizes)*95/100]
		cp.SizeMean = float64(s.liveCount) / float64(len(sizes))

		live := make([]bool, n)
		copy(live, s.live)
		var fallbacks int
		data.truth, fallbacks = liveTruthOf(t, data, live, *benchK)
		cp.TruthFallbacks = fallbacks
		cpResult := &benchResult{Arm: "postingedge", Dataset: data.name, Vectors: s.liveCount, Queries: len(data.queries), K: *benchK, Config: config}
		runPostingEdgePlan(t, s.ix, data, gridUC, grid, cpResult, fmt.Sprintf("%s-cp%d.json", base, percent))
		cp.Points = cpResult.Points

		cp.Batch, err = s.edgeRecall(ctx, logger, batchSample)
		require.NoError(t, err)
		if upto > half {
			inserted := sampleOf(rand.New(rand.NewPCG(3, uint64(percent))), order[half:upto], *benchStreamSample)
			cp.Inserted, err = s.edgeRecall(ctx, logger, inserted)
			require.NoError(t, err)
		}
		result.Checkpoints = append(result.Checkpoints, cp)
		write()
		perInsert := 0.0
		if s.stats.Inserted > 0 {
			perInsert = float64(insertSec) * 1e6 / float64(s.stats.Inserted)
		}
		t.Logf("checkpoint %d%%: %d live in %d postings (mean %.1f, p50 %d, p95 %d, max %d) | edge recall at insert %.4f "+
			"(postings %.4f, %d sampled), now: inserted %.4f (postings %.4f, degree %.2f), batch %.4f (postings %.4f, degree %.2f) "+
			"| splits %d (failed %d), non-nearest %.4f, inverse edges %d, repaired %d | %.0f us per insert",
			percent, cp.Live, cp.Postings, cp.SizeMean, cp.SizeP50, cp.SizeP95, cp.SizeMax,
			cp.AtInsert.recall(), cp.AtInsert.postingRecall(), cp.AtInsert.Vectors,
			cp.Inserted.recall(), cp.Inserted.postingRecall(), cp.Inserted.degree(),
			cp.Batch.recall(), cp.Batch.postingRecall(), cp.Batch.degree(), s.stats.Splits, s.stats.SplitsFailed,
			float64(s.stats.NonNearest)/float64(max(s.stats.Inserted, 1)), s.stats.InverseEdges, s.stats.Repaired, perInsert)
		if atInsert.Vectors > 0 && atInsert.recall() < *benchStreamMinEdgeRecall {
			result.Stopped = fmt.Sprintf("edge recall at insert %.4f in the stretch to %d%%", atInsert.recall(), percent)
			write()
			t.Fatalf("stop and report: %s", result.Stopped)
		}
	}

	measure(50, half, 0, edgeCounts{})
	inserted, insertTime := half, time.Duration(0)
	for i, percent := range parseInts(t, *benchStreamCheckpoints) {
		upto := n * percent / 100
		// The guard counts a sample of the stretch's inserts as each is
		// written; what it takes is kept out of the insert time.
		sampled := map[uint64]bool{}
		for _, id := range sampleOf(rand.New(rand.NewPCG(4, uint64(i))), order[inserted:upto], *benchStreamSample) {
			sampled[id] = true
		}
		var atInsert edgeCounts
		var guardTime time.Duration
		s.onWrite = func(docID uint64, p uint32, edges []uint32) error {
			if !sampled[docID] {
				return nil
			}
			clock := time.Now()
			c, err := s.edgeRecallAtInsert(ctx, logger, docID, p, edges)
			atInsert.add(c)
			guardTime += time.Since(clock)
			return err
		}
		require.NoError(t, s.ix.UpdateUserConfig(insertUC, func() {}))
		begin := time.Now()
		for _, id := range order[inserted:upto] {
			require.NoError(t, s.insert(ctx, id, data.train[id]))
		}
		insertTime += time.Since(begin) - guardTime
		s.onWrite = nil
		inserted = upto
		measure(percent, upto, insertTime.Seconds(), atInsert)
	}

	if reference == nil {
		return
	}
	// The timed frontiers, in the same process: the reference as it is, the
	// reference converted to doc id keys, which tells the representation
	// from the maintenance, and the cell.
	require.Equal(t, n, s.liveCount, "the frontiers are timed over the whole train set")
	data.truth = liveTruth(t, data, nil, *benchK)
	plan := benchSweepPlan(t)
	timed := func(index *Index, name string) {
		res := &benchResult{Arm: "postingedge", Dataset: data.name, Vectors: n, Queries: len(data.queries), K: *benchK, Config: config}
		runPostingEdgePlan(t, index, data, uc, plan, res, fmt.Sprintf("%s-timed-%s.json", base, name))
	}
	timed(reference, "reference")
	_, err = newStreamIndex(reference, n, streamConfig{threshold: n, seed: 1}, vectorOf)
	require.NoError(t, err)
	timed(reference, "reference-docid")
	timed(s.ix, "cell")
}

// calibrateStream runs the assignment and the split alone, without edges or
// searches, for every threshold, from a batch build of its own, and reports
// the posting sizes each ends with.
func calibrateStream(t *testing.T, ctx context.Context, data *benchData, order []uint64, half int,
	build func([]uint64) *Index, vectorOf func(uint64) []float32, base string,
) {
	n := len(data.train)
	type calibration struct {
		Threshold    int     `json:"threshold"`
		BatchMean    float64 `json:"batchMean"`
		Postings     int     `json:"postings"`
		SizeMean     float64 `json:"sizeMean"`
		SizeP50      int     `json:"sizeP50"`
		SizeP95      int     `json:"sizeP95"`
		SizeMax      int     `json:"sizeMax"`
		Splits       int     `json:"splits"`
		SplitsFailed int     `json:"splitsFailed"`
		NonNearest   float64 `json:"nonNearest"`
		Seconds      float64 `json:"seconds"`
	}
	var out []calibration
	for _, threshold := range parseInts(t, *benchStreamThresholds) {
		s, err := newStreamIndex(build(order[:half]), n, streamConfig{threshold: threshold, assignOnly: true, seed: 1}, vectorOf)
		require.NoError(t, err)
		c := calibration{Threshold: threshold, BatchMean: float64(s.liveCount) / float64(len(s.postingSizes()))}
		begin := time.Now()
		for _, id := range order[half:] {
			require.NoError(t, s.insert(ctx, id, data.train[id]))
		}
		c.Seconds = time.Since(begin).Seconds()
		require.NoError(t, s.checkCoverage())
		sizes := s.postingSizes()
		slices.Sort(sizes)
		c.Postings, c.SizeMax = len(sizes), sizes[len(sizes)-1]
		c.SizeP50, c.SizeP95 = sizes[len(sizes)/2], sizes[len(sizes)*95/100]
		c.SizeMean = float64(s.liveCount) / float64(len(sizes))
		c.Splits, c.SplitsFailed = s.stats.Splits, s.stats.SplitsFailed
		c.NonNearest = float64(s.stats.NonNearest) / float64(max(s.stats.Inserted, 1))
		out = append(out, c)
		t.Logf("calibration: threshold %d: batch mean %.1f, %d postings of mean %.1f (p50 %d, p95 %d, max %d), %d splits (%d failed), "+
			"non-nearest %.4f, %.0f s", threshold, c.BatchMean, c.Postings, c.SizeMean, c.SizeP50, c.SizeP95, c.SizeMax,
			c.Splits, c.SplitsFailed, c.NonNearest, c.Seconds)
		raw, err := json.MarshalIndent(out, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(base+".json", raw, 0o644))
		require.NoError(t, s.ix.Shutdown(ctx))
	}
}

// sampleOf draws up to size of the doc ids, without replacement.
func sampleOf(rng *rand.Rand, docIDs []uint64, size int) []uint64 {
	picked := slices.Clone(docIDs)
	rng.Shuffle(len(picked), func(i, j int) { picked[i], picked[j] = picked[j], picked[i] })
	return picked[:min(size, len(picked))]
}

// liveTruth is liveTruthOf over the whole train set, nil live.
func liveTruth(t *testing.T, data *benchData, live []bool, k int) [][]uint64 {
	truth, fallbacks := liveTruthOf(t, data, live, k)
	require.Zero(t, fallbacks, "the shipped ground truth holds the whole train set")
	return truth
}

// liveTruthOf returns the k nearest live train vectors of every query: the
// first k live ones of the shipped ground truth, which ranks the whole train
// set, or, for a query whose shipped neighbors hold fewer than k live ones,
// a brute force over the live vectors. A nil live is the whole train set.
func liveTruthOf(t *testing.T, data *benchData, live []bool, k int) ([][]uint64, int) {
	truth := make([][]uint64, len(data.queries))
	var brute []int
	for i, shipped := range data.neighbors {
		for _, id := range shipped {
			if live == nil || live[id] {
				truth[i] = append(truth[i], id)
				if len(truth[i]) == k {
					break
				}
			}
		}
		if len(truth[i]) < k {
			brute = append(brute, i)
		}
	}
	provider := distancer.NewCosineDistanceProvider()
	parallelFor(len(brute), func(b int) {
		i := brute[b]
		var nearest topK
		nearest.reset(k)
		for id, vec := range data.train {
			if live != nil && !live[id] {
				continue
			}
			dist, err := provider.SingleDist(data.queries[i], vec)
			if err != nil {
				panic(err)
			}
			nearest.push(uint64(id), dist)
		}
		truth[i] = truth[i][:0]
		for _, e := range nearest.sorted() {
			truth[i] = append(truth[i], e.id)
		}
	})
	return truth, len(brute)
}

// bruteNeighbors ranks the train set for every query, the depth nearest first.
func bruteNeighbors(data *benchData, depth int) [][]uint64 {
	out := make([][]uint64, len(data.queries))
	provider := distancer.NewCosineDistanceProvider()
	parallelFor(len(data.queries), func(i int) {
		var nearest topK
		nearest.reset(depth)
		for id, vec := range data.train {
			dist, err := provider.SingleDist(data.queries[i], vec)
			if err != nil {
				panic(err)
			}
			nearest.push(uint64(id), dist)
		}
		for _, e := range nearest.sorted() {
			out[i] = append(out[i], e.id)
		}
	})
	return out
}
