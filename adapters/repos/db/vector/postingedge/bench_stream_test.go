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
	"cmp"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

var (
	benchStream = flag.String("bench.stream", "", "the streaming experiment of v2.0 (TestStream): calibrate, buildwork "+
		"(a batch build over the whole train set, for the counts of its work alone, v2.2), or a cell: "+
		"principal, h1 (inverse edges), h2 (edge repair), both, siblings (cell 5, sibling repair at a split), or "+
		"inverse-siblings (cell 6, inverse edges with sibling repair)")
	benchStreamStart = flag.Int("bench.streamstart", 50, "with -bench.stream: the share of the train set, in percent, "+
		"the batch build takes; the rest is inserted (v2.2)")
	benchStreamOrder = flag.String("bench.streamorder", "shuffled", "with -bench.stream: the order the train set is "+
		"built and inserted in, shuffled (seed 1) or natural")
	benchStreamThreshold  = flag.Int("bench.streamthreshold", 0, "with a -bench.stream cell: the size past which a posting splits")
	benchStreamThresholds = flag.String("bench.streamthresholds", "280,320,360,400,472", "with -bench.stream calibrate: "+
		"the thresholds to try")
	benchStreamInsertEF    = flag.Int("bench.streaminsertef", 320, "with -bench.stream: the ef of the search for a new vector's edges")
	benchStreamCheckpoints = flag.String("bench.streamcheckpoints", "60,70,80,90,100", "with -bench.stream: the shares of "+
		"the train set, in percent, measured after the batch part")
	benchStreamGuards = flag.String("bench.streamguards", "", "with a -bench.stream cell: the shares of the train set, in "+
		"percent, where only the guard of the edge builder and the coverage are checked and the counts kept, without the "+
		"measurements of a checkpoint (v2.1)")
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
	TruthFallbacks int `json:"truthFallbacks"`
	// Cohorts counts the edges of all the live vectors, not of a sample, by
	// when they entered the index: the batch part first, and then the
	// inserts of each stretch between stops so far.
	Cohorts   []cohortDegree `json:"cohorts,omitempty"`
	Stats     streamStats    `json:"stats"`
	InsertSec float64        `json:"insertSec"`
	Points    []benchPoint   `json:"points"`
}

// streamGuard is what a cell keeps at a share of the train set where it only
// checks the guard of the edge builder and the coverage: the edges of the
// sample of the stretch's inserts, as streamCheckpoint.AtInsert, and the
// counts so far.
type streamGuard struct {
	Percent   int         `json:"percent"`
	Live      int         `json:"live"`
	AtInsert  edgeCounts  `json:"atInsert"`
	Coverage  string      `json:"coverage"`
	Stats     streamStats `json:"stats"`
	InsertSec float64     `json:"insertSec"`
}

type streamResult struct {
	Cell      string `json:"cell"`
	Order     string `json:"order"`
	Threshold int    `json:"threshold"`
	InsertEF  int    `json:"insertEF"`
	// Start is the share of the train set, in percent, the batch build took.
	Start   int            `json:"start"`
	Dataset string         `json:"dataset"`
	Vectors int            `json:"vectors"`
	Queries int            `json:"queries"`
	Config  map[string]any `json:"config"`
	// BatchBuild is the work of the batch build the cell starts from.
	BatchBuild  *buildWork         `json:"batchBuild,omitempty"`
	Checkpoints []streamCheckpoint `json:"checkpoints"`
	Guards      []streamGuard      `json:"guards,omitempty"`
	Stopped     string             `json:"stopped,omitempty"`
}

// buildWork is the work of a batch build, in counts, and the postings it
// leaves.
type buildWork struct {
	Vectors  int `json:"vectors"`
	Postings int `json:"postings"`
	Edges    int `json:"edges"`
	// The live postings' sizes.
	SizeMean float64 `json:"sizeMean"`
	SizeP50  int     `json:"sizeP50"`
	SizeP95  int     `json:"sizeP95"`
	SizeMax  int     `json:"sizeMax"`
	// What the edge builder took: the postings it read candidates from, the
	// candidates it ranked by code and those it compared at full precision.
	PostingsProbed     int64 `json:"postingsProbed"`
	CandidatesScored   int64 `json:"candidatesScored"`
	CandidatesRescored int64 `json:"candidatesRescored"`
	// The full-precision distances of the other stages: the 2-means of the
	// partition, the exact assignment, and the build of the centroid graph.
	PartitionDistances     int64 `json:"partitionDistances"`
	AssignDistances        int64 `json:"assignDistances"`
	CentroidGraphDistances int64 `json:"centroidGraphDistances"`
	// Seconds is indicative: other builds may run at once.
	Seconds float64 `json:"seconds"`
}

// workOfBuild reads the work of the batch build of an index that is still as
// the build left it. graphDistances is what its centroid graph has computed.
func workOfBuild(t *testing.T, index *Index, graphDistances int64) *buildWork {
	stats, ok := index.BuildStats()
	require.True(t, ok)
	store := index.state.Load().postings
	require.Nil(t, store.lists, "the index is as the batch build left it")
	sizes := make([]int, store.postings())
	for p := range sizes {
		sizes[p] = int(store.start[p+1] - store.start[p])
	}
	slices.Sort(sizes)
	return &buildWork{
		Vectors: stats.Vectors, Postings: stats.Postings, Edges: stats.Edges,
		SizeMean: float64(stats.Vectors) / float64(len(sizes)), SizeP50: sizes[len(sizes)/2],
		SizeP95: sizes[len(sizes)*95/100], SizeMax: sizes[len(sizes)-1],
		PostingsProbed: stats.PostingsProbed, CandidatesScored: stats.CandidatesScored,
		CandidatesRescored: stats.CandidatesRescored, PartitionDistances: stats.PartitionDistances,
		AssignDistances: stats.AssignDistances, CentroidGraphDistances: graphDistances,
		Seconds: stats.TotalTime.Seconds(),
	}
}

// streamStop is a share of the train set where the inserts pause: for a
// checkpoint, or for the guard alone.
type streamStop struct {
	percent    int
	checkpoint bool
}

// streamStops merges the checkpoints and the guards of a cell, in order. They
// come after the start, the share the batch build took. A share is one or the
// other, and the last one is a checkpoint: the stretch that ends a cell is
// measured.
func streamStops(start int, checkpoints, guards []int) ([]streamStop, error) {
	stops := make([]streamStop, 0, len(checkpoints)+len(guards))
	for _, percent := range checkpoints {
		stops = append(stops, streamStop{percent: percent, checkpoint: true})
	}
	for _, percent := range guards {
		stops = append(stops, streamStop{percent: percent})
	}
	slices.SortStableFunc(stops, func(a, b streamStop) int { return cmp.Compare(a.percent, b.percent) })
	for i, stop := range stops {
		switch {
		case stop.percent <= start || stop.percent > 100:
			return nil, errors.Errorf("a stop at %d %% is outside the inserted part, past %d %%", stop.percent, start)
		case i > 0 && stops[i-1].percent == stop.percent:
			return nil, errors.Errorf("%d %% is given twice", stop.percent)
		}
	}
	if len(stops) == 0 || !stops[len(stops)-1].checkpoint {
		return nil, errors.New("the last stop of a cell is a checkpoint")
	}
	return stops, nil
}

// TestStream runs the streaming experiment of v2.0 (bench-report.md, "v2.0:
// streaming en memoria") on the dataset of -bench.dataset: a batch build over
// the first -bench.streamstart percent of the train set, half of it by
// default, in the order of -bench.streamorder, and the rest inserted by the
// incremental path, measured untimed at every checkpoint. -bench.stream
// calibrate only assigns and splits, for every threshold of
// -bench.streamthresholds, and reports the posting sizes it ends with.
// -bench.stream buildwork builds the whole train set in batch, as the
// reference of a cell is built, and writes the counts of its work alone.
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
	require.True(t, *benchStreamStart > 0 && *benchStreamStart < 100, "-bench.streamstart is a share of the train set")
	built := n * *benchStreamStart / 100
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
	// build builds the ids in batch. With a count, the centroid graph of the
	// index adds every distance it computes to it, from its build on.
	build := func(ids []uint64, graphDistances *atomic.Int64) *Index {
		begin := time.Now()
		cfg := Config{
			ID:                           "stream",
			RootPath:                     t.TempDir(),
			Logger:                       logger,
			DistanceProvider:             distancer.NewCosineDistanceProvider(),
			TempVectorForIDWithViewThunk: tempVectorForID,
			GetViewThunk:                 func() common.BucketView { return noopBucketView{} },
			Seed:                         1,
		}
		if graphDistances != nil {
			cfg.centroidProvider = countingProvider{Provider: distancer.NewL2SquaredProvider(), count: graphDistances}
		}
		index, err := New(cfg, uc)
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

	writeJSON := func(path string, doc any) {
		raw, err := json.MarshalIndent(doc, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, raw, 0o644))
	}
	// reference builds the whole train set in batch, in its natural order:
	// what a cell is compared with.
	reference := func(graphDistances *atomic.Int64) *Index {
		all := slices.Clone(order)
		slices.Sort(all)
		return build(all, graphDistances)
	}

	switch *benchStream {
	case "calibrate":
		calibrateStream(t, ctx, data, order, built, func(ids []uint64) *Index { return build(ids, nil) }, vectorOf, base)
		return
	case "buildwork":
		graphDistances := new(atomic.Int64)
		index := reference(graphDistances)
		work := workOfBuild(t, index, graphDistances.Load())
		writeJSON(base+".json", work)
		t.Logf("build work: %d vectors in %d postings (mean %.1f, p50 %d, p95 %d, max %d) | %d postings probed, "+
			"%d candidates scored, %d rescored | distances: partition %d, assignment %d, centroid graph %d",
			work.Vectors, work.Postings, work.SizeMean, work.SizeP50, work.SizeP95, work.SizeMax, work.PostingsProbed,
			work.CandidatesScored, work.CandidatesRescored, work.PartitionDistances, work.AssignDistances,
			work.CentroidGraphDistances)
		require.NoError(t, index.Shutdown(ctx))
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
	case "siblings":
		cfg.siblings = true
	case "inverse-siblings":
		cfg.inverse, cfg.siblings = true, true
	default:
		t.Fatalf("unknown -bench.stream %q", *benchStream)
	}
	require.Positive(t, cfg.threshold, "a cell needs -bench.streamthreshold")
	result := &streamResult{
		Cell: *benchStream, Order: *benchStreamOrder, Threshold: cfg.threshold, InsertEF: *benchStreamInsertEF,
		Start: *benchStreamStart, Dataset: data.name, Vectors: n, Queries: len(data.queries), Config: config,
	}
	write := func() { writeJSON(base+".json", result) }
	grid := []sweep{{
		efs: streamGrid, threads: []int{1}, rescores: []int{uc.RescoreLimit}, batches: []int{DefaultLoadBatch},
		margins: []float64{DefaultCosineSourceMargin}, untimed: true,
	}}
	gridUC := uc

	var batchReference *Index
	if *benchStreamReference {
		batchReference = reference(nil)
		data.truth = liveTruth(t, data, nil, *benchK)
		refResult := &benchResult{Arm: "postingedge", Dataset: data.name, Vectors: n, Queries: len(data.queries), K: *benchK, Config: config}
		runPostingEdgePlan(t, batchReference, data, gridUC, grid, refResult, base+"-reference.json")
	}

	// The centroid graph of the cell counts its distances: the inserts tell
	// what it took for each of their steps.
	cfg.centroidDistances = new(atomic.Int64)
	batch := build(order[:built], cfg.centroidDistances)
	result.BatchBuild = workOfBuild(t, batch, cfg.centroidDistances.Load())
	s, err := newStreamIndex(batch, n, cfg, vectorOf)
	require.NoError(t, err)
	insertUC := uc
	insertUC.EF = *benchStreamInsertEF
	rng := rand.New(rand.NewPCG(2, 2))
	batchSample := sampleOf(rng, order[:built], *benchStreamSample)
	// The place of every doc id in the order, and the places where the
	// cohorts of a checkpoint part it: the batch part, and every stop so far.
	rank := make([]uint32, n)
	for place, id := range order {
		rank[id] = uint32(place)
	}
	cohortBounds := []int{0, built}

	measure := func(percent, upto int, insertSec float64, atInsert edgeCounts) {
		cp := streamCheckpoint{Percent: percent, Live: s.liveCount, InsertSec: insertSec, AtInsert: atInsert}
		if err := s.cleanCentroidGraph(); err != nil {
			t.Logf("checkpoint %d%%: cleaning the centroid graph failed, measured with its tombstones: %v", percent, err)
		}
		// With what the cleanup took.
		cp.Stats = s.stats.clone()
		cp.Cohorts = s.degreeByCohort(rank, cohortBounds)
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
		if upto > built {
			inserted := sampleOf(rand.New(rand.NewPCG(3, uint64(percent))), order[built:upto], *benchStreamSample)
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
			"| splits %d (failed %d), non-nearest %.4f, inverse edges %d, repaired %d, sibling edges %d | %.0f us per insert",
			percent, cp.Live, cp.Postings, cp.SizeMean, cp.SizeP50, cp.SizeP95, cp.SizeMax,
			cp.AtInsert.recall(), cp.AtInsert.postingRecall(), cp.AtInsert.Vectors,
			cp.Inserted.recall(), cp.Inserted.postingRecall(), cp.Inserted.degree(),
			cp.Batch.recall(), cp.Batch.postingRecall(), cp.Batch.degree(), s.stats.Splits, s.stats.SplitsFailed,
			float64(s.stats.NonNearest)/float64(max(s.stats.Inserted, 1)), s.stats.InverseEdges, s.stats.Repaired,
			s.stats.SiblingEdges, perInsert)
		logInsertSearch(t, fmt.Sprintf("checkpoint %d%%", percent), s.stats)
		for _, c := range cp.Cohorts {
			t.Logf("checkpoint %d%%: the vectors that entered at %d to %d: %d with %.2f edges each",
				percent, c.From, c.To, c.Vectors, float64(c.Edges)/float64(max(c.Vectors, 1)))
		}
		if atInsert.Vectors > 0 && atInsert.recall() < *benchStreamMinEdgeRecall {
			result.Stopped = fmt.Sprintf("edge recall at insert %.4f in the stretch to %d%%", atInsert.recall(), percent)
			write()
			t.Fatalf("stop and report: %s", result.Stopped)
		}
	}
	// guard is what a stop does that is not a checkpoint: the two stops of
	// the cell, and the counts.
	guard := func(percent int, insertSec float64, atInsert edgeCounts) {
		g := streamGuard{
			Percent: percent, Live: s.liveCount, AtInsert: atInsert, Coverage: "complete",
			Stats: s.stats.clone(), InsertSec: insertSec,
		}
		if err := s.checkCoverage(); err != nil {
			g.Coverage = err.Error()
			result.Stopped = fmt.Sprintf("coverage at %d%%: %v", percent, err)
		} else if atInsert.Vectors > 0 && atInsert.recall() < *benchStreamMinEdgeRecall {
			result.Stopped = fmt.Sprintf("edge recall at insert %.4f in the stretch to %d%%", atInsert.recall(), percent)
		}
		result.Guards = append(result.Guards, g)
		write()
		t.Logf("guard %d%%: %d live | edge recall at insert %.4f (postings %.4f, %d sampled) | splits %d (failed %d), "+
			"non-nearest %.4f, inverse edges %d | %.0f us per insert",
			percent, g.Live, atInsert.recall(), atInsert.postingRecall(), atInsert.Vectors, s.stats.Splits, s.stats.SplitsFailed,
			float64(s.stats.NonNearest)/float64(max(s.stats.Inserted, 1)), s.stats.InverseEdges,
			insertSec*1e6/float64(max(s.stats.Inserted, 1)))
		logInsertSearch(t, fmt.Sprintf("guard %d%%", percent), s.stats)
		if result.Stopped != "" {
			t.Fatalf("stop and report: %s", result.Stopped)
		}
	}

	var guards []int
	if *benchStreamGuards != "" {
		guards = parseInts(t, *benchStreamGuards)
	}
	stops, err := streamStops(*benchStreamStart, parseInts(t, *benchStreamCheckpoints), guards)
	require.NoError(t, err)
	measure(*benchStreamStart, built, 0, edgeCounts{})
	inserted, insertTime := built, time.Duration(0)
	for i, stop := range stops {
		percent := stop.percent
		upto := n * percent / 100
		cohortBounds = append(cohortBounds, upto)
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
		if stop.checkpoint {
			measure(percent, upto, insertTime.Seconds(), atInsert)
		} else {
			guard(percent, insertTime.Seconds(), atInsert)
		}
	}

	if batchReference == nil {
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
	timed(batchReference, "reference")
	_, err = newStreamIndex(batchReference, n, streamConfig{threshold: n, seed: 1}, vectorOf)
	require.NoError(t, err)
	timed(batchReference, "reference-docid")
	timed(s.ix, "cell")
}

// logInsertSearch logs the mean work of the edge search of an insert so far.
func logInsertSearch(t *testing.T, at string, stats streamStats) {
	if stats.Inserted == 0 || stats.SearchLoadsHist == nil {
		return
	}
	per := 1 / float64(stats.Inserted)
	t.Logf("%s: the search of an insert: %.1f loads, %.1f rounds, %.0f scanned, %.0f endpoints, %.1f rescored",
		at, float64(stats.SearchLoads)*per, float64(stats.SearchRounds)*per, float64(stats.SearchScanned)*per,
		float64(stats.SearchEndpoints)*per, float64(stats.SearchRescored)*per)
	t.Logf("%s: the exact distances of an insert: %.1f to find its posting, %.1f for the entry postings of its search, "+
		"%.1f for its inverse edges, %.1f in splits and %.1f in the centroid graph for them, %.1f in cleanups",
		at, float64(stats.AssignDistances)*per, float64(stats.SearchCentroidDistances)*per,
		float64(stats.InverseDistances)*per, float64(stats.SplitDistances)*per, float64(stats.SplitGraphDistances)*per,
		float64(stats.CleanDistances)*per)
}

// calibrateStream runs the assignment and the split alone, without edges or
// searches, for every threshold, from a batch build of its own, and reports
// the posting sizes each ends with.
func calibrateStream(t *testing.T, ctx context.Context, data *benchData, order []uint64, built int,
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
		s, err := newStreamIndex(build(order[:built]), n, streamConfig{threshold: threshold, assignOnly: true, seed: 1}, vectorOf)
		require.NoError(t, err)
		c := calibration{Threshold: threshold, BatchMean: float64(s.liveCount) / float64(len(s.postingSizes()))}
		begin := time.Now()
		for _, id := range order[built:] {
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

func TestStreamStops(t *testing.T) {
	tests := []struct {
		name        string
		start       int
		checkpoints []int
		guards      []int
		want        []streamStop
		wantErr     string
	}{
		{
			name:        "the checkpoints of v2.0",
			start:       50,
			checkpoints: []int{60, 70, 80, 90, 100},
			want:        []streamStop{{60, true}, {70, true}, {80, true}, {90, true}, {100, true}},
		},
		{
			name:        "one checkpoint behind the guards of v2.1",
			start:       50,
			checkpoints: []int{100},
			guards:      []int{60, 70, 80, 90},
			want:        []streamStop{{60, false}, {70, false}, {80, false}, {90, false}, {100, true}},
		},
		{
			name:        "the checkpoints of v2.2, from a tenth",
			start:       10,
			checkpoints: []int{20, 40, 60, 80, 100},
			want:        []streamStop{{20, true}, {40, true}, {60, true}, {80, true}, {100, true}},
		},
		{
			name:        "in any order",
			start:       50,
			checkpoints: []int{100, 70},
			guards:      []int{90, 60},
			want:        []streamStop{{60, false}, {70, true}, {90, false}, {100, true}},
		},
		{name: "a share twice", start: 50, checkpoints: []int{80, 100}, guards: []int{80}, wantErr: "80 % is given twice"},
		{name: "a guard at the end", start: 50, checkpoints: []int{90}, guards: []int{100}, wantErr: "the last stop of a cell is a checkpoint"},
		{name: "at the start", start: 50, checkpoints: []int{100}, guards: []int{50}, wantErr: "outside the inserted part, past 50 %"},
		{name: "inside the batch part", start: 10, checkpoints: []int{5, 100}, wantErr: "a stop at 5 % is outside the inserted part, past 10 %"},
		{name: "past the train set", start: 50, checkpoints: []int{110}, wantErr: "outside the inserted part"},
		{name: "nothing", start: 50, wantErr: "the last stop of a cell is a checkpoint"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := streamStops(tt.start, tt.checkpoints, tt.guards)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
