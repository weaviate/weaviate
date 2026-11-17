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
	"os"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/concurrency"
)

var (
	benchHalo = flag.String("bench.halo", "", "the halo experiment of v3.0 (TestHalo): the widths, in bits, of the "+
		"sketches the endpoints of the edges are ranked with, each a cell next to the reference that ranks them with "+
		"the full code, e.g. 256,384,512")
	benchHaloEFs = flag.String("bench.haloefs", "10,20,40,80,160,320,640,1024,2048", "with -bench.halo: the ef of the "+
		"searches of every cell")
	benchHaloGate    = flag.Float64("bench.halogate", 0.99, "with -bench.halo: the recall a cell has to reach by raising ef")
	benchHaloRescore = flag.Int("bench.halorescore", 200, "with -bench.halo: the rescore limit a cell that does not "+
		"reach the gate with the index's own is swept with again")
)

// haloSpread is how a count spreads over the postings.
type haloSpread struct {
	Mean float64 `json:"mean"`
	P50  int     `json:"p50"`
	P95  int     `json:"p95"`
	Max  int     `json:"max"`
}

// haloLayout is what the postings of the index hold, in the units of their
// blocks on disk: members, edges, and the distinct endpoints of those edges,
// which are the entries of a halo. Counts has the three of every posting.
type haloLayout struct {
	Postings int        `json:"postings"`
	Members  haloSpread `json:"members"`
	Edges    haloSpread `json:"edges"`
	Distinct haloSpread `json:"distinct"`
	Counts   [][3]int   `json:"counts"`
}

// haloPoint is one search configuration of a cell, over all the queries: its
// recall, and per query the postings it loads, what it would read of each
// block of them, and its work.
type haloPoint struct {
	EF           int     `json:"ef"`
	RescoreLimit int     `json:"rescoreLimit"`
	Recall       float64 `json:"recall"`
	LoadsMean    float64 `json:"loadsMean"`
	LoadsP95     float64 `json:"loadsP95"`
	RoundsMean   float64 `json:"roundsMean"`
	// MembersMean, EdgesMean and EntriesMean count the members, the edges
	// and the halo entries of the loaded postings.
	MembersMean   float64 `json:"membersMean"`
	EdgesMean     float64 `json:"edgesMean"`
	EntriesMean   float64 `json:"entriesMean"`
	EntriesP95    float64 `json:"entriesP95"`
	EndpointsMean float64 `json:"endpointsMean"`
	RescoredMean  float64 `json:"rescoredMean"`
}

// haloCell is the index searched with the endpoints of its edges ranked by
// one code: the full one, which is the reference, or a sketch.
type haloCell struct {
	// RoutingBits is the width of the sketch, 0 for the full code.
	RoutingBits int `json:"routingBits"`
	// CodeBytes is what the code of one endpoint takes, with its header.
	CodeBytes int         `json:"codeBytes"`
	Points    []haloPoint `json:"points"`
	// Fallback is the cell swept again with FallbackRescore, when it does
	// not reach the gate with the rescore limit of the index.
	FallbackRescore int         `json:"fallbackRescore,omitempty"`
	Fallback        []haloPoint `json:"fallback,omitempty"`
}

type haloResult struct {
	Dataset string         `json:"dataset"`
	Vectors int            `json:"vectors"`
	Queries int            `json:"queries"`
	K       int            `json:"k"`
	Dims    int            `json:"dims"`
	Config  map[string]any `json:"config"`
	Gate    float64        `json:"gate"`
	Build   *buildWork     `json:"build"`
	// FullCodeBytes is what the code the scan reads takes, with its header.
	FullCodeBytes int        `json:"fullCodeBytes"`
	Layout        haloLayout `json:"layout"`
	Cells         []haloCell `json:"cells"`
}

func spreadOfCounts(counts []int) haloSpread {
	sorted := slices.Clone(counts)
	slices.Sort(sorted)
	sum := 0
	for _, c := range sorted {
		sum += c
	}
	return haloSpread{
		Mean: float64(sum) / float64(len(sorted)), P50: sorted[len(sorted)/2],
		P95: sorted[len(sorted)*95/100], Max: sorted[len(sorted)-1],
	}
}

// TestHalo runs the halo experiment of v3.0 (bench-report.md, "v3.0: punto de
// operación del halo") on the dataset of -bench.dataset: one batch build with
// the full code and, over it, the reference, which ranks the endpoints of
// the edges with that code, and one cell per width of -bench.halo, which
// ranks them with a sketch. Every cell is searched over the ef of
// -bench.haloefs, untimed, and what each search loads is counted block by
// block.
func TestHalo(t *testing.T) {
	if *benchDatasetDir == "" || *benchHalo == "" {
		t.Skip("no -bench.dataset and -bench.halo given")
	}
	require.NotEmpty(t, *benchOut, "-bench.halo writes its results to -bench.out")
	ctx := context.Background()
	data := loadBenchData(t, *benchDatasetDir, *benchLimit, *benchQueries, *benchK, false)
	if *benchLimit > 0 {
		data.neighbors = bruteNeighbors(data, streamTruthDepth)
	}
	require.NotNil(t, data.neighbors, "the ground truth is drawn from the shipped one")
	data.truth = liveTruth(t, data, nil, *benchK)
	n := len(data.train)
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	uc := NewDefaultUserConfig()
	uc.TargetPostingSize = *benchPosting
	uc.BuildProbe = *benchProbe
	uc.RescueLimit = *benchRescue
	uc.TrainingLimit = *benchTraining
	uc.LoadBatch = DefaultLoadBatch
	uc.SourceMargin = DefaultCosineSourceMargin
	widths := parseInts(t, *benchHalo)
	efs := parseInts(t, *benchHaloEFs)
	result := &haloResult{
		Dataset: data.name, Vectors: n, Queries: len(data.queries), K: *benchK, Dims: data.dims, Gate: *benchHaloGate,
		Config: map[string]any{
			"targetPostingSize": uc.TargetPostingSize, "buildProbe": uc.BuildProbe, "rescueLimit": uc.RescueLimit,
			"edges": uc.Edges, "centroidEF": centroidEF, "rescoreLimit": uc.RescoreLimit, "loadBatch": uc.LoadBatch,
			"sourceMargin": uc.SourceMargin, "entryPostings": uc.EntryPostings,
		},
	}
	write := func() {
		raw, err := json.MarshalIndent(result, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(*benchOut, raw, 0o644))
	}

	begin := time.Now()
	_, tempVectorForID := benchThunks(data)
	index, err := New(Config{
		ID:                           "halo",
		RootPath:                     t.TempDir(),
		Logger:                       logger,
		DistanceProvider:             distancer.NewCosineDistanceProvider(),
		TempVectorForIDWithViewThunk: tempVectorForID,
		GetViewThunk:                 func() common.BucketView { return noopBucketView{} },
		Seed:                         1,
	}, uc)
	require.NoError(t, err)
	const chunk = 10000
	for from := 0; from < n; from += chunk {
		to := min(from+chunk, n)
		ids := make([]uint64, to-from)
		for i := range ids {
			ids[i] = uint64(from + i)
		}
		require.NoError(t, index.AddBatch(ctx, ids, data.train[from:to]))
	}
	require.NoError(t, index.Build(ctx))
	t.Logf("built %d vectors in %s", n, time.Since(begin).Round(time.Second))
	built := index.state.Load()
	result.Build = workOfBuild(t, index, 0)
	result.FullCodeBytes = built.side.codeWords * 8

	postings, err := haloPostings(built)
	require.NoError(t, err)
	members, edges, distinct := make([]int, len(postings)), make([]int, len(postings)), make([]int, len(postings))
	result.Layout.Postings = len(postings)
	for p, c := range postings {
		members[p], edges[p], distinct[p] = c.Members, c.Edges, c.Distinct
		result.Layout.Counts = append(result.Layout.Counts, [3]int{c.Members, c.Edges, c.Distinct})
	}
	result.Layout.Members, result.Layout.Edges = spreadOfCounts(members), spreadOfCounts(edges)
	result.Layout.Distinct = spreadOfCounts(distinct)
	write()
	t.Logf("layout: %d postings | members mean %.1f, p95 %d | edges mean %.1f, p95 %d | distinct endpoints mean %.1f, p95 %d "+
		"(%.3f per edge) | the full code takes %d bytes",
		len(postings), result.Layout.Members.Mean, result.Layout.Members.P95, result.Layout.Edges.Mean, result.Layout.Edges.P95,
		result.Layout.Distinct.Mean, result.Layout.Distinct.P95, result.Layout.Distinct.Mean/result.Layout.Edges.Mean,
		result.FullCodeBytes)

	// measure searches every query with one ef and rescore limit, untimed.
	searchCtx := concurrency.CtxWithBudget(ctx, 1)
	measure := func(ef, rescore int) haloPoint {
		pointUC := uc
		pointUC.EF, pointUC.RescoreLimit = ef, rescore
		require.NoError(t, index.UpdateUserConfig(pointUC, func() {}))
		reads := make([]haloRead, len(data.queries))
		var hits atomic.Int64
		err := parallelChunks(ctx, logger, len(data.queries), 8, func(_, start, end int) error {
			found := 0
			for i := start; i < end; i++ {
				ids, read, err := haloSearch(searchCtx, index, data.queries[i], *benchK, postings)
				if err != nil {
					return err
				}
				reads[i] = read
				for _, want := range data.truth[i] {
					if slices.Contains(ids, want) {
						found++
					}
				}
			}
			hits.Add(int64(found))
			return nil
		})
		require.NoError(t, err)

		point := haloPoint{EF: ef, RescoreLimit: rescore}
		total := 0
		q := float64(len(reads))
		loads, entries := make([]float64, len(reads)), make([]float64, len(reads))
		for i, read := range reads {
			total += len(data.truth[i])
			loads[i], entries[i] = float64(read.Loads), float64(read.Entries)
			point.LoadsMean += float64(read.Loads) / q
			point.RoundsMean += float64(read.Stats.Rounds) / q
			point.MembersMean += float64(read.Members) / q
			point.EdgesMean += float64(read.Edges) / q
			point.EntriesMean += float64(read.Entries) / q
			point.EndpointsMean += float64(read.Stats.EndpointsScored) / q
			point.RescoredMean += float64(read.Stats.Rescored) / q
		}
		slices.Sort(loads)
		slices.Sort(entries)
		point.LoadsP95, point.EntriesP95 = percentileOf(loads, 0.95), percentileOf(entries, 0.95)
		point.Recall = float64(hits.Load()) / float64(total)
		return point
	}
	// sweep measures a cell over the ef grid, and tells the best recall.
	sweep := func(name string, rescore int) ([]haloPoint, float64) {
		var points []haloPoint
		best := 0.0
		for _, ef := range efs {
			point := measure(ef, rescore)
			points = append(points, point)
			best = max(best, point.Recall)
			t.Logf("%s, rescore %d, ef %d: recall %.4f | %.2f loads (p95 %.0f), %.2f rounds | read: %.0f members, "+
				"%.0f edges, %.0f halo entries | %.0f endpoints scored, %.1f rescored",
				name, rescore, ef, point.Recall, point.LoadsMean, point.LoadsP95, point.RoundsMean, point.MembersMean,
				point.EdgesMean, point.EntriesMean, point.EndpointsMean, point.RescoredMean)
		}
		return points, best
	}
	// cell measures the index as its state is now.
	cell := func(name string, bits, codeBytes int) {
		c := haloCell{RoutingBits: bits, CodeBytes: codeBytes}
		var best float64
		c.Points, best = sweep(name, uc.RescoreLimit)
		if best < *benchHaloGate && *benchHaloRescore > uc.RescoreLimit {
			t.Logf("%s: the best recall is %.4f, under the gate of %.2f: swept again with rescore %d",
				name, best, *benchHaloGate, *benchHaloRescore)
			c.FallbackRescore = *benchHaloRescore
			c.Fallback, best = sweep(name, *benchHaloRescore)
		}
		if best < *benchHaloGate {
			t.Logf("%s: the best recall is %.4f: the cell does not pass the gate of %.2f", name, best, *benchHaloGate)
		}
		result.Cells = append(result.Cells, c)
		write()
	}

	cell("reference, the full code", 0, result.FullCodeBytes)
	vectorOf := func(id uint64) []float32 { return data.train[id] }
	for _, bits := range widths {
		routing, err := built.quantizer.withBits(bits)
		require.NoError(t, err)
		begin := time.Now()
		halo, err := haloState(ctx, logger, built, routing, vectorOf)
		require.NoError(t, err)
		t.Logf("sketch of %d bits: %d bytes per endpoint, encoded in %s", bits, halo.side.codeWords*8,
			time.Since(begin).Round(time.Second))
		index.state.Store(halo)
		cell(fmt.Sprintf("sketch of %d bits", bits), bits, halo.side.codeWords*8)
		index.state.Store(built)
	}
	require.NoError(t, index.Shutdown(ctx))
}
