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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
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
	benchHaloPrune = flag.String("bench.haloprune", "", "the pruned halo of v3.1 (TestHaloPrune): the widths, in bits, "+
		"of the sketches the endpoints are ranked with, each measured over every set of edges (all, one per target "+
		"posting, capped, and both) with every margin of -bench.halomargins, e.g. 384,512. The ef, the gate and the "+
		"second rescore limit are those of -bench.halo")
	benchHaloMargins = flag.String("bench.halomargins", "0.04,0.06,0.08", "with -bench.haloprune: the source margins "+
		"every set of edges is searched with")
	benchHaloCap = flag.Int("bench.halocap", 8, "with -bench.haloprune: the cap on the degree")
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

// haloBench is what the halo experiments share: the data, the index built in
// batch over it with the full code, and the searches that count what they
// load.
type haloBench struct {
	t      *testing.T
	ctx    context.Context
	logger *logrus.Logger
	data   *benchData
	uc     UserConfig
	efs    []int
	index  *Index
	// built is the state of the build, with the full code in the sidecar.
	built *state
	build time.Duration
}

// newHaloBench loads the dataset of -bench.dataset and builds it in batch
// with the build flags of the benchmarks and the search defaults.
func newHaloBench(t *testing.T, id string) *haloBench {
	ctx := context.Background()
	// With -bench.streamtrain the index takes the train set straight from
	// the dataset files, and the copy the benchmark reads is loaded once the
	// index is built: an index holds a copy of every vector while it builds,
	// and a large train set does not fit in memory twice.
	stream := *benchStreamTrain
	if stream {
		require.Zero(t, *benchLimit, "-bench.streamtrain reads the whole train set")
	}
	data := loadBenchData(t, *benchDatasetDir, *benchLimit, *benchQueries, *benchK, stream)
	if *benchLimit > 0 {
		data.neighbors = bruteNeighbors(data, streamTruthDepth)
	}
	require.NotNil(t, data.neighbors, "the ground truth is drawn from the shipped one")
	data.truth = liveTruth(t, data, nil, *benchK)
	n := data.rows
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	uc := NewDefaultUserConfig()
	uc.TargetPostingSize = *benchPosting
	uc.BuildProbe = *benchProbe
	uc.RescueLimit = *benchRescue
	uc.TrainingLimit = *benchTraining
	uc.LoadBatch = DefaultLoadBatch
	uc.SourceMargin = DefaultCosineSourceMargin

	begin := time.Now()
	_, tempVectorForID := benchThunks(data)
	index, err := New(Config{
		ID:                           id,
		RootPath:                     t.TempDir(),
		Logger:                       logger,
		DistanceProvider:             distancer.NewCosineDistanceProvider(),
		TempVectorForIDWithViewThunk: tempVectorForID,
		GetViewThunk:                 func() common.BucketView { return noopBucketView{} },
		Seed:                         1,
	}, uc)
	require.NoError(t, err)
	const chunk = 10000
	add := func(from int, vectors [][]float32) error {
		ids := make([]uint64, len(vectors))
		for i := range ids {
			ids[i] = uint64(from + i)
		}
		return index.AddBatch(ctx, ids, vectors)
	}
	if stream {
		// Unit length first, as loadBenchData leaves the train set: the
		// index then holds the same bits either way.
		require.NoError(t, testinghelpers.ReadRawTrainChunks(data.dir, chunk, func(first int, vectors [][]float32) error {
			for _, vector := range vectors {
				distancer.NormalizeInPlace(vector)
			}
			return add(first, vectors)
		}))
	} else {
		for from := 0; from < n; from += chunk {
			require.NoError(t, add(from, data.train[from:min(from+chunk, n)]))
		}
	}
	require.NoError(t, index.Build(ctx))
	if stream {
		data.loadTrain(t)
		require.Len(t, data.train, n)
	}
	b := &haloBench{
		t: t, ctx: ctx, logger: logger, data: data, uc: uc, efs: parseInts(t, *benchHaloEFs), index: index,
		built: index.state.Load(), build: time.Since(begin),
	}
	t.Logf("built %d vectors in %s", n, b.build.Round(time.Second))
	return b
}

// config is the build and the search defaults, as the results carry them.
func (b *haloBench) config() map[string]any {
	return map[string]any{
		"targetPostingSize": b.uc.TargetPostingSize, "buildProbe": b.uc.BuildProbe, "rescueLimit": b.uc.RescueLimit,
		"edges": b.uc.Edges, "centroidEF": centroidEF, "rescoreLimit": b.uc.RescoreLimit, "loadBatch": b.uc.LoadBatch,
		"sourceMargin": b.uc.SourceMargin, "entryPostings": b.uc.EntryPostings,
	}
}

// layout counts the postings of the state the index has now, in the units of
// their blocks on disk.
func (b *haloBench) layout() ([]haloPosting, haloLayout) {
	postings, err := haloPostings(b.index.state.Load())
	require.NoError(b.t, err)
	members, edges, distinct := make([]int, len(postings)), make([]int, len(postings)), make([]int, len(postings))
	layout := haloLayout{Postings: len(postings)}
	for p, c := range postings {
		members[p], edges[p], distinct[p] = c.Members, c.Edges, c.Distinct
		layout.Counts = append(layout.Counts, [3]int{c.Members, c.Edges, c.Distinct})
	}
	layout.Members, layout.Edges, layout.Distinct = spreadOfCounts(members), spreadOfCounts(edges), spreadOfCounts(distinct)
	return postings, layout
}

// measure searches every query over the state the index has now, with one
// ef, rescore limit and source margin, untimed. postings is the layout of
// that state.
func (b *haloBench) measure(postings []haloPosting, ef, rescore int, margin float32) haloPoint {
	t, data := b.t, b.data
	pointUC := b.uc
	pointUC.EF, pointUC.RescoreLimit, pointUC.SourceMargin = ef, rescore, margin
	require.NoError(t, b.index.UpdateUserConfig(pointUC, func() {}))
	searchCtx := concurrency.CtxWithBudget(b.ctx, 1)
	reads := make([]haloRead, len(data.queries))
	var hits atomic.Int64
	err := parallelChunks(b.ctx, b.logger, len(data.queries), 8, func(_, start, end int) error {
		found := 0
		for i := start; i < end; i++ {
			ids, read, err := haloSearch(searchCtx, b.index, data.queries[i], *benchK, postings)
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

// sweep measures the state of the index over the ef grid, and tells the best
// recall.
func (b *haloBench) sweep(name string, postings []haloPosting, rescore int, margin float32) ([]haloPoint, float64) {
	var points []haloPoint
	best := 0.0
	for _, ef := range b.efs {
		point := b.measure(postings, ef, rescore, margin)
		points = append(points, point)
		best = max(best, point.Recall)
		b.t.Logf("%s, rescore %d, ef %d: recall %.4f | %.2f loads (p95 %.0f), %.2f rounds | read: %.0f members, "+
			"%.0f edges, %.0f halo entries | %.0f endpoints scored, %.1f rescored",
			name, rescore, ef, point.Recall, point.LoadsMean, point.LoadsP95, point.RoundsMean, point.MembersMean,
			point.EdgesMean, point.EntriesMean, point.EndpointsMean, point.RescoredMean)
	}
	return points, best
}

// cell measures the state of the index as one cell: over the ef grid with
// the rescore limit of the index and, if it does not reach the gate, again
// with that of -bench.halorescore.
func (b *haloBench) cell(name string, postings []haloPosting, bits, codeBytes int, margin float32) haloCell {
	t := b.t
	c := haloCell{RoutingBits: bits, CodeBytes: codeBytes}
	var best float64
	c.Points, best = b.sweep(name, postings, b.uc.RescoreLimit, margin)
	if best < *benchHaloGate && *benchHaloRescore > b.uc.RescoreLimit {
		t.Logf("%s: the best recall is %.4f, under the gate of %.2f: swept again with rescore %d",
			name, best, *benchHaloGate, *benchHaloRescore)
		c.FallbackRescore = *benchHaloRescore
		c.Fallback, best = b.sweep(name, postings, *benchHaloRescore, margin)
	}
	if best < *benchHaloGate {
		t.Logf("%s: the best recall is %.4f: the cell does not pass the gate of %.2f", name, best, *benchHaloGate)
	}
	return c
}

// sketch lays over the build a state whose endpoints are ranked with a
// sketch of the given width.
func (b *haloBench) sketch(bits int) *state {
	t := b.t
	routing, err := b.built.quantizer.withBits(bits)
	require.NoError(t, err)
	begin := time.Now()
	halo, err := haloState(b.ctx, b.logger, b.built, routing, func(id uint64) []float32 { return b.data.train[id] })
	require.NoError(t, err)
	t.Logf("sketch of %d bits: %d bytes per endpoint, encoded in %s", bits, halo.side.codeWords*8,
		time.Since(begin).Round(time.Second))
	return halo
}

func writeHaloResult(t *testing.T, result any) {
	raw, err := json.MarshalIndent(result, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(*benchOut, raw, 0o644))
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
	widths := parseInts(t, *benchHalo)
	b := newHaloBench(t, "halo")
	index, built, data := b.index, b.built, b.data
	margin := b.uc.SourceMargin
	result := &haloResult{
		Dataset: data.name, Vectors: len(data.train), Queries: len(data.queries), K: *benchK, Dims: data.dims,
		Gate: *benchHaloGate, Config: b.config(), Build: workOfBuild(t, index, 0), FullCodeBytes: built.side.codeWords * 8,
	}
	var postings []haloPosting
	postings, result.Layout = b.layout()
	writeHaloResult(t, result)
	t.Logf("layout: %d postings | members mean %.1f, p95 %d | edges mean %.1f, p95 %d | distinct endpoints mean %.1f, p95 %d "+
		"(%.3f per edge) | the full code takes %d bytes",
		len(postings), result.Layout.Members.Mean, result.Layout.Members.P95, result.Layout.Edges.Mean, result.Layout.Edges.P95,
		result.Layout.Distinct.Mean, result.Layout.Distinct.P95, result.Layout.Distinct.Mean/result.Layout.Edges.Mean,
		result.FullCodeBytes)

	result.Cells = append(result.Cells, b.cell("reference, the full code", postings, 0, result.FullCodeBytes, margin))
	writeHaloResult(t, result)
	for _, bits := range widths {
		halo := b.sketch(bits)
		index.state.Store(halo)
		result.Cells = append(result.Cells,
			b.cell(fmt.Sprintf("sketch of %d bits", bits), postings, bits, halo.side.codeWords*8, margin))
		writeHaloResult(t, result)
		index.state.Store(built)
	}
	require.NoError(t, index.Shutdown(b.ctx))
}

// haloEdgeSet is one set of edges over the build: all of them, or what a
// pruning leaves, with the layout of the postings under it.
type haloEdgeSet struct {
	Name         string `json:"name"`
	OnePerTarget bool   `json:"onePerTarget"`
	DegreeCap    int    `json:"degreeCap"`
	Edges        int    `json:"edges"`
	// Targets is the target postings of every vector, summed, and Shared the
	// edges whose target posting a nearer edge of their vector leads to.
	Targets   int        `json:"targets"`
	Shared    int        `json:"shared"`
	MaxDegree int        `json:"maxDegree"`
	Layout    haloLayout `json:"layout"`
}

// haloPruneCell is the index searched over one set of edges with one source
// margin, its endpoints ranked with one sketch.
type haloPruneCell struct {
	Edges  string  `json:"edges"`
	Margin float64 `json:"margin"`
	haloCell
}

type haloPruneResult struct {
	Dataset       string         `json:"dataset"`
	Vectors       int            `json:"vectors"`
	Queries       int            `json:"queries"`
	K             int            `json:"k"`
	Dims          int            `json:"dims"`
	Config        map[string]any `json:"config"`
	Gate          float64        `json:"gate"`
	EFs           []int          `json:"efs"`
	Build         *buildWork     `json:"build"`
	FullCodeBytes int            `json:"fullCodeBytes"`
	EdgeSets      []haloEdgeSet  `json:"edgeSets"`
	// Reference ranks the endpoints with the full code, over all the edges
	// and with the margin of the index: the layout with the resident sidecar.
	Reference *haloCell       `json:"reference,omitempty"`
	Cells     []haloPruneCell `json:"cells"`
}

// TestHaloPrune runs the pruned halo of v3.1 (bench-report.md, "v3.1: podar
// el halo") on the dataset of -bench.dataset: one batch build with the full
// code, the reference of v3.0 over it and, per width of -bench.haloprune, a
// grid of cells: every set of edges (all, one per target posting, capped at
// -bench.halocap, and both) searched with every margin of -bench.halomargins.
// The cell with all the edges and the margin of the index is the base, the
// cell of v3.0. Every cell is searched over the ef of -bench.haloefs,
// untimed, and what each search loads is counted block by block. A pruning
// that does not leave what it should fails the run before anything is
// measured.
func TestHaloPrune(t *testing.T) {
	if *benchDatasetDir == "" || *benchHaloPrune == "" {
		t.Skip("no -bench.dataset and -bench.haloprune given")
	}
	require.NotEmpty(t, *benchOut, "-bench.haloprune writes its results to -bench.out")
	widths := parseInts(t, *benchHaloPrune)
	margins := parseFloats(t, *benchHaloMargins)
	degree := *benchHaloCap
	require.Positive(t, degree, "-bench.halocap")
	b := newHaloBench(t, "haloprune")
	index, built, data := b.index, b.built, b.data
	result := &haloPruneResult{
		Dataset: data.name, Vectors: len(data.train), Queries: len(data.queries), K: *benchK, Dims: data.dims,
		Gate: *benchHaloGate, EFs: b.efs, Config: b.config(), Build: workOfBuild(t, index, 0),
		FullCodeBytes: built.side.codeWords * 8,
	}

	// The sets of edges, every one checked against the full edges and laid
	// out before anything is searched.
	fullStart, full, postingOf := built.postings.edgeStart, built.postings.edges, built.side.postingOf
	fullTargets := edgeRedundancyOf(fullStart, full, postingOf).Targets
	type edgeSetOf struct {
		edgeSet
		postings []haloPosting
	}
	sets := make([]edgeSetOf, 0, 4)
	for _, set := range []haloEdgeSet{
		{Name: "all"},
		{Name: "onePerTarget", OnePerTarget: true},
		{Name: fmt.Sprintf("cap%d", degree), DegreeCap: degree},
		{Name: fmt.Sprintf("onePerTargetCap%d", degree), OnePerTarget: true, DegreeCap: degree},
	} {
		start, kept := pruneEdges(fullStart, full, postingOf, set.OnePerTarget, set.DegreeCap)
		require.NoErrorf(t, checkPruned(fullStart, full, start, kept, postingOf, set.OnePerTarget, set.DegreeCap),
			"the edges of %s are not what the pruning leaves: nothing measured", set.Name)
		redundancy := edgeRedundancyOf(start, kept, postingOf)
		set.Edges, set.Targets, set.Shared = len(kept), redundancy.Targets, redundancy.Shared
		for slot := 0; slot+1 < len(start); slot++ {
			set.MaxDegree = max(set.MaxDegree, int(start[slot+1]-start[slot]))
		}
		if set.OnePerTarget {
			require.Zerof(t, set.Shared, "%s: edges of a vector share a target posting", set.Name)
		}
		if set.OnePerTarget && set.DegreeCap == 0 {
			require.Equalf(t, fullTargets, set.Targets, "%s: a vector loses a target posting", set.Name)
		}
		if set.DegreeCap > 0 {
			require.LessOrEqualf(t, set.MaxDegree, set.DegreeCap, "%s: a vector above the cap", set.Name)
		}
		require.NoError(t, index.replaceEdges(start, kept))
		var postings []haloPosting
		postings, set.Layout = b.layout()
		sets = append(sets, edgeSetOf{edgeSet: edgeSet{start: start, edges: kept}, postings: postings})
		result.EdgeSets = append(result.EdgeSets, set)
		t.Logf("edges %s: %d edges, %.2f per vector (at most %d), into %d target postings, %d of them shared | "+
			"per posting: edges mean %.1f, p95 %d | distinct endpoints mean %.1f, p95 %d (%.3f per edge)",
			set.Name, set.Edges, float64(set.Edges)/float64(len(data.train)), set.MaxDegree, set.Targets, set.Shared,
			set.Layout.Edges.Mean, set.Layout.Edges.P95, set.Layout.Distinct.Mean, set.Layout.Distinct.P95,
			set.Layout.Distinct.Mean/set.Layout.Edges.Mean)
	}
	require.NoError(t, index.replaceEdges(fullStart, full))
	writeHaloResult(t, result)

	reference := b.cell("reference, the full code, all the edges", sets[0].postings, 0, result.FullCodeBytes, b.uc.SourceMargin)
	result.Reference = &reference
	writeHaloResult(t, result)

	for _, bits := range widths {
		halo := b.sketch(bits)
		for i, set := range sets {
			// replaceEdges republishes the state the index has, which is
			// the one with the sketches.
			index.state.Store(halo)
			require.NoError(t, index.replaceEdges(set.start, set.edges))
			for _, margin := range margins {
				name := fmt.Sprintf("sketch of %d bits, edges %s, margin %.2f", bits, result.EdgeSets[i].Name, margin)
				result.Cells = append(result.Cells, haloPruneCell{
					Edges: result.EdgeSets[i].Name, Margin: margin,
					haloCell: b.cell(name, set.postings, bits, halo.side.codeWords*8, float32(margin)),
				})
				writeHaloResult(t, result)
			}
		}
		index.state.Store(built)
	}
	require.NoError(t, index.Shutdown(b.ctx))
}
