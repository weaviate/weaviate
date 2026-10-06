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

package hnsw

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// Regression tests for weaviate/0-weaviate-issues#670: inserting many
// identical coordinates into a geo index was pathologically slow. Once a
// neighbor's connection list is full of zero-distance duplicates, every
// backlink re-ran the selection heuristic's O(n²) peer-distance loop even
// though on all-zero ties its strict peerDist < distToQuery rule can never
// prune anything.

type dupNoopBucketView struct{}

func (n *dupNoopBucketView) ReleaseView() {}

// distTallyProvider wraps a distancer.Provider and counts every distance
// computation, both direct (SingleDist) and through a prepared distancer.
// (countingProvider in pathseer_test.go only counts the latter.)
type distTallyProvider struct {
	distancer.Provider
	count atomic.Int64
}

func (c *distTallyProvider) SingleDist(a, b []float32) (float32, error) {
	c.count.Add(1)
	return c.Provider.SingleDist(a, b)
}

func (c *distTallyProvider) New(vec []float32) distancer.Distancer {
	return &distTallyDistancer{d: c.Provider.New(vec), count: &c.count}
}

type distTallyDistancer struct {
	d     distancer.Distancer
	count *atomic.Int64
}

func (c *distTallyDistancer) Distance(v []float32) (float32, error) {
	c.count.Add(1)
	return c.d.Distance(v)
}

func newDuplicateTestIndexWithThunk(t *testing.T, provider distancer.Provider,
	thunk func(ctx context.Context, id uint64) ([]float32, error),
) *hnsw {
	t.Helper()
	index, err := New(Config{
		AllocChecker:          memwatch.NewDummyMonitor(),
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "duplicate-vectors-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      provider,
		VectorForIDThunk:      thunk,
		GetViewThunk:          func() common.BucketView { return &dupNoopBucketView{} },
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
	}, ent.UserConfig{
		// mirrors the geo index config in adapters/repos/db/vector/geo/geo.go
		MaxConnections: 64,
		EFConstruction: 128,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	return index
}

// fixLevelSeed makes the random level assignment deterministic so graph
// shapes (and with them orphan counts and recall) are reproducible.
func fixLevelSeed(index *hnsw) {
	seed := uint64(42)
	index.randFunc = func() float64 {
		seed = seed*6364136223846793005 + 1442695040888963407
		return float64(seed>>11) / float64(1<<53)
	}
}

func newDuplicateTestIndex(t *testing.T, provider distancer.Provider, vectors [][]float32) *hnsw {
	t.Helper()
	return newDuplicateTestIndexWithThunk(t, provider,
		func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		})
}

// zeroInDegreeAtLayer0 returns how many nodes no other node links to at
// layer 0. Such nodes cannot be reached by a layer-0 graph walk, so beyond
// the occasional straggler they translate directly into lost recall.
func zeroInDegreeAtLayer0(h *hnsw) int {
	hasIncoming := make(map[uint64]bool)
	var exists []uint64

	var buf []uint64
	for id, node := range h.nodes {
		if node == nil {
			continue
		}
		exists = append(exists, uint64(id))
		buf = node.connections.CopyLayer(buf[:0], 0)
		for _, target := range buf {
			if target != uint64(id) {
				hasIncoming[target] = true
			}
		}
	}

	var zero int
	for _, id := range exists {
		if !hasIncoming[id] {
			zero++
		}
	}
	return zero
}

// The shortcut gate must match the real Type() strings of the providers:
// it must be on for every metric that never produces negative distances,
// and off for dot product, whose distance is the negated dot product and
// goes negative for similar vectors — there a zero queue top would not
// mean all-zero, and a pairwise prune (peerDist < 0) is possible. A
// renamed Type() string would silently disable the shortcut (or, for dot,
// silently enable an unsound one) — this pins both.
func TestDistancesNonNegativeGating(t *testing.T) {
	point := []float32{1, 0}
	cases := []struct {
		provider distancer.Provider
		expected bool
	}{
		{distancer.NewGeoProvider(), true},
		{distancer.NewL2SquaredProvider(), true},
		{distancer.NewCosineDistanceProvider(), true},
		{distancer.NewManhattanProvider(), true},
		{distancer.NewHammingProvider(), true},
		{distancer.NewDotProductProvider(), false},
	}

	for _, tc := range cases {
		t.Run(tc.provider.Type(), func(t *testing.T) {
			index := newDuplicateTestIndexWithThunk(t, tc.provider,
				func(ctx context.Context, id uint64) ([]float32, error) {
					return point, nil
				})
			require.Equal(t, tc.expected, index.distancesNonNegative)
		})
	}
}

// The all-at-zero shortcut must produce exactly the full heuristic's
// output. The full pairwise loop on all-zero candidates prunes nothing
// (strictly peerDist < distToQuery never fires), so skipping it must not
// change the result — including in the presence of deny-listed and
// vanished candidates, which are filtered before the pairwise loop. Each
// case runs the real selectNeighborsHeuristic twice on identical input,
// once with the shortcut (geo metric default) and once with it forced
// off, and requires identical output.
func TestAllAtZeroSkipMatchesFullHeuristic(t *testing.T) {
	point := []float32{48.13743, 11.57549}

	run := func(t *testing.T, shortcut bool, numCandidates, max int, vanishedID int64, denied []uint64) []uint64 {
		t.Helper()
		thunk := func(ctx context.Context, id uint64) ([]float32, error) {
			if vanishedID >= 0 && id == uint64(vanishedID) {
				return nil, storobj.NewErrNotFoundf(id, "vanished")
			}
			return point, nil
		}
		index := newDuplicateTestIndexWithThunk(t, distancer.NewGeoProvider(), thunk)
		index.distancesNonNegative = shortcut

		input := priorityqueue.NewMax[any](numCandidates)
		for i := 1; i <= numCandidates; i++ {
			input.Insert(uint64(i), 0)
		}

		var deny helpers.AllowList
		if len(denied) > 0 {
			deny = helpers.NewAllowList(denied...)
		}

		require.NoError(t, index.selectNeighborsHeuristic(input, max, deny))

		out := make([]uint64, 0, input.Len())
		for input.Len() > 0 {
			out = append(out, input.Pop().ID)
		}
		return out
	}

	cases := []struct {
		name          string
		numCandidates int
		max           int
		vanishedID    int64
		denied        []uint64
	}{
		{name: "full list", numCandidates: 129, max: 128, vanishedID: -1},
		{name: "list over capacity", numCandidates: 168, max: 128, vanishedID: -1},
		{name: "vanished connection", numCandidates: 129, max: 128, vanishedID: 7},
		{name: "deny-listed connection", numCandidates: 129, max: 128, vanishedID: -1, denied: []uint64{13}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			full := run(t, false, tc.numCandidates, tc.max, tc.vanishedID, tc.denied)
			skip := run(t, true, tc.numCandidates, tc.max, tc.vanishedID, tc.denied)
			require.Equal(t, full, skip,
				"all-at-zero shortcut diverged from the full heuristic")
		})
	}
}

// The shortcut does NOT assume candidates at zero distance from the query
// are identical to each other: with cosine, float32 rounding clamps
// distinct near-duplicates to a distance of exactly zero against the
// query while their pairwise distance stays positive. That is fine —
// with every distToQuery zero, the strict peerDist < distToQuery prune
// would need a negative peerDist, which a non-negative metric cannot
// produce — but it must be pinned: run the heuristic on exactly such a
// candidate set with the shortcut on and off and require identical
// output.
func TestAllAtZeroSkipCosineNearDuplicates(t *testing.T) {
	provider := distancer.NewCosineDistanceProvider()

	// two distinct vectors whose angle to the query is small enough that
	// float32 rounds their cosine distance to the query to exactly zero,
	// while the angle between them is twice as large and survives rounding
	query := []float32{1, 0}
	vecA := []float32{1, 2.4e-4}
	vecB := []float32{1, -2.4e-4}

	distQA, err := provider.SingleDist(query, vecA)
	require.NoError(t, err)
	distQB, err := provider.SingleDist(query, vecB)
	require.NoError(t, err)
	distAB, err := provider.SingleDist(vecA, vecB)
	require.NoError(t, err)
	require.Zero(t, distQA, "test setup: vecA must clamp to zero against the query")
	require.Zero(t, distQB, "test setup: vecB must clamp to zero against the query")
	require.Greater(t, distAB, float32(0), "test setup: the pair must keep a positive distance")

	const numCandidates = 129
	const max = 128

	run := func(t *testing.T, shortcut bool) []uint64 {
		t.Helper()
		index := newDuplicateTestIndexWithThunk(t, provider,
			func(ctx context.Context, id uint64) ([]float32, error) {
				if id%2 == 0 {
					return vecB, nil
				}
				return vecA, nil
			})
		index.distancesNonNegative = shortcut

		input := priorityqueue.NewMax[any](numCandidates)
		for i := 1; i <= numCandidates; i++ {
			input.Insert(uint64(i), 0)
		}
		require.NoError(t, index.selectNeighborsHeuristic(input, max, nil))

		out := make([]uint64, 0, input.Len())
		for input.Len() > 0 {
			out = append(out, input.Pop().ID)
		}
		return out
	}

	require.Equal(t, run(t, false), run(t, true),
		"shortcut diverged on near-duplicates that clamp to zero against the query")
}

// Inserting duplicates must not run the heuristic's O(n²) peer-distance
// loop per backlink: on an all-zero-distance candidate set its outcome is
// fixed, and computing it anyway costs O(M²) distance calculations per
// backlink, O(M³) per insert. 300 inserts took ~63M distance computations
// before the shortcut; the bound sits far from both sides.
func TestDuplicateVectorsInsertCost(t *testing.T) {
	const n = 300
	point := []float32{48.13743, 11.57549}
	vectors := make([][]float32, n)
	for i := range vectors {
		vectors[i] = point
	}

	provider := &distTallyProvider{Provider: distancer.NewGeoProvider()}
	index := newDuplicateTestIndex(t, provider, vectors)

	ctx := context.Background()
	for i, vec := range vectors {
		require.NoError(t, index.Add(ctx, uint64(i), vec))
	}

	count := provider.count.Load()
	t.Logf("%d distance computations for %d duplicate inserts", count, n)
	require.Less(t, count, int64(20_000_000),
		"inserting %d duplicate points must not run the O(n²) peer-distance loop per backlink", n)
}

// The shortcut must preserve the duplicates' reachability: every duplicate
// keeps incoming links from later inserts, so a range query that is allowed
// to return them all (ef >= n) finds them all. This also pins the behavior
// against "fixes" that skip or prune zero-distance backlinks: those leave
// most duplicates with zero incoming edges and recall collapses to ~M/n.
func TestDuplicateVectorsReachability(t *testing.T) {
	t.Run("one location, more duplicates than maxConnections", func(t *testing.T) {
		const n = 500
		point := []float32{48.13743, 11.57549}
		vectors := make([][]float32, n)
		for i := range vectors {
			vectors[i] = point
		}

		index := newDuplicateTestIndex(t, distancer.NewGeoProvider(), vectors)
		// deterministic level assignment: the orphan count depends on the
		// random level sequence (an unlucky draw yields a second straggler,
		// with or without the shortcut), so pin it instead of flaking in CI
		fixLevelSeed(index)
		ctx := context.Background()
		for i, vec := range vectors {
			require.NoError(t, index.Add(ctx, uint64(i), vec))
		}

		zeroIn := zeroInDegreeAtLayer0(index)
		require.LessOrEqual(t, zeroIn, 1,
			"duplicates lost their incoming links and became unreachable")

		ids, err := index.KnnSearchByVectorMaxDist(ctx, point, 0, n, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(ids), n*99/100,
			"radius-0 range query should find (nearly) all duplicates")
	})

	t.Run("many locations, duplicates per location", func(t *testing.T) {
		const locations = 20
		const perLocation = 60

		locs := make([][]float32, locations)
		for i := range locs {
			locs[i] = []float32{45 + float32(i/5), 5 + float32(i%5)}
		}
		vectors := make([][]float32, locations*perLocation)
		for i := range vectors {
			vectors[i] = locs[i/perLocation]
		}

		index := newDuplicateTestIndex(t, distancer.NewGeoProvider(), vectors)
		fixLevelSeed(index)
		ctx := context.Background()
		for i, vec := range vectors {
			require.NoError(t, index.Add(ctx, uint64(i), vec))
		}

		var totalFound int
		for li, loc := range locs {
			ids, err := index.KnnSearchByVectorMaxDist(ctx, loc, 0, 800, nil)
			require.NoError(t, err)
			expected := make(map[uint64]struct{}, perLocation)
			for i := 0; i < perLocation; i++ {
				expected[uint64(li*perLocation+i)] = struct{}{}
			}
			for _, id := range ids {
				if _, ok := expected[id]; ok {
					totalFound++
				}
			}
		}
		require.GreaterOrEqual(t, float64(totalFound)/float64(locations*perLocation), 0.95,
			"radius-0 range queries per location should find nearly all objects")
	})
}
