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
	"math/rand/v2"
	"sort"
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// newScanTestIndex builds a compressed centered-rq1 index over biased unit
// vectors, activating through the real deferred-training path.
func newScanTestIndex(t *testing.T, vectors [][]float32, trainingLimit int) *hnsw {
	t.Helper()
	ctx := context.Background()
	provider := distancer.NewCosineDistanceProvider()
	logger, _ := test.NewNullLogger()
	_ = logger

	uc := ent.UserConfig{}
	uc.SetDefaults()
	uc.MaxConnections = 16
	uc.EFConstruction = 32
	uc.VectorCacheMaxObjects = 10e12
	uc.RQ = ent.RQConfig{
		Enabled: true, Bits: 1, Centering: true,
		TrainingLimit: trainingLimit, RescoreLimit: 20,
	}

	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "filtered-scan-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      provider,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			return vectors[int(id)], nil
		},
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
		GetViewThunk:      func() common.BucketView { return &noopBucketView{} },
		AllocChecker:      memwatch.NewDummyMonitor(),
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, uc, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = index.Shutdown(context.Background()) })
	index.PostStartup(ctx)

	nTrain := trainingLimit + 1
	for id := 0; id < nTrain; id++ {
		require.NoError(t, index.Add(ctx, uint64(id), vectors[id]))
	}
	var wg sync.WaitGroup
	wg.Add(1)
	require.NoError(t, index.Upgrade(wg.Done))
	wg.Wait()
	require.True(t, index.Compressed())
	for id := nTrain; id < len(vectors); id++ {
		require.NoError(t, index.Add(ctx, uint64(id), vectors[id]))
	}
	return index
}

// bruteForceFiltered returns the exact top-k of query within the allowlist.
func bruteForceFiltered(query []float32, vectors [][]float32, allow map[uint64]bool, k int) []uint64 {
	type c struct {
		id uint64
		d  float64
	}
	var cands []c
	for id := range allow {
		var dot float64
		v := vectors[id]
		for i := range v {
			dot += float64(query[i]) * float64(v[i])
		}
		cands = append(cands, c{id, 1 - dot})
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].d < cands[j].d })
	if k > len(cands) {
		k = len(cands)
	}
	out := make([]uint64, k)
	for i := range out {
		out[i] = cands[i].id
	}
	return out
}

func TestFilteredPrefixScan(t *testing.T) {
	ctx := context.Background()
	dims := 128
	n := 6000
	all := biasedUnitVecs(n+20, dims, 11)
	vectors, queries := all[:n], all[n:]
	index := newScanTestIndex(t, vectors, 1000)

	floatsFor := func(id uint64) []float32 { return vectors[id] }
	rng := rand.New(rand.NewPCG(3, 4))

	t.Run("exact when budgets cover the allowlist", func(t *testing.T) {
		// With Budget1 >= |allowlist| and Budget2 >= Budget1, no candidate is
		// ever dropped before the exact rescore: the scan must equal brute
		// force EXACTLY, for every query. This is the correctness invariant
		// (the anti-correlated gate in the measurement harness relies on it).
		allowSet := map[uint64]bool{}
		var ids []uint64
		for len(allowSet) < 400 {
			id := uint64(rng.IntN(n))
			if !allowSet[id] {
				allowSet[id] = true
				ids = append(ids, id)
			}
		}
		allow := helpers.NewAllowList(ids...)
		cfg := FilteredScanConfig{Budget1: 500, Budget2: 500, FloatsForID: floatsFor}
		scratch := NewFilteredScanScratch(cfg)
		for qi, q := range queries {
			got, _, stats, err := index.FilteredPrefixScan(ctx, q, 10, allow, cfg, scratch)
			require.NoError(t, err)
			want := bruteForceFiltered(q, vectors, allowSet, 10)
			assert.Equal(t, want, got, "query %d must be exact (stats %+v)", qi, stats)
		}
	})

	t.Run("constrained budgets keep high recall", func(t *testing.T) {
		allowSet := map[uint64]bool{}
		var ids []uint64
		for len(allowSet) < 3000 {
			id := uint64(rng.IntN(n))
			if !allowSet[id] {
				allowSet[id] = true
				ids = append(ids, id)
			}
		}
		allow := helpers.NewAllowList(ids...)
		cfg := FilteredScanConfig{Budget1: 600, Budget2: 200, FloatsForID: floatsFor}
		scratch := NewFilteredScanScratch(cfg)
		var hits, wanted int
		for _, q := range queries {
			got, _, _, err := index.FilteredPrefixScan(ctx, q, 10, allow, cfg, scratch)
			require.NoError(t, err)
			want := bruteForceFiltered(q, vectors, allowSet, 10)
			wantSet := map[uint64]bool{}
			for _, id := range want {
				wantSet[id] = true
			}
			for _, id := range got {
				if wantSet[id] {
					hits++
				}
			}
			wanted += len(want)
		}
		recall := float64(hits) / float64(wanted)
		t.Logf("constrained-budget recall@10: %.4f", recall)
		assert.Greater(t, recall, 0.85, "pipeline sanity floor; dataset gates run in the measurement harness")
	})

	t.Run("stats accounting", func(t *testing.T) {
		allow := helpers.NewAllowList(1, 2, 3, 500, 501, 502, 4999)
		cfg := FilteredScanConfig{FloatsForID: floatsFor}
		got, dists, stats, err := index.FilteredPrefixScan(ctx, queries[0], 3, allow, cfg, nil)
		require.NoError(t, err)
		assert.Equal(t, 7, stats.Members)
		assert.Equal(t, 7, stats.Survivors1, "all members survive when budget exceeds allowlist")
		assert.Equal(t, int64(7*64), stats.Stage1Bytes)
		assert.Len(t, got, 3)
		assert.Len(t, dists, 3)
		// distances sorted ascending
		assert.LessOrEqual(t, dists[0], dists[1])
		// record lines: 128d → 256-dim padded code = 1+4 words = 40 B → one
		// 64 B line per record in stage 2
		assert.Equal(t, int64(7*64), stats.Stage2Bytes)
	})

	t.Run("ids in allowlist but not in index are skipped", func(t *testing.T) {
		allow := helpers.NewAllowList(0, 1, uint64(n+100), uint64(n+200))
		cfg := FilteredScanConfig{FloatsForID: floatsFor}
		got, _, stats, err := index.FilteredPrefixScan(ctx, queries[1], 5, allow, cfg, nil)
		require.NoError(t, err)
		assert.Equal(t, 2, stats.Survivors1)
		assert.LessOrEqual(t, len(got), 2)
	})
}

// TestRoutedFilteredScanToggle pins the routing contract: threshold unset →
// never handled; set → small allowlists take the scan and produce brute-
// force-exact results (budgets cover the list), large allowlists fall
// through to the graph path.
func TestRoutedFilteredScanToggle(t *testing.T) {
	ctx := context.Background()
	dims := 128
	n := 3000
	all := biasedUnitVecs(n+5, dims, 13)
	vectors, queries := all[:n], all[n:]
	index := newScanTestIndex(t, vectors, 1000)

	var ids []uint64
	allowSet := map[uint64]bool{}
	for id := uint64(0); id < 200; id++ {
		ids = append(ids, id)
		allowSet[id] = true
	}
	allow := helpers.NewAllowList(ids...)

	// toggle off (default): tryRoutedFilteredScan must not handle
	_, _, handled, err := index.tryRoutedFilteredScan(ctx, queries[0], 5, allow)
	require.NoError(t, err)
	require.False(t, handled, "routing must be off by default")

	// toggle on: small list handled, exact against brute force
	prev := scanRouteThreshold
	scanRouteThreshold = 500
	defer func() { scanRouteThreshold = prev }()

	got, _, handled, err := index.tryRoutedFilteredScan(ctx, queries[0], 5, allow)
	require.NoError(t, err)
	require.True(t, handled)
	want := bruteForceFiltered(queries[0], vectors, allowSet, 5)
	assert.Equal(t, want, got, "routed scan must be exact when budgets cover the list")

	// large list: falls through
	var bigIDs []uint64
	for id := uint64(0); id < 1000; id++ {
		bigIDs = append(bigIDs, id)
	}
	big := helpers.NewAllowList(bigIDs...)
	_, _, handled, err = index.tryRoutedFilteredScan(ctx, queries[0], 5, big)
	require.NoError(t, err)
	require.False(t, handled, "above-threshold lists must fall through")

	// and the full SearchByVector path with the toggle on returns the same
	// exact results for the small list
	sIDs, _, err := index.SearchByVector(ctx, queries[0], 5, allow)
	require.NoError(t, err)
	assert.Equal(t, want, sIDs)
}
