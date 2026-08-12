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

//go:build integrationTest

package hnsw

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// storeWithObjectsBucket gives the index a real objects bucket so the gate gets past
// its bucket-presence check and actually evaluates eligibility — otherwise every
// index would fall back to serial for the wrong reason.
func storeWithObjectsBucket(t *testing.T) *lsmkv.Store {
	store := testinghelpers.NewDummyStore(t)
	require.NoError(t, store.CreateOrLoadBucket(context.Background(), helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace)))
	return store
}

func newPrefillRoutingIndex(t *testing.T, id string, uc ent.UserConfig, store *lsmkv.Store,
	docs [][][]float32,
) *hnsw {
	idx, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    id,
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewDotProductProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return []float32{0.1, 0.2, 0.3}, nil
		},
		MultiVectorForIDThunk: func(ctx context.Context, id uint64) ([][]float32, error) {
			return docs[id], nil
		},
		MakeBucketOptions:   lsmkv.MakeNoopBucketOptions,
		AllocChecker:        memwatch.NewDummyMonitor(),
		GetViewThunk:        func() common.BucketView { return &multivectorNoopBucketView{} },
		WaitForCachePrefill: true,
	}, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, idx.Shutdown(context.Background())) })
	return idx
}

// prefillRoutingUserConfig is the baseline single-vector config that satisfies the
// parallel-prefill gate (unbounded cache); tests layer Multivector/Muvera on top to
// flip the expected routing.
func prefillRoutingUserConfig() ent.UserConfig {
	return ent.UserConfig{VectorCacheMaxObjects: 1e12, MaxConnections: 8, EFConstruction: 64, EF: 64}
}

func muveraUserConfig() ent.UserConfig {
	uc := prefillRoutingUserConfig()
	uc.Multivector = ent.MultivectorConfig{
		Enabled:      true,
		MuveraConfig: ent.MuveraConfig{Enabled: true, KSim: 2, DProjections: 3, Repetitions: 5},
	}
	return uc
}

// TestUseParallelPrefillRoutingRealIndex builds real indexes (objects bucket present,
// sync prefill, unbounded cache) and confirms the config→atomic→gate wiring routes
// each index type to the right prefill source.
func TestUseParallelPrefillRoutingRealIndex(t *testing.T) {
	tests := []struct {
		name string
		uc   func() ent.UserConfig
		want prefillScanSource
	}{
		{"single-vector uncompressed scans objects", prefillRoutingUserConfig, prefillScanObjects},
		{"true multivector keeps serial path", func() ent.UserConfig {
			uc := prefillRoutingUserConfig()
			uc.Multivector = ent.MultivectorConfig{Enabled: true}
			return uc
		}, prefillScanNone},
		{"muvera scans the muvera bucket", muveraUserConfig, prefillScanMuvera},
		// reachable through the schema, and New builds the muvera bucket for it; see
		// parallelPrefillSource
		{"muvera without multivector keeps serial path", func() ent.UserConfig {
			uc := prefillRoutingUserConfig()
			uc.Multivector = ent.MultivectorConfig{
				MuveraConfig: ent.MuveraConfig{Enabled: true, KSim: 2, DProjections: 3, Repetitions: 5},
			}
			return uc
		}, prefillScanNone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := newPrefillRoutingIndex(t, "routing", tt.uc(), storeWithObjectsBucket(t), multiVectors)
			require.Equal(t, tt.want, idx.parallelPrefillSource())
		})
	}
}

// muveraPrefillDocs must be large enough that the flushed bucket yields quantile
// seeds; below that the scan collapses to one full-range cursor and never exercises
// the seed arithmetic. requireParallelRanges asserts that rather than trust the number.
const muveraPrefillDocs = 3000

// generateMultiVectors stands in for the multiVectors fixture, which at 3 docs is too
// small to flush into a segment.
func generateMultiVectors(n int) [][][]float32 {
	docs := make([][][]float32, n)
	for i := range docs {
		passages := make([][]float32, 2+i%3)
		for p := range passages {
			f := float32(i*10 + p)
			passages[p] = []float32{f, f + 0.5, f - 0.5}
		}
		docs[i] = passages
	}
	return docs
}

// newFlushedMuveraIndex populates a muvera index and flushes its vectors bucket, which
// QuantileKeys needs to seed anything.
func newFlushedMuveraIndex(t *testing.T, id string, docs [][][]float32) *hnsw {
	ctx := context.Background()
	store := storeWithObjectsBucket(t)
	idx := newPrefillRoutingIndex(t, id, muveraUserConfig(), store, docs)

	ids := make([]uint64, len(docs))
	for i := range docs {
		ids[i] = uint64(i)
	}
	require.NoError(t, idx.AddMultiBatch(ctx, ids, docs))
	require.NoError(t, store.Bucket(helpers.GetMuveraBucketName(id)).FlushAndSwitch())
	return idx
}

// muveraPrefillGroundTruth reads the expectation straight from the encoder/bucket, so
// a prefill that loads the wrong vectors fails rather than agreeing with itself.
func muveraPrefillGroundTruth(t *testing.T, idx *hnsw, docs int) map[uint64][]float32 {
	t.Helper()
	expected := make(map[uint64][]float32, docs)
	for i := 0; i < docs; i++ {
		v, err := idx.muveraEncoder.GetMuveraVectorForID(uint64(i), helpers.GetMuveraBucketName(idx.id))
		require.NoError(t, err)
		expected[uint64(i)] = v
	}
	return expected
}

func requireMuveraCacheMatches(t *testing.T, idx *hnsw, expected map[uint64][]float32) {
	t.Helper()
	require.Equal(t, int64(len(expected)), idx.cache.CountVectors())
	for id, want := range expected {
		got, err := idx.cache.Get(context.Background(), id)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

// evictCachedVectors empties the cache the way a cold start would leave it. Not
// cache.Drop(): Drop notifies a deletion watcher that exits on the first notification,
// so the deferred Shutdown — the first call through releaseVectorsOnce — would then
// block forever on its own Drop.
func evictCachedVectors(t *testing.T, idx *hnsw, docs int) {
	t.Helper()
	for id := 0; id < docs; id++ {
		idx.cache.Delete(context.Background(), uint64(id))
	}
	require.Equal(t, int64(0), idx.cache.CountVectors())
}

// requireParallelRanges fails unless the bucket seeds more than one cursor range: a
// fixture that shrinks below the segment threshold would silently stop testing the
// parallel scan.
func requireParallelRanges(t *testing.T, idx *hnsw) {
	t.Helper()
	seeds := idx.store.Bucket(helpers.GetMuveraBucketName(idx.id)).QuantileKeys(prefillScanParallelism() - 1)
	require.NotEmpty(t, seeds, "muvera bucket yields no quantile seeds: the scan would run as one range")
}

// TestMuveraParallelPrefillPopulatesCacheRealIndex: cold-restart shape against a real
// muvera index — the scan must repopulate the float32 cache across several ranges.
func TestMuveraParallelPrefillPopulatesCacheRealIndex(t *testing.T) {
	ctx := context.Background()
	docs := generateMultiVectors(muveraPrefillDocs)
	idx := newFlushedMuveraIndex(t, "muvera-prefill-parallel", docs)

	require.Equal(t, prefillScanMuvera, idx.parallelPrefillSource())
	requireParallelRanges(t, idx)
	expected := muveraPrefillGroundTruth(t, idx, len(docs))

	evictCachedVectors(t, idx, len(docs))

	require.NoError(t, idx.prefillMuveraCacheParallel(ctx))
	requireMuveraCacheMatches(t, idx, expected)
}

// TestMuveraPrefillCacheRoutesToParallelScan drives the shipping entry point rather
// than the scan directly: prefillCache must pick the muvera arm and run it.
func TestMuveraPrefillCacheRoutesToParallelScan(t *testing.T) {
	ctx := context.Background()
	docs := generateMultiVectors(muveraPrefillDocs)
	idx := newFlushedMuveraIndex(t, "muvera-prefill-cache", docs)

	requireParallelRanges(t, idx)
	expected := muveraPrefillGroundTruth(t, idx, len(docs))

	evictCachedVectors(t, idx, len(docs))
	// the noop commit logger restores no state, so init() took restoreFromDisk's
	// fresh-index shortcut and already marked the cache prefilled
	idx.cachePrefilled.Store(false)

	idx.prefillCache(ctx)

	require.True(t, idx.cachePrefilled.Load())
	requireMuveraCacheMatches(t, idx, expected)
}

// TestMuveraSerialPrefillPopulatesCacheRealIndex guards the serial by-id fallback
// muvera still uses when the parallel gate fails (e.g. bounded cache): it must load
// from the dedicated muvera bucket, not the objects bucket.
func TestMuveraSerialPrefillPopulatesCacheRealIndex(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	idx := newFlushedMuveraIndex(t, "muvera-prefill-serial", multiVectors)

	require.NotEqual(t, prefillScanObjects, idx.parallelPrefillSource(),
		"muvera must never take the objects-bucket scan")

	expected := muveraPrefillGroundTruth(t, idx, len(multiVectors))

	evictCachedVectors(t, idx, len(multiVectors))

	require.NoError(t, newVectorCachePrefiller(idx.cache, idx, logger).Prefill(ctx, int(idx.cache.CopyMaxSize())))
	requireMuveraCacheMatches(t, idx, expected)
}
