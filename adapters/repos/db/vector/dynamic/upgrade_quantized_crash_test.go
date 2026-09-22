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

package dynamic

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bbolt "go.etcd.io/bbolt"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// TestUpgrade_InterruptedQuantizedUpgradeCorruptsCompressedBucket reproduces
// https://github.com/weaviate/weaviate/issues/451.
//
// The dynamic index's flat stage and the in-progress HNSW share ONE on-disk
// compressed LSM bucket (both derive it from GetCompressedBucketName(targetVector)),
// keyed by docID. When the flat stage and the HNSW target use different-length
// quantizers that both compress on insert, doUpgrade -> copyToVectorIndex ->
// AddBatch overwrites the flat stage's codes with the HNSW's codes, batch by
// batch, in that shared bucket. The "upgraded" verdict is committed only AFTER
// the whole copy.
//
// If the process is interrupted mid-copy (simulated by panicking between copy
// batches, which skips doUpgrade's cleanup exactly as a crash would), the shared
// compressed bucket is left as a mix of the two code formats — of different byte
// lengths — with no upgrade verdict recorded. On restart the dynamic index rolls
// back to the flat stage and, on the buggy code, reads that mixed bucket: its
// quantizer's distancer rejects the wrong-length codes with "vector lengths
// don't match".
//
// The fix persists an UPGRADING marker before the copy and, on restart, rebuilds
// the flat compressed bucket from the intact raw vectors bucket, so searches
// succeed with the flat stage's own codes again.
func TestUpgrade_InterruptedQuantizedUpgradeCorruptsCompressedBucket(t *testing.T) {
	rq8 := func() flatent.UserConfig {
		fuc := flatent.UserConfig{}
		fuc.SetDefaults()
		fuc.RQ.Enabled = true
		fuc.RQ.Bits = 8
		return fuc
	}
	bqFlat := func() flatent.UserConfig {
		fuc := flatent.UserConfig{}
		fuc.SetDefaults()
		fuc.BQ.Enabled = true
		return fuc
	}
	hnswWith := func(mutate func(*hnswent.UserConfig)) hnswent.UserConfig {
		hnswuc := hnswent.UserConfig{
			MaxConnections:        30,
			EFConstruction:        64,
			EF:                    32,
			VectorCacheMaxObjects: 1_000_000,
		}
		hnswuc.SetDefaults()
		mutate(&hnswuc)
		return hnswuc
	}

	tests := []struct {
		name   string
		flatUC flatent.UserConfig
		hnswUC hnswent.UserConfig
	}{
		{
			// flat RQ8 (80-byte codes) reads HNSW BQ codes (8 bytes) => "8 vs 80"
			name:   "flat RQ8 -> hnsw BQ",
			flatUC: rq8(),
			hnswUC: hnswWith(func(uc *hnswent.UserConfig) { uc.BQ.Enabled = true }),
		},
		{
			// the reverse pairing: flat BQ reads HNSW RQ8 codes
			name:   "flat BQ -> hnsw RQ8",
			flatUC: bqFlat(),
			hnswUC: hnswWith(func(uc *hnswent.UserConfig) { uc.RQ.Enabled = true; uc.RQ.Bits = 8 }),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runInterruptedQuantizedUpgrade(t, tc.flatUC, tc.hnswUC)
		})
	}
}

func runInterruptedQuantizedUpgrade(t *testing.T, flatUC flatent.UserConfig, hnswUC hnswent.UserConfig) {
	t.Helper()
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	dimensions := 32
	vectorsSize := 2 * batchSize // > batchSize so the copy spans >1 batch
	queriesSize := 20
	k := 10

	vectors, queries := testinghelpers.RandomVecs(vectorsSize, queriesSize, dimensions)
	dist := distancer.NewL2SquaredProvider()

	dirName := t.TempDir()
	indexID := "interrupted-quantized-upgrade" // unnamed vector: no dir-based inference

	db, err := bbolt.Open(filepath.Join(t.TempDir(), "index.db"), 0o666, nil)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	noopCallback := cyclemanager.NewCallbackGroupNoop()

	config := Config{
		AllocChecker:                 memwatch.NewDummyMonitor(),
		RootPath:                     dirName,
		ID:                           indexID,
		Logger:                       logger,
		DistanceProvider:             dist,
		MakeCommitLoggerThunk:        hnsw.MakeNoopCommitLogger,
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			vec := vectors[int(id)]
			if vec == nil {
				return nil, storobj.NewErrNotFoundf(id, "nil vec")
			}
			return vec, nil
		},
		TombstoneCallbacks:   noopCallback,
		SharedDB:             db,
		MakeBucketOptions:    lsmkv.MakeNoopBucketOptions,
		AsyncIndexingEnabled: true,
	}

	uc := ent.UserConfig{
		Threshold: uint64(vectorsSize),
		Distance:  dist.Type(),
		HnswUC:    hnswUC,
		FlatUC:    flatUC,
	}

	store := testinghelpers.NewDummyStore(t)

	idx, err := New(config, uc, store)
	require.NoError(t, err)
	idx.PostStartup(ctx)

	for i := 0; i < vectorsSize; i++ {
		require.NoError(t, idx.Add(ctx, uint64(i), vectors[i]))
	}
	require.True(t, idx.Compressed(), "flat stage must be compressed before upgrade")

	// Baseline: the flat stage's own answers before any upgrade. The repair
	// re-encodes from the raw bucket, so it must reproduce these exactly —
	// a quantizer-agnostic proof that recovery restored the real codes (not
	// just silenced the length error or returned garbage).
	baseline := make([][]uint64, queriesSize)
	for i := range queries {
		ids, _, err := idx.SearchByVector(ctx, queries[i], k, nil)
		require.NoError(t, err)
		baseline[i] = ids
	}

	// Interrupt the upgrade mid-copy: panic between copy batches, after the
	// first batch has already written HNSW codes over the flat codes for
	// ids 0..batchSize-1. GoWrapper recovers the panic, so doUpgrade's cleanup
	// never runs — exactly the on-disk residue a hard crash leaves.
	var once sync.Once
	idx.betweenCopyBatchesHook = func() {
		once.Do(func() { panic("simulated crash during quantized dynamic upgrade") })
	}

	done := make(chan struct{})
	require.NoError(t, idx.Upgrade(func() { close(done) }))
	<-done
	require.False(t, idx.IsUpgraded(), "upgrade must have been interrupted, not completed")

	// Persist the partially-written buckets and reopen the index — the restart.
	require.NoError(t, idx.Flush())
	require.NoError(t, idx.Shutdown(ctx))

	idx, err = New(config, uc, store)
	require.NoError(t, err)
	idx.PostStartup(ctx)
	t.Cleanup(func() { idx.Shutdown(context.Background()) })

	require.False(t, idx.IsUpgraded(),
		"an interrupted upgrade must roll back to the flat stage on restart")

	// The core regression: every search must succeed. On the buggy code the flat
	// stage reads wrong-length codes for ids 0..batchSize-1 and fails with
	// "vector lengths don't match". With the fix, results must match the
	// pre-upgrade baseline exactly.
	for i := range queries {
		ids, _, err := idx.SearchByVector(ctx, queries[i], k, nil)
		require.NoErrorf(t, err,
			"search after an interrupted quantized upgrade must not fail on a corrupt compressed bucket (query %d)", i)
		assert.ElementsMatchf(t, baseline[i], ids,
			"repaired flat stage must return its original results for query %d", i)
	}
}
