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

package hfresh

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/compact"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
)

func TestQuantizationDataSurvivesRestart(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	index := makeHFreshWithConfig(t, store, cfg, uc)

	vectors, _ := testinghelpers.RandomVecs(1, 0, 64)
	err := index.Add(t.Context(), 0, vectors[0])
	require.NoError(t, err)

	// capture quantization data before shutdown
	require.NotNil(t, index.quantizer)
	dataBefore := index.quantizer.Data()

	err = index.Shutdown(t.Context())
	require.NoError(t, err)

	// reopen with the same store and config
	index2 := makeHFreshWithConfig(t, store, cfg, uc)

	require.NotNil(t, index2.quantizer)
	dataAfter := index2.quantizer.Data()

	require.Equal(t, dataBefore.InputDim, dataAfter.InputDim)
	require.Equal(t, dataBefore.Bits, dataAfter.Bits)
	require.Equal(t, dataBefore.Rounding, dataAfter.Rounding)

	require.Equal(t, dataBefore.Rotation.OutputDim, dataAfter.Rotation.OutputDim)
	require.Equal(t, dataBefore.Rotation.Rounds, dataAfter.Rotation.Rounds)
	require.Equal(t, dataBefore.Rotation.Swaps, dataAfter.Rotation.Swaps)
	require.Equal(t, dataBefore.Rotation.Signs, dataAfter.Rotation.Signs)
}

func TestSingleCentroidSurvivesCompactionAndRestart(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)
	vectors, _ := testinghelpers.RandomVecsFixedSeed(3, 1, 64)
	cfg.VectorForIDThunk = func(_ context.Context, id uint64) ([]float32, error) {
		return vectors[id], nil
	}
	index := makeHFreshWithConfig(t, store, cfg, uc)
	for id, vector := range vectors {
		require.NoError(t, index.Add(t.Context(), uint64(id), vector))
	}
	require.Eventually(t, func() bool { return index.taskQueue.Size() == 0 }, 10*time.Second, 10*time.Millisecond)

	for round := 0; round < 3; round++ {
		for _, vector := range vectors {
			ids, _, err := index.SearchByVector(t.Context(), vector, len(vectors), nil)
			require.NoError(t, err)
			require.ElementsMatch(t, []uint64{0, 1, 2}, ids, "restart %d", round)
		}
		if round == 2 {
			break
		}
		require.NoError(t, index.Shutdown(t.Context()))
		dir := filepath.Join(cfg.RootPath, "centroids.hnsw.commitlog.d")
		// A newer active WAL makes the closed log eligible for compaction,
		// as happens on restart, regardless of the size of the closed log.
		require.NoError(t, os.WriteFile(filepath.Join(dir, "999999999999999999"), nil, 0o600))
		compactor := compact.NewCompactor(compact.DefaultCompactorConfig(dir), cfg.Logger)
		action, err := compactor.RunCycle(nil)
		require.NoError(t, err)
		if round == 0 {
			require.Equal(t, compact.ActionCreateSnapshot, action)
		}
		index = makeHFreshWithConfig(t, store, cfg, uc)
	}
}

func TestRestoreMetadataMigratesPostingMapV1ToV2(t *testing.T) {
	ctx := t.Context()
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	bucket, err := NewSharedBucket(store, cfg.ID, cfg.Store)
	require.NoError(t, err)

	err = NewIndexMetadataStore(bucket).SetDimensions(64)
	require.NoError(t, err)

	err = mustBucket(t, bucket).Put(postingMapKey(postingMapBucketPrefixV1, 42), legacyPackedPostingMetadata(10, 20, 30))
	require.NoError(t, err)

	index := makeHFreshWithConfig(t, store, cfg, uc)

	metadata, err := index.PostingMap.Get(ctx, 42)
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 20, 30}, decodePacked(metadata.PackedPostingMetadata))

	size, err := index.PostingSizes.Get(ctx, 42)
	require.NoError(t, err)
	require.EqualValues(t, 3, size)

	persisted, err := NewPostingMapStore(bucket, postingMapBucketPrefixV2).Get(ctx, 42)
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 20, 30}, decodePacked(persisted))

	persistedSize, err := NewPostingSizesStore(bucket, postingSizesBucketPrefix).Get(ctx, 42)
	require.NoError(t, err)
	require.EqualValues(t, 3, persistedSize)
	require.Equal(t, 0, countKeysWithPrefix(mustBucket(t, bucket), postingMapBucketPrefixV1))
}

func TestStartupDeletesLegacyReassignBucketKey(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	err := store.CreateOrLoadBucket(context.Background(),
		sharedBucketName(cfg.ID),
		cfg.Store.MakeBucketOptions(lsmkv.StrategyReplace, lsmkv.WithForceCompaction(true))...,
	)
	require.NoError(t, err)

	bucket := store.Bucket(sharedBucketName(cfg.ID))
	require.NoError(t, bucket.Put(reassignBucketKey, []byte("legacy-reassign-dedup")))

	unrelatedKey := []byte{sharedBucketVersionV1, 9, 0}
	unrelatedValue := []byte("keep-me")
	require.NoError(t, bucket.Put(unrelatedKey, unrelatedValue))

	index := makeHFreshWithConfig(t, store, cfg, uc)

	data, err := mustBucket(t, index.IndexMetadata.bucket).Get(reassignBucketKey)
	require.NoError(t, err)
	require.Nil(t, data)

	data, err = mustBucket(t, index.IndexMetadata.bucket).Get(unrelatedKey)
	require.NoError(t, err)
	require.Equal(t, unrelatedValue, data)
}

// singlePostingRestartConfig wires a real centroid commit logger and a
// vector store, so the bootstrap split can run after a restart.
func singlePostingRestartConfig(t *testing.T, threshold uint32) (*Config, ent.UserConfig, *testVectorStore) {
	cfg, uc := makeHFreshConfig(t)
	cfg.BootstrapThreshold = threshold
	uc.MaxPostingSizeKB = ent.MaxPostingSizeKBFloor
	vectors := &testVectorStore{m: make(map[uint64][]float32)}
	cfg.VectorForIDThunk = vectors.get
	return cfg, uc, vectors
}

func addRestartVectors(t *testing.T, index *HFresh, store *testVectorStore, from, to int) [][]float32 {
	t.Helper()
	all, _ := testinghelpers.RandomVecsFixedSeed(to, 0, singlePostingDims)
	for i := from; i < to; i++ {
		store.put(uint64(i), all[i])
		require.NoError(t, index.Add(t.Context(), uint64(i), all[i]))
	}
	return all
}

func requireSelfSearchExcept(t *testing.T, index *HFresh, vectors [][]float32, skip int) {
	t.Helper()
	for i, v := range vectors {
		if i == skip {
			continue
		}
		ids, _, err := index.SearchByVector(t.Context(), v, 1, nil)
		require.NoError(t, err)
		require.Equal(t, []uint64{uint64(i)}, ids, "vector %d", i)
	}
}

func TestSinglePostingSurvivesRestart(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc, vectors := singlePostingRestartConfig(t, singlePostingThreshold)

	index := makeHFreshWithConfig(t, store, cfg, uc)
	all := addRestartVectors(t, index, vectors, 0, 300)
	require.NoError(t, index.Delete(7))
	require.NoError(t, index.Shutdown(t.Context()))

	index = makeHFreshWithConfig(t, store, cfg, uc)
	require.False(t, index.Centroids.Built())
	size, err := index.PostingSizes.Get(t.Context(), 1)
	require.NoError(t, err)
	require.EqualValues(t, 300, size)

	// every version is in memory before the first query, so the scan never
	// faults versions in from the store, and the persisted delete wins
	for id := range uint64(300) {
		page, slot := index.VersionMap.data.GetPageFor(id)
		require.NotNil(t, page, "vector %d", id)
		require.NotZero(t, page[slot], "vector %d", id)
		require.Equal(t, id == 7, page[slot].Deleted(), "vector %d", id)
	}

	ids, _, err := index.SearchByVector(t.Context(), all[7], 1, nil)
	require.NoError(t, err)
	require.NotEqual(t, []uint64{7}, ids, "delete must survive the restart")
	requireSelfSearchExcept(t, index, all, 7)

	// the restored posting keeps counting towards the threshold
	all = addRestartVectors(t, index, vectors, 300, singlePostingThreshold+1)
	require.True(t, index.Centroids.Built())
	index.waitForMaintenance(t)
	require.Greater(t, index.PostingSizes.Count(), uint64(2))
	requireSelfSearchExcept(t, index, all, 7)
}

func TestSinglePostingRecoveredWithoutRecord(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc, vectors := singlePostingRestartConfig(t, singlePostingThreshold)

	index := makeHFreshWithConfig(t, store, cfg, uc)
	all := addRestartVectors(t, index, vectors, 0, 300)

	// an index written before the record existed has posting 1 on disk only
	bucket, release, err := index.IndexMetadata.bucket.acquire()
	require.NoError(t, err)
	require.NoError(t, bucket.Delete(index.IndexMetadata.key(singlePostingKey)))
	release()
	require.NoError(t, index.Shutdown(t.Context()))

	index = makeHFreshWithConfig(t, store, cfg, uc)
	require.False(t, index.Centroids.Built())
	size, err := index.PostingSizes.Get(t.Context(), 1)
	require.NoError(t, err)
	require.EqualValues(t, 300, size)
	requireSelfSearchExcept(t, index, all, -1)

	all = addRestartVectors(t, index, vectors, 300, singlePostingThreshold+1)
	require.True(t, index.Centroids.Built())
	index.waitForMaintenance(t)
	require.Greater(t, index.PostingSizes.Count(), uint64(2))
	requireSelfSearchExcept(t, index, all, -1)
}

func TestBuiltCentroidIndexSurvivesRestart(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc, vectors := singlePostingRestartConfig(t, singlePostingThreshold)

	index := makeHFreshWithConfig(t, store, cfg, uc)
	all := addRestartVectors(t, index, vectors, 0, singlePostingThreshold+1)
	require.True(t, index.Centroids.Built())
	index.waitForMaintenance(t)
	postings := index.PostingSizes.Count()
	require.Greater(t, postings, uint64(2))
	require.NoError(t, index.Shutdown(t.Context()))

	index = makeHFreshWithConfig(t, store, cfg, uc)
	require.True(t, index.Centroids.Built(), "a built centroid index must be reloaded")
	require.Equal(t, postings, index.PostingSizes.Count())
	requireSelfSearchExcept(t, index, all, -1)
}
