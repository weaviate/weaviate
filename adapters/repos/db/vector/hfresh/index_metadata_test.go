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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
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

// The first posting gets its vectors through the insert fast path only; its
// analyze task must not be needed for search to find them after a restart.
func TestPostingFoundAfterRestartBeforeAnalyze(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)
	// a scheduler that never runs, so the analyze tasks stay pending
	cfg.Scheduler = queue.NewScheduler(queue.SchedulerOptions{Logger: cfg.Logger})
	vectors := &testVectorStore{m: make(map[uint64][]float32)}
	cfg.VectorForIDThunk = vectors.get

	index := makeHFreshWithConfig(t, store, cfg, uc)
	vecs, _ := testinghelpers.RandomVecs(20, 0, 32)
	for i, v := range vecs {
		vectors.put(uint64(i), v)
		require.NoError(t, index.Add(t.Context(), uint64(i), v))
	}
	require.EqualValues(t, 1, index.PostingSizes.Count(), "the test needs a single, never analyzed posting")

	ids, _, err := index.SearchByVector(t.Context(), vecs[7], 3, nil)
	require.NoError(t, err)
	require.Contains(t, ids, uint64(7))

	require.NoError(t, index.Shutdown(t.Context()))
	index = makeHFreshWithConfig(t, store, cfg, uc)

	ids, _, err = index.SearchByVector(t.Context(), vecs[7], 3, nil)
	require.NoError(t, err)
	require.Contains(t, ids, uint64(7))
}
