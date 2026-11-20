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
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
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
	setSinglePostingLimit(t, threshold)
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

// inMemoryVersion reads a version without faulting it in from the store.
func inMemoryVersion(v *VersionMap, id uint64) VectorVersion {
	page, slot := v.data.GetPageFor(id)
	if page == nil {
		return 0
	}
	v.locks.RLock(id)
	defer v.locks.RUnlock(id)
	return page[slot]
}

func TestSinglePostingSurvivesRestart(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc, vectors := singlePostingRestartConfig(t, singlePostingThreshold)

	index := makeHFreshWithConfig(t, store, cfg, uc)
	all := addRestartVectors(t, index, vectors, 0, 300)
	// persisted by the appends, without waiting for a shutdown
	requireSinglePostingSize(t, index, 300)
	requireNoSinglePostingIDs(t, index)
	require.NoError(t, index.Delete(7))
	require.NoError(t, index.Shutdown(t.Context()))

	index = makeHFreshWithConfig(t, store, cfg, uc)
	require.False(t, index.Centroids.Built())
	requireSinglePostingSize(t, index, 300)
	requireNoSinglePostingIDs(t, index)

	require.Eventually(t, func() bool {
		return inMemoryVersion(index.VersionMap, 7).Deleted()
	}, 10*time.Second, 10*time.Millisecond, "the version warmup must load the delete")

	ids, _, err := index.SearchByVector(t.Context(), all[7], 1, nil)
	require.NoError(t, err)
	require.NotEqual(t, []uint64{7}, ids, "delete must survive the restart")
	requireSelfSearchExcept(t, index, all, 7)

	// the restored posting keeps counting towards the threshold
	all = addRestartVectors(t, index, vectors, 300, singlePostingThreshold+1)
	index.waitForMaintenance(t)
	require.True(t, index.Centroids.Built())
	require.Greater(t, index.PostingSizes.Count(), uint64(2))
	requireSelfSearchExcept(t, index, all, 7)
}

// requireSinglePostingSize checks posting 1's size, in memory and in the
// store, where each append persists it.
func requireSinglePostingSize(t *testing.T, index *HFresh, size uint32) {
	t.Helper()
	inMemory, err := index.PostingSizes.Get(t.Context(), 1)
	require.NoError(t, err)
	require.Equal(t, size, inMemory)
	persisted, err := index.PostingSizes.store.Get(t.Context(), 1)
	require.NoError(t, err)
	require.Equal(t, size, persisted)
}

// requireNoSinglePostingIDs checks that posting 1 keeps no id list.
func requireNoSinglePostingIDs(t *testing.T, index *HFresh) {
	t.Helper()
	_, err := index.PostingMap.Get(t.Context(), 1)
	require.ErrorIs(t, err, ErrPostingNotFound)
}

// A merge rewrites the single posting and persists its size, which later
// appends must keep current for the restart.
func TestSinglePostingRestartAfterMerge(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc, vectors := singlePostingRestartConfig(t, singlePostingThreshold)

	index := makeHFreshWithConfig(t, store, cfg, uc)
	addRestartVectors(t, index, vectors, 0, 300)
	for id := range uint64(10) {
		require.NoError(t, index.Delete(id))
		vectors.remove(id)
	}
	require.NoError(t, index.doMerge(t.Context(), 1))
	requireSinglePostingSize(t, index, 290)

	all := addRestartVectors(t, index, vectors, 300, 400)
	requireSinglePostingSize(t, index, 390)
	require.NoError(t, index.Shutdown(t.Context()))

	index = makeHFreshWithConfig(t, store, cfg, uc)
	require.False(t, index.Centroids.Built())
	requireSinglePostingSize(t, index, 390)
	for i := 10; i < 400; i++ {
		ids, _, err := index.SearchByVector(t.Context(), all[i], 1, nil)
		require.NoError(t, err)
		require.Equal(t, []uint64{uint64(i)}, ids, "vector %d", i)
	}
}

// A restart finds the single posting by its fixed id, even when the ID
// sequence was already past it before the first insert.
func TestSinglePostingKeepsIDOne(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc, vectors := singlePostingRestartConfig(t, singlePostingThreshold)

	index := makeHFreshWithConfig(t, store, cfg, uc)
	// a crash after reserving an ID range restarts the sequence above it
	for range 5 {
		_, err := index.IDs.Next()
		require.NoError(t, err)
	}
	all := addRestartVectors(t, index, vectors, 0, 300)
	require.True(t, index.Centroids.Exists(1))
	next, err := index.IDs.Next()
	require.NoError(t, err)
	require.Greater(t, next, uint64(1), "no later posting may take the single posting's id")
	require.NoError(t, index.Shutdown(t.Context()))

	index = makeHFreshWithConfig(t, store, cfg, uc)
	require.True(t, index.Centroids.Exists(1))
	requireSelfSearchExcept(t, index, all, -1)
}

// The single posting has no centroid in the graph, so the startup repair of
// postings without a centroid must not take it for one.
func TestSinglePostingRestartReassignsNothing(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc, vectors := singlePostingRestartConfig(t, singlePostingThreshold)

	index := makeHFreshWithConfig(t, store, cfg, uc)
	all := addRestartVectors(t, index, vectors, 0, 300)
	require.NoError(t, index.Shutdown(t.Context()))

	index = makeHFreshWithConfig(t, store, cfg, uc)
	require.False(t, index.Centroids.Built())
	require.True(t, index.Centroids.Exists(1), "the repair must not delete the single centroid")
	require.Zero(t, index.taskQueue.reassignQueue.Size())
	requireSelfSearchExcept(t, index, all, -1)
}

// Deletes only bump versions, so the posting keeps entries whose objects are
// gone, and the restart must take its centroid from a live one.
func TestSinglePostingRestartSkipsDeletedEntries(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc, vectors := singlePostingRestartConfig(t, singlePostingThreshold)

	index := makeHFreshWithConfig(t, store, cfg, uc)
	all := addRestartVectors(t, index, vectors, 0, 300)
	require.NoError(t, index.Delete(0))
	vectors.remove(0)
	require.NoError(t, index.Shutdown(t.Context()))

	index = makeHFreshWithConfig(t, store, cfg, uc)
	require.False(t, index.Centroids.Built())
	requireSelfSearchExcept(t, index, all, 0)
}

func TestSinglePostingRestartAllDeleted(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc, vectors := singlePostingRestartConfig(t, singlePostingThreshold)

	index := makeHFreshWithConfig(t, store, cfg, uc)
	addRestartVectors(t, index, vectors, 0, 10)
	for id := range uint64(10) {
		require.NoError(t, index.Delete(id))
		vectors.remove(id)
	}
	require.NoError(t, index.Shutdown(t.Context()))

	index = makeHFreshWithConfig(t, store, cfg, uc)
	all := addRestartVectors(t, index, vectors, 10, 300)
	for id := 10; id < 300; id++ {
		ids, _, err := index.SearchByVector(t.Context(), all[id], 1, nil)
		require.NoError(t, err)
		require.Equal(t, []uint64{uint64(id)}, ids, "vector %d", id)
	}
}

// Crossing the limit only queues the first split, which builds the graph out
// of the two halves of the single posting.
func TestFirstSplitBuildsCentroidGraph(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc, vectors := singlePostingRestartConfig(t, 200)

	index := makeHFreshWithConfig(t, store, cfg, uc)
	require.NoError(t, index.taskQueue.splitQueue.Pause(t.Context()))
	require.NoError(t, index.taskQueue.reassignQueue.Pause(t.Context()))
	all := addRestartVectors(t, index, vectors, 0, 600)
	require.False(t, index.Centroids.Built(), "crossing the limit only queues the split")
	requireSelfSearchExcept(t, index, all, -1)

	index.taskQueue.splitQueue.Resume()
	require.Eventually(t, index.Centroids.Built, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, index.taskQueue.splitQueue.Wait(t.Context()))
	require.False(t, index.Centroids.Exists(1))
	require.Zero(t, index.taskQueue.reassignQueue.Size(), "the single posting has no centroid to compare against")

	index.taskQueue.reassignQueue.Resume()
	index.waitForMaintenance(t)
	require.Greater(t, index.PostingSizes.Count(), uint64(1))
	requireSelfSearchExcept(t, index, all, -1)
}

// A first split cut short by a crash leaves postings without a centroid:
// the children before the graph exists, posting 1 after it.
func TestFirstSplitCutShortRecoversOnRestart(t *testing.T) {
	tests := []struct {
		name      string
		interrupt func(t *testing.T, index *HFresh, cfg *Config) (leftover uint64)
		wantBuilt bool
	}{
		{"before the graph exists", func(t *testing.T, index *HFresh, _ *Config) uint64 {
			posting, err := index.PostingStore.Get(t.Context(), 1)
			require.NoError(t, err)
			child, err := index.IDs.Next()
			require.NoError(t, err)
			require.NoError(t, index.PostingStore.Put(t.Context(), child, posting[:50]))
			require.NoError(t, index.setPostingVectorIDs(t.Context(), child, posting[:50]))
			return child
		}, false},
		{"after the graph exists", func(t *testing.T, _ *HFresh, cfg *Config) uint64 {
			hnswCfg := cfg.Centroids.HNSWConfig
			require.NoError(t, os.MkdirAll(filepath.Join(hnswCfg.RootPath, hnswCfg.ID+".hnsw.commitlog.d"), os.ModePerm))
			return 0
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := testinghelpers.NewDummyStore(t)
			cfg, uc, vectors := singlePostingRestartConfig(t, singlePostingThreshold)

			index := makeHFreshWithConfig(t, store, cfg, uc)
			all := addRestartVectors(t, index, vectors, 0, 300)
			leftover := tt.interrupt(t, index, cfg)
			require.NoError(t, index.Shutdown(t.Context()))

			index = makeHFreshWithConfig(t, store, cfg, uc)
			require.Equal(t, tt.wantBuilt, index.Centroids.Built())
			if leftover != 0 {
				require.Positive(t, index.taskQueue.reassignQueue.Size(), "the leftover's vectors are reassigned")
			}
			index.waitForMaintenance(t)
			// the reassigned vectors take posting 1 past the max posting size,
			// but not past the limit
			require.Equal(t, tt.wantBuilt, index.Centroids.Built())
			requireSelfSearchExcept(t, index, all, -1)
		})
	}
}

// A backup taken while the first split runs holds the split's postings but
// not the new graph's commit log directory, whose only file is still active.
func TestFirstSplitSurvivesLostCommitLogDirectory(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc, vectors := singlePostingRestartConfig(t, 200)

	index := makeHFreshWithConfig(t, store, cfg, uc)
	all := addRestartVectors(t, index, vectors, 0, 600)
	index.waitForMaintenance(t)
	require.True(t, index.Centroids.Built())
	require.NoError(t, index.Shutdown(t.Context()))
	hnswCfg := cfg.Centroids.HNSWConfig
	require.NoError(t, os.RemoveAll(filepath.Join(hnswCfg.RootPath, hnswCfg.ID+".hnsw.commitlog.d")))

	index = makeHFreshWithConfig(t, store, cfg, uc)
	index.waitForMaintenance(t)
	requireSelfSearchExcept(t, index, all, -1)
}

// The queues keep running during a backup, and so does the first split: a
// backup that misses the new graph restores the single posting or is repaired
// at startup.
func TestFirstSplitRunsDuringBackup(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc, vectors := singlePostingRestartConfig(t, 200)

	index := makeHFreshWithConfig(t, store, cfg, uc)
	require.NoError(t, index.PrepareForBackup(t.Context()))
	all := addRestartVectors(t, index, vectors, 0, 600)
	index.waitForMaintenance(t)
	require.True(t, index.Centroids.Built())
	requireSelfSearchExcept(t, index, all, -1)

	require.NoError(t, index.ResumeAfterBackup(t.Context()))
	index.waitForMaintenance(t)
	requireSelfSearchExcept(t, index, all, -1)
}

// The queued first split sits in its own queue file, which a crash can lose;
// the restart queues it again for a read-only tenant.
func TestFirstSplitLostInCrashIsRequeued(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc, vectors := singlePostingRestartConfig(t, 200)

	index := makeHFreshWithConfig(t, store, cfg, uc)
	require.NoError(t, index.taskQueue.splitQueue.Pause(t.Context()))
	all := addRestartVectors(t, index, vectors, 0, 600)
	require.NoError(t, index.Shutdown(t.Context()))
	require.NoError(t, os.RemoveAll(filepath.Join(cfg.RootPath, "split.queue.d")))
	index.taskQueue.splitQueue.Resume()

	index = makeHFreshWithConfig(t, store, cfg, uc)
	index.waitForMaintenance(t)
	require.True(t, index.Centroids.Built())
	requireSelfSearchExcept(t, index, all, -1)
}

func TestBuiltCentroidIndexSurvivesRestart(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc, vectors := singlePostingRestartConfig(t, singlePostingThreshold)

	index := makeHFreshWithConfig(t, store, cfg, uc)
	all := addRestartVectors(t, index, vectors, 0, singlePostingThreshold+1)
	index.waitForMaintenance(t)
	require.True(t, index.Centroids.Built())
	postings := index.PostingSizes.Count()
	require.Greater(t, postings, uint64(2))
	require.NoError(t, index.Shutdown(t.Context()))

	index = makeHFreshWithConfig(t, store, cfg, uc)
	require.True(t, index.Centroids.Built(), "a built centroid index must be reloaded")
	require.Greater(t, index.PostingMap.Size(), 2)
	require.Equal(t, postings, index.PostingSizes.Count())
	requireSelfSearchExcept(t, index, all, -1)
}

// Turning the feature off creates the graph at open, and the startup repair
// moves the single posting into it, whatever its size.
func TestSinglePostingReopenedWithFeatureOff(t *testing.T) {
	tests := []struct {
		name    string
		vectors int
	}{
		{"above the max posting size", 600},
		{"below the max posting size", 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := testinghelpers.NewDummyStore(t)
			cfg, uc, vectors := singlePostingRestartConfig(t, singlePostingThreshold)

			index := makeHFreshWithConfig(t, store, cfg, uc)
			all := addRestartVectors(t, index, vectors, 0, tt.vectors)
			require.NoError(t, index.Shutdown(t.Context()))

			setSinglePostingLimit(t, 0)
			index = makeHFreshWithConfig(t, store, cfg, uc)
			require.True(t, index.Centroids.Built())
			index.waitForMaintenance(t)
			require.False(t, index.Centroids.Exists(1))
			requireSelfSearchExcept(t, index, all, -1)
		})
	}
}

// A lowered limit must apply when the shard opens, not only on the next
// insert, which a read-only tenant never makes. A posting too small to split
// stays single until it grows.
func TestSinglePostingReopenedWithLowerLimit(t *testing.T) {
	tests := []struct {
		name      string
		vectors   int
		limit     uint32 // when reopened, in vectors
		wantBuilt bool
	}{
		{"over the lowered limit", 600, 100, true},
		{"under the lowered limit", 600, 800, false},
		{"too small to split", 300, 100, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := testinghelpers.NewDummyStore(t)
			cfg, uc, vectors := singlePostingRestartConfig(t, singlePostingThreshold)

			index := makeHFreshWithConfig(t, store, cfg, uc)
			all := addRestartVectors(t, index, vectors, 0, tt.vectors)
			require.NoError(t, index.Shutdown(t.Context()))

			setSinglePostingLimit(t, tt.limit)
			index = makeHFreshWithConfig(t, store, cfg, uc)
			index.waitForMaintenance(t)
			require.Equal(t, tt.wantBuilt, index.Centroids.Built())
			requireSelfSearchExcept(t, index, all, -1)
		})
	}
}

// An index restarted before its first insert has no dimensions to restore,
// yet must come back in the mode its commit log directory records.
func TestEmptyIndexReopenKeepsCentroidMode(t *testing.T) {
	tests := []struct {
		name          string
		createVectors uint32 // single-posting limit when created, in vectors
		reopenVectors uint32 // and when reopened
		wantBuilt     bool
	}{
		{"feature off, reopened off", 0, 0, true},
		{"feature off, reopened on", 0, singlePostingThreshold, true},
		{"feature on, reopened off", singlePostingThreshold, 0, true},
		{"feature on, reopened on", singlePostingThreshold, singlePostingThreshold, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := testinghelpers.NewDummyStore(t)
			cfg, uc, vectors := singlePostingRestartConfig(t, tt.createVectors)

			index := makeHFreshWithConfig(t, store, cfg, uc)
			require.NoError(t, index.Shutdown(t.Context()))

			setSinglePostingLimit(t, tt.reopenVectors)
			index = makeHFreshWithConfig(t, store, cfg, uc)
			require.Equal(t, tt.wantBuilt, index.Centroids.Built())

			// enough vectors to split, and to cross the single-posting limit
			all := addRestartVectors(t, index, vectors, 0, singlePostingThreshold+200)
			index.waitForMaintenance(t)
			require.Greater(t, index.PostingSizes.Count(), uint64(1), "the posting must split")
			requireSelfSearchExcept(t, index, all, -1)
		})
	}
}

// The commit log can record a posting ID before the store's next flush, so
// the sequence bound covering that ID must already be in its file.
func TestPostingIDBoundReachesFileBeforeUse(t *testing.T) {
	storeDir := t.TempDir()
	store := testinghelpers.NewDummyStoreFromFolder(storeDir, t)
	cfg, uc := makeHFreshConfig(t)
	t.Cleanup(func() { cfg.Scheduler.Close(context.Background()) })
	index := makeHFreshWithConfig(t, store, cfg, uc)

	id, err := index.IDs.Next()
	require.NoError(t, err)

	// copying the files of a running store keeps only what reached them,
	// as a process crash does
	crashed := filepath.Join(t.TempDir(), "crashed")
	require.NoError(t, os.CopyFS(crashed, os.DirFS(storeDir)))
	crashedStore := testinghelpers.NewDummyStoreFromFolder(crashed, t)
	bucket, err := NewSharedBucket(crashedStore, cfg.ID, cfg.Store)
	require.NoError(t, err)
	bound, err := NewBucketStore(bucket).Load()
	require.NoError(t, err)
	require.GreaterOrEqual(t, bound, id)
}

// A reassign that saved its version bump but lost its new copy leaves the
// orphan's stale entry as the only record of the vector, so the repair must
// not drop it.
func TestRepairKeepsOrphanWithOnlyStaleEntries(t *testing.T) {
	tf := createHFreshIndex(t)
	postingID, posting := createPostingWithVectors(t, &tf, [][]float32{{1, 0, 0, 0}, {0, 1, 0, 0}}, 100)
	require.NoError(t, tf.Index.PostingStore.Put(t.Context(), postingID, posting))
	require.NoError(t, tf.Index.setPostingVectorIDs(t.Context(), postingID, posting))
	for _, v := range posting {
		_, err := tf.Index.VersionMap.Increment(t.Context(), v.ID(), v.Version())
		require.NoError(t, err)
	}

	tf.Index.repairOrphanPostings()

	metadata, err := tf.Index.PostingMap.Get(t.Context(), postingID)
	require.NoError(t, err)
	require.EqualValues(t, 2, metadata.Count())
	stored, err := tf.Index.PostingStore.Get(t.Context(), postingID)
	require.NoError(t, err)
	require.Len(t, stored, 2)
}

// partialFlushCase rolls one directory back to a snapshot taken before mutate
// ran, as a process crash leaves it when that side's buffered writes never
// reached the file while the other side's did.
type partialFlushCase struct {
	name     string
	setup    func(t *testing.T, h *HFresh)
	mutate   func(t *testing.T, h *HFresh) (deleted []uint64)
	rollback func(cfg *Config, storeDir string) string
	// reachable lists the vectors that must be found after the restart
	reachable func(deleted []uint64) []uint64
	// check runs on the restarted index, before any new write
	check func(t *testing.T, h *HFresh)
}

func TestRecoveryAfterPartialFlush(t *testing.T) {
	// the final phase adds 320 vectors, more than one posting holds at the
	// 8 KB floor and 8 dimensions (316), so even an empty recovered store splits
	vectors, _ := testinghelpers.RandomVecsFixedSeed(720, 0, 8)
	idRange := func(from, to int) []uint64 {
		ids := make([]uint64, 0, to-from)
		for id := from; id < to; id++ {
			ids = append(ids, uint64(id))
		}
		return ids
	}
	add := func(t *testing.T, h *HFresh, from, to int) {
		for id := from; id < to; id++ {
			require.NoError(t, h.Add(t.Context(), uint64(id), vectors[id]))
		}
	}
	waitIdle := func(t *testing.T, h *HFresh) {
		require.Eventually(t, func() bool { return h.taskQueue.Size() == 0 },
			30*time.Second, 10*time.Millisecond)
	}
	// addGradually lets each split run before the next batch, so no posting
	// grows past what one split brings back under the limit
	addGradually := func(t *testing.T, h *HFresh, from, to int) {
		for start := from; start < to; start += 100 {
			add(t, h, start, min(start+100, to))
			waitIdle(t, h)
		}
	}
	commitLog := func(cfg *Config, _ string) string {
		return filepath.Join(cfg.Centroids.HNSWConfig.RootPath, cfg.Centroids.HNSWConfig.ID+".hnsw.commitlog.d")
	}
	wholeStore := func(_ *Config, storeDir string) string { return storeDir }
	centroidVectorsBucket := func(_ *Config, storeDir string) string {
		return filepath.Join(storeDir, helpers.VectorsCompressedBucketLSM)
	}
	livePostings := func(h *HFresh) (live int, smallest uint64) {
		var smallestCount uint32
		for id, md := range h.PostingMap.Iter() {
			if md.Count() == 0 || !h.Centroids.Exists(id) {
				continue
			}
			live++
			if smallestCount == 0 || md.Count() < smallestCount {
				smallest, smallestCount = id, md.Count()
			}
		}
		return live, smallest
	}
	// shrinkAndMerge deletes most of the smallest posting and merges it
	shrinkAndMerge := func(t *testing.T, h *HFresh) []uint64 {
		live, smallest := livePostings(h)
		require.Greater(t, live, 1)

		md, err := h.PostingMap.Get(t.Context(), smallest)
		require.NoError(t, err)
		var deleted []uint64
		for id := range md.Iter() {
			if len(deleted) < int(md.Count())-5 {
				deleted = append(deleted, id)
			}
		}
		require.NoError(t, h.Delete(deleted...))
		require.NoError(t, h.doMerge(t.Context(), smallest))
		after, _ := livePostings(h)
		require.Equal(t, live-1, after, "the merge must remove a posting")
		return deleted
	}

	var highestPostingID uint64

	cases := []partialFlushCase{
		{
			name:      "split, commit log behind store",
			setup:     func(t *testing.T, h *HFresh) { add(t, h, 0, 100) },
			mutate:    func(t *testing.T, h *HFresh) []uint64 { add(t, h, 100, 400); return nil },
			rollback:  commitLog,
			reachable: func([]uint64) []uint64 { return idRange(0, 400) },
		},
		{
			name:      "split, store behind commit log",
			setup:     func(t *testing.T, h *HFresh) { add(t, h, 0, 100) },
			mutate:    func(t *testing.T, h *HFresh) []uint64 { add(t, h, 100, 400); return nil },
			rollback:  wholeStore,
			reachable: func([]uint64) []uint64 { return idRange(0, 100) },
		},
		{
			name:      "merge, store behind commit log",
			setup:     func(t *testing.T, h *HFresh) { addGradually(t, h, 0, 400) },
			mutate:    shrinkAndMerge,
			rollback:  wholeStore,
			reachable: func([]uint64) []uint64 { return idRange(0, 400) },
		},
		{
			name:     "merge, commit log behind store",
			setup:    func(t *testing.T, h *HFresh) { addGradually(t, h, 0, 400) },
			mutate:   shrinkAndMerge,
			rollback: commitLog,
			reachable: func(deleted []uint64) []uint64 {
				return slices.DeleteFunc(idRange(0, 400), func(id uint64) bool { return slices.Contains(deleted, id) })
			},
		},
		{
			name:  "IDs of cleaned up centroids, store behind commit log",
			setup: func(t *testing.T, h *HFresh) { add(t, h, 0, 100) },
			mutate: func(t *testing.T, h *HFresh) []uint64 {
				add(t, h, 100, 400)
				waitIdle(t, h)
				require.NoError(t, h.Centroids.state.Load().hnsw.CleanUpTombstonedNodes(func() bool { return false }))
				for id := range h.PostingMap.Iter() {
					highestPostingID = max(highestPostingID, id)
				}
				return nil
			},
			rollback:  wholeStore,
			reachable: func([]uint64) []uint64 { return idRange(0, 100) },
			check: func(t *testing.T, h *HFresh) {
				id, err := h.IDs.Next()
				require.NoError(t, err)
				require.Greater(t, id, highestPostingID, "a freed centroid slot may still be cleaned up or tombstoned")
			},
		},
		{
			name:      "split, centroid vectors bucket behind commit log",
			setup:     func(t *testing.T, h *HFresh) { add(t, h, 0, 100) },
			mutate:    func(t *testing.T, h *HFresh) []uint64 { add(t, h, 100, 400); return nil },
			rollback:  centroidVectorsBucket,
			reachable: func([]uint64) []uint64 { return idRange(0, 400) },
		},
		{
			name:      "first posting, commit log behind store",
			setup:     func(*testing.T, *HFresh) {},
			mutate:    func(t *testing.T, h *HFresh) []uint64 { add(t, h, 0, 100); return nil },
			rollback:  commitLog,
			reachable: func([]uint64) []uint64 { return idRange(0, 100) },
		},
		{
			name:      "first posting, store behind commit log",
			setup:     func(*testing.T, *HFresh) {},
			mutate:    func(t *testing.T, h *HFresh) []uint64 { add(t, h, 0, 100); return nil },
			rollback:  wholeStore,
			reachable: func([]uint64) []uint64 { return nil },
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			storeDir := t.TempDir()
			store := testinghelpers.NewDummyStoreFromFolder(storeDir, t)
			cfg, uc := makeHFreshConfig(t)
			uc.MaxPostingSizeKB = ent.MaxPostingSizeKBFloor
			t.Cleanup(func() { cfg.Scheduler.Close(context.Background()) })
			cfg.VectorForIDThunk = func(_ context.Context, id uint64) ([]float32, error) {
				return vectors[id], nil
			}

			// each phase ends with a clean shutdown of the index and the store
			phase := func(run func(h *HFresh)) {
				h := makeHFreshWithConfig(t, store, cfg, uc)
				run(h)
				waitIdle(t, h)
				require.NoError(t, h.Shutdown(t.Context()))
				require.NoError(t, store.Shutdown(t.Context()))
				store = testinghelpers.NewDummyStoreFromFolder(storeDir, t)
			}
			requireReachable := func(h *HFresh, ids []uint64) {
				var missing []uint64
				for _, id := range ids {
					found, _, err := h.SearchByVector(t.Context(), vectors[id], 1, nil)
					require.NoError(t, err)
					if len(found) == 0 || found[0] != id {
						missing = append(missing, id)
					}
				}
				require.Empty(t, missing, "%d of %d vectors are unreachable", len(missing), len(ids))
			}

			phase(func(h *HFresh) { c.setup(t, h) })

			dir := c.rollback(cfg, storeDir)
			snapshot := filepath.Join(t.TempDir(), "snapshot")
			_, err := os.Stat(dir)
			hadDir := err == nil
			if hadDir {
				require.NoError(t, os.CopyFS(snapshot, os.DirFS(dir)))
			}

			var deleted []uint64
			phase(func(h *HFresh) { deleted = c.mutate(t, h) })

			// the posting ID bound is written to its file before any of its IDs
			// is used, so no crash loses it
			idBound := func() *BucketStore {
				bucket, err := NewSharedBucket(store, cfg.ID, cfg.Store)
				require.NoError(t, err)
				return NewBucketStore(bucket)
			}
			flushedBound, err := idBound().Load()
			require.NoError(t, err)

			require.NoError(t, store.Shutdown(t.Context()))
			require.NoError(t, os.RemoveAll(dir))
			if hadDir {
				require.NoError(t, os.CopyFS(dir, os.DirFS(snapshot)))
			}
			store = testinghelpers.NewDummyStoreFromFolder(storeDir, t)
			bound, err := idBound().Load()
			require.NoError(t, err)
			if bound < flushedBound {
				require.NoError(t, idBound().Store(flushedBound))
			}

			reachable := c.reachable(deleted)
			phase(func(h *HFresh) {
				// the startup repair works through the task queues
				waitIdle(t, h)
				requireReachable(h, reachable)
				if c.check != nil {
					c.check(t, h)
				}
			})

			// the recovered index must keep accepting writes and splitting
			phase(func(h *HFresh) {
				before, _ := livePostings(h)
				add(t, h, 400, 720)
				waitIdle(t, h)
				// going from no posting to one is not a split
				after, _ := livePostings(h)
				require.Greater(t, after, max(before, 1), "the recovered index must split")
				requireReachable(h, append(reachable, idRange(400, 720)...))
			})
		})
	}
}
