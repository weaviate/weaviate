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
			ids, _, err := index.SearchByVector(t.Context(), vector, len(vectors), nil, nil)
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
				require.NoError(t, h.Centroids.hnsw.CleanUpTombstonedNodes(func() bool { return false }))
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
					found, _, err := h.SearchByVector(t.Context(), vectors[id], 1, nil, nil)
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
