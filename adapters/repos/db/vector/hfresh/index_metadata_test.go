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
}

func TestRecoveryAfterPartialFlush(t *testing.T) {
	vectors, _ := testinghelpers.RandomVecsFixedSeed(1400, 0, 8)
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
	commitLog := func(cfg *Config, _ string) string {
		return filepath.Join(cfg.Centroids.HNSWConfig.RootPath, cfg.Centroids.HNSWConfig.ID+".hnsw.commitlog.d")
	}
	wholeStore := func(_ *Config, storeDir string) string { return storeDir }

	cases := []partialFlushCase{
		{
			name:      "split, commit log behind store",
			setup:     func(t *testing.T, h *HFresh) { add(t, h, 0, 200) },
			mutate:    func(t *testing.T, h *HFresh) []uint64 { add(t, h, 200, 800); return nil },
			rollback:  commitLog,
			reachable: func([]uint64) []uint64 { return idRange(0, 800) },
		},
		{
			name:      "split, store behind commit log",
			setup:     func(t *testing.T, h *HFresh) { add(t, h, 0, 200) },
			mutate:    func(t *testing.T, h *HFresh) []uint64 { add(t, h, 200, 800); return nil },
			rollback:  wholeStore,
			reachable: func([]uint64) []uint64 { return idRange(0, 200) },
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
				require.Eventually(t, func() bool { return h.taskQueue.Size() == 0 },
					30*time.Second, 10*time.Millisecond)
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

			require.NoError(t, store.Shutdown(t.Context()))
			require.NoError(t, os.RemoveAll(dir))
			if hadDir {
				require.NoError(t, os.CopyFS(dir, os.DirFS(snapshot)))
			}
			store = testinghelpers.NewDummyStoreFromFolder(storeDir, t)

			reachable := c.reachable(deleted)
			phase(func(h *HFresh) { requireReachable(h, reachable) })

			// the recovered index must keep accepting writes and splitting
			phase(func(h *HFresh) {
				add(t, h, 800, 1400)
				require.Eventually(t, func() bool { return h.taskQueue.Size() == 0 },
					30*time.Second, 10*time.Millisecond)
				requireReachable(h, append(reachable, idRange(800, 1400)...))
			})
		})
	}
}
