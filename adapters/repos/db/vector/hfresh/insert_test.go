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
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func TestHFreshOptimizedPostingSize(t *testing.T) {
	tests := []struct {
		name                   string
		maxPostingSizeKB       uint32
		vectorDim              int
		expectedMaxPostingSize int
	}{
		{
			name:                   "max posting size kb defaults 1536 dim",
			maxPostingSizeKB:       48,
			vectorDim:              1536,
			expectedMaxPostingSize: 227,
		},
		{
			name:                   "max posting size kb defaults 768 dim",
			maxPostingSizeKB:       48,
			vectorDim:              768,
			expectedMaxPostingSize: 407,
		},
		{
			name:                   "max posting size kb defaults 512 dim",
			maxPostingSizeKB:       48,
			vectorDim:              512,
			expectedMaxPostingSize: 553,
		},
		{
			name:                   "max posting size kb defaults 256 dim",
			maxPostingSizeKB:       48,
			vectorDim:              256,
			expectedMaxPostingSize: 863,
		},
		{
			name:                   "max posting size kb set by the user",
			maxPostingSizeKB:       56,
			vectorDim:              256,
			expectedMaxPostingSize: 1007,
		},
		{
			name:                   "max posting size kb large vector",
			maxPostingSizeKB:       8,
			vectorDim:              4096,
			expectedMaxPostingSize: 16,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			scheduler := queue.NewScheduler(
				queue.SchedulerOptions{
					Logger: logrus.New(),
				},
			)
			cfg.Scheduler = scheduler
			cfg.RootPath = t.TempDir()
			cfg.Centroids.HNSWConfig = &hnsw.Config{
				RootPath:              t.TempDir(),
				ID:                    "hfresh",
				MakeCommitLoggerThunk: makeNoopCommitLogger,
				DistanceProvider:      distancer.NewCosineDistanceProvider(),
				MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
				AllocChecker:          memwatch.NewDummyMonitor(),
				GetViewThunk:          getViewThunk,
			}
			cfg.TombstoneCallbacks = cyclemanager.NewCallbackGroupNoop()

			scheduler.Start()
			defer scheduler.Close()

			uc := ent.NewDefaultUserConfig()
			uc.MaxPostingSizeKB = tt.maxPostingSizeKB
			store := testinghelpers.NewDummyStore(t)

			index, err := New(cfg, uc, store)
			require.NoError(t, err)
			defer index.Shutdown(t.Context())

			vector := make([]float32, tt.vectorDim)
			err = index.Add(t.Context(), 0, vector)
			require.NoError(t, err)

			maxPostingSize := index.maxPostingSize
			require.Equal(t, tt.expectedMaxPostingSize, int(maxPostingSize))
		})
	}
}

func TestValidateBeforeInsert(t *testing.T) {
	t.Run("accepts any vector when dims not yet set", func(t *testing.T) {
		tf := createHFreshIndex(t)
		defer tf.Index.Shutdown(t.Context())

		err := tf.Index.ValidateBeforeInsert([]float32{1, 2, 3})
		require.NoError(t, err)
	})

	t.Run("accepts matching dimensions", func(t *testing.T) {
		tf := createHFreshIndex(t)
		defer tf.Index.Shutdown(t.Context())

		vectors, _ := testinghelpers.RandomVecs(2, 0, 32)
		err := tf.Index.Add(t.Context(), 0, vectors[0])
		require.NoError(t, err)

		err = tf.Index.ValidateBeforeInsert(vectors[1])
		require.NoError(t, err)
	})

	t.Run("rejects mismatched dimensions", func(t *testing.T) {
		tf := createHFreshIndex(t)
		defer tf.Index.Shutdown(t.Context())

		vectors32, _ := testinghelpers.RandomVecs(1, 0, 32)
		vectors64, _ := testinghelpers.RandomVecs(1, 0, 64)

		err := tf.Index.Add(t.Context(), 0, vectors32[0])
		require.NoError(t, err)

		err = tf.Index.ValidateBeforeInsert(vectors64[0])
		require.Error(t, err)
		assert.Contains(t, err.Error(), "length 64")
		assert.Contains(t, err.Error(), "length 32")
	})

	t.Run("rejects mismatched dimensions after restart", func(t *testing.T) {
		store := testinghelpers.NewDummyStore(t)
		cfg, uc := makeHFreshConfig(t)

		index := makeHFreshWithConfig(t, store, cfg, uc)

		vectors32, _ := testinghelpers.RandomVecs(1, 0, 32)
		vectors64, _ := testinghelpers.RandomVecs(1, 0, 64)

		err := index.Add(t.Context(), 0, vectors32[0])
		require.NoError(t, err)
		require.Equal(t, uint32(32), index.dims)

		err = index.Shutdown(t.Context())
		require.NoError(t, err)

		index2 := makeHFreshWithConfig(t, store, cfg, uc)

		require.Equal(t, uint32(32), index2.dims)

		err = index2.ValidateBeforeInsert(vectors64[0])
		require.Error(t, err)
		assert.Contains(t, err.Error(), "length 64")
		assert.Contains(t, err.Error(), "length 32")

		err = index2.ValidateBeforeInsert(vectors32[0])
		require.NoError(t, err)
	})
}

func TestAdd(t *testing.T) {
	t.Run("add single vector", func(t *testing.T) {
		tf := createHFreshIndex(t)
		defer tf.Index.Shutdown(t.Context())

		vectors, _ := testinghelpers.RandomVecs(1, 0, 32)

		err := tf.Index.Add(t.Context(), 1, vectors[0])
		require.NoError(t, err)

		// verify dimensions were initialized
		require.Equal(t, uint32(32), tf.Index.dims)
	})

	t.Run("add multiple vectors sequentially", func(t *testing.T) {
		tf := createHFreshIndex(t)
		defer tf.Index.Shutdown(t.Context())

		count := 20
		vectors, _ := testinghelpers.RandomVecs(count, 0, 32)
		for i := range count {
			err := tf.Index.Add(t.Context(), uint64(i), vectors[i])
			require.NoError(t, err)
		}

		for i := range count {
			require.True(t, tf.Index.ContainsDoc(uint64(i)), "vector %d should exist", i)
		}
	})

	t.Run("add with cancelled context", func(t *testing.T) {
		tf := createHFreshIndex(t)
		defer tf.Index.Shutdown(t.Context())

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		vectors, _ := testinghelpers.RandomVecs(1, 0, 32)
		err := tf.Index.Add(ctx, 1, vectors[0])
		require.Error(t, err)
	})
}

func TestAddBatch(t *testing.T) {
	t.Run("add batch of vectors", func(t *testing.T) {
		tf := createHFreshIndex(t)
		defer tf.Index.Shutdown(t.Context())

		count := 10
		vectors, _ := testinghelpers.RandomVecs(count, 0, 32)
		ids := make([]uint64, count)
		for i := range count {
			ids[i] = uint64(i)
		}

		err := tf.Index.AddBatch(t.Context(), ids, vectors)
		require.NoError(t, err)

		for _, id := range ids {
			require.True(t, tf.Index.ContainsDoc(id), "vector %d should exist", id)
		}
	})

	t.Run("error on mismatched ids and vectors length", func(t *testing.T) {
		tf := createHFreshIndex(t)
		defer tf.Index.Shutdown(t.Context())

		vectors, _ := testinghelpers.RandomVecs(2, 0, 32)
		ids := []uint64{0, 1, 2}

		err := tf.Index.AddBatch(t.Context(), ids, vectors)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not match")
	})

	t.Run("error on empty lists", func(t *testing.T) {
		tf := createHFreshIndex(t)
		defer tf.Index.Shutdown(t.Context())

		err := tf.Index.AddBatch(t.Context(), []uint64{}, [][]float32{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty")
	})

	t.Run("cancelled context stops batch midway", func(t *testing.T) {
		tf := createHFreshIndex(t)
		defer tf.Index.Shutdown(t.Context())

		count := 20
		vectors, _ := testinghelpers.RandomVecs(count, 0, 32)
		ids := make([]uint64, count)
		for i := range count {
			ids[i] = uint64(i)
		}

		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Nanosecond)
		defer cancel()
		// Give it a moment so the context actually expires
		time.Sleep(1 * time.Millisecond)

		err := tf.Index.AddBatch(ctx, ids, vectors)
		require.Error(t, err)
	})
}

func TestDelete(t *testing.T) {
	t.Run("delete existing vector", func(t *testing.T) {
		tf := createHFreshIndex(t)
		defer tf.Index.Shutdown(t.Context())

		vectors, _ := testinghelpers.RandomVecs(1, 0, 32)
		err := tf.Index.Add(t.Context(), 1, vectors[0])
		require.NoError(t, err)
		require.True(t, tf.Index.ContainsDoc(1))

		err = tf.Index.Delete(1)
		require.NoError(t, err)
		require.False(t, tf.Index.ContainsDoc(1))
	})

	t.Run("delete non-existing vector marks as deleted", func(t *testing.T) {
		tf := createHFreshIndex(t)
		defer tf.Index.Shutdown(t.Context())

		// deleting a never-inserted ID creates a tombstone without error
		err := tf.Index.Delete(999)
		require.NoError(t, err)

		deleted, err := tf.Index.VersionMap.IsDeleted(t.Context(), 999)
		require.NoError(t, err)
		require.True(t, deleted)
	})

	t.Run("delete multiple vectors", func(t *testing.T) {
		tf := createHFreshIndex(t)
		defer tf.Index.Shutdown(t.Context())

		vectors, _ := testinghelpers.RandomVecs(5, 0, 32)
		for i := range 5 {
			err := tf.Index.Add(t.Context(), uint64(i), vectors[i])
			require.NoError(t, err)
		}

		err := tf.Index.Delete(1, 3)
		require.NoError(t, err)

		require.True(t, tf.Index.ContainsDoc(0))
		require.False(t, tf.Index.ContainsDoc(1))
		require.True(t, tf.Index.ContainsDoc(2))
		require.False(t, tf.Index.ContainsDoc(3))
		require.True(t, tf.Index.ContainsDoc(4))
	})

	t.Run("delete is idempotent after first call", func(t *testing.T) {
		tf := createHFreshIndex(t)
		defer tf.Index.Shutdown(t.Context())

		vectors, _ := testinghelpers.RandomVecs(1, 0, 32)
		err := tf.Index.Add(t.Context(), 1, vectors[0])
		require.NoError(t, err)

		err = tf.Index.Delete(1)
		require.NoError(t, err)
		require.False(t, tf.Index.ContainsDoc(1))

		// Second delete should still succeed (tombstone already set)
		err = tf.Index.Delete(1)
		require.NoError(t, err)
	})
}
