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
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
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
			expectedMaxPostingSize: 192,
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
			setDelegatingTempThunk(cfg)

			scheduler.Start()
			defer scheduler.Close(t.Context())

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

func TestAppendImmediatelySplitsWhenPostingFarAboveThreshold(t *testing.T) {
	tf := createHFreshIndex(t)
	defer tf.Index.Shutdown(t.Context())

	vectors := createTestVectors(4, 16)
	postingID, posting := createPostingWithVectors(t, &tf, vectors[:15], 100)
	tf.Index.distancer = NewDistancer(tf.Index.quantizer, tf.Index.config.DistanceProvider)

	uncompressed := make([]float32, 4)
	for _, vec := range vectors[:15] {
		for i := range vec {
			uncompressed[i] += vec[i]
		}
	}
	for i := range uncompressed {
		uncompressed[i] /= float32(len(vectors[:15]))
	}

	compressed := tf.Index.quantizer.CompressedBytes(tf.Index.quantizer.Encode(uncompressed))
	err := tf.Index.Centroids.Insert(postingID, &Centroid{
		Uncompressed: uncompressed,
		Compressed:   compressed,
		Deleted:      false,
	})
	require.NoError(t, err)

	err = tf.Index.PostingStore.Put(t.Context(), postingID, posting)
	require.NoError(t, err)

	err = tf.Index.setPostingVectorIDs(t.Context(), postingID, posting)
	require.NoError(t, err)

	originalMax := tf.Index.maxPostingSize
	tf.Index.maxPostingSize = 3
	defer func() { tf.Index.maxPostingSize = originalMax }()

	vectorID := uint64(10_000)
	version := VectorVersion(1)
	err = tf.Index.VersionMap.store.Set(t.Context(), vectorID, version)
	require.NoError(t, err)

	vector := NewVector(vectorID, version, tf.Index.quantizer.CompressedBytes(tf.Index.quantizer.Encode(vectors[15])))
	ok, err := tf.Index.append(t.Context(), vector, postingID, false)
	require.NoError(t, err)
	require.True(t, ok)

	require.False(t, tf.Index.Centroids.Exists(postingID))
	require.Equal(t, int64(0), tf.Index.taskQueue.splitQueue.Size())

	size, err := tf.Index.PostingSizes.Get(t.Context(), postingID)
	require.NoError(t, err)
	require.Equal(t, uint32(0), size)
}

func TestAppendMissingPostingDoesNotEnqueueReassign(t *testing.T) {
	tf := createHFreshIndex(t)
	defer tf.Index.Shutdown(t.Context())

	vector := NewVector(777, VectorVersion(1), nil)
	added, err := tf.Index.append(t.Context(), vector, 4242, true)
	require.NoError(t, err)
	require.False(t, added)
	require.Equal(t, int64(0), tf.Index.taskQueue.reassignQueue.Size())
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

// Regression tests: single vectors must never reach a muvera
// index. Before this guard, a single vector inserted into an empty muvera
// index initialized dims with the token dimensionality instead of the FDE
// dimensionality, corrupting every subsequent AddMulti.
func TestSingleVectorRejectedOnMuveraIndex(t *testing.T) {
	tf := createMuveraHFreshIndex(t)
	vec := []float32{0.1, 0.2, 0.3}

	t.Run("Add", func(t *testing.T) {
		err := tf.Index.Add(t.Context(), 0, vec)
		require.ErrorContains(t, err, "single vectors are not supported")
	})

	t.Run("AddBatch", func(t *testing.T) {
		err := tf.Index.AddBatch(t.Context(), []uint64{0}, [][]float32{vec})
		require.ErrorContains(t, err, "single vectors are not supported")
	})

	t.Run("ValidateBeforeInsert", func(t *testing.T) {
		err := tf.Index.ValidateBeforeInsert(vec)
		require.ErrorContains(t, err, "single vectors are not supported")
	})

	t.Run("index left untouched", func(t *testing.T) {
		// the rejected inserts must not have initialized dimensions
		require.Zero(t, tf.Index.dims)

		// and a proper multi-vector insert still works afterwards
		require.NoError(t, tf.Index.AddMulti(t.Context(), 1, [][]float32{{0.1, 0.2, 0.3, 0.4}}))
	})
}

// Regression tests: ValidateMultiBeforeInsert must mirror
// AddMulti's empty checks so that async-indexing enqueue rejects the same
// payloads the sync path does.
func TestValidateMultiBeforeInsertEmpty(t *testing.T) {
	tests := []struct {
		name   string
		vec    [][]float32
		expErr string
	}{
		{name: "no tokens", vec: [][]float32{}, expErr: "cannot be empty"},
		{name: "nil", vec: nil, expErr: "cannot be empty"},
		{name: "empty token", vec: [][]float32{{}}, expErr: "cannot be empty"},
		{name: "empty tokens", vec: [][]float32{{}, {}}, expErr: "cannot be empty"},
		{name: "inconsistent dims", vec: [][]float32{{0.1, 0.2}, {0.3}}, expErr: "inconsistent dimensions"},
		{name: "valid", vec: [][]float32{{0.1, 0.2}, {0.3, 0.4}}},
	}

	tf := createMuveraHFreshIndex(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tf.Index.ValidateMultiBeforeInsert(tt.vec)
			if tt.expErr != "" {
				require.ErrorContains(t, err, tt.expErr)
				return
			}
			require.NoError(t, err)
		})
	}

	t.Run("multi-vector rejected on single-vector index", func(t *testing.T) {
		single := createHFreshIndex(t)
		err := single.Index.ValidateMultiBeforeInsert([][]float32{{0.1, 0.2}})
		require.ErrorContains(t, err, "muvera is not enabled")
	})
}

// installPosting wires a posting built by createPostingWithVectors into the
// index: centroid (mean of the vectors) into the SPTAG, posting content into
// the store, and the membership and size caches.
func installPosting(t *testing.T, tf *TestHFresh, postingID uint64, posting Posting, vectors [][]float32) {
	t.Helper()
	mean := make([]float32, len(vectors[0]))
	for _, vec := range vectors {
		for i, x := range vec {
			mean[i] += x
		}
	}
	for i := range mean {
		mean[i] /= float32(len(vectors))
	}
	installPostingWithCentroid(t, tf, postingID, posting, mean)
}

// installPostingWithCentroid is installPosting with an explicit centroid
// position, as real splits produce (the clustering centroid generally differs
// from the plain mean of the members).
func installPostingWithCentroid(t *testing.T, tf *TestHFresh, postingID uint64, posting Posting, centroid []float32) {
	t.Helper()
	require.NoError(t, tf.Index.Centroids.Insert(postingID, &Centroid{
		Uncompressed: centroid,
		Compressed:   tf.Index.quantizer.CompressedBytes(tf.Index.quantizer.Encode(centroid)),
	}))
	require.NoError(t, tf.Index.PostingStore.Put(t.Context(), postingID, posting))
	require.NoError(t, tf.Index.setPostingVectorIDs(t.Context(), postingID, posting))
}

// pauseAllTaskQueues stops the background maintenance workers from picking up
// tasks, so a test controls every state transition itself. Pushing to the
// queues remains possible.
func pauseAllTaskQueues(t *testing.T, tf *TestHFresh) {
	t.Helper()
	require.NoError(t, tf.Index.taskQueue.analyzeQueue.Pause(t.Context()))
	require.NoError(t, tf.Index.taskQueue.splitQueue.Pause(t.Context()))
	require.NoError(t, tf.Index.taskQueue.mergeQueue.Pause(t.Context()))
	require.NoError(t, tf.Index.taskQueue.reassignQueue.Pause(t.Context()))
}

// waitForParkedAppend blocks until a goroutine is parked inside
// (*HFresh).append — i.e. it has made its routing decision and is now waiting
// on the posting lock held by the test. This is a convergence wait, not a
// timing assumption: the test cannot proceed before the insert is provably
// past RNGSelect.
func waitForParkedAppend(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	buf := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		n := runtime.Stack(buf, true)
		if strings.Contains(string(buf[:n]), "hfresh.(*HFresh).append(") {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("insert goroutine never reached append")
}

// TestAddRerouteWhenSplitWinsTheRace deterministically forces the loser
// interleaving of the insert-vs-split race:
//  1. Add()'s RNGSelect picks posting P (the only posting),
//  2. before its append acquires P's posting lock, a complete split of P
//     publishes two replacement postings and deletes P's centroid,
//  3. append then finds the centroid gone and reports !added.
//
// The insert must re-route synchronously: the acked vector has to be
// searchable without any help from the async reassign queue (paused here, as
// it can lag arbitrarily in production), and nothing may be left parked in
// that queue or its dedup list.
func TestAddRerouteWhenSplitWinsTheRace(t *testing.T) {
	// ids 0..14 pre-exist in posting P; id 15 is the racing insert
	vectors := make([][]float32, 16)
	for i := range vectors {
		vectors[i] = []float32{float32(i), float32(i % 3), float32(i % 5), 1}
	}
	newID := uint64(15)

	tf := createHFreshIndexWithVectorStore(t, vectors)
	pauseAllTaskQueues(t, &tf)

	postingID, posting := createPostingWithVectors(t, &tf, vectors[:15], 0)
	installPosting(t, &tf, postingID, posting, vectors[:15])

	tf.Index.postingLocks.Lock(postingID)
	locked := true
	defer func() {
		if locked {
			tf.Index.postingLocks.Unlock(postingID)
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		errCh <- tf.Index.Add(context.Background(), newID, vectors[newID])
	}()
	waitForParkedAppend(t)

	// play a complete split of P while the insert is parked: publish the two
	// replacement postings first, then delete P — the same order as doSplit
	leftID, left := createPostingWithVectors(t, &tf, vectors[:8], 0)
	installPosting(t, &tf, leftID, left, vectors[:8])
	rightID, right := createPostingWithVectors(t, &tf, vectors[8:15], 8)
	installPosting(t, &tf, rightID, right, vectors[8:15])
	require.NoError(t, tf.Index.Centroids.MarkAsDeleted(postingID))
	require.NoError(t, tf.Index.setPostingVectorIDs(t.Context(), postingID, Posting{}))
	require.NoError(t, tf.Index.PostingStore.Put(t.Context(), postingID, Posting{}))

	tf.Index.postingLocks.Unlock(postingID)
	locked = false
	addErr := <-errCh
	require.NoError(t, addErr, "Add must return nil when the re-route succeeds")

	ids, _, err := tf.Index.SearchByVector(t.Context(), vectors[newID], len(vectors), nil)
	require.NoError(t, err)
	require.Contains(t, ids, newID,
		"acked insert that lost the routing race must be searchable without the async reassign queue")

	require.Zero(t, tf.Index.taskQueue.reassignQueue.Size(),
		"synchronous re-route must not enqueue a reassign")
	require.False(t, tf.Index.taskQueue.reassignList.Contains(newID),
		"reassign dedup bit must not be set when nothing was enqueued")
}

// TestAddFallsBackToReassignQueueWhenNoLiveCentroidRemains pins the
// empty-centroids fallback: when the re-route's first RNGSelect finds no live
// posting at all (every centroid vanished mid-insert), the retry loop exits
// on its first iteration and add() must still ack the insert, parking the
// vector in the reassign queue with its dedup bit set — the pre-existing
// behavior. The bounded retry must never turn this into an insert failure.
//
// The retry-exhaustion branch (maxInsertRerouteAttempts consecutive losses
// against live centroids) ends at the same fallback statement but is
// deliberately uncovered: forcing N chained lose-the-race interleavings
// deterministically would need either a production hook in the retry loop or
// a chain of posting-lock parks whose detection depends on lock striping —
// both worse than the documented gap.
func TestAddFallsBackToReassignQueueWhenNoLiveCentroidRemains(t *testing.T) {
	vectors := make([][]float32, 16)
	for i := range vectors {
		vectors[i] = []float32{float32(i), float32(i % 3), float32(i % 5), 1}
	}
	newID := uint64(15)

	tf := createHFreshIndexWithVectorStore(t, vectors)
	pauseAllTaskQueues(t, &tf)

	postingID, posting := createPostingWithVectors(t, &tf, vectors[:15], 0)
	installPosting(t, &tf, postingID, posting, vectors[:15])

	tf.Index.postingLocks.Lock(postingID)
	locked := true
	defer func() {
		if locked {
			tf.Index.postingLocks.Unlock(postingID)
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		errCh <- tf.Index.Add(context.Background(), newID, vectors[newID])
	}()
	waitForParkedAppend(t)

	// delete the only centroid and publish no replacement: the re-route's
	// first RNGSelect comes back empty and the loop exits immediately
	require.NoError(t, tf.Index.Centroids.MarkAsDeleted(postingID))
	require.NoError(t, tf.Index.setPostingVectorIDs(t.Context(), postingID, Posting{}))
	require.NoError(t, tf.Index.PostingStore.Put(t.Context(), postingID, Posting{}))

	tf.Index.postingLocks.Unlock(postingID)
	locked = false
	addErr := <-errCh
	require.NoError(t, addErr, "Add must return nil when re-routing falls back to the reassign queue")

	require.EqualValues(t, 1, tf.Index.taskQueue.reassignQueue.Size(),
		"fallback must park exactly one reassign task")
	require.True(t, tf.Index.taskQueue.reassignList.Contains(newID),
		"parked reassign must keep its dedup bit until the task runs")
}

// countVectorCopies returns how many copies of vecID each of the given
// postings holds, plus the total. The caller enumerates every posting that
// exists in the test scenario, so the total is the vector's real replication
// count.
func countVectorCopies(t *testing.T, tf *TestHFresh, postingIDs []uint64, vecID uint64) (int, map[uint64]int) {
	t.Helper()
	total := 0
	perPosting := make(map[uint64]int, len(postingIDs))
	for _, pid := range postingIDs {
		p, err := tf.Index.PostingStore.Get(t.Context(), pid)
		if err != nil {
			if errors.Is(err, ErrPostingNotFound) {
				continue
			}
			require.NoError(t, err)
		}
		n := 0
		for _, v := range p {
			if v.ID() == vecID {
				n++
			}
		}
		perPosting[pid] = n
		total += n
	}
	return total, perPosting
}

// TestAddReroutePartialLossKeepsExactReplication covers the partial loser:
// the initial routing selects two postings, the append to P1 lands, the
// append to P2 loses against a concurrent split. The re-route must place
// exactly one additional copy (in P2's replacement child) and must not
// re-append to P1, which already holds a same-version copy that garbage
// collection could never deduplicate. Total replication is pinned to an
// exact number: 2 — P1 plus the child — because the post-split centroid set
// seen by the retry is {P1, child} and P1 is skipped. Anything above 2 means
// the re-route inflated replication.
func TestAddReroutePartialLossKeepsExactReplication(t *testing.T) {
	// ids 0..4 live in P1, ids 5..9 in P2, id 10 is the racing insert.
	// P1 sits much closer to the query (d² 0.04) than P2 and its clone
	// (d² 0.81): with the capped retry stopping after one successful
	// append, P1 must be unambiguously the FIRST candidate the retry
	// visits, so that only the appendedTo skip guard — not an accidental
	// distance or id tie-break — keeps the duplicate copy out of P1. Both
	// postings still enter the initial selection (RNG pruning at factor 10
	// only drops candidates ~10x closer to a replica than to the query;
	// dist²(P1,P2)=0.85 is far above that).
	vectors := make([][]float32, 11)
	for i := 0; i < 5; i++ {
		vectors[i] = []float32{0, 0.2, 0, 1}
	}
	for i := 5; i < 10; i++ {
		vectors[i] = []float32{0.9, 0, 0, 1}
	}
	newID := uint64(10)
	vectors[newID] = []float32{0, 0, 0, 1}

	tf := createHFreshIndexWithVectorStore(t, vectors)
	pauseAllTaskQueues(t, &tf)

	p1ID, p1 := createPostingWithVectors(t, &tf, vectors[:5], 0)
	installPosting(t, &tf, p1ID, p1, vectors[:5])
	p2ID, p2 := createPostingWithVectors(t, &tf, vectors[5:10], 5)
	installPosting(t, &tf, p2ID, p2, vectors[5:10])

	tf.Index.postingLocks.Lock(p2ID)
	locked := true
	defer func() {
		if locked {
			tf.Index.postingLocks.Unlock(p2ID)
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		errCh <- tf.Index.Add(context.Background(), newID, vectors[newID])
	}()
	waitForParkedAppend(t)

	// stage P2's disappearance while the insert is parked. This is not a
	// real splitPosting partition: the "child" is a clone of P2 (same
	// vectors, same location) published before P2's centroid is deleted —
	// the same publish-then-delete order and centroid-set transition a real
	// doSplit produces, which is all append and the re-route observe.
	childID, child := createPostingWithVectors(t, &tf, vectors[5:10], 5)
	installPosting(t, &tf, childID, child, vectors[5:10])
	require.NoError(t, tf.Index.Centroids.MarkAsDeleted(p2ID))
	require.NoError(t, tf.Index.setPostingVectorIDs(t.Context(), p2ID, Posting{}))
	require.NoError(t, tf.Index.PostingStore.Put(t.Context(), p2ID, Posting{}))

	tf.Index.postingLocks.Unlock(p2ID)
	locked = false
	addErr := <-errCh
	require.NoError(t, addErr, "Add must return nil on a partial loss")

	ids, _, err := tf.Index.SearchByVector(t.Context(), vectors[newID], len(vectors), nil)
	require.NoError(t, err)
	require.Contains(t, ids, newID, "partially lost insert must stay searchable")

	total, perPosting := countVectorCopies(t, &tf, []uint64{p1ID, p2ID, childID}, newID)
	require.Equal(t, 2, total,
		"re-route must add exactly one copy: P1 kept its copy, the child got one, nothing else")
	require.Equal(t, 1, perPosting[p1ID],
		"P1 must hold exactly one copy: the re-route may not re-append to a posting that already has one")
	require.Equal(t, 0, perPosting[p2ID], "the split-away posting must hold no copy")
	require.Equal(t, 1, perPosting[childID], "the replacement child must hold the re-routed copy")

	require.Zero(t, tf.Index.taskQueue.reassignQueue.Size(),
		"successful re-route must not enqueue a reassign")
	require.False(t, tf.Index.taskQueue.reassignList.Contains(newID))
}

// faultyDistanceProvider delegates to a real provider until failing is set;
// from then on every distance computation errors. Wired through the test
// constructor's withDistanceProvider option, it makes RNGSelect fail
// mid-flight without any production hook.
type faultyDistanceProvider struct {
	distancer.Provider
	failing atomic.Bool
}

func (f *faultyDistanceProvider) SingleDist(a, b []float32) (float32, error) {
	if f.failing.Load() {
		return 0, errors.New("injected distance failure")
	}
	return f.Provider.SingleDist(a, b)
}

func (f *faultyDistanceProvider) New(vec []float32) distancer.Distancer {
	if f.failing.Load() {
		return failingDistancer{}
	}
	return f.Provider.New(vec)
}

type failingDistancer struct{}

func (failingDistancer) Distance([]float32) (float32, error) {
	return 0, errors.New("injected distance failure")
}

// TestAddAcksAndParksWhenRerouteRoutingFails pins the routing-failure
// fallback: when RNGSelect errors inside the re-route, Add must still return
// nil (the vector was acked on every other loser path, so no loser path may
// surface as an insert error) and park the vector in the reassign queue. The
// warn-log assertion proves the error branch ran, not the empty-centroids
// branch, which ends at the same queue.
func TestAddAcksAndParksWhenRerouteRoutingFails(t *testing.T) {
	faulty := &faultyDistanceProvider{Provider: distancer.NewL2SquaredProvider()}
	tf := createHFreshIndex(t, withDistanceProvider(faulty))
	pauseAllTaskQueues(t, &tf)

	vectors := make([][]float32, 15)
	for i := range vectors {
		vectors[i] = []float32{1, 0, 0, 0}
	}
	postingID, posting := createPostingWithVectors(t, &tf, vectors, 0)
	installPosting(t, &tf, postingID, posting, vectors)

	newID := uint64(100)
	newVec := []float32{1, 0, 0, 0}

	tf.Index.postingLocks.Lock(postingID)
	locked := true
	defer func() {
		if locked {
			tf.Index.postingLocks.Unlock(postingID)
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		errCh <- tf.Index.Add(context.Background(), newID, newVec)
	}()
	waitForParkedAppend(t)

	// publish two live replacements far apart so the retry's RNGSelect has
	// distances to compute, delete the original, then break every distance
	// computation: the retry's routing must fail, not come back empty
	childAID, childA := createPostingWithVectors(t, &tf, vectors[:8], 0)
	installPosting(t, &tf, childAID, childA, vectors[:8])
	farVectors := make([][]float32, 7)
	for i := range farVectors {
		farVectors[i] = []float32{0, 1, 0, 0}
	}
	childBID, childB := createPostingWithVectors(t, &tf, farVectors, 8)
	installPosting(t, &tf, childBID, childB, farVectors)
	require.NoError(t, tf.Index.Centroids.MarkAsDeleted(postingID))
	require.NoError(t, tf.Index.setPostingVectorIDs(t.Context(), postingID, Posting{}))
	require.NoError(t, tf.Index.PostingStore.Put(t.Context(), postingID, Posting{}))
	faulty.failing.Store(true)

	tf.Index.postingLocks.Unlock(postingID)
	locked = false
	addErr := <-errCh
	require.NoError(t, addErr,
		"Add must return nil when re-route routing fails; the vector is parked, not the insert failed")

	logged := false
	for _, e := range tf.Logs.AllEntries() {
		if strings.Contains(e.Message, "RNG selection failed") {
			logged = true
			break
		}
	}
	require.True(t, logged, "the re-route must have taken the routing-failure branch")

	require.EqualValues(t, 1, tf.Index.taskQueue.reassignQueue.Size(),
		"routing failure must park exactly one reassign task")
	require.True(t, tf.Index.taskQueue.reassignList.Contains(newID),
		"parked reassign must keep its dedup bit until the task runs")
}

// TestAddRerouteCapsReplacementsAtLostCount pins the replacement cap with the
// posting-set transition a real split produces: two children, both far enough
// apart to survive RNG pruning, so the retry's selection is {P1, childA,
// childB}. The insert lost exactly one placement (P2), so the re-route must
// add exactly one copy — the nearest child — and stop. Without the cap, both
// children would receive same-version copies that garbage collection can
// never retire (the red state of this test): one lost placement must not
// inflate replication.
func TestAddRerouteCapsReplacementsAtLostCount(t *testing.T) {
	// ids 0..4 live in P1, ids 5..9 in P2, id 10 is the racing insert
	vectors := make([][]float32, 11)
	for i := 0; i < 5; i++ {
		vectors[i] = []float32{0, 0.9, 0, 1}
	}
	for i := 5; i < 10; i++ {
		vectors[i] = []float32{0.9, 0, 0, 1}
	}
	newID := uint64(10)
	vectors[newID] = []float32{0, 0, 0, 1}

	tf := createHFreshIndexWithVectorStore(t, vectors)
	pauseAllTaskQueues(t, &tf)

	p1ID, p1 := createPostingWithVectors(t, &tf, vectors[:5], 0)
	installPosting(t, &tf, p1ID, p1, vectors[:5])
	p2ID, p2 := createPostingWithVectors(t, &tf, vectors[5:10], 5)
	installPosting(t, &tf, p2ID, p2, vectors[5:10])

	tf.Index.postingLocks.Lock(p2ID)
	locked := true
	defer func() {
		if locked {
			tf.Index.postingLocks.Unlock(p2ID)
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		errCh <- tf.Index.Add(context.Background(), newID, vectors[newID])
	}()
	waitForParkedAppend(t)

	// split P2 into two children while the insert is parked: members are
	// partitioned, and the centroids sit at deliberately DIFFERENT distances
	// from the query — childA at d² 0.90, childB at d² 1.30 — so the test
	// can pin that the cap keeps the NEAREST new target, not an arbitrary
	// one. Their separation (squared distance 1.0) stays far above the RNG
	// factor-1/10 pruning threshold, so both survive selection. P2's
	// centroid is deleted last — publish before delete, the doSplit order.
	childAID, childA := createPostingWithVectors(t, &tf, vectors[5:8], 5)
	installPostingWithCentroid(t, &tf, childAID, childA, []float32{0.9, 0, 0.3, 1})
	childBID, childB := createPostingWithVectors(t, &tf, vectors[8:10], 8)
	installPostingWithCentroid(t, &tf, childBID, childB, []float32{0.9, 0, -0.7, 1})
	require.NoError(t, tf.Index.Centroids.MarkAsDeleted(p2ID))
	require.NoError(t, tf.Index.setPostingVectorIDs(t.Context(), p2ID, Posting{}))
	require.NoError(t, tf.Index.PostingStore.Put(t.Context(), p2ID, Posting{}))

	tf.Index.postingLocks.Unlock(p2ID)
	locked = false
	addErr := <-errCh
	require.NoError(t, addErr, "Add must return nil when the capped re-route succeeds")

	ids, _, err := tf.Index.SearchByVector(t.Context(), vectors[newID], len(vectors), nil)
	require.NoError(t, err)
	require.Contains(t, ids, newID, "re-routed insert must stay searchable")

	total, perPosting := countVectorCopies(t, &tf, []uint64{p1ID, p2ID, childAID, childBID}, newID)
	require.Equal(t, 2, total,
		"one lost placement must produce exactly one replacement: P1's copy plus one child copy")
	require.Equal(t, 1, perPosting[p1ID], "P1 must keep exactly one copy")
	require.Equal(t, 0, perPosting[p2ID], "the split-away posting must hold no copy")
	require.Equal(t, 1, perPosting[childAID],
		"the replacement copy must land in the NEAREST child: the cap consumes targets in ascending distance order")
	require.Equal(t, 0, perPosting[childBID],
		"the farther child must not receive a copy once the deficit is covered")

	require.Zero(t, tf.Index.taskQueue.reassignQueue.Size(),
		"a fully covered deficit must not enqueue a reassign")
	require.False(t, tf.Index.taskQueue.reassignList.Contains(newID))
}
