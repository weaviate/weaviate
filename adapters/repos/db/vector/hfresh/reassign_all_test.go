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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func TestEnqueueReassignAllRecoversEmptyCentroidGraph(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)
	vectors := &testVectorStore{m: make(map[uint64][]float32)}
	cfg.VectorForIDThunk = vectors.get
	t.Cleanup(func() { cfg.Scheduler.Close(context.Background()) })
	index := makeHFreshWithConfig(t, store, cfg, uc)
	tf := TestHFresh{Index: index, Vectors: vectors}
	vecs := make([][]float32, 3)
	for i := range vecs {
		vecs[i] = make([]float32, 64)
		vecs[i][i] = 1
	}
	require.NoError(t, index.initDimensions(vecs[0]))
	// Model the compacted graph losing its sole centroid while the posting,
	// its membership, and the source vectors survive.
	postingID, posting := createPostingWithVectors(t, &tf, vecs, 100)
	require.NoError(t, index.PostingStore.Put(t.Context(), postingID, posting))
	require.NoError(t, index.setPostingVectorIDs(t.Context(), postingID, posting))
	ids, _, err := index.SearchByVector(t.Context(), vecs[0], 3, nil)
	require.NoError(t, err)
	require.Empty(t, ids)

	for round := 0; round < 2; round++ {
		stats, err := index.EnqueueReassignAll(t.Context())
		require.NoError(t, err)
		require.Positive(t, stats.Enqueued)
		destinations, err := index.Centroids.Search(vecs[0], 10, nil)
		require.NoError(t, err)
		require.Len(t, destinations.data, 1, "a retry must reuse the recovered centroid")
		require.NotEqual(t, postingID, destinations.data[0].ID)
		_, err = index.taskQueue.reassignQueue.ForceSwitch(t.Context(), cfg.RootPath)
		require.NoError(t, err)
		require.Eventually(t, func() bool { return index.taskQueue.reassignQueue.Size() == 0 }, 10*time.Second, 10*time.Millisecond)
		for id := uint64(100); id < 103; id++ {
			version, err := index.VersionMap.Get(t.Context(), id)
			require.NoError(t, err)
			require.Equal(t, VectorVersion(2), version, "retry must not reassign an already healthy vector")
		}
		for _, result := range destinations.data {
			p, err := index.PostingStore.Get(t.Context(), result.ID)
			require.NoError(t, err)
			p, err = p.GarbageCollect(index.VersionMap)
			require.NoError(t, err)
			require.Len(t, p, 3)
			require.NoError(t, index.PostingStore.Put(t.Context(), result.ID, p))
			require.NoError(t, index.setPostingVectorIDs(t.Context(), result.ID, p))
		}
		ids, _, err = index.SearchByVector(t.Context(), vecs[0], 3, nil)
		require.NoError(t, err)
		require.ElementsMatch(t, []uint64{100, 101, 102}, ids)
	}

	require.NoError(t, index.Shutdown(t.Context()))
	index = makeHFreshWithConfig(t, store, cfg, uc)
	ids, _, err = index.SearchByVector(t.Context(), vecs[0], 3, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []uint64{100, 101, 102}, ids, "recovery must survive restart")
}

func TestEnqueueReassignAllBootstrapFailureIsRetryable(t *testing.T) {
	for _, failure := range []string{"read error", "invalid dimensions", "canceled read"} {
		t.Run(failure, func(t *testing.T) {
			tf := createHFreshIndex(t)
			postingID, posting := createPostingWithVectors(t, &tf, [][]float32{{1, 0, 0, 0}}, 100)
			require.NoError(t, tf.Index.PostingStore.Put(t.Context(), postingID, posting))
			require.NoError(t, tf.Index.setPostingVectorIDs(t.Context(), postingID, posting))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			tf.Index.config.VectorForIDThunk = func(context.Context, uint64) ([]float32, error) {
				switch failure {
				case "read error":
					return nil, errors.New("source unavailable")
				case "invalid dimensions":
					return []float32{1}, nil
				default:
					cancel()
					return []float32{1, 0, 0, 0}, nil
				}
			}
			stats, err := tf.Index.EnqueueReassignAll(ctx)
			require.Error(t, err)
			require.Zero(t, stats.Enqueued)
			require.Zero(t, tf.Index.taskQueue.reassignQueue.Size())
			version, err := tf.Index.VersionMap.Get(t.Context(), 100)
			require.NoError(t, err)
			require.Equal(t, VectorVersion(1), version)
			destinations, err := tf.Index.Centroids.Search([]float32{1, 0, 0, 0}, 10, nil)
			require.NoError(t, err)
			require.Empty(t, destinations.data)

			tf.Index.config.VectorForIDThunk = tf.Vectors.get
			stats, err = tf.Index.EnqueueReassignAll(t.Context())
			require.NoError(t, err)
			require.Equal(t, 1, stats.Enqueued)
		})
	}
}

func TestEnqueueReassignAllConcurrentBootstrap(t *testing.T) {
	tf := createHFreshIndex(t)
	postingID, posting := createPostingWithVectors(t, &tf, [][]float32{{1, 0, 0, 0}}, 100)
	require.NoError(t, tf.Index.PostingStore.Put(t.Context(), postingID, posting))
	require.NoError(t, tf.Index.setPostingVectorIDs(t.Context(), postingID, posting))
	// Hold the queued work so this test isolates concurrent bootstrap calls.
	require.NoError(t, tf.Index.taskQueue.reassignQueue.Pause(t.Context()))
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			_, err := tf.Index.EnqueueReassignAll(t.Context())
			errs <- err
		}, tf.Index.logger)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	destinations, err := tf.Index.Centroids.Search([]float32{1, 0, 0, 0}, 10, nil)
	require.NoError(t, err)
	require.Len(t, destinations.data, 1)
	require.NotEqual(t, postingID, destinations.data[0].ID)
	require.Equal(t, int64(1), tf.Index.taskQueue.reassignQueue.Size())
}

func TestEnqueueReassignAllUninitialized(t *testing.T) {
	tf := createHFreshIndex(t)

	_, err := tf.Index.EnqueueReassignAll(t.Context())
	require.ErrorContains(t, err, "not initialized")
}

// The scan runs without a shard reference, so it must stop on the index's
// own lifecycle context even when the caller's context stays alive.
func TestEnqueueReassignAllStopsOnIndexShutdown(t *testing.T) {
	tf := createHFreshIndex(t)

	vectors := createTestVectors(4, 3)
	postingID, posting := createPostingWithVectors(t, &tf, vectors, 300)
	err := tf.Index.PostingStore.Put(t.Context(), postingID, posting)
	require.NoError(t, err)
	err = tf.Index.setPostingVectorIDs(t.Context(), postingID, posting)
	require.NoError(t, err)

	tf.Index.cancel()

	_, err = tf.Index.EnqueueReassignAll(t.Context())
	require.ErrorIs(t, err, context.Canceled)
}

func TestEnqueueReassignAll(t *testing.T) {
	tf := createHFreshIndex(t)

	vectorsA := createTestVectors(4, 5)
	postingA, pa := createPostingWithVectors(t, &tf, vectorsA, 100)
	vectorsB := createTestVectors(4, 3)
	postingB, pb := createPostingWithVectors(t, &tf, vectorsB, 200)

	for _, p := range []struct {
		id       uint64
		posting  Posting
		centroid []float32
	}{
		{postingA, pa, []float32{1.0, 0.0, 0.0, 0.0}},
		{postingB, pb, []float32{0.0, 1.0, 0.0, 0.0}},
	} {
		compressed := tf.Index.quantizer.CompressedBytes(tf.Index.quantizer.Encode(p.centroid))
		err := tf.Index.Centroids.Insert(p.id, &Centroid{
			Uncompressed: p.centroid,
			Compressed:   compressed,
			Deleted:      false,
		})
		require.NoError(t, err)

		err = tf.Index.PostingStore.Put(t.Context(), p.id, p.posting)
		require.NoError(t, err)

		err = tf.Index.setPostingVectorIDs(t.Context(), p.id, p.posting)
		require.NoError(t, err)
	}

	// vector 100 is deleted, vector 200's stored entry is stale (the live
	// version moved on) — neither may be enqueued
	_, err := tf.Index.VersionMap.MarkDeleted(t.Context(), 100)
	require.NoError(t, err)
	_, err = tf.Index.VersionMap.Increment(t.Context(), 200, VectorVersion(1))
	require.NoError(t, err)

	stats, err := tf.Index.EnqueueReassignAll(t.Context())
	require.NoError(t, err)

	require.Equal(t, 2, stats.Postings)
	require.Equal(t, 6, stats.Enqueued, "8 stored entries minus 1 deleted minus 1 stale")
	require.Equal(t, 1, stats.SkippedDeleted)
	require.Equal(t, 1, stats.SkippedStale)

	for _, id := range []uint64{101, 102, 103, 104, 201, 202} {
		require.True(t, tf.Index.taskQueue.reassignList.Contains(id),
			"live vector %d should be enqueued", id)
	}
	for _, id := range []uint64{100, 200} {
		require.False(t, tf.Index.taskQueue.reassignList.Contains(id),
			"vector %d should be skipped", id)
	}
}
