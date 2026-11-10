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

package tachiomfresh

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storagestate"
)

// port of hfresh's TestReassignEnqueuePushFailureClearsDedup: a push that
// fails leaves no dedup mark, so the reassign can be enqueued again.
func TestReassignEnqueuePushFailureClearsDedup(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
	ref := tokenRef{DocID: 400, Pos: 2}
	require.NoError(t, tf.Index.taskQueue.reassignQueue.Close(t.Context()))

	err := tf.Index.taskQueue.EnqueueReassign(1, ref)
	require.ErrorContains(t, err, "failed to push reassign operation to queue")
	require.True(t, tf.Index.taskQueue.reassignList.TryAdd(1, ref))
}

// port of hfresh's TestReassignTaskQueueDedupDoesNotPersistOnClose. hfresh
// writes its dedup set to a bucket; this index keeps it in memory only, so a
// reopened index has no mark for the task it enqueued before the restart.
func TestReassignTaskQueueDedupDoesNotPersistOnClose(t *testing.T) {
	dir := t.TempDir()
	open := func() testIndex {
		return newTestIndexAt(t, dir, 64, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
	}
	tf := open()
	ref := tokenRef{DocID: 300, Pos: 1}
	require.NoError(t, tf.Index.taskQueue.EnqueueReassign(1, ref))
	tf.close(t)

	reopened := open()
	require.True(t, reopened.Index.taskQueue.reassignList.TryAdd(1, ref))
	reopened.close(t)
}

// A split, a cleanup and a reassign queued when the index closes run after it
// reopens.
func TestPendingTasksRunAfterReopen(t *testing.T) {
	dir := t.TempDir()
	open := func() testIndex {
		tf := newTestIndexAt(t, dir, 10, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
		tf.Index.opts.disableReassign = true
		return tf
	}
	tf := open()
	docs := map[uint64][][]float32{}
	store := func(tf *testIndex) {
		for id, doc := range docs {
			tf.mvStore.storeMultiVector(id, doc)
		}
	}

	// a posting over its limit, for the split
	big := createTestVectors(4, 15)
	bigID, bigRefs := createPostingWithTokens(t, &tf, big, 100)
	registerPosting(t, &tf, bigID, bigRefs, meanVector(big))
	for id, doc := range oneTokenDocs(big, 100) {
		docs[id] = doc
	}
	// a posting with dead entries, for the cleanup
	dying := [][]float32{{0, 0, 1, 0}, {0, 0, 1, 0.1}, {0, 0, 1, 0.2}, {0, 0, 1, 0.3}}
	dyingID, dyingRefs := createPostingWithTokens(t, &tf, dying, 200)
	registerPosting(t, &tf, dyingID, dyingRefs, dying[0])
	for id, doc := range oneTokenDocs(dying, 200) {
		docs[id] = doc
	}
	require.NoError(t, tf.Index.Delete(200, 201))
	// a token in the wrong posting, for the reassign
	near := [][]float32{{0, 0, 0, 1}}
	nearID, nearRefs := createPostingWithTokens(t, &tf, near, 300)
	registerPosting(t, &tf, nearID, nearRefs, near[0])
	docs[300] = near
	moving := placeToken(t, &tf, 400, []float32{0, 0.05, 0, 1}, dyingID)
	docs[400] = [][]float32{{0, 0.05, 0, 1}}

	for _, q := range tf.Index.taskQueue.queues() {
		require.NoError(t, q.Pause(t.Context()))
	}
	require.NoError(t, tf.Index.taskQueue.EnqueueSplit(bigID))
	require.NoError(t, tf.Index.taskQueue.EnqueueCleanup(dyingID))
	require.NoError(t, tf.Index.taskQueue.EnqueueReassign(dyingID, moving))
	tf.close(t)

	reopened := open()
	store(&reopened)
	reopened.Index.waitForMaintenance(t)
	require.False(t, reopened.Index.Centroids.Exists(bigID), "the split ran")
	for _, ref := range reopened.posting(dyingID) {
		require.NotContains(t, []uint32{200, 201}, ref.DocID, "the cleanup ran")
	}
	// the reassign ran: the token left the posting it was put in by hand,
	// for its nearest centroid (by dot product, the large vectors' postings
	// win over nearID); checkInvariants below finds it in one posting
	require.NotContains(t, reopened.posting(dyingID), moving, "the reassign ran")
	checkInvariants(t, &reopened, docs, map[uint64]bool{200: true, 201: true}, false)
	reopened.close(t)
}

// A shutdown while a split reads its tokens leaves an index that reopens with
// every token in one posting.
func TestShutdownDuringASplit(t *testing.T) {
	dir := t.TempDir()
	open := func() testIndex {
		return newTestIndexAt(t, dir, 10, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
	}
	tf := open()
	tf.Index.opts.disableReassign = true
	vectors := createTestVectors(4, 15)
	postingID, refs := createPostingWithTokens(t, &tf, vectors, 100)
	registerPosting(t, &tf, postingID, refs, meanVector(vectors))
	docs := oneTokenDocs(vectors, 100)

	reading, release := make(chan struct{}), make(chan struct{})
	var blocked atomic.Bool
	read := tf.Index.tokenForID
	tf.Index.tokenForID = func(ctx context.Context, id uint64, pos int, dims int) ([]float32, error) {
		if blocked.CompareAndSwap(false, true) {
			close(reading)
			<-release
		}
		return read(ctx, id, pos, dims)
	}
	require.NoError(t, tf.Index.taskQueue.EnqueueSplit(postingID))
	require.NoError(t, tf.Index.taskQueue.Flush())
	tf.Index.scheduler.Schedule(t.Context())
	<-reading

	var wg sync.WaitGroup
	wg.Add(1)
	var shutdownErr error
	enterrors.GoWrapper(func() {
		defer wg.Done()
		shutdownErr = tf.Index.Shutdown(t.Context())
	}, tf.Index.logger)
	// Shutdown cancels the index's context first; the split is released
	// once it has
	<-tf.Index.ctx.Done()
	close(release)
	wg.Wait()
	require.NoError(t, shutdownErr)
	require.NoError(t, tf.store.Shutdown(t.Context()))

	reopened := open()
	for id, doc := range docs {
		reopened.mvStore.storeMultiVector(id, doc)
	}
	reopened.Index.waitForMaintenance(t)
	checkInvariants(t, &reopened, docs, nil, false)
	reopened.close(t)
}

// The type table's reference of spreads: refreshing twice changes nothing; a
// type restored under spreadMinEntries entries joins it once it grows past
// them, and only once; a spread that fails to persist stays to be written.
func TestTypeTableReference(t *testing.T) {
	t.Run("refreshing twice", func(t *testing.T) {
		tf := newTestIndex(t, 4, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
		largeAndSmallType(t, &tf, rand.New(rand.NewSource(8)), 900, 12)
		tt := tf.Index.types
		tt.refreshVariances()
		sum, count, ref := tt.refSum, tt.refCount, tt.reference.Load()
		require.Positive(t, count)
		tt.refreshVariances()
		require.Equal(t, sum, tt.refSum)
		require.Equal(t, count, tt.refCount)
		require.Equal(t, ref, tt.reference.Load())
	})

	t.Run("a type restored under the minimum joins once", func(t *testing.T) {
		dir := t.TempDir()
		open := func() testIndex {
			return newTestIndexAt(t, dir, 4, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
		}
		tf := open()
		rng := rand.New(rand.NewSource(9))
		// type 2 gets 200 entries: observed (from spreadObserveEntries), not
		// yet counted (from spreadMinEntries)
		largeAndSmallType(t, &tf, rng, 900, 200)
		require.NotZero(t, tf.Index.types.group(2).seen)
		tf.close(t)

		reopened := open()
		g := reopened.Index.types.group(2)
		require.Less(t, g.entries.Load(), int64(spreadMinEntries))
		require.False(t, g.counted)
		before := reopened.Index.types.refCount
		for i := range 300 {
			doc := randomMultiVector(rng, 1, 16)
			id := uint64(5000 + i)
			reopened.mvStore.storeMultiVector(id, doc)
			require.NoError(t, reopened.Index.AddMultiWithTypes(t.Context(), id, doc, []uint32{2}))
		}
		reopened.Index.waitForMaintenance(t)
		require.True(t, g.counted)
		require.Equal(t, before+1, reopened.Index.types.refCount)
		reopened.Index.types.refreshVariances()
		require.Equal(t, before+1, reopened.Index.types.refCount)
		reopened.close(t)
	})

	t.Run("a failing persist keeps the sums to write", func(t *testing.T) {
		tf := newTestIndex(t, 4, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
		largeAndSmallType(t, &tf, rand.New(rand.NewSource(10)), 900, 12)
		g := tf.Index.types.group(1)
		g.spreadMu.Lock()
		g.dirty = true
		g.spreadMu.Unlock()

		bucket := tf.store.Bucket(tf.Index.types.bucket)
		bucket.UpdateStatus(storagestate.StatusReadOnly)
		require.Error(t, tf.Index.types.persistSpread())
		require.True(t, g.dirty)

		bucket.UpdateStatus(storagestate.StatusReady)
		require.NoError(t, tf.Index.types.persistSpread())
		require.False(t, g.dirty)
	})
}
