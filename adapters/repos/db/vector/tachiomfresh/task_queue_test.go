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
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// port of TestTaskQueueRegisterIsExplicit, through New: split, reassign and
// merge queues, no analyze queue
func TestTaskQueueRegistersThreeQueues(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction)
	require.Equal(t, 3, tf.Index.scheduler.QueueCount())
}

// port of TestWaitForMaintenanceDrainsQueuedTasks
func TestWaitForMaintenanceDrainsQueuedTasks(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction)
	require.NoError(t, tf.Index.initDimensions([]float32{1, 0, 0, 0}))
	const postingID = uint64(42)

	require.NoError(t, tf.Index.taskQueue.EnqueueSplit(postingID))
	require.True(t, tf.Index.taskQueue.splitList.Contains(postingID))
	require.EqualValues(t, 1, tf.Index.taskQueue.Size())

	tf.Index.waitForMaintenance(t)

	require.Zero(t, tf.Index.taskQueue.Size())
	require.False(t, tf.Index.taskQueue.splitList.Contains(postingID))
}

// port of TestWaitForMaintenanceDrainsFollowUpTasks: the split of an
// oversized posting runs, with its reassign checks, and whatever it queued
// is drained too.
func TestWaitForMaintenanceDrainsFollowUpTasks(t *testing.T) {
	tf := newTestIndex(t, 10, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
	vectors := createTestVectors(4, 30)
	postingID, refs := createPostingWithTokens(t, &tf, vectors, 100)
	registerPosting(t, &tf, postingID, refs, meanVector(vectors))

	require.NoError(t, tf.Index.taskQueue.EnqueueSplit(postingID))
	tf.Index.waitForMaintenance(t)

	require.False(t, tf.Index.Centroids.Exists(postingID), "the split ran")

	require.Zero(t, tf.Index.taskQueue.Size())
	require.Zero(t, tf.Index.taskQueue.splitList.LivePages())
	require.Zero(t, tf.Index.taskQueue.reassignList.Len())
}

// port of TestReassignTaskQueueOperations and the dedup tests: a token is
// enqueued once until its task is done
func TestReassignTaskQueueDedup(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction)
	dedup := tf.Index.taskQueue.reassignList
	ref := tokenRef{DocID: 600, Pos: 3}

	require.True(t, dedup.TryAdd(1, ref), "first add should succeed")
	require.False(t, dedup.TryAdd(1, ref), "duplicate add should fail")
	require.True(t, dedup.TryAdd(1, tokenRef{DocID: 600, Pos: 4}), "another token of the same document is a different key")
	require.True(t, dedup.TryAdd(2, ref), "the same token in another posting is a different key")

	tf.Index.taskQueue.ReassignDone(1, ref)
	require.True(t, dedup.TryAdd(1, ref), "should be able to add again after done")
}

// ports of hfresh's task decode tests, for the four records of this queue
func TestDecodeTask(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction)
	tq := &tf.Index.taskQueue
	ref := tokenRef{DocID: 0xA1B2C3D4, Pos: 0xE5F6}

	tests := []struct {
		name    string
		data    []byte
		check   func(t *testing.T, task queue.Task)
		wantErr string
	}{
		{
			name: "split",
			data: encodeTask(11, taskQueueSplitOp),
			check: func(t *testing.T, task queue.Task) {
				split, ok := task.(*SplitTask)
				require.True(t, ok)
				require.Equal(t, uint64(11), split.id)
			},
		},
		{
			name: "merge",
			data: encodeTask(12, taskQueueMergeOp),
			check: func(t *testing.T, task queue.Task) {
				merge, ok := task.(*MergeTask)
				require.True(t, ok)
				require.Equal(t, uint64(12), merge.id)
			},
		},
		{
			name: "reassign",
			data: encodeReassignTask(ref, 1<<40+7),
			check: func(t *testing.T, task queue.Task) {
				reassign, ok := task.(*ReassignTask)
				require.True(t, ok)
				require.Equal(t, ref, reassign.ref)
				require.Equal(t, uint64(1<<40+7), reassign.postingID)
				require.Equal(t, ref.key(), reassign.Key())
			},
		},
		{
			name: "cleanup",
			data: encodeTask(13, taskQueueCleanupOp),
			check: func(t *testing.T, task queue.Task) {
				cleanup, ok := task.(*CleanupTask)
				require.True(t, ok)
				require.Equal(t, uint64(13), cleanup.id)
				require.Equal(t, tf.Index.postingLocks.Hash(13), cleanup.Key())
			},
		},
		{name: "reassign too short", data: encodeReassignTask(ref, 7)[:10], wantErr: "invalid reassign task length"},
		{name: "cleanup too short", data: []byte{taskQueueCleanupOp, 1, 2}, wantErr: "invalid cleanup task length"},
		{name: "split too short", data: []byte{taskQueueSplitOp, 1, 2}, wantErr: "invalid split task length"},
		{name: "unknown operation", data: encodeTask(1, 99), wantErr: "unknown operation"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task, err := tq.DecodeTask(tt.data)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			tt.check(t, task)
		})
	}
}

// A shard that held many tokens gives its map back when it empties: a Go
// map keeps its buckets when its entries are deleted.
func TestTokenSetReleasesMemoryWhenEmptied(t *testing.T) {
	tests := []struct {
		name     string
		tokens   int
		wantHeld bool // the shards keep the maps they filled
	}{
		{name: "a small set keeps its maps", tokens: tokenSetShards * 8, wantHeld: true},
		{name: "a large set starts new maps", tokens: tokenSetShards * tokenSetShrinkAt * 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTokenSet()
			before := make([]uintptr, tokenSetShards)
			for i := range s.shards {
				before[i] = reflect.ValueOf(s.shards[i].m).Pointer()
			}
			for i := range tt.tokens {
				require.True(t, s.TryAdd(1, tokenRef{DocID: uint32(i), Pos: 1}))
			}
			require.Equal(t, tt.tokens, s.Len())
			for i := range tt.tokens {
				s.Delete(1, tokenRef{DocID: uint32(i), Pos: 1})
			}
			require.Zero(t, s.Len())

			var held int
			for i := range s.shards {
				if reflect.ValueOf(s.shards[i].m).Pointer() == before[i] {
					held++
				}
			}
			if tt.wantHeld {
				require.Equal(t, tokenSetShards, held)
			} else {
				require.Zero(t, held)
			}
			// and the set still works
			require.True(t, s.TryAdd(1, tokenRef{DocID: 1, Pos: 1}))
			require.False(t, s.TryAdd(1, tokenRef{DocID: 1, Pos: 1}))
		})
	}
}

// TestFailedEnqueueClearsItsMarker: a push that fails must not leave the
// posting marked as queued, or every later enqueue of the same task for
// that posting returns without queueing anything.
func TestFailedEnqueueClearsItsMarker(t *testing.T) {
	tests := []struct {
		name    string
		enqueue func(tq *TaskQueue, id uint64) error
		marked  func(tq *TaskQueue, id uint64) bool
	}{
		{
			name:    "split",
			enqueue: (*TaskQueue).EnqueueSplit,
			marked:  func(tq *TaskQueue, id uint64) bool { return tq.splitList.Contains(id) },
		},
		{
			name:    "merge",
			enqueue: (*TaskQueue).EnqueueMerge,
			marked:  func(tq *TaskQueue, id uint64) bool { return tq.mergeList.Contains(id) },
		},
		{
			name:    "cleanup",
			enqueue: (*TaskQueue).EnqueueCleanup,
			marked:  func(tq *TaskQueue, id uint64) bool { return tq.cleanupList.Contains(id) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tf := newTestIndex(t, testMaxPostingSize, testKCentroids, testKDocsToScore, hfreshEFConstruction)
			defer func() {
				// the queues are closed already
				_ = tf.Index.Shutdown(t.Context())
				require.NoError(t, tf.store.Shutdown(t.Context()))
			}()
			tq := &tf.Index.taskQueue
			for _, q := range tq.queues() {
				require.NoError(t, q.Close(t.Context()))
			}

			require.Error(t, tt.enqueue(tq, 5))
			require.False(t, tt.marked(tq, 5), "the posting stays marked as queued")
		})
	}
}

// TestReassignOfATokenFromAnotherPostingIsQueued: a split that moved a
// token to a new posting asks for its reassign there while a reassign of
// the token in its old posting is still queued. The second must be queued
// too: the first finds its posting gone and does nothing.
func TestReassignOfATokenFromAnotherPostingIsQueued(t *testing.T) {
	tf := newTestIndex(t, testMaxPostingSize, testKCentroids, testKDocsToScore, hfreshEFConstruction)
	defer tf.close(t)
	tq := &tf.Index.taskQueue
	require.NoError(t, tq.reassignQueue.Pause(t.Context()))
	defer tq.reassignQueue.Resume()

	ref := tokenRef{DocID: 600, Pos: 3}
	require.NoError(t, tq.EnqueueReassign(1, ref))
	require.NoError(t, tq.EnqueueReassign(2, ref))
	require.NoError(t, tq.EnqueueReassign(2, ref))
	require.Equal(t, 2, tq.reassignList.Len(), "reassigns queued")
}

// port of TestTaskQueuePendingMetricsAfterProcessing: the pending-task gauge
// is updated once a processed chunk is removed, so a drained queue reports
// no backlog.
func TestTaskQueuePendingMetricsAfterProcessing(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction)
	require.NoError(t, tf.Index.initDimensions([]float32{1, 0, 0, 0}))
	pending, err := monitoring.GetMetrics().VectorIndexPendingBackgroundOperations.GetMetricWith(prometheus.Labels{
		"class_name": tf.metricsClass, "shard_name": "", "operation": "split",
	})
	require.NoError(t, err)

	require.NoError(t, tf.Index.taskQueue.EnqueueSplit(42))
	require.Equal(t, 1.0, testutil.ToFloat64(pending))

	require.Eventually(t, func() bool {
		return tf.Index.taskQueue.splitQueue.Size() == 0
	}, 10*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(pending) == 0
	}, time.Second, 10*time.Millisecond, "the gauge still reports the processed chunk")
}

// port of TestTaskQueueOnBatchProcessedWritesLSMLogs: the postings live in
// the shard's buckets, so the hook writes their logs out before the queue
// removes the processed chunk.
func TestTaskQueueOnBatchProcessedWritesLSMLogs(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction)
	require.NoError(t, tf.store.CreateOrLoadBucket(t.Context(), "probe", lsmkv.WithStrategy(lsmkv.StrategyReplace)))
	bucket := tf.store.Bucket("probe")
	// small enough to stay in the log's buffer
	require.NoError(t, bucket.Put([]byte("key"), []byte("value")))
	walSize := func() int64 {
		paths, err := filepath.Glob(filepath.Join(bucket.GetDir(), "*.wal"))
		require.NoError(t, err)
		var size int64
		for _, p := range paths {
			fi, err := os.Stat(p)
			require.NoError(t, err)
			size += fi.Size()
		}
		return size
	}
	before := walSize()

	require.NoError(t, tf.Index.taskQueue.OnBatchProcessed())
	require.Greater(t, walSize(), before, "the bucket log was not written out")
}
