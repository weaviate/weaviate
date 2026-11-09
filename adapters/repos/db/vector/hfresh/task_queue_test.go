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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func TestTaskQueueRegisterIsExplicit(t *testing.T) {
	logger := logrus.New()
	scheduler := queue.NewScheduler(queue.SchedulerOptions{Logger: logger})
	scheduler.Start()
	t.Cleanup(func() {
		require.NoError(t, scheduler.Close(t.Context()))
	})

	cfg := DefaultConfig()
	cfg.ID = "hfresh"
	cfg.ShardName = "shard"
	cfg.VectorForIDThunk = func(context.Context, uint64) ([]float32, error) {
		return nil, fmt.Errorf("no vector store wired in this test")
	}
	cfg.RootPath = t.TempDir()
	cfg.Logger = logger
	cfg.Scheduler = scheduler

	store := testinghelpers.NewDummyStore(t)
	bucket, err := NewSharedBucket(store, cfg.ID, cfg.Store)
	require.NoError(t, err)

	index := &HFresh{
		id:        cfg.ID,
		logger:    logger,
		config:    cfg,
		scheduler: scheduler,
	}
	taskQueue, err := NewTaskQueue(index, bucket)
	require.NoError(t, err)
	index.taskQueue = *taskQueue
	t.Cleanup(func() {
		require.NoError(t, index.taskQueue.Close(t.Context()))
	})

	require.Equal(t, 0, scheduler.QueueCount())

	index.taskQueue.Register()

	require.Equal(t, 4, scheduler.QueueCount())
}

func TestWaitForMaintenanceDrainsQueuedTasks(t *testing.T) {
	tf := createHFreshIndex(t)
	const postingID = uint64(42)

	require.NoError(t, tf.Index.taskQueue.EnqueueAnalyze(postingID))
	require.True(t, tf.Index.taskQueue.analyzeList.Contains(postingID))
	require.EqualValues(t, 1, tf.Index.taskQueue.Size())

	tf.Index.waitForMaintenance(t)

	require.Zero(t, tf.Index.taskQueue.Size())
	require.False(t, tf.Index.taskQueue.analyzeList.Contains(postingID))
}

func TestWaitForMaintenanceDrainsFollowUpTasks(t *testing.T) {
	vector := []float32{1, 0, 0, 0}
	tf := createHFreshIndexWithVectorStore(t, [][]float32{vector})
	addVectorToIndex(t, &tf, 0, vector)
	tf.Index.waitForMaintenance(t)

	require.NoError(t, tf.Index.taskQueue.EnqueueReassign(42, 0))
	tf.Index.waitForMaintenance(t)

	require.Zero(t, tf.Index.taskQueue.Size())
	require.Zero(t, tf.Index.taskQueue.analyzeList.LivePages())
	require.Zero(t, tf.Index.taskQueue.reassignList.LivePages())
}

// The pending-task gauges are updated once a processed chunk is removed, so a
// drained queue reports no backlog.
func TestTaskQueuePendingMetricsAfterProcessing(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, ucfg := makeHFreshConfig(t)
	index := makeHFreshWithConfig(t, store, cfg, ucfg)

	require.NoError(t, index.taskQueue.EnqueueAnalyze(1))
	require.Equal(t, 1.0, testutil.ToFloat64(index.metrics.analyzePending))

	require.Eventually(t, func() bool {
		return index.taskQueue.analyzeQueue.Size() == 0
	}, 10*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(index.metrics.analyzePending) == 0
	}, time.Second, 10*time.Millisecond, "the gauge still reports the processed chunk")
}

func TestDecodeReassignTaskLegacyFormat(t *testing.T) {
	tq := &TaskQueue{}
	vecID := uint64(1234)

	task, err := tq.DecodeTask(encodeTask(vecID, taskQueueReassignOp))

	require.NoError(t, err)
	reassignTask, ok := task.(*ReassignTask)
	require.True(t, ok)
	require.Equal(t, vecID, reassignTask.vecID)
	require.Zero(t, reassignTask.postingID)
}

func TestDecodeReassignTaskWithPostingHint(t *testing.T) {
	tq := &TaskQueue{}
	vecID := uint64(1234)
	postingID := uint64(5678)

	task, err := tq.DecodeTask(encodeReassignTask(vecID, postingID))

	require.NoError(t, err)
	reassignTask, ok := task.(*ReassignTask)
	require.True(t, ok)
	require.Equal(t, vecID, reassignTask.vecID)
	require.Equal(t, postingID, reassignTask.postingID)
}

func TestDecodeMixedLegacyAndHintReassignTasks(t *testing.T) {
	tq := &TaskQueue{}

	records := [][]byte{
		encodeTask(1234, taskQueueReassignOp),
		encodeReassignTask(5678, 9012),
	}

	for _, record := range records {
		task, err := tq.DecodeTask(record)
		require.NoError(t, err)
		require.IsType(t, &ReassignTask{}, task)
	}
}

func TestDecodeReassignTaskIgnoresFutureTrailingFields(t *testing.T) {
	tq := &TaskQueue{}
	vecID := uint64(1234)
	postingID := uint64(5678)
	record := append(encodeReassignTask(vecID, postingID), []byte{1, 2, 3, 4}...)

	task, err := tq.DecodeTask(record)

	require.NoError(t, err)
	reassignTask, ok := task.(*ReassignTask)
	require.True(t, ok)
	require.Equal(t, vecID, reassignTask.vecID)
	require.Equal(t, postingID, reassignTask.postingID)
}

func TestEncodeReassignTaskKeepsLegacyPrefix(t *testing.T) {
	vecID := uint64(1234)
	postingID := uint64(5678)

	legacy := encodeTask(vecID, taskQueueReassignOp)
	encoded := encodeReassignTask(vecID, postingID)

	require.Len(t, encoded, len(legacy)+8)
	require.Equal(t, legacy, encoded[:len(legacy)])
}

func TestDecodeReassignTaskRejectsMalformedLength(t *testing.T) {
	tq := &TaskQueue{}
	record := []byte{taskQueueReassignOp, 1, 2, 3}

	_, err := tq.DecodeTask(record)

	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid reassign task length")
}

func TestDecodeTaskNonReassignFormatsUnchanged(t *testing.T) {
	tq := &TaskQueue{}

	tests := []struct {
		name string
		op   uint8
		id   uint64
		want any
	}{
		{
			name: "analyze",
			op:   taskQueueAnalyzeOp,
			id:   11,
			want: &AnalyzeTask{},
		},
		{
			name: "split",
			op:   taskQueueSplitOp,
			id:   22,
			want: &SplitTask{},
		},
		{
			name: "merge",
			op:   taskQueueMergeOp,
			id:   33,
			want: &MergeTask{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task, err := tq.DecodeTask(encodeTask(tt.id, tt.op))

			require.NoError(t, err)
			require.IsType(t, tt.want, task)
		})
	}
}

// Postings live in the shard's buckets, so the task queue hook writes their
// logs out before the queue removes the processed chunk.
func TestTaskQueueOnBatchProcessedWritesLSMLogs(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, ucfg := makeHFreshConfig(t)
	index := makeHFreshWithConfig(t, store, cfg, ucfg)

	bucket := store.Bucket(helpers.ObjectsBucketLSM)
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

	require.NoError(t, index.taskQueue.OnBatchProcessed())
	require.Greater(t, walSize(), before, "the bucket log was not written out")
}
