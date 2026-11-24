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
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	enterrors "github.com/weaviate/weaviate/entities/errors"
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

func newTestTaskQueue(t testing.TB) *TaskQueue {
	t.Helper()
	cfg := DefaultConfig()
	cfg.RootPath = t.TempDir()
	logger := logrus.New()
	index := &HFresh{
		config:    cfg,
		logger:    logger,
		scheduler: queue.NewScheduler(queue.SchedulerOptions{Logger: logger}),
		metrics:   NewMetrics(nil, "", ""),
	}
	// Leave the scheduler stopped so tasks remain queued for inspection.
	tq, err := NewTaskQueue(index, bucketRef{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tq.Close(t.Context())) })
	return tq
}

func TestTaskQueueEnqueueRetryAfterPushFailure(t *testing.T) {
	for _, tt := range []struct {
		name    string
		enqueue func(*TaskQueue, uint64) error
	}{
		{"analyze", (*TaskQueue).EnqueueAnalyze},
		{"split", (*TaskQueue).EnqueueSplit},
		{"merge", (*TaskQueue).EnqueueMerge},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tq := newTestTaskQueue(t)
			dir := filepath.Join(tq.index.config.RootPath, tt.name+".queue.d")
			// Fail the first chunk creation, before the writer accepts a record.
			require.NoError(t, os.Remove(dir))
			require.NoError(t, os.WriteFile(dir, nil, 0o600))
			require.Error(t, tt.enqueue(tq, 42))
			require.Zero(t, tq.Size())
			require.NoError(t, os.Remove(dir))
			require.NoError(t, os.Mkdir(dir, 0o755))

			require.NoError(t, tt.enqueue(tq, 42))
			require.NoError(t, tt.enqueue(tq, 42))
			require.EqualValues(t, 1, tq.Size(), "retry must queue exactly one task")
		})
	}
}

func TestTaskQueueConcurrentEnqueueDeduplicates(t *testing.T) {
	tq := newTestTaskQueue(t)
	start := make(chan struct{})
	errs := make(chan error, 8)
	for range cap(errs) {
		enterrors.GoWrapper(func() {
			<-start
			errs <- tq.EnqueueMerge(42)
		}, tq.index.logger)
	}
	close(start)
	for range cap(errs) {
		require.NoError(t, <-errs)
	}
	require.EqualValues(t, 1, tq.Size())
}

func BenchmarkTaskQueueEnqueue(b *testing.B) {
	for _, tt := range []struct {
		name    string
		enqueue func(*TaskQueue, uint64) error
	}{
		{"analyze", (*TaskQueue).EnqueueAnalyze},
		{"split", (*TaskQueue).EnqueueSplit},
		{"merge", (*TaskQueue).EnqueueMerge},
	} {
		b.Run(tt.name, func(b *testing.B) {
			tq := newTestTaskQueue(b)
			b.ReportAllocs()
			var id uint64
			for b.Loop() {
				if err := tt.enqueue(tq, id); err != nil {
					b.Fatal(err)
				}
				id++
			}
			require.EqualValues(b, id, tq.Size())
		})
	}
}

// A maintenance task may hand work to any of the other queues, or to itself.
// Pausing scheduling must not close the writers while such tasks are running.
func TestTaskQueueCloseDrainsAllWorkersBeforeClosingWriters(t *testing.T) {
	logger := logrus.New()
	scheduler := queue.NewScheduler(queue.SchedulerOptions{Logger: logger, Workers: 4, ScheduleInterval: time.Millisecond})
	scheduler.Start()
	t.Cleanup(func() { require.NoError(t, scheduler.Close(context.Background())) })

	started := make(chan struct{}, 4)
	var release [4]chan struct{}
	var once [4]sync.Once
	unblock := func(i int) { once[i].Do(func() { close(release[i]) }) }
	results := make(chan error, 4)
	queues := make([]*queue.DiskQueue, 4)
	// analyze -> split, split -> split, reassign -> analyze, merge -> reassign
	targets := []int{1, 1, 0, 2}
	for i := range queues {
		release[i] = make(chan struct{})
		t.Cleanup(func() { unblock(i) })
		decoder := shutdownTaskDecoder{execute: func(context.Context) error {
			started <- struct{}{}
			<-release[i]
			err := queues[targets[i]].Push([]byte{1})
			results <- err
			return err
		}}
		var err error
		queues[i], err = queue.NewDiskQueue(queue.DiskQueueOptions{
			ID: fmt.Sprint(i), Dir: filepath.Join(t.TempDir(), "queue"), Logger: logger,
			Scheduler: scheduler, TaskDecoder: decoder,
		})
		require.NoError(t, err)
		require.NoError(t, queues[i].Init())
		require.NoError(t, queues[i].Push([]byte{1}))
		require.NoError(t, queues[i].Flush())
	}
	tq := TaskQueue{analyzeQueue: &analyzeQueue{DiskQueue: queues[0]}, splitQueue: queues[1], reassignQueue: queues[2], mergeQueue: queues[3], scheduler: scheduler}
	for _, q := range queues {
		scheduler.RegisterQueue(q)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for range queues {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("maintenance worker did not start")
		}
	}
	done := make(chan error, 1)
	enterrors.GoWrapper(func() { done <- tq.Close(ctx) }, logger)
	// Keep each worker active until its own drain starts, including the last
	// queue, when an omitted drain would have closed its downstream writer.
	var released [4]bool
	require.Eventually(t, func() bool {
		all := true
		for i, q := range queues {
			if !released[i] && scheduler.IsQueuePaused(q.ID()) {
				released[i] = true
				unblock(i)
			}
			all = all && released[i]
		}
		return all
	}, 5*time.Second, time.Millisecond)
	for range queues {
		select {
		case err := <-results:
			require.NoError(t, err, "in-flight maintenance must be able to enqueue follow-up work")
		case <-ctx.Done():
			t.Fatal("maintenance worker did not finish")
		}
	}
	require.NoError(t, <-done)
	require.Zero(t, scheduler.QueueCount())
}

type shutdownTaskDecoder struct{ execute func(context.Context) error }

func (d shutdownTaskDecoder) DecodeTask([]byte) (queue.Task, error) { return shutdownQueueTask(d), nil }

type shutdownQueueTask shutdownTaskDecoder

func (t shutdownQueueTask) Op() uint8                         { return 1 }
func (t shutdownQueueTask) Key() uint64                       { return 0 }
func (t shutdownQueueTask) Execute(ctx context.Context) error { return t.execute(ctx) }
