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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
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
