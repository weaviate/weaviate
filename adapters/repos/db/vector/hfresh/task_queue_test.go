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
