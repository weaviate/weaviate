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

package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
)

// newPausableTestQueue uses a real, started Scheduler (not newMovementTestQueue's
// zero-value one) so IsQueuePaused reflects actual state; ScheduleInterval is set
// huge so the scheduler's own tick can't race the test's direct BeforeSchedule call.
func newPausableTestQueue(t *testing.T, vi VectorIndex) *VectorIndexQueue {
	t.Helper()

	s := queue.NewScheduler(queue.SchedulerOptions{ScheduleInterval: time.Hour})
	s.Start()
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	viq := &VectorIndexQueue{
		scheduler:   s,
		shard:       &Shard{},
		vectorIndex: vi,
	}

	dq, err := queue.NewDiskQueue(queue.DiskQueueOptions{
		ID:          "vector_index_queue_upgrade_pause_test",
		Scheduler:   s,
		Dir:         t.TempDir(),
		TaskDecoder: &vectorIndexQueueDecoder{q: viq},
	})
	require.NoError(t, err)
	require.NoError(t, dq.Init())
	viq.DiskQueue = dq

	s.RegisterQueue(viq)

	return viq
}

// Regression test (weaviate/0-weaviate-issues#296): BeforeSchedule must resume
// the queue itself if Upgrade errors without having invoked its callback.
func TestVectorIndexQueue_BeforeSchedule_DoesNotLeakPauseOnUpgradeError(t *testing.T) {
	fake := &movementFakeUpgradable{
		shouldUpgrade: true,
		upgradeAt:     0,
		indexed:       1,
		upgradeErr:    errors.New("ctx cancelled during shutdown"),
	}
	iq := newPausableTestQueue(t, fake)

	skip := iq.BeforeSchedule()

	require.True(t, skip, "the upgrade branch must have been taken")
	require.True(t, fake.upgradeCalled)
	require.False(t, iq.scheduler.IsQueuePaused(iq.ID()),
		"BeforeSchedule must resume the queue itself when Upgrade errors without having called its callback")
}
