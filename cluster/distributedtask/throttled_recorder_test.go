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

package distributedtask

import (
	"context"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestThrottledRecorder_ProgressWithinInterval_NotForwarded(t *testing.T) {
	clock := clockwork.NewFakeClock()
	inner := NewMockTaskCompletionRecorder(t)
	recorder := NewThrottledRecorder(inner, 30*time.Second, clock)

	inner.EXPECT().UpdateDistributedTaskSubUnitProgress(
		mock.Anything, "ns", "task", uint64(1), "node", "su-1", float32(0.1),
	).Return(nil).Once()

	// First call should go through
	err := recorder.UpdateDistributedTaskSubUnitProgress(context.Background(), "ns", "task", 1, "node", "su-1", 0.1)
	require.NoError(t, err)

	// Second call within interval should be skipped
	clock.Advance(10 * time.Second)
	err = recorder.UpdateDistributedTaskSubUnitProgress(context.Background(), "ns", "task", 1, "node", "su-1", 0.2)
	require.NoError(t, err)
}

func TestThrottledRecorder_ProgressAfterInterval_Forwarded(t *testing.T) {
	clock := clockwork.NewFakeClock()
	inner := NewMockTaskCompletionRecorder(t)
	recorder := NewThrottledRecorder(inner, 30*time.Second, clock)

	inner.EXPECT().UpdateDistributedTaskSubUnitProgress(
		mock.Anything, "ns", "task", uint64(1), "node", "su-1", float32(0.1),
	).Return(nil).Once()

	inner.EXPECT().UpdateDistributedTaskSubUnitProgress(
		mock.Anything, "ns", "task", uint64(1), "node", "su-1", float32(0.5),
	).Return(nil).Once()

	err := recorder.UpdateDistributedTaskSubUnitProgress(context.Background(), "ns", "task", 1, "node", "su-1", 0.1)
	require.NoError(t, err)

	clock.Advance(31 * time.Second)

	err = recorder.UpdateDistributedTaskSubUnitProgress(context.Background(), "ns", "task", 1, "node", "su-1", 0.5)
	require.NoError(t, err)
}

func TestThrottledRecorder_CompletionNeverThrottled(t *testing.T) {
	clock := clockwork.NewFakeClock()
	inner := NewMockTaskCompletionRecorder(t)
	recorder := NewThrottledRecorder(inner, 30*time.Second, clock)

	inner.EXPECT().RecordDistributedTaskSubUnitCompletion(
		mock.Anything, "ns", "task", uint64(1), "node", "su-1",
	).Return(nil).Once()

	inner.EXPECT().RecordDistributedTaskSubUnitFailure(
		mock.Anything, "ns", "task", uint64(1), "node", "su-2", "err",
	).Return(nil).Once()

	inner.EXPECT().RecordDistributedTaskNodeCompletion(
		mock.Anything, "ns", "task", uint64(1),
	).Return(nil).Once()

	inner.EXPECT().RecordDistributedTaskNodeFailure(
		mock.Anything, "ns", "task", uint64(1), "err",
	).Return(nil).Once()

	err := recorder.RecordDistributedTaskSubUnitCompletion(context.Background(), "ns", "task", 1, "node", "su-1")
	require.NoError(t, err)

	err = recorder.RecordDistributedTaskSubUnitFailure(context.Background(), "ns", "task", 1, "node", "su-2", "err")
	require.NoError(t, err)

	err = recorder.RecordDistributedTaskNodeCompletion(context.Background(), "ns", "task", 1)
	require.NoError(t, err)

	err = recorder.RecordDistributedTaskNodeFailure(context.Background(), "ns", "task", 1, "err")
	require.NoError(t, err)
}

func TestThrottledRecorder_DifferentSubUnitsTrackedIndependently(t *testing.T) {
	clock := clockwork.NewFakeClock()
	inner := NewMockTaskCompletionRecorder(t)
	recorder := NewThrottledRecorder(inner, 30*time.Second, clock)

	inner.EXPECT().UpdateDistributedTaskSubUnitProgress(
		mock.Anything, "ns", "task", uint64(1), "node", "su-1", float32(0.1),
	).Return(nil).Once()

	inner.EXPECT().UpdateDistributedTaskSubUnitProgress(
		mock.Anything, "ns", "task", uint64(1), "node", "su-2", float32(0.2),
	).Return(nil).Once()

	// Both first calls should go through
	err := recorder.UpdateDistributedTaskSubUnitProgress(context.Background(), "ns", "task", 1, "node", "su-1", 0.1)
	require.NoError(t, err)

	err = recorder.UpdateDistributedTaskSubUnitProgress(context.Background(), "ns", "task", 1, "node", "su-2", 0.2)
	require.NoError(t, err)

	// Both second calls should be throttled
	clock.Advance(10 * time.Second)
	err = recorder.UpdateDistributedTaskSubUnitProgress(context.Background(), "ns", "task", 1, "node", "su-1", 0.3)
	require.NoError(t, err)

	err = recorder.UpdateDistributedTaskSubUnitProgress(context.Background(), "ns", "task", 1, "node", "su-2", 0.4)
	require.NoError(t, err)
}
