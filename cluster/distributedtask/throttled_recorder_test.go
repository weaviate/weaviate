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

// TestThrottledRecorder_DefaultInterval_PinnedAt3s pins the production
// throttling interval at 3 seconds. Pre-existing sub-report 03 from
// weaviate/0-weaviate-issues#243 caught a doc/code drift: the type godoc on
// `ThrottledRecorder` claimed "default 30s" and `doc.go` said "30 seconds",
// while `scheduler.Start` was wiring `3*time.Second` since the migration
// from the old default. Without a test pinning this, the documentation and
// the constant could diverge again silently.
//
// If you intentionally change the interval, also update:
//   - the rationale comment in `scheduler.go` Start()
//   - the "Progress throttling" prose in `doc.go`
//   - the godoc on `DefaultThrottleInterval`
//
// The 3s rationale: ~20 samples per minute per unit on the RAFT hot path
// keeps the UI live without flooding the log; coarser caps (the old 30s)
// made the progress bar appear to jump in large increments on 60-90s
// reindexes.
func TestThrottledRecorder_DefaultInterval_PinnedAt3s(t *testing.T) {
	require.Equal(t, 3*time.Second, DefaultThrottleInterval,
		"production throttle interval must remain at 3s; if you change this, "+
			"also update scheduler.Start rationale + doc.go prose + the godoc on this constant")
}

func newTestThrottledRecorder(t *testing.T) (*ThrottledRecorder, *clockwork.FakeClock, *MockTaskCompletionRecorder) {
	clock := clockwork.NewFakeClock()
	inner := NewMockTaskCompletionRecorder(t)
	return NewThrottledRecorder(inner, 30*time.Second, clock), clock, inner
}

func TestThrottledRecorder_ProgressWithinInterval_NotForwarded(t *testing.T) {
	recorder, clock, inner := newTestThrottledRecorder(t)

	inner.EXPECT().UpdateDistributedTaskUnitProgress(
		mock.Anything, "ns", "task", uint64(1), "node", "su-1", float32(0.1),
	).Return(nil).Once()

	// First call should go through
	err := recorder.UpdateDistributedTaskUnitProgress(context.Background(), "ns", "task", 1, "node", "su-1", 0.1)
	require.NoError(t, err)

	// Second call within interval should be skipped
	clock.Advance(10 * time.Second)
	err = recorder.UpdateDistributedTaskUnitProgress(context.Background(), "ns", "task", 1, "node", "su-1", 0.2)
	require.NoError(t, err)
}

func TestThrottledRecorder_ProgressAfterInterval_Forwarded(t *testing.T) {
	recorder, clock, inner := newTestThrottledRecorder(t)

	inner.EXPECT().UpdateDistributedTaskUnitProgress(
		mock.Anything, "ns", "task", uint64(1), "node", "su-1", float32(0.1),
	).Return(nil).Once()

	inner.EXPECT().UpdateDistributedTaskUnitProgress(
		mock.Anything, "ns", "task", uint64(1), "node", "su-1", float32(0.5),
	).Return(nil).Once()

	err := recorder.UpdateDistributedTaskUnitProgress(context.Background(), "ns", "task", 1, "node", "su-1", 0.1)
	require.NoError(t, err)

	clock.Advance(31 * time.Second)

	err = recorder.UpdateDistributedTaskUnitProgress(context.Background(), "ns", "task", 1, "node", "su-1", 0.5)
	require.NoError(t, err)
}

func TestThrottledRecorder_CompletionNeverThrottled(t *testing.T) {
	recorder, _, inner := newTestThrottledRecorder(t)

	inner.EXPECT().RecordDistributedTaskUnitCompletion(
		mock.Anything, "ns", "task", uint64(1), "node", "su-1",
	).Return(nil).Once()

	inner.EXPECT().RecordDistributedTaskUnitFailure(
		mock.Anything, "ns", "task", uint64(1), "node", "su-2", "err",
	).Return(nil).Once()

	err := recorder.RecordDistributedTaskUnitCompletion(context.Background(), "ns", "task", 1, "node", "su-1")
	require.NoError(t, err)

	err = recorder.RecordDistributedTaskUnitFailure(context.Background(), "ns", "task", 1, "node", "su-2", "err")
	require.NoError(t, err)
}

func TestThrottledRecorder_CompletionCleansUpThrottleEntry(t *testing.T) {
	recorder, _, inner := newTestThrottledRecorder(t)

	// First progress call goes through
	inner.EXPECT().UpdateDistributedTaskUnitProgress(
		mock.Anything, "ns", "task", uint64(1), "node", "su-1", float32(0.1),
	).Return(nil).Once()

	err := recorder.UpdateDistributedTaskUnitProgress(context.Background(), "ns", "task", 1, "node", "su-1", 0.1)
	require.NoError(t, err)

	// Complete the unit — this should clean up the throttle entry
	inner.EXPECT().RecordDistributedTaskUnitCompletion(
		mock.Anything, "ns", "task", uint64(1), "node", "su-1",
	).Return(nil).Once()

	err = recorder.RecordDistributedTaskUnitCompletion(context.Background(), "ns", "task", 1, "node", "su-1")
	require.NoError(t, err)

	// Verify the throttle entry was cleaned up — the lastSent map should not
	// contain an entry for this unit anymore. We can check indirectly by
	// verifying the map length.
	recorder.mu.Lock()
	require.Empty(t, recorder.lastSent)
	recorder.mu.Unlock()
}

func TestThrottledRecorder_FailureCleansUpThrottleEntry(t *testing.T) {
	recorder, _, inner := newTestThrottledRecorder(t)

	// First progress call goes through
	inner.EXPECT().UpdateDistributedTaskUnitProgress(
		mock.Anything, "ns", "task", uint64(1), "node", "su-1", float32(0.1),
	).Return(nil).Once()

	err := recorder.UpdateDistributedTaskUnitProgress(context.Background(), "ns", "task", 1, "node", "su-1", 0.1)
	require.NoError(t, err)

	// Fail the unit — this should clean up the throttle entry
	inner.EXPECT().RecordDistributedTaskUnitFailure(
		mock.Anything, "ns", "task", uint64(1), "node", "su-1", "err",
	).Return(nil).Once()

	err = recorder.RecordDistributedTaskUnitFailure(context.Background(), "ns", "task", 1, "node", "su-1", "err")
	require.NoError(t, err)

	recorder.mu.Lock()
	require.Empty(t, recorder.lastSent)
	recorder.mu.Unlock()
}

func TestThrottledRecorder_DifferentUnitsTrackedIndependently(t *testing.T) {
	recorder, clock, inner := newTestThrottledRecorder(t)

	inner.EXPECT().UpdateDistributedTaskUnitProgress(
		mock.Anything, "ns", "task", uint64(1), "node", "su-1", float32(0.1),
	).Return(nil).Once()

	inner.EXPECT().UpdateDistributedTaskUnitProgress(
		mock.Anything, "ns", "task", uint64(1), "node", "su-2", float32(0.2),
	).Return(nil).Once()

	// Both first calls should go through
	err := recorder.UpdateDistributedTaskUnitProgress(context.Background(), "ns", "task", 1, "node", "su-1", 0.1)
	require.NoError(t, err)

	err = recorder.UpdateDistributedTaskUnitProgress(context.Background(), "ns", "task", 1, "node", "su-2", 0.2)
	require.NoError(t, err)

	// Both second calls should be throttled
	clock.Advance(10 * time.Second)
	err = recorder.UpdateDistributedTaskUnitProgress(context.Background(), "ns", "task", 1, "node", "su-1", 0.3)
	require.NoError(t, err)

	err = recorder.UpdateDistributedTaskUnitProgress(context.Background(), "ns", "task", 1, "node", "su-2", 0.4)
	require.NoError(t, err)
}

// TestThrottledRecorder_ErroredCall_RetryNotSilentlyDropped pins
// weaviate/0-weaviate-issues#240 Symptom B's root-cause race:
//
// The current implementation updates `lastSent[key]` BEFORE forwarding
// to the inner recorder. If the inner call fails (e.g. RAFT leadership
// transfer in progress), the throttler still considers the call "sent"
// at time T1. A retry within the throttle interval (default 3s in
// production via NewThrottledRecorder(..., 3*time.Second, ...)) returns
// nil without forwarding — silently dropping the retried call.
//
// For most progress updates this is benign. But for the INITIAL
// progress=0.0 call in [ReindexProvider.processOneUnit] this call is
// the unit-CLAIM: the only path that sets Unit.NodeID. If both the
// first attempt fails AND the retry is silently dropped, the unit's
// NodeID stays empty through eventual RecordUnitCompletion (which
// does NOT set NodeID itself), so the unit is orphaned at
// AllUnitsTerminal time. LocalGroupUnitIDs filters by u.NodeID ==
// nodeID; the orphan is excluded from every node's local group, PREP
// never runs, the local sentinel chain stops at REINDEXED, and the
// canonical bucket on the affected replica stays at OLD tokenization
// while the cluster-wide schema flip commits.
//
// Expected behavior: a retry within the throttle window AFTER an
// errored first attempt should be forwarded. Either:
//   - lastSent updated only on successful forward, OR
//   - lastSent reset on error
//
// Current behavior (pre-fix): the retry is silently dropped.
func TestThrottledRecorder_ErroredCall_RetryNotSilentlyDropped(t *testing.T) {
	recorder, clock, inner := newTestThrottledRecorder(t)

	// First call: forward returns an error simulating a RAFT
	// leadership-transfer-in-progress error from the inner recorder.
	inner.EXPECT().UpdateDistributedTaskUnitProgress(
		mock.Anything, "ns", "task", uint64(1), "node", "su-1", float32(0.0),
	).Return(assertErr("leadership transfer in progress")).Once()

	err := recorder.UpdateDistributedTaskUnitProgress(context.Background(), "ns", "task", 1, "node", "su-1", 0.0)
	require.Error(t, err, "first call should propagate the inner error")

	// Retry within the throttle interval (1 second after the failed
	// first call). The throttle interval is 30s in this test fixture.
	// The retry MUST be forwarded — without this guarantee, the
	// unit-CLAIM is silently dropped and the unit never has its
	// NodeID set by the FSM.
	inner.EXPECT().UpdateDistributedTaskUnitProgress(
		mock.Anything, "ns", "task", uint64(1), "node", "su-1", float32(0.0),
	).Return(nil).Once()

	clock.Advance(1 * time.Second)
	err = recorder.UpdateDistributedTaskUnitProgress(context.Background(), "ns", "task", 1, "node", "su-1", 0.0)
	require.NoError(t, err, "retry within throttle window after an errored first attempt must forward, not silently drop")
}

// assertErr is a tiny helper to construct an error with a message.
// Imported helpers like errors.New live in the std library; pulling
// that in here just for one test would be heavier than this inline.
type assertErr string

func (e assertErr) Error() string { return string(e) }
