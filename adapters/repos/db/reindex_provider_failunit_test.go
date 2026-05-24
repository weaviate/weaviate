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
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/reindex"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// stubRecorder is a tiny TaskCompletionRecorder that lets a test program the
// failure sequence for RecordDistributedTaskUnitFailure. The other methods
// fail loudly so the test catches accidental cross-method calls.
type stubRecorder struct {
	// failureResults is consumed left-to-right per call. If a call goes beyond
	// the slice, the recorder returns nil (success).
	failureResults []error
	failureCalls   atomic.Int32
}

func (s *stubRecorder) RecordDistributedTaskUnitFailure(
	_ context.Context, _, _ string, _ uint64, _, _, _ string,
) error {
	idx := int(s.failureCalls.Add(1)) - 1
	if idx < len(s.failureResults) {
		return s.failureResults[idx]
	}
	return nil
}

func (s *stubRecorder) RecordDistributedTaskUnitCompletion(
	_ context.Context, _, _ string, _ uint64, _, _ string,
) error {
	panic("RecordDistributedTaskUnitCompletion should not be called from failUnit tests")
}

func (s *stubRecorder) UpdateDistributedTaskUnitProgress(
	_ context.Context, _, _ string, _ uint64, _, _ string, _ float32,
) error {
	panic("UpdateDistributedTaskUnitProgress should not be called from failUnit tests")
}

func newTestProvider(t *testing.T) (*reindex.ReindexProvider, *test.Hook) {
	t.Helper()
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	return &reindex.ReindexProvider{
		logger:    logger,
		localNode: "node-A",
	}, hook
}

func newFailUnitTestTask() *distributedtask.Task {
	return &distributedtask.Task{
		Namespace: "reindex",
		TaskDescriptor: distributedtask.TaskDescriptor{
			ID:      "C:enable-filterable:foo:abcd",
			Version: 1,
		},
	}
}

// Happy path: a single successful Record call returns immediately, no retry.
func TestFailUnit_SuccessOnFirstAttempt(t *testing.T) {
	p, hook := newTestProvider(t)
	rec := &stubRecorder{failureResults: nil} // all nil → success

	p.failUnit(context.Background(), newFailUnitTestTask(), "unit-1", rec, "something broke")

	require.Equal(t, int32(1), rec.failureCalls.Load(),
		"happy path: exactly one Record call, no retry")

	// First log entry: the failure reason. No second "failed to record" line.
	require.GreaterOrEqual(t, len(hook.Entries), 1)
	require.Contains(t, hook.LastEntry().Message, "unit failed")
}

// Transient failure on first attempt, success on second.
func TestFailUnit_RetriesTransientFailure(t *testing.T) {
	p, hook := newTestProvider(t)
	rec := &stubRecorder{failureResults: []error{errors.New("leadership lost")}}

	p.failUnit(context.Background(), newFailUnitTestTask(), "unit-1", rec, "something broke")

	require.Equal(t, int32(2), rec.failureCalls.Load(),
		"first call fails, second succeeds, third is never made")

	// We do NOT want the final "failed to record after retries" line on a
	// recovered call — it would falsely alarm operators.
	for _, e := range hook.AllEntries() {
		require.NotContains(t, e.Message, "failed to record unit failure after retries",
			"a recovered retry must not emit the final alarm line; got %q", e.Message)
	}
}

// All retries fail: emit the loud "manual operator action required" log
// with both the original failure reason and the recorder error so the
// failure is replayable.
func TestFailUnit_AllRetriesFail_EmitsLoudAlarm(t *testing.T) {
	p, hook := newTestProvider(t)
	recorderErr := errors.New("raft applyTimeout")
	rec := &stubRecorder{failureResults: []error{recorderErr, recorderErr, recorderErr}}

	p.failUnit(context.Background(), newFailUnitTestTask(), "unit-1", rec, "disk full")

	require.Equal(t, int32(3), rec.failureCalls.Load(),
		"three attempts before giving up")

	// Find the final alarm line and check both signals are present.
	var found bool
	for _, e := range hook.AllEntries() {
		if e.Level != logrus.ErrorLevel {
			continue
		}
		if !contains(e.Message, "failed to record unit failure after retries") {
			continue
		}
		found = true
		require.Equal(t, "disk full", e.Data["originalFailure"],
			"original failure reason must be on the alarm line so operators can replay")
		require.Equal(t, recorderErr.Error(), e.Data["recorderError"],
			"recorder error must be on the alarm line so the cause is visible")
	}
	require.True(t, found, "must emit the final alarm line when retries are exhausted")
}

// Context cancellation between retries aborts the loop cleanly without
// extra Record calls. The cancellation is logged so the missing FSM
// update is visible.
func TestFailUnit_ContextCancelledBetweenRetries(t *testing.T) {
	p, hook := newTestProvider(t)
	rec := &stubRecorder{failureResults: []error{errors.New("leadership lost")}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before the call so the first attempt's wait short-circuits

	p.failUnit(ctx, newFailUnitTestTask(), "unit-1", rec, "something broke")

	require.Equal(t, int32(1), rec.failureCalls.Load(),
		"after the first failure, the cancelled context aborts further attempts")

	var found bool
	for _, e := range hook.AllEntries() {
		if contains(e.Message, "context cancelled while recording unit failure") {
			found = true
		}
	}
	require.True(t, found, "context cancellation between retries must be logged so the missing FSM update is visible")
}

// Permanent FSM rejection: retry would return the same error on every
// attempt and the loud "manual operator action required" alarm would
// fire even though the FSM is internally consistent. Verify that
// permanent rejections short-circuit after the first call and log at
// warning level instead of the alarm.
//
// Each row exercises a specific permanent path and asserts both the
// short-circuit (no retry, no alarm) and the correct log signal:
//   - typed sentinel path: only the "FSM rejected ... as permanent"
//     warning fires (no legacy-fallback log).
//   - legacy substring path: BOTH the rejection warning and the
//     legacy-fallback warning fire, so an operator can detect that a
//     pre-sentinel peer is still in the cluster.
func TestFailUnit_PermanentRejection_NoRetryNoAlarm(t *testing.T) {
	// The typed-sentinel rows mirror what callers see in production:
	// Manager.RecordUnitCompletion (and its gRPC-rehydrated form) chain
	// the umbrella ErrPermanentRejection so errors.Is fires the fast
	// path. We chain via errors.Join(sentinel, ErrPermanentRejection),
	// which is the same shape produced by both
	// distributedtask.wrapPermanent and
	// distributedtask.RehydratePermanentRejection.
	typedErr := func(sentinel error, humanMsg string) error {
		return fmt.Errorf("executing command: %w",
			errors.Join(sentinel, distributedtask.ErrPermanentRejection, errors.New(humanMsg)))
	}

	cases := []struct {
		name           string
		err            error
		expectFallback bool
	}{
		{
			name:           "typed/task-not-running",
			err:            typedErr(distributedtask.ErrTaskNotRunning, "task ns/Foo/3 is no longer running"),
			expectFallback: false,
		},
		{
			name:           "typed/task-not-exist",
			err:            typedErr(distributedtask.ErrTaskDoesNotExist, "task ns/Foo/3 does not exist"),
			expectFallback: false,
		},
		{
			name:           "typed/unit-terminal",
			err:            typedErr(distributedtask.ErrUnitAlreadyTerminal, "unit u1 in task ns/Foo/3 is already terminal"),
			expectFallback: false,
		},
		{
			name:           "typed/unit-wrong-node",
			err:            typedErr(distributedtask.ErrUnitWrongNode, "unit u1 belongs to node X, not Y"),
			expectFallback: false,
		},
		{
			name:           "typed/umbrella-only",
			err:            fmt.Errorf("executing command: %w", distributedtask.ErrPermanentRejection),
			expectFallback: false,
		},
		{
			name:           "legacy-substring/task-not-running",
			err:            errors.New("executing command: task reindex/Foo/3 is no longer running"),
			expectFallback: true,
		},
		{
			name:           "legacy-substring/task-not-exist",
			err:            errors.New("executing command: task reindex/Foo/3 does not exist"),
			expectFallback: true,
		},
		{
			name:           "legacy-substring/unit-terminal",
			err:            errors.New("executing command: unit u1 in task reindex/Foo/3 is already terminal"),
			expectFallback: true,
		},
		{
			name:           "legacy-substring/unit-wrong-node",
			err:            errors.New("executing command: unit u1 in task reindex/Foo/3 belongs to node node-B, not node-A"),
			expectFallback: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, hook := newTestProvider(t)
			rec := &stubRecorder{failureResults: []error{tc.err}}

			p.failUnit(context.Background(), newFailUnitTestTask(), "u1", rec, "disk full")

			require.Equal(t, int32(1), rec.failureCalls.Load(),
				"permanent FSM rejection: exactly one Record call, no retry")

			// Must NOT emit the loud alarm.
			for _, e := range hook.AllEntries() {
				require.NotContains(t, e.Message, "failed to record unit failure after retries",
					"permanent rejection must not trigger the loud alarm")
			}

			// Must emit a Warn-level rejection log.
			var foundRejectionWarn, foundFallbackWarn bool
			for _, e := range hook.AllEntries() {
				if e.Level != logrus.WarnLevel {
					continue
				}
				if contains(e.Message, "FSM rejected unit-failure record as permanent") {
					foundRejectionWarn = true
				}
				if contains(e.Message, "permanent-rejection detected via legacy substring fallback") {
					foundFallbackWarn = true
				}
			}
			require.True(t, foundRejectionWarn, "permanent rejection must emit a warning-level log")
			require.Equal(t, tc.expectFallback, foundFallbackWarn,
				"legacy-substring inputs must trigger the fallback warning; "+
					"typed-sentinel inputs must NOT")
		})
	}
}

// Regression: a transient error MUST still retry even though the loop
// structure now early-exits on permanent rejections.
func TestFailUnit_TransientError_StillRetriesAfterPermanentBranchAdded(t *testing.T) {
	p, _ := newTestProvider(t)
	rec := &stubRecorder{failureResults: []error{errors.New("raft applyTimeout")}}

	p.failUnit(context.Background(), newFailUnitTestTask(), "unit-1", rec, "disk full")

	require.Equal(t, int32(2), rec.failureCalls.Load(),
		"transient error path retries; second call succeeds")
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
