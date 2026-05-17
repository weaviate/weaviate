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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// TestOnGroupCompleted_TerminalStatusShortCircuit pins the recovery-replay
// short-circuit at the top of [ReindexProvider.OnGroupCompleted]. When
// the scheduler invokes OnGroupCompleted for a task whose status is
// already terminal (FINISHED / FAILED / CANCELLED) — e.g. a startup
// recovery replay of a long-finished migration — the function must
// return nil immediately without attempting to load the payload or
// dispatch to RunSwapOnShard. Without this guard, the dispatch falls
// through to runtimeSwap which errors with "reindex bucket %q not
// found" because the swap dirs were trimmed at migration completion,
// spamming every node restart with bogus ERROR-level log entries.
//
// See 0-weaviate-issues#217 (spillover from 0-weaviate-issues#214 A5).
//
// The test exercises every status value defined in
// [distributedtask.TaskStatus] to make the boundary explicit: STARTED
// and FINALIZING continue the dispatch (returning an error from a
// missing payload — proves we got past the short-circuit), while the
// terminal trio returns nil without touching the payload loader.
func TestOnGroupCompleted_TerminalStatusShortCircuit(t *testing.T) {
	tests := []struct {
		status       distributedtask.TaskStatus
		shortCircuit bool
	}{
		{distributedtask.TaskStatusStarted, false},
		{distributedtask.TaskStatusFinalizing, false},
		{distributedtask.TaskStatusFinished, true},
		{distributedtask.TaskStatusFailed, true},
		{distributedtask.TaskStatusCancelled, true},
	}

	for _, tc := range tests {
		t.Run(string(tc.status), func(t *testing.T) {
			p, hook := newTestProvider(t)
			task := newFailUnitTestTask()
			task.Status = tc.status
			// Intentionally do NOT set Payload — if the short-circuit
			// doesn't fire, loadPayload errors and OnGroupCompleted
			// returns a non-nil error. That's how we observe whether
			// the short-circuit fired.

			err := p.OnGroupCompleted(task, "", []string{"unit-1"})

			if tc.shortCircuit {
				require.NoError(t, err,
					"OnGroupCompleted should short-circuit (return nil) for terminal status %q",
					tc.status)
				// Should have logged a Debug line about skipping —
				// loose check so the log message can evolve.
				var sawSkip bool
				for _, entry := range hook.AllEntries() {
					if entry.Message == "reindex provider: OnGroupCompleted: skipping replay on past-terminal task" {
						sawSkip = true
						break
					}
				}
				require.True(t, sawSkip,
					"expected debug log about skipping past-terminal task for status %q",
					tc.status)
			} else {
				// Non-terminal: dispatch falls through to loadPayload
				// which fails because we didn't populate Payload. That
				// failure proves the short-circuit did NOT fire — which
				// is exactly the correctness assertion we want for
				// STARTED/FINALIZING.
				require.Error(t, err,
					"OnGroupCompleted should NOT short-circuit for non-terminal status %q (expected payload load error)",
					tc.status)
			}
		})
	}
}
