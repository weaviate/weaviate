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

package dynamic

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
)

// BeforeSchedule pauses the queue and delivers the resume through Upgrade's
// callback. A failed upgrade leaves ShouldUpgrade() true, so the scheduler comes
// back — and the retry has to actually run, or the queue stays paused for the
// lifetime of the shard with nothing left to resume it.
func TestUpgradeRetriesAfterFailedAttempt(t *testing.T) {
	idx, _, _ := newDynamicAboveThreshold(t)

	var attempts atomic.Int64
	idx.makeCommitLoggerThunk = func() (hnsw.CommitLogger, error) {
		attempts.Add(1)
		return nil, errors.New("commit logger unavailable")
	}

	upgradeOnce := func(t *testing.T, what string) {
		t.Helper()
		done := make(chan struct{})
		require.NoError(t, idx.Upgrade(func() { close(done) }))
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatalf("%s: callback never ran, so the caller's paused queue is never resumed", what)
		}
	}

	upgradeOnce(t, "first attempt")
	require.False(t, idx.Upgraded(), "the upgrade failed, so the index must not report itself upgraded")

	upgradeOnce(t, "retry after failure")
	require.GreaterOrEqual(t, attempts.Load(), int64(2),
		"the retry must re-attempt the upgrade rather than silently no-op")
}
