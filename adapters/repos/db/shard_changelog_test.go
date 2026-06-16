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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/replication/changelog"
)

// A transient error on attempt N must not leak when attempt N+1 succeeds;
// otherwise every filesystem hiccup aborts a movement.
func TestAppendWithRetry_RetriesTransientThenSucceeds(t *testing.T) {
	s := &Shard{}
	calls := 0
	lsn, err := s.appendWithRetry(func() (uint64, error) {
		calls++
		if calls < 3 {
			return 0, errors.New("transient i/o hiccup")
		}
		return 42, nil
	})
	require.NoError(t, err)
	require.Equal(t, uint64(42), lsn)
	require.Equal(t, 3, calls, "appendWithRetry should have used all 3 attempts")
}

// Retry must stop after the budget is spent and wrap the last underlying
// error — handleChangeLogFailure uses that chain to abort the movement.
func TestAppendWithRetry_ExhaustsAndWraps(t *testing.T) {
	s := &Shard{}
	cause := errors.New("disk full")
	calls := 0
	_, err := s.appendWithRetry(func() (uint64, error) {
		calls++
		return 0, cause
	})
	require.Error(t, err)
	require.Equal(t, 3, calls, "retry budget should be fully consumed")
	require.ErrorIs(t, err, cause, "wrapped error should chain back to the underlying cause")
}

// ErrLogFinalized/ErrLogDeactivated must not be retried — retrying can't
// help and extends the lock-hold window on the write path.
func TestAppendWithRetry_TerminalStatesShortCircuit(t *testing.T) {
	s := &Shard{}
	for _, terminal := range []error{changelog.ErrLogFinalized, changelog.ErrLogDeactivated} {
		calls := 0
		_, err := s.appendWithRetry(func() (uint64, error) {
			calls++
			return 0, terminal
		})
		require.ErrorIs(t, err, terminal)
		require.Equal(t, 1, calls, "terminal state %v must return after the first attempt", terminal)
	}
}
