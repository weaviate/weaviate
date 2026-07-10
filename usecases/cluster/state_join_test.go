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

package cluster

import (
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// deadJoinAddr resolves, so Init gets past the net.LookupIP guard, but refuses
// the gossip dial — which is exactly what a peer whose port has not bound yet
// looks like.
const deadJoinAddr = "127.0.0.1:1"

// shrinkJoinBudget makes the retry window small enough to sleep through in a
// unit test, and restores it afterwards.
func shrinkJoinBudget(t *testing.T, initial, total time.Duration) {
	t.Helper()

	origInitial, origTotal := joinInitialInterval, joinTimeout
	joinInitialInterval, joinTimeout = initial, total
	t.Cleanup(func() { joinInitialInterval, joinTimeout = origInitial, origTotal })
}

// TestInitJoinRetryPolicy pins who is allowed to wait for a peer.
func TestInitJoinRetryPolicy(t *testing.T) {
	// Long enough that a single retry is unmistakable, short enough to wait out.
	const (
		testInitialInterval = 200 * time.Millisecond
		testJoinTimeout     = 2 * time.Second
	)

	tests := []struct {
		name            string
		bootstrapExpect int
		wantErr         bool
		wantRetry       bool
	}{
		{
			name:            "sole seed: unreachable join address is fatal, and immediately so",
			bootstrapExpect: 1,
			wantErr:         true,
			wantRetry:       false,
		},
		{
			name:            "unset bootstrap expect is treated as a sole seed",
			bootstrapExpect: 0,
			wantErr:         true,
			wantRetry:       false,
		},
		{
			name:            "peers expected: retry the window, then start and let rejoin converge",
			bootstrapExpect: 3,
			wantErr:         false,
			wantRetry:       true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			shrinkJoinBudget(t, testInitialInterval, testJoinTimeout)
			logger, hook := logrustest.NewNullLogger()

			cfg := Config{
				Hostname:            "node1",
				Localhost:           true,
				Join:                deadJoinAddr,
				RaftBootstrapExpect: test.bootstrapExpect,
			}

			start := time.Now()
			state, err := Init(cfg, 1, t.TempDir(), nil, logger)
			elapsed := time.Since(start)

			if test.wantErr {
				require.Error(t, err)
				assert.Nil(t, state)
			} else {
				require.NoError(t, err)
				require.NotNil(t, state)
				t.Cleanup(func() { _ = state.list.Shutdown() })
			}

			// The only signal that separates "retried" from "gave up at once" is
			// time: one backoff sleep is the smallest observable retry.
			if test.wantRetry {
				assert.GreaterOrEqual(t, elapsed, testInitialInterval,
					"returned before it could have slept even once; the join was not retried")
			} else {
				assert.Less(t, elapsed, testInitialInterval,
					"slept at least one backoff interval; the sole seed retried instead of failing fast")
			}

			// Neither giving up nor carrying on may happen quietly.
			assert.NotEmpty(t, hook.AllEntries(), "an unreachable join must be logged")
		})
	}
}
