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
	"net"
	"strings"
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

// freeGossipPort returns a port nothing is listening on, so the test does not
// collide with memberlist's default 7946
func freeGossipPort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	return port
}

// shrinkJoinInterval speeds up the retry backoff for a unit test; the retry
// window itself comes from Config.BootstrapTimeout.
func shrinkJoinInterval(t *testing.T, initial time.Duration) {
	t.Helper()

	orig := joinInitialInterval
	joinInitialInterval = initial
	t.Cleanup(func() { joinInitialInterval = orig })
}

// TestInitJoinRetryPolicy verifies a failed startup join is retried and never
// fatal, for both single and multi-node seeds.
func TestInitJoinRetryPolicy(t *testing.T) {
	const (
		testInitialInterval = 200 * time.Millisecond
		testJoinTimeout     = 1 * time.Second
	)

	tests := []struct {
		name            string
		bootstrapExpect int
		wantLog         string
	}{
		{"single-node seed", 1, "continuing as single-node cluster"},
		{"unset bootstrap expect", 0, "continuing as single-node cluster"},
		{"multi-node seed", 3, "periodic rejoin will retry"},
	}

	gossipPort := freeGossipPort(t)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			shrinkJoinInterval(t, testInitialInterval)
			logger, hook := logrustest.NewNullLogger()

			cfg := Config{
				Hostname:            "node1",
				Localhost:           true,
				Join:                deadJoinAddr,
				GossipBindPort:      gossipPort,
				RaftBootstrapExpect: test.bootstrapExpect,
				BootstrapTimeout:    testJoinTimeout,
			}

			start := time.Now()
			state, err := Init(cfg, 1, t.TempDir(), nil, logger)
			elapsed := time.Since(start)

			require.NoError(t, err)
			require.NotNil(t, state)
			t.Cleanup(func() { _ = state.list.Shutdown() })

			assert.GreaterOrEqual(t, elapsed, testInitialInterval,
				"returned before it could sleep even once; the join was not retried")

			var logged bool
			for _, e := range hook.AllEntries() {
				if strings.Contains(e.Message, test.wantLog) {
					logged = true
					break
				}
			}
			assert.True(t, logged, "expected a log entry containing %q", test.wantLog)
		})
	}
}
