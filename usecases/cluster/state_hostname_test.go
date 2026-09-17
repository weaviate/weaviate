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
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newHostnameTestState(tb testing.TB, hostname, join string, dataPort int) (*State, int) {
	tb.Helper()

	logger, _ := logrustest.NewNullLogger()
	gossipPort := freeGossipPort(tb)
	state, err := Init(Config{
		Hostname:       hostname,
		Localhost:      true,
		BindAddr:       "127.0.0.1",
		AdvertiseAddr:  "127.0.0.1",
		Join:           join,
		GossipBindPort: gossipPort,
		DataBindPort:   dataPort,
	}, 1, tb.TempDir(), nil, logger)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, state.Shutdown())
	})

	return state, gossipPort
}

func TestNodeHostnameCached(t *testing.T) {
	state, _ := newHostnameTestState(t, "hn-cached-node1", "", 8080)

	cold, ok := state.NodeHostname("hn-cached-node1")
	require.True(t, ok)
	require.Equal(t, "127.0.0.1:8080", cold)

	allocs := testing.AllocsPerRun(100, func() {
		warm, ok := state.NodeHostname("hn-cached-node1")
		if !ok || warm != cold {
			t.Errorf("warm lookup diverged: %q %v", warm, ok)
		}
	})
	assert.Zero(t, allocs)

	_, ok = state.NodeHostname("hn-cached-unknown")
	assert.False(t, ok)
}

func TestNodeHostnameSeesJoinedNode(t *testing.T) {
	state1, gossipPort1 := newHostnameTestState(t, "hn-join-node1", "", 8080)

	_, ok := state1.NodeHostname("hn-join-node1")
	require.True(t, ok)
	_, ok = state1.NodeHostname("hn-join-node2")
	require.False(t, ok)

	newHostnameTestState(t, "hn-join-node2", "127.0.0.1:"+strconv.Itoa(gossipPort1), 8081)

	require.Eventually(t, func() bool {
		addr, ok := state1.NodeHostname("hn-join-node2")
		return ok && addr == "127.0.0.1:8081"
	}, 10*time.Second, 50*time.Millisecond)
}

func TestNodeHostnameConcurrentChurn(t *testing.T) {
	state1, gossipPort1 := newHostnameTestState(t, "hn-churn-node1", "", 8080)

	expected, ok := state1.NodeHostname("hn-churn-node1")
	require.True(t, ok)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if addr, ok := state1.NodeHostname("hn-churn-node1"); !ok || addr != expected {
					t.Errorf("stable node resolution broke: %q %v", addr, ok)
					return
				}
				if _, ok := state1.NodeHostname("hn-churn-never"); ok {
					t.Error("unknown node resolved")
					return
				}
			}
		}()
	}

	for cycle := 0; cycle < 3; cycle++ {
		name := fmt.Sprintf("hn-churn-extra%d", cycle)
		logger, _ := logrustest.NewNullLogger()
		extra, err := Init(Config{
			Hostname:       name,
			Localhost:      true,
			BindAddr:       "127.0.0.1",
			AdvertiseAddr:  "127.0.0.1",
			Join:           "127.0.0.1:" + strconv.Itoa(gossipPort1),
			GossipBindPort: freeGossipPort(t),
			DataBindPort:   8081,
		}, 1, t.TempDir(), nil, logger)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			_, ok := state1.NodeHostname(name)
			return ok
		}, 10*time.Second, 50*time.Millisecond)

		require.NoError(t, extra.Leave())
		require.NoError(t, extra.Shutdown())

		require.Eventually(t, func() bool {
			_, ok := state1.NodeHostname(name)
			return !ok
		}, 10*time.Second, 50*time.Millisecond)
	}

	close(stop)
	wg.Wait()
}

func BenchmarkNodeHostname(b *testing.B) {
	state, _ := newHostnameTestState(b, "hn-bench-node1", "", 8080)

	if _, ok := state.NodeHostname("hn-bench-node1"); !ok {
		b.Fatal("local node not resolved")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := state.NodeHostname("hn-bench-node1"); !ok {
			b.Fatal("node not resolved")
		}
	}
}
