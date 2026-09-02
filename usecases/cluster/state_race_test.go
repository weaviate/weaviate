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
	"strings"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// TestNodeReadersDoNotRaceAliveUpdates pins resolver reads racing memberlist's in-place alive updates.
func TestNodeReadersDoNotRaceAliveUpdates(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()

	port1 := freeGossipPort(t)
	s1, err := Init(Config{
		Hostname:             "race-n1",
		Localhost:            true,
		GossipBindPort:       port1,
		DataBindPort:         port1 + 1,
		RaftBootstrapExpect:  1,
		RaftBootstrapTimeout: time.Second,
	}, 1, t.TempDir(), nil, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s1.list.Shutdown() })

	port2 := freeGossipPort(t)
	s2, err := Init(Config{
		Hostname:             "race-n2",
		Localhost:            true,
		GossipBindPort:       port2,
		DataBindPort:         port2 + 1,
		Join:                 fmt.Sprintf("127.0.0.1:%d", port1),
		RaftBootstrapExpect:  2,
		RaftBootstrapTimeout: 5 * time.Second,
	}, 1, t.TempDir(), nil, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.list.Shutdown() })

	require.Eventually(t, func() bool {
		_, ok := s1.NodeHostname("race-n2")
		return ok
	}, 10*time.Second, 50*time.Millisecond, "nodes never saw each other")

	done := make(chan struct{})
	readerDone := make(chan struct{})
	var readerErr error
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-done:
				return
			default:
			}
			if hostname, ok := s1.NodeHostname("race-n2"); ok && !strings.Contains(hostname, fmt.Sprintf(":%d", port2+1)) {
				readerErr = fmt.Errorf("unexpected data port in %q", hostname)
				return
			}
			s1.AllHostnames()
			s1.Hostnames()
			s1.AllOtherClusterMembers(1234)
			_, _ = s1.NodeGRPCPort("race-n2")
			s1.LocalAddr()
		}
	}()

	for i := 0; i < 30; i++ {
		require.NoError(t, s2.list.UpdateNode(2*time.Second))
		time.Sleep(20 * time.Millisecond)
	}
	close(done)
	<-readerDone
	require.NoError(t, readerErr)
}
