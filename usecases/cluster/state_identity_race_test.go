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
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// TestNodeHostnameConcurrentWithAliveUpdates: the resolvers must not read live
// memberlist Node fields, which the gossip goroutine rewrites in place on
// every alive update; run with -race.
func TestNodeHostnameConcurrentWithAliveUpdates(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()

	port1 := freeGossipPort(t)
	s1, err := Init(Config{Hostname: "identity-node1", Localhost: true, GossipBindPort: port1}, 1, t.TempDir(), nil, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s1.list.Shutdown() })

	s2, err := Init(Config{
		Hostname: "identity-node2", Localhost: true,
		GossipBindPort: freeGossipPort(t),
		Join:           fmt.Sprintf("127.0.0.1:%d", port1),
	}, 1, t.TempDir(), nil, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.list.Shutdown() })

	require.Eventually(t, func() bool {
		_, ok := s2.NodeHostname("identity-node1")
		return ok
	}, 10*time.Second, 100*time.Millisecond, "node2 never resolved node1 after join")

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Each UpdateNode bumps the incarnation, making node2's gossip goroutine
		// rewrite node1's Node fields in place.
		for i := 0; i < 100; i++ {
			_ = s1.list.UpdateNode(2 * time.Second)
		}
	}()
	for i := 0; i < 2000; i++ {
		s2.NodeHostname("identity-node1")
		s2.AllHostnames()
	}
	<-done
}
