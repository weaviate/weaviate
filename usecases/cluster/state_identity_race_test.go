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
	"sync"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// TestResolversConcurrentWithAliveUpdates: run with -race; each UpdateNode makes the gossip goroutines rewrite Node fields in place.
func TestResolversConcurrentWithAliveUpdates(t *testing.T) {
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

	var writers sync.WaitGroup
	for _, s := range []*State{s1, s2} {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for i := 0; i < 100; i++ {
				if err := s.list.UpdateNode(2 * time.Second); err != nil {
					t.Logf("update node: %v", err)
				}
			}
		}()
	}
	stop := make(chan struct{})
	go func() {
		writers.Wait()
		close(stop)
	}()

	// One dedicated goroutine per resolver: interleaving them in a single loop dilutes the post-Members() race window.
	resolvers := []func(){
		func() { s2.NodeHostname("identity-node1") },
		func() { s2.AllHostnames() },
		func() { s2.Hostnames() },
		func() { s2.AllOtherClusterMembers(8300) },
		func() {
			if _, err := s2.NodeGRPCPort("identity-node1"); err != nil {
				t.Error(err)
			}
		},
		func() { s2.LocalAddr() },
	}
	var readers sync.WaitGroup
	for _, resolve := range resolvers {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					resolve()
				}
			}
		}()
	}
	readers.Wait()
}
