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

package lazyshardunloading

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/test/docker"
)

var idleKnobs = knobs{"idle_ttl": "5s"}

// minPollsAcrossIdleTTL is 3s of 200ms polls after every shard was seen loaded.
const minPollsAcrossIdleTTL int64 = 15

// pollNodesVerbose reads every shard's status on the class until the test ends.
func pollNodesVerbose(ctx context.Context, t *testing.T, compose *docker.DockerCompose, class string) *atomic.Int64 {
	t.Helper()
	cl := newClient(compose.GetWeaviate().URI())
	pctx, stop := context.WithCancel(ctx)
	var polls atomic.Int64
	var pollErr atomic.Pointer[error]
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-pctx.Done():
				return
			case <-ticker.C:
				if _, err := nodeLoadedShards(pctx, cl, class); err != nil {
					if pctx.Err() == nil {
						pollErr.CompareAndSwap(nil, &err)
					}
					continue
				}
				polls.Add(1)
			}
		}
	}()
	t.Cleanup(func() {
		stop()
		wg.Wait()
		if err := pollErr.Load(); err != nil {
			t.Errorf("nodes API poll failed: %v", *err)
		}
	})
	return &polls
}

func TestIdleTTLUnloadsWhileNodesAPIIsPolled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	compose := start(ctx, t, newCompose(t, true, 1, fastKnobs(idleKnobs)))
	cl := newClient(compose.GetWeaviate().URI())
	waitLimit(ctx, t, compose, 1, 0)

	const class = "IdlePolled"
	tenants := tenantNames("t", 20)
	l := newLedger()
	createClass(ctx, t, cl, vectorClass(class, true, 1))
	createTenants(ctx, t, cl, class, tenants)
	polls := pollNodesVerbose(ctx, t, compose, class)
	seed(ctx, t, cl, l, class, tenants, 2, nil)
	eventuallyLoaded(ctx, t, compose, 1, 4*time.Second, func(n int) bool { return n == len(tenants) }, "every tenant is loaded")
	polls.Store(0)
	eventuallyLoaded(ctx, t, compose, 1, time.Minute, func(n int) bool { return n == 0 },
		"idle shards unload although the verbose nodes API reads them continuously")
	require.GreaterOrEqual(t, polls.Load(), minPollsAcrossIdleTTL, "the poll ran across the idle ttl")
	verifyAll(ctx, t, cl, l, class, tenants)
}

func TestMultiNodeIdleTTLUnloadsUnderHashbeatAndPolling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	compose := start(ctx, t, newCompose(t, true, clusterSize, fastKnobs(idleKnobs)))
	cl := newClient(compose.GetWeaviateNode(1).URI())
	waitLimit(ctx, t, compose, clusterSize, 0)

	const class = "IdleReplicated"
	tenants := tenantNames("t", 10)
	l := newLedger()
	createClass(ctx, t, cl, vectorClass(class, true, clusterSize))
	createTenants(ctx, t, cl, class, tenants)
	polls := pollNodesVerbose(ctx, t, compose, class)
	seed(ctx, t, cl, l, class, tenants, 3, &all)
	eventuallyLoaded(ctx, t, compose, clusterSize, 4*time.Second, func(n int) bool { return n == len(tenants) }, "every replica is loaded")
	polls.Store(0)
	eventuallyLoaded(ctx, t, compose, clusterSize, 2*time.Minute, func(n int) bool { return n == 0 },
		"loaded replicas unload when idle although peers' hashbeats and the nodes API read them")
	require.GreaterOrEqual(t, polls.Load(), minPollsAcrossIdleTTL, "the poll ran across the idle ttl")
	verifyAll(ctx, t, cl, l, class, tenants)
}
