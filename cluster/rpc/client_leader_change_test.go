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

package rpc

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"google.golang.org/grpc/connectivity"
)

// TestClient_Query_LeaderChangeMidRPC reproduces weaviate/0-weaviate-issues#284:
// a concurrent getConn observing a new leader used to Close() the conn another
// RPC had just acquired, surfacing "grpc: the client connection is closing" on
// leader-forwarded requests during rolling restarts. The hook deterministically
// forces the leader change into the window between getConn and the RPC call.
func TestClient_Query_LeaderChangeMidRPC(t *testing.T) {
	addr1, stop1 := startTestServer(t, &testServer{})
	defer stop1()
	addr2, stop2 := startTestServer(t, &testServer{})
	defer stop2()

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		cl := NewClient(&testResolver{}, fourMiB, false, logrus.StandardLogger())

		var fired atomic.Bool
		cl.testOnConnAcquired = func() {
			// fire only for the outer Query; the nested Query must not recurse
			if !fired.CompareAndSwap(false, true) {
				return
			}
			_, err := cl.Query(ctx, addr2, &cmd.QueryRequest{})
			require.NoError(t, err)
		}

		_, err := cl.Query(ctx, addr1, &cmd.QueryRequest{})
		require.NoError(t, err, "iteration %d: leader change mid-RPC must not kill the in-flight query", i)
		cl.Close()
	}
}

// TestClient_getConn_RetiredConnClosesAfterLastRelease pins the drain half of
// the swap-then-drain contract: a conn retired by a leader change must still be
// closed once its last in-flight RPC releases it, otherwise every leader change
// would leak a conn.
func TestClient_getConn_RetiredConnClosesAfterLastRelease(t *testing.T) {
	addr1, stop1 := startTestServer(t, &testServer{})
	defer stop1()
	addr2, stop2 := startTestServer(t, &testServer{})
	defer stop2()

	ctx := context.Background()
	cl := NewClient(&testResolver{}, fourMiB, false, logrus.StandardLogger())
	defer cl.Close()

	conn1, release1, err := cl.getConn(ctx, addr1)
	require.NoError(t, err)

	// leader change retires conn1 but must not close it: a ref is still held
	_, release2, err := cl.getConn(ctx, addr2)
	require.NoError(t, err)
	release2()
	require.NotEqual(t, connectivity.Shutdown, conn1.GetState())

	release1()
	require.Equal(t, connectivity.Shutdown, conn1.GetState())
}

// TestClient_Query_ConcurrentLeaderFlap hammers Query with the client's view of
// the leader flapping between two nodes; no request may ever observe its conn
// closed underneath it, regardless of interleaving.
func TestClient_Query_ConcurrentLeaderFlap(t *testing.T) {
	addr1, stop1 := startTestServer(t, &testServer{})
	defer stop1()
	addr2, stop2 := startTestServer(t, &testServer{})
	defer stop2()

	cl := NewClient(&testResolver{}, fourMiB, false, logrus.StandardLogger())
	defer cl.Close()

	ctx := context.Background()
	addrs := []string{addr1, addr2}

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if _, err := cl.Query(ctx, addr, &cmd.QueryRequest{}); err != nil {
					errs <- err
					return
				}
			}
		}(addrs[g%2])
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("query failed during leader flap: %v", err)
	}
}
