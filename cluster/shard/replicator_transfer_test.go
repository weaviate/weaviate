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

package shard_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	routerTypes "github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/cluster/shard"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	"google.golang.org/grpc"
)

// stubApplyClient short-circuits the forwarding RPC to the target node's
// Server in-process. Server handlers return status.Error values, so the
// client-side code classification is exercised end-to-end without sockets.
type stubApplyClient struct {
	shardproto.ShardReplicationServiceClient
	srv *shard.Server
}

func (c stubApplyClient) Apply(ctx context.Context, in *shardproto.ApplyRequest, _ ...grpc.CallOption) (*shardproto.ApplyResponse, error) {
	return c.srv.Apply(ctx, in)
}

func (c stubApplyClient) GetLastAppliedIndex(ctx context.Context, in *shardproto.GetLastAppliedIndexRequest, _ ...grpc.CallOption) (*shardproto.GetLastAppliedIndexResponse, error) {
	return c.srv.GetLastAppliedIndex(ctx, in)
}

// TestReplicator_LeadershipTransfer_NoClientErrors pins the operator's
// acceptance criterion for brief leadership transitions: an import running
// through the replicator write path (the SSB server side) must complete every
// PutObject without a client-visible error while shard leadership moves
// between nodes — whether the coordinating node is the leader (local Apply
// path) or a follower (forwarded RPC path). Leadership churn surfaces as
// ErrLeadershipLost / ErrNotLeader / proposal backpressure inside the stack;
// all of it must be absorbed by server-side retry within the retry budget.
func TestReplicator_LeadershipTransfer_NoClientErrors(t *testing.T) {
	const (
		electionTimeout = 200 * time.Millisecond
		writers         = 4
		transfers       = 2
	)

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	nodeIDs := []string{"node-a", "node-b", "node-c"}
	recs := make([]*recordingShard, len(nodeIDs))
	specs := make([]shard.TestStoreSpec, len(nodeIDs))
	for i, id := range nodeIDs {
		recs[i] = newRecordingShard(t, 0)
		specs[i] = shard.TestStoreSpec{NodeID: id, Shard: recs[i].mock}
	}
	stores := shard.BuildTestClusterWithOptions(t, testClassName, testShardName, specs,
		shard.TestClusterOptions{
			TickInterval:      20 * time.Millisecond,
			HeartbeatTimeout:  40 * time.Millisecond,
			ElectionTimeout:   electionTimeout,
			SnapshotThreshold: 4096,
		})

	// Production write topology per node: Raft manager -> Registry -> Server.
	servers := map[string]*shard.Server{}
	managers := make([]*shard.Raft, len(nodeIDs))
	for i, id := range nodeIDs {
		managers[i] = shard.NewTestRaftManager(testClassName, logger, electionTimeout,
			map[string]*shard.Store{testShardName: stores[i]})
		reg := shard.NewTestRegistry(testClassName, logger, managers[i])
		servers[id] = shard.NewServer(reg, logger)
	}

	// The client's entry point: node 0's replicator. Forwards resolve to the
	// in-process Server of whichever node currently leads.
	repl := shard.Newreplicator(shard.RouterConfig{
		NodeID:    nodeIDs[0],
		Logger:    logger,
		Raft:      managers[0],
		ClassName: testClassName,
		RpcClientMaker: func(_ context.Context, nodeID string) (shardproto.ShardReplicationServiceClient, error) {
			srv, ok := servers[nodeID]
			if !ok {
				return nil, fmt.Errorf("no server for node %s", nodeID)
			}
			return stubApplyClient{srv: srv}, nil
		},
	})

	for _, s := range stores {
		require.NoError(t, s.Start(context.Background()))
	}
	waitForClusterLeader(t, stores)

	// Import: continuous PutObject stream through the replicator. EVERY error
	// is client-visible by definition — the assertion is zero.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	var clientErrs atomic.Int64
	var firstErr atomic.Value
	var applied atomic.Int64
	for w := 0; w < writers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := 0; ctx.Err() == nil; seq++ {
				putCtx, putCancel := context.WithTimeout(context.Background(), 30*time.Second)
				err := repl.PutObject(putCtx, testShardName,
					makeTestObjectWithID(testUUID(w, seq)), routerTypes.ConsistencyLevelStrong, 0)
				putCancel()
				if err != nil {
					clientErrs.Add(1)
					firstErr.CompareAndSwap(nil, err)
				} else {
					applied.Add(1)
				}
			}
		}()
	}

	// Force leadership transfers mid-import, round-robin across the nodes.
	moved := 0
	for i := 0; i < transfers; i++ {
		time.Sleep(400 * time.Millisecond)
		cur := waitForClusterLeader(t, stores)
		next := (cur + 1) % len(stores)
		shard.TransferLeadership(stores[cur], nodeIDs[next])
		require.Eventuallyf(t, func() bool {
			return stores[next].IsLeader()
		}, 5*time.Second, 20*time.Millisecond, "leadership transfer %d -> %d never completed", cur, next)
		moved++
	}
	time.Sleep(300 * time.Millisecond)
	cancel()
	wg.Wait()

	require.Equal(t, transfers, moved, "test did not exercise the intended number of transfers")
	require.NotZero(t, applied.Load(), "import made no progress")
	require.Zerof(t, clientErrs.Load(),
		"%d of %d client writes failed across %d leadership transfers (first: %v) — leadership churn leaked to the client",
		clientErrs.Load(), clientErrs.Load()+applied.Load(), moved, firstErr.Load())
}
