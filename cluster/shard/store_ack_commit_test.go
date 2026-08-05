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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	routerTypes "github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/cluster/shard"
	"github.com/weaviate/weaviate/cluster/shard/mocks"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"google.golang.org/grpc"
)

// gatedShard is a mock shard whose PutObject blocks on a gate until released,
// so tests can hold the apply pipeline while commit proceeds. release is
// idempotent and MUST be registered as a cleanup after the store is built:
// Stop waits for the apply worker's in-flight dispatch, so an unreleased gate
// deadlocks teardown.
type gatedShard struct {
	mock      *mocks.Mockshard
	gate      chan struct{}
	completed atomic.Int32
	once      sync.Once
}

func newGatedShard(t *testing.T) *gatedShard {
	g := &gatedShard{
		mock: mocks.NewMockshard(t),
		gate: make(chan struct{}),
	}
	g.mock.EXPECT().PutObject(mock.Anything, mock.Anything).RunAndReturn(
		func(context.Context, *storobj.Object) error {
			<-g.gate
			g.completed.Add(1)
			return nil
		},
	).Maybe()
	g.mock.EXPECT().ReadOnlyErr().Return(nil).Maybe()
	g.mock.EXPECT().ClassPresent().Return(true).Maybe()
	return g
}

func (g *gatedShard) release() {
	g.once.Do(func() { close(g.gate) })
}

// TestStore_ApplyAcksAtCommit_SlowApply pins the ack site: Apply returns at
// quorum commit, not after local FSM apply. With the apply pipeline gated, the
// ack must still arrive — before any dispatch completes and while the applied
// watermark is below the acked index — and releasing the gate must materialize
// the entry.
func TestStore_ApplyAcksAtCommit_SlowApply(t *testing.T) {
	g := newGatedShard(t)
	store := shard.BuildTestStore(t, testClassName, testShardName, testNodeID,
		[]string{testNodeID}, g.mock)
	startAndWaitForLeader(t, store)
	t.Cleanup(g.release)

	req := buildPutObjectApplyRequest(t, testClassName, testShardName, makeTestObject())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	idx, err := store.Apply(ctx, req)
	require.NoError(t, err, "Apply must ack at quorum commit, not wait for the gated local apply")
	require.Equal(t, int32(0), g.completed.Load(), "ack must precede FSM dispatch completion")
	require.Less(t, store.LastAppliedIndex(), idx, "acked index must not be locally applied while the gate holds")

	g.release()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer waitCancel()
	require.NoError(t, store.WaitForAppliedIndex(waitCtx, idx),
		"released apply pipeline must materialize the acked entry")
	require.Equal(t, int32(1), g.completed.Load())
}

// TestStore_VerifyLeader_WaitsForAppliedIndex pins read-your-writes for the
// linearizable read barrier: with Apply acking at commit, VerifyLeader must
// not complete until the local FSM has applied at least the ReadState's commit
// index — which covers every previously acked write. While the apply pipeline
// is gated behind an acked write, VerifyLeader must block (surface the
// caller's deadline); once released it must succeed with the applied watermark
// covering the acked index.
func TestStore_VerifyLeader_WaitsForAppliedIndex(t *testing.T) {
	g := newGatedShard(t)
	store := shard.BuildTestStore(t, testClassName, testShardName, testNodeID,
		[]string{testNodeID}, g.mock)
	startAndWaitForLeader(t, store)
	t.Cleanup(g.release)

	req := buildPutObjectApplyRequest(t, testClassName, testShardName, makeTestObject())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	idx, err := store.Apply(ctx, req)
	require.NoError(t, err, "Apply must ack at quorum commit with the apply pipeline gated")

	shortCtx, shortCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer shortCancel()
	err = store.VerifyLeader(shortCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"VerifyLeader must wait for applied >= the ReadState index while the acked write is unapplied")

	g.release()
	okCtx, okCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer okCancel()
	require.NoError(t, store.VerifyLeader(okCtx))
	require.GreaterOrEqual(t, store.LastAppliedIndex(), idx,
		"a completed VerifyLeader must guarantee the acked write is locally applied")
}

// TestStore_Apply_ShardNotSet_FailsFast pins the dispatch-error contract
// under ack-at-commit: the one realistic lifecycle error — Apply before
// SetShard — fails before proposing, so no entry enters the log (the error
// can no longer ride the post-apply result, which no longer reaches clients).
func TestStore_Apply_ShardNotSet_FailsFast(t *testing.T) {
	store := shard.BuildTestStore(t, testClassName, testShardName, testNodeID,
		[]string{testNodeID}, nil /* no shard wired */)
	startAndWaitForLeader(t, store)

	// Let the bootstrap entries (conf change + leader no-op) finish applying
	// so the applied watermark is quiescent before the negative assertion.
	settled := settleAppliedIndex(t, store)

	req := buildPutObjectApplyRequest(t, testClassName, testShardName, makeTestObject())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := store.Apply(ctx, req)
	require.ErrorContains(t, err, "shard not set")
	require.NoError(t, ctx.Err(), "the failure must be fail-fast, not a context expiry")

	// No entry may have been proposed: the applied watermark must not move.
	time.Sleep(250 * time.Millisecond)
	require.Equal(t, settled, store.LastAppliedIndex(),
		"a shard-not-set Apply must not commit an entry")
}

// TestStore_Apply_BackpressureOnCommitApplyLag pins the commit→apply lag
// bound: with the apply pipeline gated, sequential Applies keep acking at
// commit only until the committed-but-unapplied backlog exceeds the cap, at
// which point proposals surface ErrProposalBackpressure (the same-node
// retryable shape the replicator absorbs) instead of growing the backlog
// without limit. Draining the pipeline clears the backpressure.
func TestStore_Apply_BackpressureOnCommitApplyLag(t *testing.T) {
	const lagCap = 2

	g := newGatedShard(t)
	store := shard.BuildTestStoreWithLagCap(t, testClassName, testShardName, testNodeID,
		[]string{testNodeID}, g.mock, lagCap)
	startAndWaitForLeader(t, store)
	t.Cleanup(g.release)

	var (
		backpressured bool
		ackedIdx      uint64
	)
	for seq := 0; seq < 10; seq++ {
		req := buildPutObjectApplyRequest(t, testClassName, testShardName,
			makeTestObjectWithID(testUUID(3, seq)))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		idx, err := store.Apply(ctx, req)
		cancel()
		if err != nil {
			require.ErrorIs(t, err, shard.ErrProposalBackpressure,
				"the lag cap must surface as retryable proposal backpressure, nothing else")
			backpressured = true
			break
		}
		ackedIdx = idx
	}
	require.True(t, backpressured,
		"proposals must be rejected once commit outruns apply by more than the cap")
	require.NotZero(t, ackedIdx, "some applies must have acked before the cap was hit")

	// Backpressure clears once the apply pipeline drains.
	g.release()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer waitCancel()
	require.NoError(t, store.WaitForAppliedIndex(waitCtx, ackedIdx))
	req := buildPutObjectApplyRequest(t, testClassName, testShardName,
		makeTestObjectWithID(testUUID(3, 99)))
	_, err := store.Apply(waitCtx, req)
	require.NoError(t, err, "a drained pipeline must accept proposals again")
}

// TestServer_GetLastAppliedIndex_CoversAckedWrites pins the RPC half of the
// read protocol under ack-at-commit: without VerifyLeader the handler must
// report the committed-staged watermark (covering every acked write even
// while local apply lags — what WaitForShardReady waits on); with
// VerifyLeader it must drive the full linearizable barrier and therefore
// block on the gated apply until the pipeline drains.
func TestServer_GetLastAppliedIndex_CoversAckedWrites(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	g := newGatedShard(t)
	store := shard.BuildTestStore(t, testClassName, testShardName, testNodeID,
		[]string{testNodeID}, g.mock)
	startAndWaitForLeader(t, store)
	t.Cleanup(g.release)

	mgr := shard.NewTestRaftManager(testClassName, logger, 80*time.Millisecond,
		map[string]*shard.Store{testShardName: store})
	reg := shard.NewTestRegistry(testClassName, logger, mgr)
	srv := shard.NewServer(reg, logger)

	applyCtx, applyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer applyCancel()
	idx, err := store.Apply(applyCtx, buildPutObjectApplyRequest(t, testClassName, testShardName, makeTestObject()))
	require.NoError(t, err)
	require.Less(t, store.LastAppliedIndex(), idx, "the gate must hold the acked write unapplied")

	// Watermark mode: covers the acked write immediately.
	resp, err := srv.GetLastAppliedIndex(applyCtx, &shardproto.GetLastAppliedIndexRequest{
		Class: testClassName, Shard: testShardName,
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, resp.LastAppliedIndex, idx,
		"the reported watermark must cover the acked write while apply lags")

	// Barrier mode: must not complete while the acked write is unapplied.
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer shortCancel()
	_, err = srv.GetLastAppliedIndex(shortCtx, &shardproto.GetLastAppliedIndexRequest{
		Class: testClassName, Shard: testShardName, VerifyLeader: true,
	})
	require.Error(t, err, "the VerifyLeader barrier must block on the gated apply")

	g.release()
	okCtx, okCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer okCancel()
	resp, err = srv.GetLastAppliedIndex(okCtx, &shardproto.GetLastAppliedIndexRequest{
		Class: testClassName, Shard: testShardName, VerifyLeader: true,
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, resp.LastAppliedIndex, idx)
	require.GreaterOrEqual(t, store.LastAppliedIndex(), idx,
		"a completed barrier guarantees the acked write is applied")
}

// TestRegistry_WaitForShardReady_LeaderWaitsForOwnApply pins the write-path
// read-after-write helper on the leader: under ack-at-commit the leader's
// local state can lag its own acked writes, so WaitForShardReady must wait
// for the apply pipeline to cover the committed watermark instead of
// returning immediately.
func TestRegistry_WaitForShardReady_LeaderWaitsForOwnApply(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	g := newGatedShard(t)
	store := shard.BuildTestStore(t, testClassName, testShardName, testNodeID,
		[]string{testNodeID}, g.mock)
	startAndWaitForLeader(t, store)
	t.Cleanup(g.release)

	mgr := shard.NewTestRaftManager(testClassName, logger, 80*time.Millisecond,
		map[string]*shard.Store{testShardName: store})
	reg := shard.NewTestRegistry(testClassName, logger, mgr)

	applyCtx, applyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer applyCancel()
	idx, err := store.Apply(applyCtx, buildPutObjectApplyRequest(t, testClassName, testShardName, makeTestObject()))
	require.NoError(t, err)

	shortCtx, shortCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer shortCancel()
	err = reg.WaitForShardReady(shortCtx, testClassName, testShardName)
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"the leader must wait for its own apply pipeline to cover the acked write")

	g.release()
	okCtx, okCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer okCancel()
	require.NoError(t, reg.WaitForShardReady(okCtx, testClassName, testShardName))
	require.GreaterOrEqual(t, store.LastAppliedIndex(), idx)
}

// stubDirectBacking is a backing Replicator whose only implemented method is
// NodeObject — the forwarded DIRECT read target — recording the call for
// ordering assertions.
type stubDirectBacking struct {
	shard.Replicator
	obj    *storobj.Object
	record func(string)
}

func (s *stubDirectBacking) NodeObject(context.Context, string, string, strfmt.UUID, search.SelectProperties, additional.Properties) (*storobj.Object, error) {
	s.record("read")
	return s.obj, nil
}

// recordingReadClient short-circuits the forwarding RPC to the leader's
// Server in-process, recording each leader-barrier call.
type recordingReadClient struct {
	shardproto.ShardReplicationServiceClient
	srv    *shard.Server
	record func(string)
	t      *testing.T
}

func (c recordingReadClient) GetLastAppliedIndex(ctx context.Context, in *shardproto.GetLastAppliedIndexRequest, _ ...grpc.CallOption) (*shardproto.GetLastAppliedIndexResponse, error) {
	require.True(c.t, in.VerifyLeader, "the pre-read barrier must be quorum-verified")
	c.record("verify")
	return c.srv.GetLastAppliedIndex(ctx, in)
}

// TestReplicator_DirectForwardedRead_VerifiesLeaderApplied pins the
// forwarded-DIRECT read barrier: a non-leader serving a DIRECT read must
// drive the leader's linearizable barrier (GetLastAppliedIndex with
// VerifyLeader) before forwarding the read via NodeObject — without it, even
// the true leader could serve state lacking an acked write.
func TestReplicator_DirectForwardedRead_VerifiesLeaderApplied(t *testing.T) {
	const electionTimeout = 200 * time.Millisecond

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	nodeIDs := []string{"node-a", "node-b"}
	specs := make([]shard.TestStoreSpec, len(nodeIDs))
	recs := make([]*recordingShard, len(nodeIDs))
	for i, id := range nodeIDs {
		recs[i] = newRecordingShard(t, 0)
		specs[i] = shard.TestStoreSpec{NodeID: id, Shard: recs[i].mock}
	}
	stores := shard.BuildTestClusterWithOptions(t, testClassName, testShardName, specs,
		shard.TestClusterOptions{
			TickInterval:      20 * time.Millisecond,
			HeartbeatTimeout:  40 * time.Millisecond,
			ElectionTimeout:   electionTimeout,
			SnapshotThreshold: 1024,
		})
	for _, s := range stores {
		require.NoError(t, s.Start(context.Background()))
	}
	leaderIdx := waitForClusterLeader(t, stores)
	followerIdx := 1 - leaderIdx

	leaderMgr := shard.NewTestRaftManager(testClassName, logger, electionTimeout,
		map[string]*shard.Store{testShardName: stores[leaderIdx]})
	leaderSrv := shard.NewServer(shard.NewTestRegistry(testClassName, logger, leaderMgr), logger)
	followerMgr := shard.NewTestRaftManager(testClassName, logger, electionTimeout,
		map[string]*shard.Store{testShardName: stores[followerIdx]})

	var (
		mu     sync.Mutex
		events []string
	)
	record := func(ev string) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	id := testUUID(5, 1)
	backing := &stubDirectBacking{obj: makeTestObjectWithID(id), record: record}
	repl := shard.Newreplicator(shard.RouterConfig{
		NodeID:            nodeIDs[followerIdx],
		Logger:            logger,
		Raft:              followerMgr,
		ClassName:         testClassName,
		BackingReplicator: backing,
		RpcClientMaker: func(_ context.Context, nodeID string) (shardproto.ShardReplicationServiceClient, error) {
			require.Equal(t, nodeIDs[leaderIdx], nodeID, "the barrier must target the current leader")
			return recordingReadClient{srv: leaderSrv, record: record, t: t}, nil
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tests := []struct {
		name string
		call func() error
	}{
		{name: "GetOne", call: func() error {
			obj, err := repl.GetOne(ctx, routerTypes.ConsistencyLevelDirect, testShardName, id,
				search.SelectProperties{}, additional.Properties{})
			if err == nil {
				require.NotNil(t, obj)
			}
			return err
		}},
		{name: "Exists", call: func() error {
			ok, err := repl.Exists(ctx, routerTypes.ConsistencyLevelDirect, testShardName, id)
			if err == nil {
				require.True(t, ok)
			}
			return err
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mu.Lock()
			events = nil
			mu.Unlock()
			require.NoError(t, tc.call())
			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, []string{"verify", "read"}, events,
				"the leader barrier must run before, and exactly once per, forwarded read")
		})
	}
}

// settleAppliedIndex waits until the store's applied index has been stable for
// a few tick intervals and returns it.
func settleAppliedIndex(t *testing.T, store *shard.Store) uint64 {
	t.Helper()
	deadline := time.After(5 * time.Second)
	last := store.LastAppliedIndex()
	stableSince := time.Now()
	for {
		select {
		case <-deadline:
			t.Fatal("applied index never settled")
		case <-time.After(25 * time.Millisecond):
			cur := store.LastAppliedIndex()
			if cur != last {
				last, stableSince = cur, time.Now()
				continue
			}
			if time.Since(stableSince) > 150*time.Millisecond {
				return last
			}
		}
	}
}
