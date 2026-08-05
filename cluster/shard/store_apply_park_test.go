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
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/shard"
	"github.com/weaviate/weaviate/cluster/shard/mocks"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// pressuredShard drives a mock shard whose batch-put path refuses with the
// resource-pressure read-only error — the exact live #10 signature, including
// its lossy whole-batch attribution (a 1-element error slice regardless of
// batch size) — while pressured, and records landed UUIDs otherwise.
type pressuredShard struct {
	pressured atomic.Bool
	refusals  atomic.Int64

	mu   sync.Mutex
	seen map[strfmt.UUID]int
}

func (p *pressuredShard) putBatch(_ context.Context, objs []*storobj.Object) []error {
	if p.pressured.Load() {
		p.refusals.Add(1)
		return []error{storagestate.ErrStatusReadOnlyWithReason("resource pressure")}
	}
	p.mu.Lock()
	for _, o := range objs {
		p.seen[o.Object.ID]++
	}
	p.mu.Unlock()
	return make([]error, len(objs))
}

func (p *pressuredShard) has(id strfmt.UUID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.seen[id] > 0
}

// buildPutBatchApplyRequest constructs a TYPE_PUT_OBJECTS_BATCH ApplyRequest
// carrying the given objects (schema version 0 — legacy passthrough, no
// fence).
func buildPutBatchApplyRequest(t *testing.T, ids ...strfmt.UUID) *shardproto.ApplyRequest {
	t.Helper()
	raws := make([][]byte, len(ids))
	for i, id := range ids {
		b, err := makeTestObjectWithID(id).MarshalBinary()
		require.NoError(t, err)
		raws[i] = b
	}
	sub, err := proto.Marshal(&shardproto.PutObjectsBatchRequest{Objects: raws})
	require.NoError(t, err)
	return &shardproto.ApplyRequest{
		Type:       shardproto.ApplyRequest_TYPE_PUT_OBJECTS_BATCH,
		Class:      testClassName,
		Shard:      testShardName,
		SubCommand: sub,
	}
}

// TestStore_ReadOnlyRefusal_SurvivesSnapshotAndRestart pins the acked-write
// durability chain against the live #10 loss signature: a batch entry whose
// materialization is refused by a read-only (resource-pressured) store must
// survive an aggressive snapshot+compaction cycle AND a full store restart,
// then materialize once the pressure clears. Losing it anywhere on that path
// is permanent silent loss of a quorum-acked write.
//
// The shard's admission-side read-only predicate is not exercised here (the
// reject-fast is best-effort: the pressure flip can land between admission
// and apply, and followers materialize without any admission step) — the park
// path must carry the durability guarantee on its own.
func TestStore_ReadOnlyRefusal_SurvivesSnapshotAndRestart(t *testing.T) {
	sh := &pressuredShard{seen: map[strfmt.UUID]int{}}

	newMock := func() *mocks.Mockshard {
		m := mocks.NewMockshard(t)
		m.EXPECT().PutObjectBatch(mock.Anything, mock.Anything).RunAndReturn(sh.putBatch).Maybe()
		m.EXPECT().DurableRaftFloor().Return(uint64(math.MaxUint64)).Maybe()
		m.EXPECT().FlushForSnapshot(mock.Anything).Return(nil).Maybe()
		m.EXPECT().Name().Return(testShardName).Maybe()
		// Admission stays writable: the reject-fast is best-effort (see the
		// test comment) — the park path must carry durability on its own.
		m.EXPECT().ReadOnlyErr().Return(nil).Maybe()
		m.EXPECT().ClassPresent().Return(true).Maybe()
		return m
	}

	logPath := filepath.Join(t.TempDir(), "shared-raft-log")
	snapRoot := t.TempDir()

	// SnapshotThreshold 1: every applied advance arms a snapshot+compaction
	// round — maximal pressure on the "compaction never covers a parked
	// entry" floor.
	store, closeInfra := shard.BuildTestStoreAt(t, testClassName, testShardName, testNodeID,
		logPath, snapRoot, 1, newMock())
	startAndWaitForLeader(t, store)

	// A healthy batch lands and a snapshot cycle moves the compaction
	// horizon past it. Cadence is evaluated per Ready round and an idle
	// single-voter store produces none, so filler applies provide the rounds
	// (the durable-floor cadence test documents the same nudge pattern).
	idAccepted := strfmt.UUID("00000000-0000-4000-8000-00000000aaaa")
	idx1, err := applyAndWait(t, store, buildPutBatchApplyRequest(t, idAccepted))
	require.NoError(t, err)
	require.True(t, sh.has(idAccepted))
	pumpDeadline := time.Now().Add(15 * time.Second)
	for seq := 0; shard.LogFirstIndex(t, store) <= idx1; seq++ {
		require.Less(t, time.Now(), pumpDeadline, "snapshot cycle past the healthy batch never happened")
		_, err := applyAndWait(t, store, buildPutBatchApplyRequest(t, testUUID(90, seq)))
		require.NoError(t, err)
		time.Sleep(30 * time.Millisecond)
	}

	// Pressure flips the store read-only; the next batch is quorum-acked but
	// refused at materialization with the live signature.
	sh.pressured.Store(true)
	idParked := strfmt.UUID("00000000-0000-4000-8000-00000000bbbb")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	idx2, err := store.Apply(ctx, buildPutBatchApplyRequest(t, idParked))
	cancel()
	require.NoError(t, err, "Apply acks at quorum commit — the write is acknowledged")
	require.Eventually(t, func() bool { return sh.refusals.Load() >= 1 },
		10*time.Second, 10*time.Millisecond, "the refusal must have reached the shard")

	// The refused entry must still be replayable: compaction may never cover
	// it while it is unmaterialized.
	require.LessOrEqual(t, shard.LogFirstIndex(t, store), idx2,
		"log compacted past a committed-but-unmaterialized entry: the acked write is unrecoverable")

	// Restart under pressure: the entry must survive the reboot too.
	closeInfra()
	store2, closeInfra2 := shard.BuildTestStoreAt(t, testClassName, testShardName, testNodeID,
		logPath, snapRoot, 1, newMock())
	defer closeInfra2()
	startAndWaitForLeader(t, store2)
	require.LessOrEqual(t, shard.LogFirstIndex(t, store2), idx2,
		"restart lost the committed-but-unmaterialized entry")

	// Pressure clears: the acked write must materialize.
	sh.pressured.Store(false)
	require.Eventually(t, func() bool { return sh.has(idParked) },
		15*time.Second, 20*time.Millisecond,
		"acked write never materialized after pressure cleared — permanent silent loss")

	// And the pipeline is live again end to end.
	idAfter := strfmt.UUID("00000000-0000-4000-8000-00000000cccc")
	_, err = applyAndWait(t, store2, buildPutBatchApplyRequest(t, idAfter))
	require.NoError(t, err)
	require.True(t, sh.has(idAfter))
}

// TestStore_Apply_RejectFast_ReadOnlyShard pins the leader's admission gate:
// a read-only shard rejects the write synchronously — before marshal/propose,
// so the raft log is untouched — with the full operator-facing reason intact,
// and BOTH retry-classification routes (local leader and forwarded RPC) agree
// the rejection is non-retryable.
func TestStore_Apply_RejectFast_ReadOnlyShard(t *testing.T) {
	m := mocks.NewMockshard(t)
	m.EXPECT().ReadOnlyErr().Return(storagestate.ErrStatusReadOnlyWithReason("resource pressure"))
	store := shard.BuildTestStore(t, testClassName, testShardName, testNodeID, []string{testNodeID}, m)
	startAndWaitForLeader(t, store)

	before := shard.LogLastIndex(t, store)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := store.Apply(ctx, buildPutBatchApplyRequest(t,
		strfmt.UUID("00000000-0000-4000-8000-00000000dddd")))

	require.Error(t, err)
	require.ErrorIs(t, err, storagestate.ErrStatusReadOnly)
	require.Contains(t, err.Error(), "store is read-only due to: resource pressure",
		"the client must see the full reason to act on it")
	require.False(t, shard.IsRetryableApplyErr(err), "local route: non-retryable")

	crossed := shard.ToRPCError(err)
	require.Equal(t, codes.FailedPrecondition, status.Code(crossed))
	require.False(t, shard.IsRetryableApplyErr(crossed), "forwarded route: non-retryable")

	require.Equal(t, before, shard.LogLastIndex(t, store),
		"rejection must happen pre-propose: log length unchanged")
}

// TestStore_Apply_RejectFast_ClassDropped pins the admission-side
// class-presence check: during a drop window (class gone from the local
// schema while the group still serves) new writes are rejected honestly with
// a non-retryable code on both routes, pre-propose.
func TestStore_Apply_RejectFast_ClassDropped(t *testing.T) {
	m := mocks.NewMockshard(t)
	m.EXPECT().ReadOnlyErr().Return(nil)
	m.EXPECT().ClassPresent().Return(false)
	store := shard.BuildTestStore(t, testClassName, testShardName, testNodeID, []string{testNodeID}, m)
	startAndWaitForLeader(t, store)

	before := shard.LogLastIndex(t, store)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := store.Apply(ctx, buildPutBatchApplyRequest(t,
		strfmt.UUID("00000000-0000-4000-8000-00000000eeee")))

	require.Error(t, err)
	require.ErrorIs(t, err, shard.ErrClassDropped)
	require.False(t, shard.IsRetryableApplyErr(err), "local route: non-retryable")

	crossed := shard.ToRPCError(err)
	require.Equal(t, codes.FailedPrecondition, status.Code(crossed))
	require.False(t, shard.IsRetryableApplyErr(crossed), "forwarded route: non-retryable")

	require.Equal(t, before, shard.LogLastIndex(t, store),
		"rejection must happen pre-propose: log length unchanged")
}

// TestStore_ParkedLeader_EngagesCommitApplyBackpressure pins the leader-side
// composition guarantee: a parked apply worker freezes the applied index, so
// sustained writes trip the commit-apply lag cap and new proposals surface
// the retryable ErrProposalBackpressure — clients are throttled instead of
// the backlog growing without bound; once the park clears, the backlog
// drains and writes flow again.
func TestStore_ParkedLeader_EngagesCommitApplyBackpressure(t *testing.T) {
	var healed atomic.Bool
	m := mocks.NewMockshard(t)
	m.EXPECT().ReadOnlyErr().Return(nil).Maybe()
	m.EXPECT().ClassPresent().Return(true).Maybe()
	m.EXPECT().PutObject(mock.Anything, mock.Anything).RunAndReturn(
		func(context.Context, *storobj.Object) error {
			if healed.Load() {
				return nil
			}
			return storagestate.ErrStatusReadOnlyWithReason("resource pressure")
		}).Maybe()

	store := shard.BuildTestStoreWithLagCap(t, testClassName, testShardName, testNodeID,
		[]string{testNodeID}, m, 8)
	startAndWaitForLeader(t, store)

	sawBackpressure := false
	for i := 0; i < 64; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := store.Apply(ctx, buildPutObjectApplyRequest(t, testClassName, testShardName,
			makeTestObjectWithID(testUUID(70, i))))
		cancel()
		if err != nil {
			require.ErrorIs(t, err, shard.ErrProposalBackpressure,
				"the only acceptable failure under a parked leader is lag-cap backpressure")
			sawBackpressure = true
			break
		}
	}
	require.True(t, sawBackpressure,
		"a parked leader must trip the commit-apply lag cap — unbounded acked-but-unapplied backlog otherwise")

	// The pressure clears: the parked backlog drains and new writes succeed.
	healed.Store(true)
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := store.Apply(ctx, buildPutObjectApplyRequest(t, testClassName, testShardName,
			makeTestObjectWithID(testUUID(71, 0))))
		return err == nil
	}, 60*time.Second, 250*time.Millisecond,
		"writes must flow again once the parked backlog drains")
}

// TestStore_ParkedReplica_PeersCarryTheShard pins the fleet-level posture: one
// replica whose shard refuses writes parks alone — the group keeps accepting
// and materializing writes on its healthy replicas throughout — and the
// parked replica recovers by replaying its retained log once its condition
// clears.
func TestStore_ParkedReplica_PeersCarryTheShard(t *testing.T) {
	nodes := []string{"n1", "n2", "n3"}
	const parkedNode = "n3"
	var healed atomic.Bool

	mkMock := func(node string) *mocks.Mockshard {
		m := mocks.NewMockshard(t)
		m.EXPECT().ReadOnlyErr().Return(nil).Maybe()
		m.EXPECT().ClassPresent().Return(true).Maybe()
		m.EXPECT().DurableRaftFloor().Return(uint64(math.MaxUint64)).Maybe()
		m.EXPECT().FlushForSnapshot(mock.Anything).Return(nil).Maybe()
		m.EXPECT().Name().Return(testShardName).Maybe()
		m.EXPECT().PutObject(mock.Anything, mock.Anything).RunAndReturn(
			func(context.Context, *storobj.Object) error {
				if node == parkedNode && !healed.Load() {
					return storagestate.ErrStatusReadOnlyWithReason("resource pressure")
				}
				return nil
			}).Maybe()
		return m
	}

	specs := make([]shard.TestStoreSpec, len(nodes))
	for i, n := range nodes {
		specs[i] = shard.TestStoreSpec{NodeID: n, Shard: mkMock(n)}
	}
	stores := shard.BuildTestClusterWithOptions(t, testClassName, testShardName, specs,
		shard.TestClusterOptions{
			TickInterval:      20 * time.Millisecond,
			HeartbeatTimeout:  40 * time.Millisecond,
			ElectionTimeout:   200 * time.Millisecond,
			SnapshotThreshold: 1 << 20, // keep the log: recovery-by-replay leg
		})
	for _, s := range stores {
		require.NoError(t, s.Start(context.Background()))
	}
	var leader *shard.Store
	require.Eventually(t, func() bool {
		for _, s := range stores {
			if s.IsLeader() {
				leader = s
				return true
			}
		}
		return false
	}, 10*time.Second, 25*time.Millisecond, "no leader elected")

	// Writes keep succeeding while one replica's shard refuses everything.
	var last uint64
	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		idx, err := leader.Apply(ctx, buildPutObjectApplyRequest(t, testClassName, testShardName,
			makeTestObjectWithID(testUUID(80, i))))
		cancel()
		require.NoError(t, err, "peers must carry the shard while one replica is parked")
		last = idx
	}

	// Healthy replicas materialize everything; the parked one is frozen
	// before the first put entry.
	parkedStore := stores[2]
	for _, s := range []*shard.Store{stores[0], stores[1]} {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		require.NoError(t, s.WaitForAppliedIndex(ctx, last),
			"healthy replicas must materialize every acked write")
		cancel()
	}
	require.Less(t, parkedStore.LastAppliedIndex(), last,
		"the parked replica's applied index must be frozen below the acked writes")

	// The condition clears: the parked replica recovers by replaying its
	// retained log.
	healed.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	require.NoError(t, parkedStore.WaitForAppliedIndex(ctx, last),
		"a parked replica must catch up by replay once its condition clears")
}
