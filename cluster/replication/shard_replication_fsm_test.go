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

package replication_test

import (
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
)

// TestShardReplicationFSM_FilterReplicas_ByState: read/write filter
// contract across a COPY lifecycle. The target receives no direct writes while
// it catches up (HYDRATING/FINALIZING) — the change-capture-log is the sole,
// ordered, LWW-safe catchup path — and is promoted to a counted read+write
// replica only at INTEGRATING.
func TestShardReplicationFSM_FilterReplicas_ByState(t *testing.T) {
	const (
		class  = "TestClass"
		shard  = "shard1"
		source = "node1"
		target = "node2"
	)
	replicas := []string{source, target}

	cases := []struct {
		name      string
		state     api.ShardReplicationState
		wantRead  []string
		wantWrite []string
	}{
		{
			name:      "REGISTERED: target not yet routable",
			state:     api.REGISTERED,
			wantRead:  []string{source},
			wantWrite: []string{source},
		},
		{
			name:      "HYDRATING: target not yet routable",
			state:     api.HYDRATING,
			wantRead:  []string{source},
			wantWrite: []string{source},
		},
		{
			name:      "FINALIZING: target receives no direct writes (CCL-only catchup)",
			state:     api.FINALIZING,
			wantRead:  []string{source},
			wantWrite: []string{source},
		},
		{
			name:      "INTEGRATING: target is a counted read+write replica, not additional",
			state:     api.INTEGRATING,
			wantRead:  []string{source, target},
			wantWrite: []string{source, target},
		},
		{
			name:      "READY: target fully promoted",
			state:     api.READY,
			wantRead:  []string{source, target},
			wantWrite: []string{source, target},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
			seedOp(t, fsm, 1)
			driveToState(t, fsm, 1, tc.state)

			gotRead := fsm.FilterOneShardReplicasRead(class, shard, replicas)
			assert.ElementsMatch(t, tc.wantRead, gotRead, "read replicas")

			gotWrite := fsm.FilterOneShardReplicasWrite(class, shard, replicas)
			assert.ElementsMatch(t, tc.wantWrite, gotWrite, "write replicas")
		})
	}
}

// TestShardReplicationFSM_AllPeersAtLeast firewalls the convergence barrier that
// gates the move/copy cutovers: the source change-capture log is sealed at
// INTEGRATING and the source replica removed at DEHYDRATING only once every node
// has applied the transition. PerNodeState is keyed per node and populated only
// by committed NodeReachedState broadcasts, so a node that has not (yet) reported
// is absent. AllPeersAtLeast must treat an absent expected node as not satisfied
// — otherwise a partial/empty map reports convergence and a node still routing
// under the old topology loses writes after the cutover.
//
// The expected set is the full cluster membership (here node1=source,
// node2=target, node3=a non-replica that can still coordinate a write), not just
// the shard replicas.
func TestShardReplicationFSM_AllPeersAtLeast(t *testing.T) {
	const opID uint64 = 1

	cases := []struct {
		name          string
		expectedNodes []string
		reached       map[string]api.ShardReplicationState
		target        api.ShardReplicationState
		want          bool
	}{
		{
			name:          "no node has reported is not convergence",
			expectedNodes: []string{"node1", "node2"},
			reached:       nil,
			target:        api.INTEGRATING,
			want:          false,
		},
		{
			name:          "only the target reported, source silent is not convergence",
			expectedNodes: []string{"node1", "node2"},
			reached:       map[string]api.ShardReplicationState{"node2": api.INTEGRATING},
			target:        api.INTEGRATING,
			want:          false,
		},
		{
			name:          "every expected node reached target",
			expectedNodes: []string{"node1", "node2"},
			reached:       map[string]api.ShardReplicationState{"node1": api.INTEGRATING, "node2": api.INTEGRATING},
			target:        api.INTEGRATING,
			want:          true,
		},
		{
			name:          "a peer below target blocks",
			expectedNodes: []string{"node1", "node2"},
			reached:       map[string]api.ShardReplicationState{"node1": api.INTEGRATING, "node2": api.HYDRATING},
			target:        api.INTEGRATING,
			want:          false,
		},
		{
			name:          "peers past target still satisfy it",
			expectedNodes: []string{"node1", "node2"},
			reached:       map[string]api.ShardReplicationState{"node1": api.READY, "node2": api.DEHYDRATING},
			target:        api.INTEGRATING,
			want:          true,
		},
		{
			name:          "non-replica coordinator that has not reported blocks",
			expectedNodes: []string{"node1", "node2", "node3"},
			reached:       map[string]api.ShardReplicationState{"node1": api.INTEGRATING, "node2": api.INTEGRATING},
			target:        api.INTEGRATING,
			want:          false,
		},
		{
			name:          "DEHYDRATING converges only when every node reports it",
			expectedNodes: []string{"node1", "node2", "node3"},
			reached:       map[string]api.ShardReplicationState{"node1": api.DEHYDRATING, "node2": api.DEHYDRATING, "node3": api.DEHYDRATING},
			target:        api.DEHYDRATING,
			want:          true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := replication.NewShardReplicationFSM(prometheus.NewRegistry())
			seedOp(t, fsm, opID) // source node1, target node2
			for node, state := range tc.reached {
				require.NoError(t, fsm.NodeReachedState(&api.ReplicationNodeReachedStateRequest{
					Version: api.ReplicationCommandVersionV0,
					Id:      opID,
					NodeId:  node,
					State:   state,
				}))
			}
			require.Equal(t, tc.want, fsm.AllPeersAtLeast(opID, tc.target, tc.expectedNodes))
		})
	}

	t.Run("unknown op is never converged", func(t *testing.T) {
		fsm := replication.NewShardReplicationFSM(prometheus.NewRegistry())
		require.False(t, fsm.AllPeersAtLeast(999, api.INTEGRATING, []string{"node1"}))
	})
}

func TestShardReplicationFSM_FilterOneReplica_Coexistence(t *testing.T) {
	const (
		coll  = "TestClass"
		shard = "shard1"
	)
	replicas := []string{"node2"}

	cases := []struct {
		name string
		// each entry seeds an op targeting node2 from a distinct source and drives it
		// to a state; the first source is reused only when there is one op.
		ops       []struct{ srcNode, state string }
		wantRead  []string
		wantWrite []string
	}{
		{
			name:      "CANCELLED + active INTEGRATING ⇒ routable",
			ops:       []struct{ srcNode, state string }{{"node1", string(api.CANCELLED)}, {"node3", string(api.INTEGRATING)}},
			wantRead:  []string{"node2"},
			wantWrite: []string{"node2"},
		},
		{
			name:      "CANCELLED only ⇒ inert, replica stays routable",
			ops:       []struct{ srcNode, state string }{{"node1", string(api.CANCELLED)}},
			wantRead:  []string{"node2"},
			wantWrite: []string{"node2"},
		},
		{
			name:      "single active INTEGRATING op ⇒ routable (single-op behaviour unchanged)",
			ops:       []struct{ srcNode, state string }{{"node1", string(api.INTEGRATING)}},
			wantRead:  []string{"node2"},
			wantWrite: []string{"node2"},
		},
		{
			name:      "single HYDRATING op ⇒ target not yet routable (single-op behaviour unchanged)",
			ops:       []struct{ srcNode, state string }{{"node1", string(api.HYDRATING)}},
			wantRead:  []string{},
			wantWrite: []string{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
			for i, o := range tc.ops {
				id := uint64(i + 1)
				seedOpFull(t, fsm, id, o.srcNode, "node2", coll, shard, api.COPY)
				if api.ShardReplicationState(o.state) == api.CANCELLED {
					driveToCancelled(t, fsm, id)
				} else {
					driveToState(t, fsm, id, api.ShardReplicationState(o.state))
				}
			}
			assert.Equal(t, tc.wantRead, fsm.FilterOneShardReplicasRead(coll, shard, replicas))
			assert.Equal(t, tc.wantWrite, fsm.FilterOneShardReplicasWrite(coll, shard, replicas))
		})
	}
}

// TestShardReplicationFSM_RemoveOneOfTwoTargetOps pins the per-target slice remove
// path: removing one of two ops coexisting on a target FQDN leaves the other routable,
// and removing the last deletes the map key so filterOneReplicaReadWrite falls through
// to the source check instead of OR-folding a lingering empty slice to (false,false) —
// which would silently drop the replica from read+write routing.
func TestShardReplicationFSM_RemoveOneOfTwoTargetOps(t *testing.T) {
	const (
		coll  = "TestClass"
		shard = "shard1"
	)
	fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
	remove := func(id uint64) {
		t.Helper()
		require.NoError(t, fsm.RemoveReplicationOp(&api.ReplicationRemoveOpRequest{
			Version: api.ReplicationCommandVersionV0,
			Id:      id,
		}))
	}

	// Two ops coexist on target node2: a terminal cancelled MOVE and an active MOVE
	// from a distinct source (admission allows the active op beside the cancelled one).
	seedOpFull(t, fsm, 1, "node1", "node2", coll, shard, api.MOVE)
	driveToCancelled(t, fsm, 1)
	seedOpFull(t, fsm, 2, "node3", "node2", coll, shard, api.MOVE)
	driveToState(t, fsm, 2, api.INTEGRATING)

	replicas := []string{"node2"}
	require.Len(t, fsm.GetOpsForTarget("node2"), 2)

	// Remove the cancelled op: the active INTEGRATING survivor keeps node2 routable.
	remove(1)
	require.Len(t, fsm.GetOpsForTarget("node2"), 1)
	require.Equal(t, []string{"node2"}, fsm.FilterOneShardReplicasRead(coll, shard, replicas))
	require.Equal(t, []string{"node2"}, fsm.FilterOneShardReplicasWrite(coll, shard, replicas))

	// Remove the last op: the empty target key is deleted, so routing falls through to
	// the source check; with no op on node2 it routes as a normal replica. Were the
	// empty-key delete dropped, the empty slice would OR-fold to (false,false) and node2
	// would be silently dropped from routing.
	remove(2)
	require.Empty(t, fsm.GetOpsForTarget("node2"))
	require.Equal(t, []string{"node2"}, fsm.FilterOneShardReplicasRead(coll, shard, replicas))
	require.Equal(t, []string{"node2"}, fsm.FilterOneShardReplicasWrite(coll, shard, replicas))
}

func TestShardReplicationFSM_HasActiveTargetReplicationForShard(t *testing.T) {
	const (
		coll  = "TestClass"
		shard = "shard1"
	)

	cases := []struct {
		name     string
		seed     func(t *testing.T, fsm *replication.ShardReplicationFSM)
		replica  string
		expected bool
	}{
		{name: "empty fsm", seed: func(t *testing.T, fsm *replication.ShardReplicationFSM) {}, replica: "node2", expected: false},
		{
			name: "op targets another node",
			seed: func(t *testing.T, fsm *replication.ShardReplicationFSM) {
				seedOpFull(t, fsm, 1, "node1", "node3", coll, shard, api.COPY)
			},
			replica: "node2", expected: false,
		},
		{
			name: "op on another collection",
			seed: func(t *testing.T, fsm *replication.ShardReplicationFSM) {
				seedOpFull(t, fsm, 1, "node1", "node2", "OtherClass", shard, api.COPY)
			},
			replica: "node2", expected: false,
		},
		{
			name: "op on another shard",
			seed: func(t *testing.T, fsm *replication.ShardReplicationFSM) {
				seedOpFull(t, fsm, 1, "node1", "node2", coll, "other-shard", api.COPY)
			},
			replica: "node2", expected: false,
		},
		{
			name: "op is REGISTERED",
			seed: func(t *testing.T, fsm *replication.ShardReplicationFSM) {
				seedOpFull(t, fsm, 1, "node1", "node2", coll, shard, api.COPY)
			},
			replica: "node2", expected: true,
		},
		{
			name: "op is HYDRATING",
			seed: func(t *testing.T, fsm *replication.ShardReplicationFSM) {
				seedOpFull(t, fsm, 1, "node1", "node2", coll, shard, api.COPY)
				driveToState(t, fsm, 1, api.HYDRATING)
			},
			replica: "node2", expected: true,
		},
		{
			name: "op is FINALIZING",
			seed: func(t *testing.T, fsm *replication.ShardReplicationFSM) {
				seedOpFull(t, fsm, 1, "node1", "node2", coll, shard, api.COPY)
				driveToState(t, fsm, 1, api.FINALIZING)
			},
			replica: "node2", expected: true,
		},
		{
			name: "op is INTEGRATING",
			seed: func(t *testing.T, fsm *replication.ShardReplicationFSM) {
				seedOpFull(t, fsm, 1, "node1", "node2", coll, shard, api.COPY)
				driveToState(t, fsm, 1, api.INTEGRATING)
			},
			replica: "node2", expected: true,
		},
		{
			name: "op is DEHYDRATING",
			seed: func(t *testing.T, fsm *replication.ShardReplicationFSM) {
				seedOpFull(t, fsm, 1, "node1", "node2", coll, shard, api.MOVE)
				driveToState(t, fsm, 1, api.DEHYDRATING)
			},
			replica: "node2", expected: true,
		},
		{
			name: "op is READY",
			seed: func(t *testing.T, fsm *replication.ShardReplicationFSM) {
				seedOpFull(t, fsm, 1, "node1", "node2", coll, shard, api.COPY)
				driveToState(t, fsm, 1, api.READY)
			},
			replica: "node2", expected: false,
		},
		{
			name: "op is CANCELLED",
			seed: func(t *testing.T, fsm *replication.ShardReplicationFSM) {
				seedOpFull(t, fsm, 1, "node1", "node2", coll, shard, api.COPY)
				driveToCancelled(t, fsm, 1)
			},
			replica: "node2", expected: false,
		},
		{
			name: "op is READY and marked for deletion",
			seed: func(t *testing.T, fsm *replication.ShardReplicationFSM) {
				uuid := seedOpFull(t, fsm, 1, "node1", "node2", coll, shard, api.COPY)
				driveToState(t, fsm, 1, api.READY)
				require.NoError(t, fsm.DeleteReplication(&api.ReplicationDeleteRequest{
					Version: api.ReplicationCommandVersionV0,
					Uuid:    uuid,
				}))
			},
			replica: "node2", expected: false,
		},
		{
			name: "queried replica is the source node",
			seed: func(t *testing.T, fsm *replication.ShardReplicationFSM) {
				seedOpFull(t, fsm, 1, "node1", "node2", coll, shard, api.COPY)
			},
			replica: "node1", expected: false,
		},
		{
			name: "active op beside terminal ops on same shard",
			seed: func(t *testing.T, fsm *replication.ShardReplicationFSM) {
				seedOpFull(t, fsm, 1, "node1", "node2", coll, shard, api.COPY)
				driveToCancelled(t, fsm, 1)
				seedOpFull(t, fsm, 2, "node1", "node3", coll, shard, api.COPY)
				driveToState(t, fsm, 2, api.READY)
				seedOpFull(t, fsm, 3, "node3", "node2", coll, shard, api.COPY)
				driveToState(t, fsm, 3, api.HYDRATING)
			},
			replica: "node2", expected: true,
		},
		{
			name: "op removed",
			seed: func(t *testing.T, fsm *replication.ShardReplicationFSM) {
				seedOpFull(t, fsm, 1, "node1", "node2", coll, shard, api.COPY)
				require.NoError(t, fsm.RemoveReplicationOp(&api.ReplicationRemoveOpRequest{
					Version: api.ReplicationCommandVersionV0,
					Id:      1,
				}))
			},
			replica: "node2", expected: false,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
			tt.seed(t, fsm)
			assert.Equal(t, tt.expected, fsm.HasActiveTargetReplicationForShard(coll, shard, tt.replica))
		})
	}
}

func TestShardReplicationFSM_HasActiveTargetReplicationForShardDoesNotAllocate(t *testing.T) {
	const (
		coll   = "TestClass"
		target = "node2"
	)
	fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
	for i := range 10_000 {
		seedOpFull(t, fsm, uint64(i+1), "node1", target, coll, fmt.Sprintf("shard-%d", i), api.COPY)
	}

	require.Zero(t, testing.AllocsPerRun(100, func() {
		fsm.HasActiveTargetReplicationForShard(coll, "shard-0", target)
	}))
	require.Zero(t, testing.AllocsPerRun(100, func() {
		fsm.HasActiveTargetReplicationForShard(coll, "shard-0", "node3")
	}))
	require.Zero(t, testing.AllocsPerRun(100, func() {
		fsm.HasActiveTargetReplicationForShard(coll, "no-such-shard", target)
	}))
}

func TestShardReplicationFSM_HasActiveTargetReplicationForShardConcurrent(t *testing.T) {
	const (
		coll        = "TestClass"
		writers     = 4
		readers     = 4
		iterations  = 500
		pinnedShard = "pinned-shard"
	)
	fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
	seedOpFull(t, fsm, writers*iterations+1, "node1", "node2", coll, pinnedShard, api.COPY)
	driveToState(t, fsm, writers*iterations+1, api.HYDRATING)

	var eg errgroup.Group
	for w := range writers {
		eg.Go(func() error {
			for i := range iterations {
				id := uint64(w*iterations + i + 1)
				shard := fmt.Sprintf("shard-%d-%d", w, i)
				if err := fsm.Replicate(id, &api.ReplicationReplicateShardRequest{
					Version:          api.ReplicationCommandVersionV0,
					Uuid:             strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", id)),
					SourceNode:       "node1",
					SourceCollection: coll,
					SourceShard:      shard,
					TargetNode:       "node2",
					TransferType:     api.COPY.String(),
				}); err != nil {
					return err
				}
				if err := fsm.UpdateReplicationOpStatus(&api.ReplicationUpdateOpStateRequest{
					Version: api.ReplicationCommandVersionV0,
					Id:      id,
					State:   api.HYDRATING,
				}); err != nil {
					return err
				}
				if i%2 == 0 {
					if err := fsm.RemoveReplicationOp(&api.ReplicationRemoveOpRequest{
						Version: api.ReplicationCommandVersionV0,
						Id:      id,
					}); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
	for r := range readers {
		eg.Go(func() error {
			for i := range iterations * writers {
				shard := fmt.Sprintf("shard-%d-%d", (r+i)%writers, i%iterations)
				fsm.HasActiveTargetReplicationForShard(coll, shard, "node2")
				fsm.HasActiveReplicationForShard(coll, shard)
				if !fsm.HasActiveTargetReplicationForShard(coll, pinnedShard, "node2") {
					return fmt.Errorf("expected pinned op on %q to gate replication", pinnedShard)
				}
			}
			return nil
		})
	}
	require.NoError(t, eg.Wait())

	for w := range writers {
		for i := range iterations {
			shard := fmt.Sprintf("shard-%d-%d", w, i)
			assert.Equal(t, i%2 != 0, fsm.HasActiveTargetReplicationForShard(coll, shard, "node2"))
		}
	}
	assert.True(t, fsm.HasActiveTargetReplicationForShard(coll, pinnedShard, "node2"))
}

// Snapshot encodes the op statuses after it releases opsLock, so it must not
// hand the encoder a PerNodeState map that NodeReachedState still writes to.
// Raft runs Persist concurrently with Apply, so both are live at once whenever
// a replication op is in flight.
func TestShardReplicationFSM_SnapshotConcurrentWithNodeReachedState(t *testing.T) {
	const (
		coll       = "TestClass"
		opID       = uint64(1)
		peers      = 8
		iterations = 200
	)
	fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
	seedOpFull(t, fsm, opID, "node1", "node2", coll, "shard-0", api.COPY)
	driveToState(t, fsm, opID, api.HYDRATING)

	var eg errgroup.Group
	for p := range peers {
		eg.Go(func() error {
			// A fresh node id per iteration keeps the map growing; repeating one
			// id would write only once, since the FSM records peer state
			// monotonically.
			for i := range iterations {
				if err := fsm.NodeReachedState(&api.ReplicationNodeReachedStateRequest{
					Version: api.ReplicationCommandVersionV0,
					Id:      opID,
					NodeId:  fmt.Sprintf("node-%d-%d", p, i),
					State:   api.HYDRATING,
				}); err != nil {
					return err
				}
			}
			return nil
		})
	}
	for range peers {
		eg.Go(func() error {
			for range iterations {
				if _, err := fsm.Snapshot(); err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(t, eg.Wait())

	// Every peer, not a sample: a Snapshot that pruned or rewrote the map it
	// hands out would still satisfy a spot check.
	peerIDs := make([]string, 0, peers*iterations)
	for p := range peers {
		for i := range iterations {
			peerIDs = append(peerIDs, fmt.Sprintf("node-%d-%d", p, i))
		}
	}
	require.True(t, fsm.AllPeersAtLeast(opID, api.HYDRATING, peerIDs))

	snap, err := fsm.Snapshot()
	require.NoError(t, err)
	restored := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
	require.NoError(t, restored.Restore(snap))
	require.True(t, restored.AllPeersAtLeast(opID, api.HYDRATING, peerIDs),
		"snapshot carries the per-peer state")
}

// The FSM keeps mutating the maps and slices inside its ops, so a caller that
// reads one after the lock is released must be holding a copy. The consumer does
// exactly that: it polls a status, transitions its own copy, and the producer
// ranges a target's ops across per-op lock acquisitions.
func TestShardReplicationFSM_HandsOutDetachedState(t *testing.T) {
	const (
		coll   = "TestClass"
		target = "node2"
		opID   = uint64(1)
	)

	tests := []struct {
		name string
		test func(t *testing.T, fsm *replication.ShardReplicationFSM)
	}{
		{
			name: "a stale consumer transition leaves the fsm history alone",
			test: func(t *testing.T, fsm *replication.ShardReplicationFSM) {
				seedOpFull(t, fsm, opID, "node1", target, coll, "shard-0", api.COPY)
				for _, state := range []api.ShardReplicationState{api.HYDRATING, api.FINALIZING, api.INTEGRATING} {
					driveToState(t, fsm, opID, state)
				}

				// The consumer polls the op, then keeps working while the FSM
				// applies further commands, so its copy goes stale.
				polled, ok := fsm.GetOpById(opID)
				require.True(t, ok)
				require.NoError(t, fsm.RegisterError(&api.ReplicationRegisterErrorRequest{
					Version:    api.ReplicationCommandVersionV0,
					Id:         opID,
					Error:      "hydration failed",
					TimeUnixMs: 1,
				}))
				driveToState(t, fsm, opID, api.READY)

				// Transitioning the stale copy archives a state the FSM has
				// already archived itself, at the same history index.
				polled.Status.ChangeState(api.READY)

				current, ok := fsm.GetOpById(opID)
				require.True(t, ok)
				history := current.Status.GetHistory()
				require.Len(t, history, 4)
				assert.Equal(t, api.INTEGRATING, history[3].State)
				assert.Len(t, history[3].Errors, 1, "the archived state keeps the error registered before it")
			},
		},
		{
			name: "per-node state from GetStatusByOps is not the fsm's map",
			test: func(t *testing.T, fsm *replication.ShardReplicationFSM) {
				seedOpFull(t, fsm, opID, "node1", target, coll, "shard-0", api.COPY)
				driveToState(t, fsm, opID, api.HYDRATING)
				require.NoError(t, fsm.NodeReachedState(&api.ReplicationNodeReachedStateRequest{
					Version: api.ReplicationCommandVersionV0,
					Id:      opID,
					NodeId:  "node1",
					State:   api.HYDRATING,
				}))

				for _, status := range fsm.GetStatusByOps() {
					status.PerNodeState["injected"] = api.READY
				}

				assert.False(t, fsm.AllPeersAtLeast(opID, api.HYDRATING, []string{"injected"}))
			},
		},
		{
			name: "per-node state from GetOpState is not the fsm's map",
			test: func(t *testing.T, fsm *replication.ShardReplicationFSM) {
				seedOpFull(t, fsm, opID, "node1", target, coll, "shard-0", api.COPY)
				driveToState(t, fsm, opID, api.HYDRATING)
				require.NoError(t, fsm.NodeReachedState(&api.ReplicationNodeReachedStateRequest{
					Version: api.ReplicationCommandVersionV0,
					Id:      opID,
					NodeId:  "node1",
					State:   api.HYDRATING,
				}))

				op, ok := fsm.GetOpById(opID)
				require.True(t, ok)
				status, ok := fsm.GetOpState(op.Op)
				require.True(t, ok)
				status.PerNodeState["injected"] = api.READY

				assert.False(t, fsm.AllPeersAtLeast(opID, api.HYDRATING, []string{"injected"}))
			},
		},
		{
			name: "target ops survive a removal mid-iteration",
			test: func(t *testing.T, fsm *replication.ShardReplicationFSM) {
				for i := range 3 {
					seedOpFull(t, fsm, uint64(i+1), "node1", target, coll, fmt.Sprintf("shard-%d", i), api.COPY)
				}
				ops := fsm.GetOpsForTarget(target)
				require.Len(t, ops, 3)

				// Removing the middle op compacts the FSM's own slice in place.
				require.NoError(t, fsm.RemoveReplicationOp(&api.ReplicationRemoveOpRequest{
					Version: api.ReplicationCommandVersionV0,
					Id:      2,
				}))

				ids := make([]uint64, len(ops))
				for i, op := range ops {
					ids[i] = op.ID
				}
				assert.Equal(t, []uint64{1, 2, 3}, ids)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.test(t, replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry()))
		})
	}
}

// Cloning the History slice copies the State entries but not the Errors slice
// inside each one, so an archived error stays shared with the FSM. Every exit
// point that hands out a status is covered: a caller reaching one of them holds
// the same aliased memory as a caller reaching any other.
func TestShardReplicationFSM_HandsOutDetachedHistoryErrors(t *testing.T) {
	const (
		coll   = "TestClass"
		source = "node1"
		target = "node2"
		shard  = "shard-0"
		opID   = uint64(1)
		errMsg = "hydration failed"
	)

	tests := []struct {
		name    string
		handOut func(t *testing.T, fsm *replication.ShardReplicationFSM) replication.ShardReplicationOpStatus
	}{
		{
			name: "GetOpById",
			handOut: func(t *testing.T, fsm *replication.ShardReplicationFSM) replication.ShardReplicationOpStatus {
				op, ok := fsm.GetOpById(opID)
				require.True(t, ok)
				return op.Status
			},
		},
		{
			name: "GetOpState",
			handOut: func(t *testing.T, fsm *replication.ShardReplicationFSM) replication.ShardReplicationOpStatus {
				op, ok := fsm.GetOpById(opID)
				require.True(t, ok)
				status, ok := fsm.GetOpState(op.Op)
				require.True(t, ok)
				return status
			},
		},
		{
			name: "GetStatusByOps",
			handOut: func(t *testing.T, fsm *replication.ShardReplicationFSM) replication.ShardReplicationOpStatus {
				statuses := fsm.GetStatusByOps()
				require.Len(t, statuses, 1)
				for _, status := range statuses {
					return status
				}
				return replication.ShardReplicationOpStatus{}
			},
		},
		{
			name: "GetOpsForCollection",
			handOut: func(t *testing.T, fsm *replication.ShardReplicationFSM) replication.ShardReplicationOpStatus {
				ops, ok := fsm.GetOpsForCollection(coll)
				require.True(t, ok)
				require.Len(t, ops, 1)
				return ops[0].Status
			},
		},
		{
			name: "GetOpsForCollectionAndShard",
			handOut: func(t *testing.T, fsm *replication.ShardReplicationFSM) replication.ShardReplicationOpStatus {
				ops, ok := fsm.GetOpsForCollectionAndShard(coll, shard)
				require.True(t, ok)
				require.Len(t, ops, 1)
				return ops[0].Status
			},
		},
		{
			name: "GetOpsForTargetNode",
			handOut: func(t *testing.T, fsm *replication.ShardReplicationFSM) replication.ShardReplicationOpStatus {
				ops, ok := fsm.GetOpsForTargetNode(target)
				require.True(t, ok)
				require.Len(t, ops, 1)
				return ops[0].Status
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
			seedOpFull(t, fsm, opID, source, target, coll, shard, api.COPY)
			require.NoError(t, fsm.RegisterError(&api.ReplicationRegisterErrorRequest{
				Version:    api.ReplicationCommandVersionV0,
				Id:         opID,
				Error:      errMsg,
				TimeUnixMs: 1,
			}))
			// Moves the error out of Current and into History.
			driveToState(t, fsm, opID, api.HYDRATING)

			status := test.handOut(t, fsm)
			history := status.GetHistory()
			require.Len(t, history, 1)
			require.Len(t, history[0].Errors, 1)
			history[0].Errors[0].Message = "overwritten by the caller"

			current, ok := fsm.GetOpById(opID)
			require.True(t, ok)
			assert.Equal(t, errMsg, current.Status.GetHistory()[0].Errors[0].Message)
		})
	}
}

func BenchmarkHasActiveTargetReplicationForShard(b *testing.B) {
	const (
		coll   = "TestClass"
		target = "node2"
	)
	for _, n := range []int{10_000, 100_000} {
		fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
		for i := range n {
			id := uint64(i + 1)
			require.NoError(b, fsm.Replicate(id, &api.ReplicationReplicateShardRequest{
				Version:          api.ReplicationCommandVersionV0,
				Uuid:             strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", id)),
				SourceNode:       "node1",
				SourceCollection: coll,
				SourceShard:      fmt.Sprintf("shard-%d", i),
				TargetNode:       target,
				TransferType:     api.COPY.String(),
			}))
		}
		b.Run(fmt.Sprintf("ops-%d/hit", n), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				fsm.HasActiveTargetReplicationForShard(coll, "shard-0", target)
			}
		})
		b.Run(fmt.Sprintf("ops-%d/miss", n), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				fsm.HasActiveTargetReplicationForShard(coll, "no-such-shard", target)
			}
		})
	}
}
