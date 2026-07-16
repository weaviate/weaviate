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
			name:      "CANCELLED only ⇒ excluded",
			ops:       []struct{ srcNode, state string }{{"node1", string(api.CANCELLED)}},
			wantRead:  []string{},
			wantWrite: []string{},
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
		coll       = "TestClass"
		writers    = 4
		readers    = 4
		iterations = 500
	)
	fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())

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
			}
			return nil
		})
	}
	require.NoError(t, eg.Wait())
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
