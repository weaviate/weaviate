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
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
)

// Covers the three bucket-iterating ForceDelete methods. Each used to range over
// the slice removeReplicationOp rewrites in place, so with three ops it skipped
// one and errored on a stale entry.
func TestForceDeleteFamily_RemovesAllMatchingOps(t *testing.T) {
	const (
		coll  = "TestClass"
		shard = "shard1"
	)

	cases := []struct {
		name        string
		forceDelete func(fsm *replication.ShardReplicationFSM) error
	}{
		{
			name: "by collection",
			forceDelete: func(fsm *replication.ShardReplicationFSM) error {
				return fsm.ForceDeleteByCollection(coll)
			},
		},
		{
			name: "by collection and shard",
			forceDelete: func(fsm *replication.ShardReplicationFSM) error {
				return fsm.ForceDeleteByCollectionAndShard(coll, shard)
			},
		},
		{
			name: "by target node",
			forceDelete: func(fsm *replication.ShardReplicationFSM) error {
				// every seeded op below shares the target node "target"
				return fsm.ForceDeleteByTargetNode("target")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
			// distinct source nodes so admission accepts all three
			for i := uint64(1); i <= 3; i++ {
				seedOpFull(t, fsm, i, fmt.Sprintf("source%d", i), "target", coll, shard, api.COPY)
			}

			require.NoError(t, tc.forceDelete(fsm))

			for i := uint64(1); i <= 3; i++ {
				_, ok := fsm.GetOpById(i)
				require.Falsef(t, ok, "op %d should have been removed", i)
			}
		})
	}
}

func TestForceDeleteAll_RemovesEverything(t *testing.T) {
	fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
	for i := uint64(1); i <= 5; i++ {
		coll := "CollA"
		if i > 3 {
			coll = "CollB"
		}
		seedOpFull(t, fsm, i, fmt.Sprintf("source%d", i), "target", coll, "shard1", api.COPY)
	}

	require.NoError(t, fsm.ForceDeleteAll())

	for i := uint64(1); i <= 5; i++ {
		_, ok := fsm.GetOpById(i)
		require.Falsef(t, ok, "op %d should have been removed", i)
	}
}

// Draining a bucket must delete its key, not leave an empty slice. A lingering
// empty key in opsByTargetFQDN OR-folds to (false,false) in
// filterOneReplicaReadWrite and drops a live replica from routing.
func TestForceDelete_LeavesNoEmptyBucketKeys(t *testing.T) {
	const (
		coll  = "TestClass"
		shard = "shard1"
	)

	cases := []struct {
		name        string
		seed        func(t *testing.T, fsm *replication.ShardReplicationFSM)
		forceDelete func(fsm *replication.ShardReplicationFSM) error
		replicas    []string
	}{
		{
			name: "batched path drains every bucket",
			seed: func(t *testing.T, fsm *replication.ShardReplicationFSM) {
				seedOpFull(t, fsm, 1, "source1", "target", coll, shard, api.COPY)
				seedOpFull(t, fsm, 2, "source2", "target", coll, shard, api.COPY)
			},
			forceDelete: func(fsm *replication.ShardReplicationFSM) error {
				return fsm.ForceDeleteByCollectionAndShard(coll, shard)
			},
			replicas: []string{"source1", "source2", "target"},
		},
		{
			name: "single-op path drains every bucket",
			seed: func(t *testing.T, fsm *replication.ShardReplicationFSM) {
				seedOpFull(t, fsm, 1, "source1", "target", coll, shard, api.COPY)
			},
			forceDelete: func(fsm *replication.ShardReplicationFSM) error {
				return fsm.ForceDeleteByUuid("00000000-0000-0000-0000-000000000001")
			},
			replicas: []string{"source1", "target"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
			tc.seed(t, fsm)

			require.NoError(t, tc.forceDelete(fsm))

			_, ok := fsm.GetOpsForCollection(coll)
			require.False(t, ok, "collection bucket key should be gone once it drains")
			_, ok = fsm.GetOpsForCollectionAndShard(coll, shard)
			require.False(t, ok, "collection/shard bucket key should be gone once it drains")
			_, ok = fsm.GetOpsForTargetNode("target")
			require.False(t, ok, "target-node bucket key should be gone once it drains")

			require.Equal(t, tc.replicas, fsm.FilterOneShardReplicasRead(coll, shard, tc.replicas))
			require.Equal(t, tc.replicas, fsm.FilterOneShardReplicasWrite(coll, shard, tc.replicas))
		})
	}
}

// The target-node getter must not hand out the FSM's own bucket. The producer
// iterates the result with no lock held, so an aliased return is an
// unsynchronized read of an array removeReplicationOps rewrites in place.
func TestGetOpsForTarget_ReturnsACopy(t *testing.T) {
	const (
		coll  = "TestClass"
		shard = "shard1"
	)

	fsm := replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry())
	for i := uint64(1); i <= 3; i++ {
		seedOpFull(t, fsm, i, fmt.Sprintf("source%d", i), "target", coll, shard, api.COPY)
	}

	snapshot := fsm.GetOpsForTarget("target")
	require.Len(t, snapshot, 3)

	// Removing the first op compacts the bucket to [2, 3] in place, leaving the
	// third slot stale. An aliased snapshot reads back as [2, 3, 3].
	require.NoError(t, fsm.ForceDeleteByIds([]uint64{1}))

	ids := make([]uint64, 0, len(snapshot))
	for _, op := range snapshot {
		ids = append(ids, op.ID)
	}
	require.Equal(t, []uint64{1, 2, 3}, ids,
		"a slice handed to a caller must not be rewritten by a later removal")

	// The bucket itself is still compacted.
	require.Len(t, fsm.GetOpsForTarget("target"), 2)
}

func TestForceDelete_GaugeReturnsToZero(t *testing.T) {
	const coll = "TestClass"

	reg := prometheus.NewPedanticRegistry()
	fsm := replication.NewShardReplicationFSM(reg)

	seedOpFull(t, fsm, 1, "source1", "target", coll, "shard1", api.COPY)
	seedOpFull(t, fsm, 2, "source2", "target", coll, "shard1", api.COPY)
	driveToState(t, fsm, 2, api.HYDRATING)
	seedOpFull(t, fsm, 3, "source3", "target", coll, "shard2", api.COPY)
	driveToState(t, fsm, 3, api.READY)

	require.NoError(t, fsm.ForceDeleteByCollection(coll))

	expected := `
# HELP weaviate_replication_operation_fsm_ops_by_state Current number of replication operations in each state of the FSM lifecycle
# TYPE weaviate_replication_operation_fsm_ops_by_state gauge
weaviate_replication_operation_fsm_ops_by_state{state="HYDRATING"} 0
weaviate_replication_operation_fsm_ops_by_state{state="READY"} 0
weaviate_replication_operation_fsm_ops_by_state{state="REGISTERED"} 0
`
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected),
		"weaviate_replication_operation_fsm_ops_by_state"))
}
