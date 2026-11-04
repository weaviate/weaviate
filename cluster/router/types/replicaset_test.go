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

package types_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
)

func TestReadReplicaSet_Shards(t *testing.T) {
	tests := []struct {
		name     string
		replicas []types.Replica
		want     []string
	}{
		{
			name:     "empty replicas",
			replicas: []types.Replica{},
			want:     []string{},
		},
		{
			name: "single replica",
			replicas: []types.Replica{
				{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1"},
			},
			want: []string{"shard_A"},
		},
		{
			name: "multiple replicas different shards",
			replicas: []types.Replica{
				{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1"},
				{ShardName: "shard_B", NodeName: "node2", HostAddr: "host2"},
				{ShardName: "shard_C", NodeName: "node3", HostAddr: "host3"},
			},
			want: []string{"shard_A", "shard_B", "shard_C"},
		},
		{
			name: "multiple replicas same shard - should deduplicate",
			replicas: []types.Replica{
				{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1"},
				{ShardName: "shard_A", NodeName: "node2", HostAddr: "host2"},
				{ShardName: "shard_A", NodeName: "node3", HostAddr: "host3"},
			},
			want: []string{"shard_A"},
		},
		{
			name: "mixed - multiple shards with duplicates",
			replicas: []types.Replica{
				{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1"},
				{ShardName: "shard_B", NodeName: "node2", HostAddr: "host2"},
				{ShardName: "shard_A", NodeName: "node3", HostAddr: "host3"}, // duplicate
				{ShardName: "shard_C", NodeName: "node4", HostAddr: "host4"},
				{ShardName: "shard_B", NodeName: "node5", HostAddr: "host5"}, // duplicate
			},
			want: []string{"shard_A", "shard_B", "shard_C"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := types.ReadReplicaSet{Replicas: tt.replicas}
			got := rs.Shards()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadReplicaSet.Shards() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWriteReplicaSet_Shards(t *testing.T) {
	tests := []struct {
		name     string
		replicas []types.Replica
		want     []string
	}{
		{
			name:     "empty replicas",
			replicas: []types.Replica{},
			want:     []string{},
		},
		{
			name: "single replica",
			replicas: []types.Replica{
				{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1"},
			},
			want: []string{"shard_A"},
		},
		{
			name: "multiple replicas same shard - should deduplicate",
			replicas: []types.Replica{
				{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1"},
				{ShardName: "shard_A", NodeName: "node2", HostAddr: "host2"},
			},
			want: []string{"shard_A"},
		},
		{
			name: "multiple different shards",
			replicas: []types.Replica{
				{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1"},
				{ShardName: "shard_B", NodeName: "node2", HostAddr: "host2"},
			},
			want: []string{"shard_A", "shard_B"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ws := types.WriteReplicaSet{Replicas: tt.replicas}
			got := ws.Shards()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("WriteReplicaSet.Shards() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadReplicaSet_OtherMethods(t *testing.T) {
	replicas := []types.Replica{
		{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1:8080"},
		{ShardName: "shard_B", NodeName: "node2", HostAddr: "host2:8080"},
	}
	rs := types.ReadReplicaSet{Replicas: replicas}

	t.Run("NodeNames", func(t *testing.T) {
		want := []string{"node1", "node2"}
		got := rs.NodeNames()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("NodeNames() = %v, want %v", got, want)
		}
	})

	t.Run("HostAddresses", func(t *testing.T) {
		want := []string{"host1:8080", "host2:8080"}
		got := rs.HostAddresses()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("HostAddresses() = %v, want %v", got, want)
		}
	})

	t.Run("EmptyReplicas", func(t *testing.T) {
		if rs.EmptyReplicas() {
			t.Error("EmptyReplicas() should return false for non-empty replica set")
		}

		emptyRS := types.ReadReplicaSet{Replicas: []types.Replica{}}
		if !emptyRS.EmptyReplicas() {
			t.Error("EmptyReplicas() should return true for empty replica set")
		}
	})
}

func TestWriteReplicaSet_OtherMethods(t *testing.T) {
	replicas := []types.Replica{
		{ShardName: "shard_A", NodeName: "node1", HostAddr: "host1:8080"},
		{ShardName: "shard_B", NodeName: "node2", HostAddr: "host2:8080"},
	}
	ws := types.WriteReplicaSet{
		Replicas: replicas,
	}

	t.Run("NodeNames", func(t *testing.T) {
		want := []string{"node1", "node2"}
		got := ws.NodeNames()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("NodeNames() = %v, want %v", got, want)
		}
	})

	t.Run("HostAddresses", func(t *testing.T) {
		want := []string{"host1:8080", "host2:8080"}
		got := ws.HostAddresses()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("HostAddresses() = %v, want %v", got, want)
		}
	})

	t.Run("IsEmpty", func(t *testing.T) {
		if ws.IsEmpty() {
			t.Error("IsEmpty() should return false for non-empty replica set")
		}

		emptyWS := types.WriteReplicaSet{Replicas: []types.Replica{}}
		if !emptyWS.IsEmpty() {
			t.Error("IsEmpty() should return true for empty replica set")
		}
	})
}

// shardWidth is a shard and the number of replicas it has, for building a replica
// set whose shards differ in width.
type shardWidth struct {
	shard string
	n     int
}

func replicasOf(widths []shardWidth) []types.Replica {
	var replicas []types.Replica
	for _, w := range widths {
		for i := range w.n {
			replicas = append(replicas, types.Replica{
				ShardName: w.shard,
				NodeName:  fmt.Sprintf("node%d", i),
				HostAddr:  fmt.Sprintf("host%d:8080", i),
			})
		}
	}
	return replicas
}

// ValidateConsistencyLevel resolves the level against each shard's own replica
// count and rejects a set whose shards disagree on the result. Index.aggregate
// builds a collection-wide plan at ONE because ONE is the only level that agrees
// for any mix of widths.
func TestReadReplicaSet_ValidateConsistencyLevel(t *testing.T) {
	for _, tt := range []struct {
		name    string
		widths  []shardWidth
		level   types.ConsistencyLevel
		want    int
		wantErr string // Substring the call must fail with; empty if it must succeed.
	}{
		{
			name:  "empty replica set",
			level: types.ConsistencyLevelAll,
			want:  0,
		},
		{
			name:   "one shard at ONE",
			widths: []shardWidth{{"shard_A", 3}},
			level:  types.ConsistencyLevelOne,
			want:   1,
		},
		{
			name:   "one shard at QUORUM",
			widths: []shardWidth{{"shard_A", 3}},
			level:  types.ConsistencyLevelQuorum,
			want:   2,
		},
		{
			name:   "one shard at ALL",
			widths: []shardWidth{{"shard_A", 3}},
			level:  types.ConsistencyLevelAll,
			want:   3,
		},
		{
			name:   "shards of equal width at ALL",
			widths: []shardWidth{{"shard_A", 3}, {"shard_B", 3}},
			level:  types.ConsistencyLevelAll,
			want:   3,
		},
		{
			// The case Index.aggregate cannot use: at ALL each shard resolves to
			// its own width, so any difference in width is a disagreement.
			name:    "shards of unequal width at ALL",
			widths:  []shardWidth{{"shard_A", 1}, {"shard_B", 3}},
			level:   types.ConsistencyLevelAll,
			wantErr: "inconsistent consistency levels",
		},
		{
			// QUORUM tolerates a width difference the two widths round away.
			name:   "shards of unequal width whose quorums agree",
			widths: []shardWidth{{"shard_A", 2}, {"shard_B", 3}},
			level:  types.ConsistencyLevelQuorum,
			want:   2,
		},
		{
			name:    "shards of unequal width whose quorums differ",
			widths:  []shardWidth{{"shard_A", 1}, {"shard_B", 3}},
			level:   types.ConsistencyLevelQuorum,
			wantErr: "inconsistent consistency levels",
		},
		{
			// What Index.aggregate relies on: ONE resolves to 1 whatever the width.
			name:   "shards of any mix of widths at ONE",
			widths: []shardWidth{{"shard_A", 1}, {"shard_B", 2}, {"shard_C", 5}},
			level:  types.ConsistencyLevelOne,
			want:   1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rs := types.ReadReplicaSet{Replicas: replicasOf(tt.widths)}

			got, err := rs.ValidateConsistencyLevel(tt.level, nil)

			if tt.wantErr != "" {
				// Which two shards the message names follows map iteration order,
				// so only the prefix is stable.
				require.ErrorContains(t, err, tt.wantErr)
				require.Zero(t, got, "resolved level on failure")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got, "resolved level")
		})
	}
}

func TestWriteReplicaSet_ValidateConsistencyLevel(t *testing.T) {
	widths := []shardWidth{{"shard_A", 1}, {"shard_B", 3}}

	t.Run("shards of unequal width at ALL", func(t *testing.T) {
		ws := types.WriteReplicaSet{Replicas: replicasOf(widths)}

		_, err := ws.ValidateConsistencyLevel(types.ConsistencyLevelAll, nil)
		require.ErrorContains(t, err, "inconsistent consistency levels")
	})

	t.Run("shards of unequal width at ONE", func(t *testing.T) {
		ws := types.WriteReplicaSet{Replicas: replicasOf(widths)}

		got, err := ws.ValidateConsistencyLevel(types.ConsistencyLevelOne, nil)
		require.NoError(t, err)
		require.Equal(t, 1, got, "resolved level")
	})
}

func TestValidateConsistencyLevel(t *testing.T) {
	replicasOf := func(shard string, nodes ...string) []types.Replica {
		replicas := make([]types.Replica, 0, len(nodes))
		for _, node := range nodes {
			replicas = append(replicas, types.Replica{ShardName: shard, NodeName: node, HostAddr: node})
		}
		return replicas
	}

	tests := []struct {
		name          string
		level         types.ConsistencyLevel
		replicas      []types.Replica
		replicaCounts map[string]int
		want          int
		wantErr       string // substring of the rejection's cause; empty means no error
	}{
		{
			name:          "QUORUM with 1 of 3 reachable is rejected",
			level:         types.ConsistencyLevelQuorum,
			replicas:      replicasOf("s1", "n1"),
			replicaCounts: map[string]int{"s1": 3},
			wantErr:       `shard "s1": 1 of 3 replicas reachable`,
		},
		{
			name:          "ALL with 1 of 3 reachable is rejected",
			level:         types.ConsistencyLevelAll,
			replicas:      replicasOf("s1", "n1"),
			replicaCounts: map[string]int{"s1": 3},
			wantErr:       `shard "s1": 1 of 3 replicas reachable`,
		},
		{
			name:          "QUORUM with 2 of 3 reachable requires 2",
			level:         types.ConsistencyLevelQuorum,
			replicas:      replicasOf("s1", "n1", "n2"),
			replicaCounts: map[string]int{"s1": 3},
			want:          2,
		},
		{
			name:          "ALL with 2 of 3 reachable requires 2",
			level:         types.ConsistencyLevelAll,
			replicas:      replicasOf("s1", "n1", "n2"),
			replicaCounts: map[string]int{"s1": 3},
			want:          2,
		},
		{
			name:          "QUORUM with 3 of 3 reachable requires 2",
			level:         types.ConsistencyLevelQuorum,
			replicas:      replicasOf("s1", "n1", "n2", "n3"),
			replicaCounts: map[string]int{"s1": 3},
			want:          2,
		},
		{
			name:          "ALL with 3 of 3 reachable requires 3",
			level:         types.ConsistencyLevelAll,
			replicas:      replicasOf("s1", "n1", "n2", "n3"),
			replicaCounts: map[string]int{"s1": 3},
			want:          3,
		},
		{
			name:          "ONE with 1 of 3 reachable requires 1",
			level:         types.ConsistencyLevelOne,
			replicas:      replicasOf("s1", "n1"),
			replicaCounts: map[string]int{"s1": 3},
			want:          1,
		},
		{
			name:          "QUORUM with 1 of 2 reachable is rejected",
			level:         types.ConsistencyLevelQuorum,
			replicas:      replicasOf("s1", "n1"),
			replicaCounts: map[string]int{"s1": 2},
			wantErr:       `shard "s1": 1 of 2 replicas reachable`,
		},
		{
			name:          "ALL with 1 of 2 reachable is rejected",
			level:         types.ConsistencyLevelAll,
			replicas:      replicasOf("s1", "n1"),
			replicaCounts: map[string]int{"s1": 2},
			wantErr:       `shard "s1": 1 of 2 replicas reachable`,
		},
		{
			name:          "QUORUM with 3 of 5 reachable requires 3",
			level:         types.ConsistencyLevelQuorum,
			replicas:      replicasOf("s1", "n1", "n2", "n3"),
			replicaCounts: map[string]int{"s1": 5},
			want:          3,
		},
		{
			name:     "nil counts skip the check: ALL with 1 of 3 reachable requires 1",
			level:    types.ConsistencyLevelAll,
			replicas: replicasOf("s1", "n1"),
			want:     1,
		},
		{
			name:          "QUORUM rejects a shard with a count but no reachable replica",
			level:         types.ConsistencyLevelQuorum,
			replicas:      replicasOf("s1", "n1", "n2", "n3"),
			replicaCounts: map[string]int{"s1": 3, "s2": 3},
			wantErr:       `shard "s2": 0 of 3 replicas reachable`,
		},
		{
			name:          "QUORUM ignores a shard with no replicas",
			level:         types.ConsistencyLevelQuorum,
			replicas:      replicasOf("s1", "n1", "n2", "n3"),
			replicaCounts: map[string]int{"s1": 3, "s2": 0},
			want:          2,
		},
		{
			name:          "ONE rejects a shard with a count but no reachable replica",
			level:         types.ConsistencyLevelOne,
			replicas:      replicasOf("s1", "n1", "n2", "n3"),
			replicaCounts: map[string]int{"s1": 3, "s2": 3},
			wantErr:       `shard "s2": 0 of 3 replicas reachable`,
		},
		{
			name:          "ONE ignores a shard with no replicas",
			level:         types.ConsistencyLevelOne,
			replicas:      replicasOf("s1", "n1", "n2", "n3"),
			replicaCounts: map[string]int{"s1": 3, "s2": 0},
			want:          1,
		},
	}

	validators := []struct {
		name     string
		validate func([]types.Replica, types.ConsistencyLevel, map[string]int) (int, error)
	}{
		{
			name: "read",
			validate: func(replicas []types.Replica, level types.ConsistencyLevel, counts map[string]int) (int, error) {
				return types.ReadReplicaSet{Replicas: replicas}.ValidateConsistencyLevel(level, counts)
			},
		},
		{
			name: "write",
			validate: func(replicas []types.Replica, level types.ConsistencyLevel, counts map[string]int) (int, error) {
				return types.WriteReplicaSet{Replicas: replicas}.ValidateConsistencyLevel(level, counts)
			},
		},
	}

	for _, tt := range tests {
		for _, v := range validators {
			t.Run(tt.name+"/"+v.name, func(t *testing.T) {
				got, err := v.validate(tt.replicas, tt.level, tt.replicaCounts)
				if tt.wantErr != "" {
					require.ErrorIs(t, err, replicaerrors.ErrReplicas)
					require.ErrorContains(t, err, "cannot reach enough replicas")
					require.ErrorContains(t, err, tt.wantErr)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			})
		}
	}
}
