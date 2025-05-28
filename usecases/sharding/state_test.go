//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package sharding

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/sharding/config"
)

func TestState(t *testing.T) {
	size := 1000

	cfg, err := config.ParseConfig(map[string]interface{}{"desiredCount": float64(4)}, 14)
	require.Nil(t, err)

	nodes := mocks.NewMockNodeSelector("node1", "node2")
	state, err := InitState("my-index", cfg, nodes.LocalName(), nodes.StorageCandidates(), 1, false)
	require.Nil(t, err)

	physicalCount := map[string]int{}
	var names [][]byte

	for i := 0; i < size; i++ {
		name := make([]byte, 16)
		rand.Read(name)
		names = append(names, name)

		phid := state.PhysicalShard(name)
		physicalCount[phid]++
	}

	// verify each shard contains at least 15% of data. The expected value would
	// be 25%, but since this is random, we should take a lower value to reduce
	// flakyness

	for name, count := range physicalCount {
		if owns := float64(count) / float64(size); owns < 0.15 {
			t.Errorf("expected shard %q to own at least 15%%, but it only owns %f", name, owns)
		}
	}

	// Marshal and recreate, verify results
	bytes, err := state.JSON()
	require.Nil(t, err)

	// destroy old version
	state = nil

	stateReloaded, err := StateFromJSON(bytes, nodes)
	require.Nil(t, err)

	physicalCountReloaded := map[string]int{}

	// hash the same values again and verify the counts are exactly the same
	for _, name := range names {
		phid := stateReloaded.PhysicalShard(name)
		physicalCountReloaded[phid]++
	}

	assert.Equal(t, physicalCount, physicalCountReloaded)
}

func TestInitState(t *testing.T) {
	type test struct {
		nodes             []string
		replicationFactor int
		shards            int
		ok                bool
	}

	// this tests asserts that nodes are assigned evenly with various
	// combinations.

	tests := []test{
		{
			nodes:             []string{"node1", "node2", "node3"},
			replicationFactor: 1,
			shards:            3,
			ok:                true,
		},
		{
			nodes:             []string{"node1", "node2", "node3"},
			replicationFactor: 2,
			shards:            3,
			ok:                true,
		},
		{
			nodes:             []string{"node1", "node2", "node3"},
			replicationFactor: 3,
			shards:            1,
			ok:                true,
		},
		{
			nodes:             []string{"node1", "node2", "node3"},
			replicationFactor: 3,
			shards:            3,
			ok:                true,
		},
		{
			nodes:             []string{"node1", "node2", "node3"},
			replicationFactor: 3,
			shards:            2,
			ok:                true,
		},
		{
			nodes:             []string{"node1", "node2", "node3", "node4", "node5", "node6"},
			replicationFactor: 4,
			shards:            6,
			ok:                true,
		},
		{
			nodes:             []string{"node1", "node2"},
			replicationFactor: 4,
			shards:            4,
			ok:                false,
		},
		{
			nodes:             []string{"node1", "node2", "node3", "node4", "node5", "node6", "node7", "node8", "node9", "node10", "node11", "node12"},
			replicationFactor: 3,
			shards:            4,
			ok:                true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("Shards=%d_RF=%d", test.shards, test.replicationFactor),
			func(t *testing.T) {
				nodes := mocks.NewMockNodeSelector(test.nodes...)
				cfg, err := config.ParseConfig(map[string]interface{}{
					"desiredCount": float64(test.shards),
					"replicas":     float64(test.replicationFactor),
				}, 3)
				require.Nil(t, err)

				state, err := InitState("my-index", cfg, nodes.LocalName(), nodes.StorageCandidates(), int64(test.replicationFactor), false)
				if !test.ok {
					require.NotNil(t, err)
					return
				}
				require.Nil(t, err)

				nodeCounter := map[string]int{}
				actual := 0
				for _, shard := range state.Physical {
					for _, node := range shard.BelongsToNodes {
						nodeCounter[node]++
						actual++
					}
				}

				assert.Equal(t, len(nodeCounter), len(test.nodes))

				// assert that total no of associations is correct
				desired := test.shards * test.replicationFactor
				assert.Equal(t, desired, actual, "correct number of node associations")

				// assert that shards are hit evenly
				expectedAssociations := test.shards * test.replicationFactor / len(test.nodes)
				for _, count := range nodeCounter {
					assert.Equal(t, expectedAssociations, count)
				}
			})
	}
}

func TestInitStateWithZeroReplicationFactor(t *testing.T) {
	nodes := mocks.NewMockNodeSelector("node1", "node2", "node3")
	cfg, err := config.ParseConfig(map[string]interface{}{"desiredCount": float64(3)}, 3)
	require.NoError(t, err)

	_, err = InitState("index-zero", cfg, nodes.LocalName(), nodes.StorageCandidates(), 0, false)
	require.Errorf(t, err, "replication factor zero is not allowed")
}

func TestAdjustReplicas(t *testing.T) {
	t.Run("1->3", func(t *testing.T) {
		nodes := mocks.NewMockNodeSelector("N1", "N2", "N3", "N4", "N5")
		shard := Physical{BelongsToNodes: []string{"N1"}}
		require.Nil(t, shard.AdjustReplicas(3, nodes))
		assert.ElementsMatch(t, []string{"N1", "N2", "N3"}, shard.BelongsToNodes)
	})

	t.Run("2->3", func(t *testing.T) {
		nodes := mocks.NewMockNodeSelector("N1", "N2", "N3", "N4", "N5")
		shard := Physical{BelongsToNodes: []string{"N2", "N3"}}
		require.Nil(t, shard.AdjustReplicas(3, nodes))
		assert.ElementsMatch(t, []string{"N1", "N2", "N3"}, shard.BelongsToNodes)
	})

	t.Run("3->3", func(t *testing.T) {
		nodes := mocks.NewMockNodeSelector("N1", "N2", "N3", "N4", "N5")
		shard := Physical{BelongsToNodes: []string{"N1", "N2", "N3"}}
		require.Nil(t, shard.AdjustReplicas(3, nodes))
		assert.ElementsMatch(t, []string{"N1", "N2", "N3"}, shard.BelongsToNodes)
	})

	t.Run("3->2", func(t *testing.T) {
		nodes := mocks.NewMockNodeSelector("N1", "N2", "N3")
		shard := Physical{BelongsToNodes: []string{"N1", "N2", "N3"}}
		require.Nil(t, shard.AdjustReplicas(2, nodes))
		assert.ElementsMatch(t, []string{"N1", "N2"}, shard.BelongsToNodes)
	})

	t.Run("Min", func(t *testing.T) {
		nodes := mocks.NewMockNodeSelector("N1", "N2", "N3")
		shard := Physical{BelongsToNodes: []string{"N1", "N2", "N3"}}
		require.NotNil(t, shard.AdjustReplicas(-1, nodes))
	})
	t.Run("Max", func(t *testing.T) {
		nodes := mocks.NewMockNodeSelector("N1", "N2", "N3")
		shard := Physical{BelongsToNodes: []string{"N1", "N2", "N3"}}
		require.NotNil(t, shard.AdjustReplicas(4, nodes))
	})
	t.Run("Bug", func(t *testing.T) {
		names := []string{"N1", "N2", "N3", "N4"}
		nodes := mocks.NewMockNodeSelector(names...) // bug
		shard := Physical{BelongsToNodes: []string{"N1", "N1", "N1", "N2", "N2"}}
		require.Nil(t, shard.AdjustReplicas(4, nodes)) // correct
		require.ElementsMatch(t, names, shard.BelongsToNodes)
	})
}

func TestGetPartitions(t *testing.T) {
	t.Run("EmptyCandidatesList", func(t *testing.T) {
		shards := []string{"H1"}
		state := State{}
		partitions, err := state.GetPartitions(mocks.NewMockNodeSelector().StorageCandidates(), shards, 1)
		require.Nil(t, partitions)
		require.ErrorContains(t, err, "empty")
	})
	t.Run("NotEnoughReplicas", func(t *testing.T) {
		shards := []string{"H1"}
		state := State{}
		partitions, err := state.GetPartitions(mocks.NewMockNodeSelector("N1").StorageCandidates(), shards, 2)
		require.Nil(t, partitions)
		require.ErrorContains(t, err, "not enough replicas")
	})
	t.Run("Success/RF3", func(t *testing.T) {
		nodes := mocks.NewMockNodeSelector("N1", "N2", "N3")
		shards := []string{"H1", "H2", "H3", "H4", "H5"}
		state := State{}
		got, err := state.GetPartitions(nodes.StorageCandidates(), shards, 3)
		require.Nil(t, err)
		want := map[string][]string{
			"H1": {"N1", "N2", "N3"},
			"H2": {"N2", "N3", "N1"},
			"H3": {"N3", "N1", "N2"},
			"H4": {"N3", "N1", "N2"},
			"H5": {"N1", "N2", "N3"},
		}
		require.Equal(t, want, got)
	})

	t.Run("Success/RF2", func(t *testing.T) {
		nodes := mocks.NewMockNodeSelector("N1", "N2", "N3", "N4", "N5", "N6", "N7")
		shards := []string{"H1", "H2", "H3", "H4", "H5"}
		state := State{}
		got, err := state.GetPartitions(nodes.StorageCandidates(), shards, 2)
		require.Nil(t, err)
		want := map[string][]string{
			"H1": {"N1", "N2"},
			"H2": {"N3", "N4"},
			"H3": {"N5", "N6"},
			"H4": {"N7", "N1"},
			"H5": {"N2", "N3"},
		}
		require.Equal(t, want, got)
	})
}

func TestAddPartition(t *testing.T) {
	var (
		nodes1 = []string{"N", "M"}
		nodes2 = []string{"L", "M", "O"}
	)
	cfg, err := config.ParseConfig(map[string]interface{}{"desiredCount": float64(4)}, 14)
	require.Nil(t, err)

	nodes := mocks.NewMockNodeSelector("node1", "node2")
	s, err := InitState("my-index", cfg, nodes.LocalName(), nodes.StorageCandidates(), 1, true)
	require.Nil(t, err)

	_, err = s.AddPartition("A", nodes1, models.TenantActivityStatusHOT)
	require.NoErrorf(t, err, "unexpect error while adding partition for tenant A")
	_, err = s.AddPartition("B", nodes2, models.TenantActivityStatusCOLD)
	require.NoErrorf(t, err, "unexpect error while adding partition for tenant B")

	want := map[string]Physical{
		"A": {Name: "A", BelongsToNodes: nodes1, OwnsPercentage: 1, Status: models.TenantActivityStatusHOT},
		"B": {Name: "B", BelongsToNodes: nodes2, OwnsPercentage: 1, Status: models.TenantActivityStatusCOLD},
	}
	require.Equal(t, want, s.Physical)
}

func TestStateDeepCopy(t *testing.T) {
	original := State{
		IndexID: "original",
		Config: config.Config{
			VirtualPerPhysical:  1,
			DesiredCount:        2,
			ActualCount:         3,
			DesiredVirtualCount: 4,
			ActualVirtualCount:  5,
			Key:                 "original",
			Strategy:            "original",
			Function:            "original",
		},
		localNodeName: "original",
		Physical: map[string]Physical{
			"physical1": {
				Name:           "original",
				OwnsVirtual:    []string{"original"},
				OwnsPercentage: 7,
				BelongsToNodes: []string{"original"},
				Status:         models.TenantActivityStatusHOT,
			},
		},
		Virtual: []Virtual{
			{
				Name:               "original",
				Upper:              8,
				OwnsPercentage:     9,
				AssignedToPhysical: "original",
			},
		},
		ReplicationFactor: 3,
	}

	control := State{
		IndexID: "original",
		Config: config.Config{
			VirtualPerPhysical:  1,
			DesiredCount:        2,
			ActualCount:         3,
			DesiredVirtualCount: 4,
			ActualVirtualCount:  5,
			Key:                 "original",
			Strategy:            "original",
			Function:            "original",
		},
		localNodeName: "original",
		Physical: map[string]Physical{
			"physical1": {
				Name:           "original",
				OwnsVirtual:    []string{"original"},
				OwnsPercentage: 7,
				BelongsToNodes: []string{"original"},
				Status:         models.TenantActivityStatusHOT,
			},
		},
		Virtual: []Virtual{
			{
				Name:               "original",
				Upper:              8,
				OwnsPercentage:     9,
				AssignedToPhysical: "original",
			},
		},
		ReplicationFactor: 3,
	}

	assert.Equal(t, control, original, "control matches initially")

	copied := original.DeepCopy()
	assert.Equal(t, control, copied, "copy matches original")

	// modify literally every field
	copied.localNodeName = "changed"
	copied.IndexID = "changed"
	copied.Config.VirtualPerPhysical = 11
	copied.Config.DesiredCount = 22
	copied.Config.ActualCount = 33
	copied.Config.DesiredVirtualCount = 44
	copied.Config.ActualVirtualCount = 55
	copied.Config.Key = "changed"
	copied.Config.Strategy = "changed"
	copied.Config.Function = "changed"
	physical1 := copied.Physical["physical1"]
	physical1.Name = "changed"
	physical1.BelongsToNodes = append(physical1.BelongsToNodes, "changed")
	physical1.OwnsPercentage = 100
	physical1.OwnsVirtual = append(physical1.OwnsVirtual, "changed")
	physical1.Status = models.TenantActivityStatusCOLD
	copied.Physical["physical1"] = physical1
	copied.Physical["physical2"] = Physical{}
	copied.Virtual[0].Name = "original"
	copied.Virtual[0].Upper = 8
	copied.Virtual[0].OwnsPercentage = 9
	copied.Virtual[0].AssignedToPhysical = "original"
	copied.Virtual = append(copied.Virtual, Virtual{})

	assert.Equal(t, control, original, "original still matches control even with changes in copy")
}

func TestBackwardCompatibilityBefore1_17(t *testing.T) {
	// As part of v1.17, replication is introduced and the structure of the
	// physical shard is slightly changed. Instead of `belongsToNode string`, the
	// association is now `belongsToNodes []string`. A migration helper was
	// introduced to make sure we're backward compatible.

	oldVersion := State{
		Physical: map[string]Physical{
			"hello-replication": {
				Name:                                 "hello-replication",
				LegacyBelongsToNodeForBackwardCompat: "the-best-node",
			},
		},
	}
	oldVersionJSON, err := json.Marshal(oldVersion)
	require.Nil(t, err)

	var newVersion State
	err = json.Unmarshal(oldVersionJSON, &newVersion)
	require.Nil(t, err)

	newVersion.MigrateFromOldFormat()

	assert.Equal(t, []string{"the-best-node"},
		newVersion.Physical["hello-replication"].BelongsToNodes)
}

func TestApplyNodeMapping(t *testing.T) {
	type test struct {
		name        string
		state       State
		control     State
		nodeMapping map[string]string
	}

	tests := []test{
		{
			name: "no mapping",
			state: State{
				Physical: map[string]Physical{
					"hello-node-mapping": {
						Name:                                 "hello-node-mapping",
						LegacyBelongsToNodeForBackwardCompat: "node1",
						BelongsToNodes:                       []string{"node1"},
					},
				},
			},
			control: State{
				Physical: map[string]Physical{
					"hello-node-mapping": {
						Name:                                 "hello-node-mapping",
						LegacyBelongsToNodeForBackwardCompat: "node1",
						BelongsToNodes:                       []string{"node1"},
					},
				},
			},
		},
		{
			name: "map one node",
			state: State{
				Physical: map[string]Physical{
					"hello-node-mapping": {
						Name:                                 "hello-node-mapping",
						LegacyBelongsToNodeForBackwardCompat: "node1",
						BelongsToNodes:                       []string{"node1"},
					},
				},
			},
			control: State{
				Physical: map[string]Physical{
					"hello-node-mapping": {
						Name:                                 "hello-node-mapping",
						LegacyBelongsToNodeForBackwardCompat: "new-node1",
						BelongsToNodes:                       []string{"new-node1"},
					},
				},
			},
			nodeMapping: map[string]string{"node1": "new-node1"},
		},
		{
			name: "map multiple nodes",
			state: State{
				Physical: map[string]Physical{
					"hello-node-mapping": {
						Name:                                 "hello-node-mapping",
						LegacyBelongsToNodeForBackwardCompat: "node1",
						BelongsToNodes:                       []string{"node1", "node2"},
					},
				},
			},
			control: State{
				Physical: map[string]Physical{
					"hello-node-mapping": {
						Name:                                 "hello-node-mapping",
						LegacyBelongsToNodeForBackwardCompat: "new-node1",
						BelongsToNodes:                       []string{"new-node1", "new-node2"},
					},
				},
			},
			nodeMapping: map[string]string{"node1": "new-node1", "node2": "new-node2"},
		},
		{
			name: "map multiple nodes with exceptions",
			state: State{
				Physical: map[string]Physical{
					"hello-node-mapping": {
						Name:                                 "hello-node-mapping",
						LegacyBelongsToNodeForBackwardCompat: "node1",
						BelongsToNodes:                       []string{"node1", "node2", "node3"},
					},
				},
			},
			control: State{
				Physical: map[string]Physical{
					"hello-node-mapping": {
						Name:                                 "hello-node-mapping",
						LegacyBelongsToNodeForBackwardCompat: "new-node1",
						BelongsToNodes:                       []string{"new-node1", "new-node2", "node3"},
					},
				},
			},
			nodeMapping: map[string]string{"node1": "new-node1", "node2": "new-node2"},
		},
		{
			name: "map multiple nodes with legacy exception",
			state: State{
				Physical: map[string]Physical{
					"hello-node-mapping": {
						Name:                                 "hello-node-mapping",
						LegacyBelongsToNodeForBackwardCompat: "node3",
						BelongsToNodes:                       []string{"node1", "node2", "node3"},
					},
				},
			},
			control: State{
				Physical: map[string]Physical{
					"hello-node-mapping": {
						Name:                                 "hello-node-mapping",
						LegacyBelongsToNodeForBackwardCompat: "node3",
						BelongsToNodes:                       []string{"new-node1", "new-node2", "node3"},
					},
				},
			},
			nodeMapping: map[string]string{"node1": "new-node1", "node2": "new-node2"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.state.ApplyNodeMapping(tc.nodeMapping)
			assert.Equal(t, tc.control, tc.state)
		})
	}
}

func TestShardReplicationFactor(t *testing.T) {
	t.Run("add replica", func(t *testing.T) {
		nodes := mocks.NewMockNodeSelector("N1", "N2", "N3", "N4", "N5")
		cfg, err := config.ParseConfig(map[string]interface{}{"desiredCount": float64(3)}, 3)
		require.NoErrorf(t, err, "unexpected error while parsing config")

		state, err := InitState("my-index", cfg, nodes.LocalName(), nodes.StorageCandidates(), 2, false)
		require.NoErrorf(t, err, "unexpected error while initializing state")

		shardName := state.AllPhysicalShards()[0]
		numberOfReplicas, err := state.NumberOfReplicas(shardName)
		require.NoErrorf(t, err, "error while getting number of replicas for shard %s", shardName)
		require.Equal(t, int64(2), numberOfReplicas, "unexpected replication factor")
		require.Equal(t, int64(2), state.ReplicationFactor, "unexpected minimum replication factor")

		initialReplicaCount := len(state.Physical[shardName].BelongsToNodes)
		require.Equal(t, 2, initialReplicaCount)

		err = state.AddReplicaToShard(shardName, "test-replica")
		require.NoErrorf(t, err, "unexpected error while adding a replica")

		numberOfReplicas, err = state.NumberOfReplicas(shardName)
		require.NoErrorf(t, err, "error while getting number of replicas for shard %s", shardName)
		require.Equal(t, int64(3), numberOfReplicas)
		require.Equal(t, 3, len(state.Physical[shardName].BelongsToNodes))
	})

	t.Run("add existing replica", func(t *testing.T) {
		nodes := mocks.NewMockNodeSelector("N1", "N2", "N3", "N4", "N5")
		cfg, err := config.ParseConfig(map[string]interface{}{"desiredCount": float64(3)}, 3)
		require.NoErrorf(t, err, "unexpected error while parsing config")

		state, err := InitState("my-index", cfg, nodes.LocalName(), nodes.StorageCandidates(), 2, false)
		require.NoErrorf(t, err, "unexpected error while initializing state")

		shardName := state.AllPhysicalShards()[0]
		numberOfReplicas, err := state.NumberOfReplicas(shardName)
		require.NoErrorf(t, err, "error while getting number of replicas for shard %s", shardName)
		require.Equal(t, int64(2), numberOfReplicas, "unexpected replication factor")
		require.Equal(t, int64(2), state.ReplicationFactor, "unexpected minimum replication factor")

		initialReplicaCount := len(state.Physical[shardName].BelongsToNodes)
		require.Equal(t, 2, initialReplicaCount)

		err = state.AddReplicaToShard(shardName, "test-replica")
		require.NoErrorf(t, err, "unexpected error while adding a replica")

		numberOfReplicas, err = state.NumberOfReplicas(shardName)
		require.NoErrorf(t, err, "error while getting number of replicas for shard %s", shardName)
		require.Equal(t, int64(3), numberOfReplicas)
		require.Equal(t, 3, len(state.Physical[shardName].BelongsToNodes))

		err = state.AddReplicaToShard(shardName, "test-replica")
		require.Errorf(t, err, "expected a failure while adding exisiting shard")

		require.Equal(t, 3, len(state.Physical[shardName].BelongsToNodes))
		require.Truef(t, strings.Contains(err.Error(), "already exists"), "expected already existing replica error")

		numberOfReplicas, err = state.NumberOfReplicas(shardName)
		require.NoErrorf(t, err, "error while getting number of replicas for shard %s", shardName)
		require.Equal(t, int64(3), numberOfReplicas)
	})

	t.Run("delete replica", func(t *testing.T) {
		nodes := mocks.NewMockNodeSelector("N1", "N2", "N3", "N4", "N5")
		cfg, err := config.ParseConfig(map[string]interface{}{"desiredCount": float64(3)}, 3)
		require.NoErrorf(t, err, "unexpected error while parsing config")

		state, err := InitState("my-index", cfg, nodes.LocalName(), nodes.StorageCandidates(), 2, false)
		require.NoErrorf(t, err, "unexpected error while initializing state")

		shardName := state.AllPhysicalShards()[0]
		initialReplicaCount := len(state.Physical[shardName].BelongsToNodes)
		require.Equal(t, 2, initialReplicaCount)

		err = state.AddReplicaToShard(shardName, "test-replica1")
		require.NoError(t, err, "unexpected error while adding replica")

		// 3 replicas with a minimum of 2, deleting a replica is exepcted to work
		replicaToDelete := state.Physical[shardName].BelongsToNodes[0]
		err = state.DeleteReplicaFromShard(shardName, replicaToDelete)
		require.NoErrorf(t, err, "unexpected error while deleting replica from shard")
	})

	t.Run("delete replica failure", func(t *testing.T) {
		nodes := mocks.NewMockNodeSelector("N1", "N2", "N3", "N4", "N5")
		cfg, err := config.ParseConfig(map[string]interface{}{"desiredCount": float64(3)}, 3)
		require.NoErrorf(t, err, "unexpected error while parsing config")

		state, err := InitState("my-index", cfg, nodes.LocalName(), nodes.StorageCandidates(), 2, false)
		require.NoErrorf(t, err, "unexpected error while initializing state")

		shardName := state.AllPhysicalShards()[0]
		initialReplicaCount := len(state.Physical[shardName].BelongsToNodes)
		require.Equal(t, 2, initialReplicaCount)

		// 2 replicas with a minimum of 2ca, deleting a replica is expected to fail
		replicaToDelete := state.Physical[shardName].BelongsToNodes[0]
		err = state.DeleteReplicaFromShard(shardName, replicaToDelete)
		require.Errorf(t, err, "expected a failure while removing a replica")
	})

	t.Run("delete non-existing replica", func(t *testing.T) {
		nodes := mocks.NewMockNodeSelector("N1", "N2", "N3", "N4", "N5")
		cfg, err := config.ParseConfig(map[string]interface{}{"desiredCount": float64(3)}, 3)
		require.NoErrorf(t, err, "unexpected error while parsing config")

		state, err := InitState("my-index", cfg, nodes.LocalName(), nodes.StorageCandidates(), 2, false)
		require.NoErrorf(t, err, "unexpected error while initializing state")

		shardName := state.AllPhysicalShards()[0]
		initialReplicaCount := len(state.Physical[shardName].BelongsToNodes)
		require.Equal(t, 2, initialReplicaCount)

		// 3 replicas with a minimum of 3, deleting a replica is expected to fail
		replicaToDelete := state.Physical[shardName].BelongsToNodes[0] + "-dummy"
		err = state.DeleteReplicaFromShard(shardName, replicaToDelete)
		require.Errorf(t, err, "expected a failure while removing a replica")
	})

	t.Run("deep copy", func(t *testing.T) {
		original := State{
			ReplicationFactor: 2,
		}

		copied := original.DeepCopy()
		require.Equal(t, int64(2), copied.ReplicationFactor)

		copied.ReplicationFactor = 4
		require.Equal(t, int64(2), original.ReplicationFactor)
	})
}
