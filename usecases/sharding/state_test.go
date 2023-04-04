//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package sharding

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestState(t *testing.T) {
	size := 1000

	cfg, err := ParseConfig(map[string]interface{}{"desiredCount": float64(4)}, 14)
	require.Nil(t, err)

	nodes := fakeNodes{[]string{"node1", "node2"}}
	state, err := InitState("my-index", cfg, nodes, 1)
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

type fakeNodes struct {
	nodes []string
}

func (f fakeNodes) Candidates() []string {
	return f.nodes
}

func (f fakeNodes) LocalName() string {
	return f.nodes[0]
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
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("Shards=%d_RF=%d", test.shards, test.replicationFactor),
			func(t *testing.T) {
				nodes := fakeNodes{test.nodes}
				cfg, err := ParseConfig(map[string]interface{}{
					"desiredCount": float64(test.shards),
					"replicas":     float64(test.replicationFactor),
				}, 3)
				require.Nil(t, err)

				state, err := InitState("my-index", cfg, nodes, int64(test.replicationFactor))
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

func TestAdjustReplicas(t *testing.T) {
	t.Run("1->3", func(t *testing.T) {
		nodes := fakeNodes{nodes: []string{"N1", "N2", "N3", "N4", "N5"}}
		shard := Physical{BelongsToNodes: []string{"N1"}}
		require.Nil(t, shard.AdjustReplicas(3, nodes))
		assert.ElementsMatch(t, []string{"N1", "N2", "N3"}, shard.BelongsToNodes)
	})

	t.Run("2->3", func(t *testing.T) {
		nodes := fakeNodes{nodes: []string{"N1", "N2", "N3", "N4", "N5"}}
		shard := Physical{BelongsToNodes: []string{"N2", "N3"}}
		require.Nil(t, shard.AdjustReplicas(3, nodes))
		assert.ElementsMatch(t, []string{"N1", "N2", "N3"}, shard.BelongsToNodes)
	})

	t.Run("3->3", func(t *testing.T) {
		nodes := fakeNodes{nodes: []string{"N1", "N2", "N3", "N4", "N5"}}
		shard := Physical{BelongsToNodes: []string{"N1", "N2", "N3"}}
		require.Nil(t, shard.AdjustReplicas(3, nodes))
		assert.ElementsMatch(t, []string{"N1", "N2", "N3"}, shard.BelongsToNodes)
	})

	t.Run("3->2", func(t *testing.T) {
		nodes := fakeNodes{nodes: []string{"N1", "N2", "N3"}}
		shard := Physical{BelongsToNodes: []string{"N1", "N2", "N3"}}
		require.Nil(t, shard.AdjustReplicas(2, nodes))
		assert.ElementsMatch(t, []string{"N1", "N2"}, shard.BelongsToNodes)
	})

	t.Run("Min", func(t *testing.T) {
		nodes := fakeNodes{nodes: []string{"N1", "N2", "N3"}}
		shard := Physical{BelongsToNodes: []string{"N1", "N2", "N3"}}
		require.NotNil(t, shard.AdjustReplicas(-1, nodes))
	})
	t.Run("Max", func(t *testing.T) {
		nodes := fakeNodes{nodes: []string{"N1", "N2", "N3"}}
		shard := Physical{BelongsToNodes: []string{"N1", "N2", "N3"}}
		require.NotNil(t, shard.AdjustReplicas(4, nodes))
	})
	t.Run("Bug", func(t *testing.T) {
		names := []string{"N1", "N2", "N3", "N4"}
		nodes := fakeNodes{nodes: names} // bug
		shard := Physical{BelongsToNodes: []string{"N1", "N1", "N1", "N2", "N2"}}
		require.Nil(t, shard.AdjustReplicas(4, nodes)) // correct
		require.ElementsMatch(t, names, shard.BelongsToNodes)
	})
}

func TestStateDeepCopy(t *testing.T) {
	original := State{
		IndexID: "original",
		Config: Config{
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
				BelongsToNodes: []string{"orignal"},
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
	}

	control := State{
		IndexID: "original",
		Config: Config{
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
				BelongsToNodes: []string{"orignal"},
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
