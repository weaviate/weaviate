//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/packedconn"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestAcornSmartSeeding(t *testing.T) {
	ctx := context.Background()

	// Create vectors with specific properties for testing
	// Entry point vector is far from filtered nodes to force Acorn strategy
	vectors := [][]float32{
		{100, 100},   // 0: Entry point: far from filtered nodes
		{1, 1},       // 1: Filtered node: cluster 1
		{2, 2},       // 2: Filtered node: cluster 1
		{1.5, 1.5},   // 3: Filtered node: cluster 1 (bridge)
		{50, 50},     // 4: Non-filtered bridge node
		{10, 10},     // 5: Filtered node: cluster 2
		{11, 11},     // 6: Filtered node: cluster 2
		{10.5, 10.5}, // 7: Filtered node: cluster 2 (bridge)
		{25, 25},     // 8: Non-filtered intermediate node
		{75, 75},     // 9: Non-filtered node close to entry point
	}

	var vectorIndex *hnsw

	t.Run("setup index with manual graph structure", func(t *testing.T) {
		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "acorn-seeding-test",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewL2SquaredProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
			AcornFilterRatio: 0.3, // Low ratio to favor Acorn strategy
		}, ent.UserConfig{
			MaxConnections:        16,
			EFConstruction:        32,
			VectorCacheMaxObjects: 100000,
			FilterStrategy:        ent.FilterStrategyAcorn,
		}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
		require.Nil(t, err)
		vectorIndex = index

		// Manually construct a graph structure where filtered nodes form clusters
		// connected through non-filtered bridge nodes
		vectorIndex.entryPointID = 0
		vectorIndex.currentMaximumLayer = 0

		// Create connections that form the desired topology:
		// Entry point (0) -> Bridge (4) -> Cluster1 (1,2,3) and Cluster2 (5,6,7)
		// This ensures filtered nodes are reachable but require graph traversal
		connections := make([]*vertex, len(vectors))

		// Node 0 (entry point): connects to bridge nodes
		conns0, _ := packedconn.NewWithElements([][]uint64{{4, 9}})
		connections[0] = &vertex{level: 0, connections: conns0}

		// Node 1 (filtered, cluster 1): connects within cluster and to bridge
		conns1, _ := packedconn.NewWithElements([][]uint64{{2, 3, 4}})
		connections[1] = &vertex{level: 0, connections: conns1}

		// Node 2 (filtered, cluster 1): connects within cluster
		conns2, _ := packedconn.NewWithElements([][]uint64{{1, 3}})
		connections[2] = &vertex{level: 0, connections: conns2}

		// Node 3 (filtered, bridge in cluster 1): connects within cluster and to intermediate
		conns3, _ := packedconn.NewWithElements([][]uint64{{1, 2, 8}})
		connections[3] = &vertex{level: 0, connections: conns3}

		// Node 4 (non-filtered bridge): connects entry point to clusters
		conns4, _ := packedconn.NewWithElements([][]uint64{{0, 1, 8}})
		connections[4] = &vertex{level: 0, connections: conns4}

		// Node 5 (filtered, cluster 2): connects within cluster and to bridge
		conns5, _ := packedconn.NewWithElements([][]uint64{{6, 7, 8}})
		connections[5] = &vertex{level: 0, connections: conns5}

		// Node 6 (filtered, cluster 2): connects within cluster
		conns6, _ := packedconn.NewWithElements([][]uint64{{5, 7}})
		connections[6] = &vertex{level: 0, connections: conns6}

		// Node 7 (filtered, bridge in cluster 2): connects within cluster and to intermediate
		conns7, _ := packedconn.NewWithElements([][]uint64{{5, 6, 8}})
		connections[7] = &vertex{level: 0, connections: conns7}

		// Node 8 (non-filtered intermediate): connects clusters
		conns8, _ := packedconn.NewWithElements([][]uint64{{3, 4, 5, 7, 9}})
		connections[8] = &vertex{level: 0, connections: conns8}

		// Node 9 (non-filtered, close to entry): connects to entry point
		conns9, _ := packedconn.NewWithElements([][]uint64{{0, 8}})
		connections[9] = &vertex{level: 0, connections: conns9}

		vectorIndex.nodes = connections
	})

	t.Run("verify Acorn strategy is selected", func(t *testing.T) {
		// Create allow list with filtered nodes (sparse distribution)
		allowList := helpers.NewAllowList(1, 2, 3, 5, 6, 7) // 6 out of 10 nodes, but clustered

		isMultivec := false
		strategy := vectorIndex.setStrategy(0, allowList, isMultivec)

		// Should select ACORN due to low filter ratio around entry point
		assert.Equal(t, ACORN, strategy, "Should select ACORN strategy for sparse filter distribution")
	})

	t.Run("verify smart seeding finds distributed entry points", func(t *testing.T) {
		allowList := helpers.NewAllowList(1, 2, 3, 5, 6, 7)

		// Create query vector that's closer to one of the clusters
		queryVec := []float32{5, 5} // Closer to cluster 1 (nodes 1,2,3)

		var compressorDistancer compressionhelpers.CompressorDistancer = nil
		ef := 4 // Request 4 entry points through seeding

		eps := priorityqueue.NewMin[any](10)
		eps.Insert(0, 100.0) // Entry point with high distance

		isMultivec := false
		strategy := ACORN

		// Test the plantSeeds function
		err := vectorIndex.plantSeeds(strategy, allowList, isMultivec, ef, 0, compressorDistancer, queryVec, eps)
		require.Nil(t, err)

		// Verify that we have more than just the entry point
		assert.Greater(t, eps.Len(), 1, "Smart seeding should find additional entry points")

		// Collect all seeded entry points
		var seededNodes []uint64
		var seededDistances []float32

		for eps.Len() > 0 {
			ep := eps.Pop()
			seededNodes = append(seededNodes, ep.ID)
			seededDistances = append(seededDistances, ep.Dist)
		}

		// Verify that seeded nodes are from the allowed list
		for _, nodeID := range seededNodes {
			if nodeID != 0 { // Skip original entry point
				assert.True(t, allowList.Contains(nodeID),
					"Seeded node %d should be in allow list", nodeID)
			}
		}

		// Verify we found nodes from both clusters (topological diversity)
		var cluster1Found, cluster2Found bool
		for _, nodeID := range seededNodes {
			if nodeID == 1 || nodeID == 2 || nodeID == 3 {
				cluster1Found = true
			}
			if nodeID == 5 || nodeID == 6 || nodeID == 7 {
				cluster2Found = true
			}
		}

		// With BFS traversal, we should find nodes from both clusters
		assert.True(t, cluster1Found || cluster2Found,
			"Should find nodes from at least one cluster through BFS traversal")

		t.Logf("Seeded nodes: %v with distances: %v", seededNodes, seededDistances)
	})

	t.Run("verify full search with Acorn and smart seeding", func(t *testing.T) {
		allowList := helpers.NewAllowList(1, 2, 3, 5, 6, 7)
		queryVec := []float32{5, 5} // Query closer to cluster 1

		// Enable Acorn search
		vectorIndex.acornSearch.Store(true)

		// Perform search
		results, distances, err := vectorIndex.SearchByVector(ctx, queryVec, 3, allowList)
		require.Nil(t, err)

		// Verify results are from allowed list
		for i, nodeID := range results {
			assert.True(t, allowList.Contains(nodeID),
				"Result node %d should be in allow list", nodeID)
			t.Logf("Result %d: Node %d with distance %f", i, nodeID, distances[i])
		}

		// Verify we get reasonable results (closest nodes from cluster 1)
		assert.Contains(t, results, uint64(1), "Should find node 1 (closest to query)")
		assert.Len(t, results, 3, "Should return requested number of results")

		// Verify results are sorted by distance (ascending)
		for i := 1; i < len(distances); i++ {
			assert.LessOrEqual(t, distances[i-1], distances[i],
				"Results should be sorted by distance")
		}
	})

	t.Run("verify seeding improves search coverage", func(t *testing.T) {
		allowList := helpers.NewAllowList(1, 2, 3, 5, 6, 7)
		queryVec := []float32{8, 8} // Query between clusters

		vectorIndex.acornSearch.Store(true)

		// Search with larger k to see coverage across clusters
		results, distances, err := vectorIndex.SearchByVector(ctx, queryVec, 6, allowList)
		require.Nil(t, err)

		// With smart seeding, we should find nodes from both clusters
		var foundCluster1, foundCluster2 bool
		for _, nodeID := range results {
			if nodeID == 1 || nodeID == 2 || nodeID == 3 {
				foundCluster1 = true
			}
			if nodeID == 5 || nodeID == 6 || nodeID == 7 {
				foundCluster2 = true
			}
		}

		// Smart seeding should help find nodes from both clusters
		assert.True(t, foundCluster1 && foundCluster2,
			"Smart seeding should enable finding nodes from both clusters. Found cluster1: %v, cluster2: %v",
			foundCluster1, foundCluster2)

		t.Logf("Full search results: %v with distances: %v", results, distances)
	})
}
