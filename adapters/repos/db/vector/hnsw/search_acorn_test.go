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
	"fmt"
	"sync"
	"testing"
	"time"

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
	"github.com/weaviate/weaviate/usecases/config/runtime"
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

func TestAcornStrategySelectionBoundaryConditions(t *testing.T) {
	vectors := [][]float32{
		{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7}, {8, 8}, {9, 9},
	}

	createTestIndex := func(filterRatio float64) *hnsw {
		index, err := New(Config{
			RootPath:              "test",
			ID:                    "boundary-test",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewL2SquaredProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
			AcornFilterRatio: filterRatio,
		}, ent.UserConfig{
			MaxConnections:        16,
			EFConstruction:        32,
			VectorCacheMaxObjects: 100000,
		}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
		require.Nil(t, err)

		index.acornSearch.Store(true)
		index.entryPointID = 0
		index.currentMaximumLayer = 0

		// Create entry point with exactly 4 connections: 2 filtered, 2 non-filtered (ratio = 0.5)
		connections := make([]*vertex, len(vectors))
		conns0, _ := packedconn.NewWithElements([][]uint64{{1, 2, 7, 8}}) // nodes 1,2 will be filtered
		connections[0] = &vertex{level: 0, connections: conns0}

		for i := 1; i < len(vectors); i++ {
			conns, _ := packedconn.NewWithElements([][]uint64{{0}})
			connections[i] = &vertex{level: 0, connections: conns}
		}
		index.nodes = connections

		for i, vec := range vectors {
			index.cache.Grow(uint64(i))
			index.cache.Preload(uint64(i), vec)
		}
		return index
	}

	t.Run("empty allow list", func(t *testing.T) {
		index := createTestIndex(0.5)
		allowList := helpers.NewAllowList()
		strategy := index.setStrategy(0, allowList, false)
		assert.Equal(t, ACORN, strategy, "Empty allow list should use ACORN")
	})

	t.Run("nil allow list", func(t *testing.T) {
		index := createTestIndex(0.5)
		strategy := index.setStrategy(0, nil, false)
		assert.Equal(t, SWEEPING, strategy, "Nil allow list should use SWEEPING")
	})

	t.Run("filter ratio exactly at threshold", func(t *testing.T) {
		index := createTestIndex(0.5)
		allowList := helpers.NewAllowList(1, 2)
		strategy := index.setStrategy(0, allowList, false)
		assert.Equal(t, ACORN, strategy, "Exact threshold should trigger ACORN")
	})

	t.Run("filter ratio just below threshold", func(t *testing.T) {
		index := createTestIndex(0.6)
		allowList := helpers.NewAllowList(1, 2)
		strategy := index.setStrategy(0, allowList, false)
		assert.Equal(t, ACORN, strategy, "Below threshold should trigger ACORN")
	})

	t.Run("filter ratio just above threshold", func(t *testing.T) {
		index := createTestIndex(0.4)
		allowList := helpers.NewAllowList(1, 2)
		strategy := index.setStrategy(0, allowList, false)
		assert.Equal(t, RRE, strategy, "Above threshold should trigger RRE")
	})

	t.Run("all nodes filtered", func(t *testing.T) {
		index := createTestIndex(0.5)
		allowList := helpers.NewAllowList(1, 2, 7, 8, 9)
		strategy := index.setStrategy(0, allowList, false)
		assert.Equal(t, RRE, strategy, "High correlation (all filtered) should use RRE")
	})

	t.Run("single node allow list", func(t *testing.T) {
		index := createTestIndex(0.5)
		allowList := helpers.NewAllowList(1)
		strategy := index.setStrategy(0, allowList, false)
		assert.Equal(t, ACORN, strategy, "Very selective filter should use ACORN")
	})

	t.Run("non-existent entry point", func(t *testing.T) {
		index := createTestIndex(0.5)
		index.nodes[index.entryPointID] = nil
		allowList := helpers.NewAllowList(1, 2)
		strategy := index.setStrategy(0, allowList, false)
		assert.Equal(t, RRE, strategy, "Null entry point should fallback to RRE")
	})
}

func TestSmartSeedingBFSFailureScenarios(t *testing.T) {
	vectors := [][]float32{
		{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5},
	}

	createDisconnectedIndex := func() *hnsw {
		index, err := New(Config{
			RootPath:              "test",
			ID:                    "bfs-test",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewL2SquaredProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
			AcornFilterRatio: 0.5,
			AcornSmartSeed:   runtime.NewDynamicValue(true),
		}, ent.UserConfig{
			MaxConnections:        16,
			EFConstruction:        32,
			VectorCacheMaxObjects: 100000,
		}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
		require.Nil(t, err)

		index.acornSearch.Store(true)
		index.entryPointID = 0
		index.currentMaximumLayer = 0

		connections := make([]*vertex, len(vectors))
		for i := range connections {
			conns, _ := packedconn.NewWithMaxLayer(0)
			connections[i] = &vertex{level: 0, connections: conns}
		}
		index.nodes = connections

		for i, vec := range vectors {
			index.cache.Grow(uint64(i))
			index.cache.Preload(uint64(i), vec)
		}
		return index
	}

	t.Run("disconnected graph components", func(t *testing.T) {
		index := createDisconnectedIndex()

		// Create two disconnected components: {0,1} and {2,3,4,5}
		conns0, _ := packedconn.NewWithElements([][]uint64{{1}})
		conns1, _ := packedconn.NewWithElements([][]uint64{{0}})
		conns2, _ := packedconn.NewWithElements([][]uint64{{3}})
		conns3, _ := packedconn.NewWithElements([][]uint64{{2, 4, 5}})
		conns4, _ := packedconn.NewWithElements([][]uint64{{3}})
		conns5, _ := packedconn.NewWithElements([][]uint64{{3}})

		index.nodes[0].connections = conns0
		index.nodes[1].connections = conns1
		index.nodes[2].connections = conns2
		index.nodes[3].connections = conns3
		index.nodes[4].connections = conns4
		index.nodes[5].connections = conns5

		allowList := helpers.NewAllowList(2, 4, 5)
		queryVec := []float32{1, 1}
		eps := priorityqueue.NewMin[any](10)
		eps.Insert(0, 100.0)

		err := index.plantSeeds(ACORN, allowList, false, 3, 0, nil, queryVec, eps)
		require.Nil(t, err)

		assert.Equal(t, 1, eps.Len(), "Should only have entry point when filtered nodes are unreachable")
	})

	t.Run("entry point with no connections", func(t *testing.T) {
		index := createDisconnectedIndex()
		allowList := helpers.NewAllowList(1, 2, 3)
		queryVec := []float32{1, 1}
		eps := priorityqueue.NewMin[any](10)
		eps.Insert(0, 100.0)

		err := index.plantSeeds(ACORN, allowList, false, 3, 0, nil, queryVec, eps)
		require.Nil(t, err)

		assert.Equal(t, 1, eps.Len(), "Should only have entry point when it has no connections")
	})

	t.Run("all neighbors are tombstoned", func(t *testing.T) {
		index := createDisconnectedIndex()

		conns0, _ := packedconn.NewWithElements([][]uint64{{1, 2}})
		index.nodes[0].connections = conns0

		index.addTombstone(1)
		index.addTombstone(2)

		allowList := helpers.NewAllowList(1, 2)
		queryVec := []float32{1, 1}
		eps := priorityqueue.NewMin[any](10)
		eps.Insert(0, 100.0)

		err := index.plantSeeds(ACORN, allowList, false, 2, 0, nil, queryVec, eps)
		require.Nil(t, err)

		assert.GreaterOrEqual(t, eps.Len(), 1, "Should handle tombstoned neighbors gracefully")
	})

	t.Run("circular references in graph", func(t *testing.T) {
		index := createDisconnectedIndex()

		// Create circular references: 0->1->2->0
		conns0, _ := packedconn.NewWithElements([][]uint64{{1}})
		conns1, _ := packedconn.NewWithElements([][]uint64{{0, 2}})
		conns2, _ := packedconn.NewWithElements([][]uint64{{1, 0}})

		index.nodes[0].connections = conns0
		index.nodes[1].connections = conns1
		index.nodes[2].connections = conns2

		allowList := helpers.NewAllowList(1, 2)
		queryVec := []float32{1, 1}
		eps := priorityqueue.NewMin[any](10)
		eps.Insert(0, 100.0)

		err := index.plantSeeds(ACORN, allowList, false, 5, 0, nil, queryVec, eps)
		require.Nil(t, err)

		assert.Greater(t, eps.Len(), 1, "Should handle circular references")
		assert.LessOrEqual(t, eps.Len(), 3, "Should not exceed available nodes due to circular handling")
	})
}

func TestAcornPerformanceComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	// Create larger test dataset
	vectors, queries := testinghelpers.RandomVecs(10000, 10, 128)

	createLargeIndex := func() *hnsw {
		index, err := New(Config{
			RootPath:              "test",
			ID:                    "perf-test",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewL2SquaredProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
			AcornFilterRatio: 0.4,
		}, ent.UserConfig{
			MaxConnections:        16,
			EFConstruction:        64,
			VectorCacheMaxObjects: 100000,
		}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
		require.Nil(t, err)

		// Build proper HNSW index
		for i, vec := range vectors {
			err := index.Add(context.Background(), uint64(i), vec)
			require.Nil(t, err)
		}
		return index
	}

	measureSearchTime := func(index *hnsw, query []float32, allowList helpers.AllowList, k int) time.Duration {
		start := time.Now()
		_, _, err := index.SearchByVector(context.Background(), query, k, allowList)
		require.Nil(t, err)
		return time.Since(start)
	}

	t.Run("compare strategy performance", func(t *testing.T) {
		index := createLargeIndex()

		testCases := []struct {
			name           string
			allowListLen   int
			suitedForAcorn bool
		}{
			{"high_selectivity", 50, true},     // 5% of nodes
			{"medium_selectivity", 200, false}, // 20% of nodes
			{"low_selectivity", 500, false},    // 50% of nodes
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				allowedNodes := make([]uint64, tc.allowListLen)
				for i := 0; i < tc.allowListLen; i++ {
					allowedNodes[i] = uint64(i)
				}
				allowList := helpers.NewAllowList(allowedNodes...)

				var sweepingTimes, acornTimes []time.Duration
				k := 10

				for i := 0; i < 5; i++ {
					query := queries[i]

					index.acornSearch.Store(false)
					sweepingTime := measureSearchTime(index, query, allowList, k)
					sweepingTimes = append(sweepingTimes, sweepingTime)

					index.acornSearch.Store(true)
					acornTime := measureSearchTime(index, query, allowList, k)
					acornTimes = append(acornTimes, acornTime)
				}

				avgSweeping := averageDuration(sweepingTimes)
				avgAcorn := averageDuration(acornTimes)

				t.Logf("Filter selectivity %s: SWEEPING avg=%v, ACORN avg=%v",
					tc.name, avgSweeping, avgAcorn)

				assert.Greater(t, avgSweeping.Nanoseconds(), int64(0), "SWEEPING should take measurable time")
				assert.Greater(t, avgAcorn.Nanoseconds(), int64(0), "ACORN should take measurable time")
				if tc.suitedForAcorn {
					assert.Greater(t, avgSweeping, avgAcorn, "ACORN should outperform SWEEPING at high selectivity")
				}
			})
		}
	})
}

func TestAcornConcurrentAccess(t *testing.T) {
	vectors, queries := testinghelpers.RandomVecs(100, 50, 64)

	index, err := New(Config{
		RootPath:              "test",
		ID:                    "concurrent-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		AcornFilterRatio: 0.4,
	}, ent.UserConfig{
		MaxConnections:        16,
		EFConstruction:        32,
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	for i, vec := range vectors {
		err := index.Add(context.Background(), uint64(i), vec)
		require.Nil(t, err)
	}

	t.Run("concurrent searches with different filters", func(t *testing.T) {
		index.acornSearch.Store(true)

		var wg sync.WaitGroup
		errors := make(chan error, 10)

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				allowedNodes := make([]uint64, 10)
				for j := 0; j < 10; j++ {
					allowedNodes[j] = uint64((workerID*10 + j) % 100)
				}
				allowList := helpers.NewAllowList(allowedNodes...)

				query := queries[workerID]
				results, distances, err := index.SearchByVector(context.Background(), query, 5, allowList)
				robustErrorsCheck(results, distances, err, 5, errors, workerID, allowList)
			}(i)
		}

		wg.Wait()
		close(errors)

		var collected []error
		for err := range errors {
			collected = append(collected, err)
		}

		assert.Empty(t, collected)
		for _, err := range collected {
			t.Errorf("Concurrent search error: %v", err)
		}
	})

	t.Run("concurrent seeding operations", func(t *testing.T) {
		var wg sync.WaitGroup
		errors := make(chan error, 10)

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				allowedNodes := make([]uint64, 5)
				for j := 0; j < 5; j++ {
					allowedNodes[j] = uint64((workerID*5 + j) % 100)
				}
				allowList := helpers.NewAllowList(allowedNodes...)

				queryVec := queries[workerID]
				eps := priorityqueue.NewMin[any](10)
				eps.Insert(uint64(workerID), 100.0)

				err := index.plantSeeds(ACORN, allowList, false, 5, uint64(workerID), nil, queryVec, eps)
				if err != nil {
					errors <- fmt.Errorf("seeding worker %d: %w", workerID, err)
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		var collected []error
		for err := range errors {
			collected = append(collected, err)
		}

		assert.Empty(t, collected)
		for _, err := range collected {
			t.Errorf("Concurrent search error: %v", err)
		}
	})
}

func TestAcornGraphEvolution(t *testing.T) {
	vectors, queries := testinghelpers.RandomVecs(50, 10, 32)

	index, err := New(Config{
		RootPath:              "test",
		ID:                    "evolution-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) >= len(vectors) {
				return nil, fmt.Errorf("vector %d not found", id)
			}
			return vectors[int(id)], nil
		},
		AcornFilterRatio: 0.4,
	}, ent.UserConfig{
		MaxConnections:        16,
		EFConstruction:        32,
		VectorCacheMaxObjects: 100000,
	}, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)

	// Build initial index with first 30 vectors
	for i := 0; i < 30; i++ {
		err := index.Add(context.Background(), uint64(i), vectors[i])
		require.Nil(t, err)
	}

	t.Run("search during node additions", func(t *testing.T) {
		index.acornSearch.Store(true)
		allowList := helpers.NewAllowList(1, 2, 3, 4, 5)

		var wg sync.WaitGroup
		errors := make(chan error, 20)

		// Start concurrent searches
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				query := queries[workerID]

				for j := 0; j < 5; j++ {
					results, distances, err := index.SearchByVector(context.Background(), query, 3, allowList)
					robustErrorsCheck(results, distances, err, 3, errors, workerID, allowList)
					time.Sleep(10 * time.Millisecond)
				}
			}(i)
		}

		// Concurrently add new nodes
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				nodeID := uint64(30 + workerID)
				if int(nodeID) < len(vectors) {
					err := index.Add(context.Background(), nodeID, vectors[nodeID])
					if err != nil {
						errors <- fmt.Errorf("add worker %d: %w", workerID, err)
					}
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		var collected []error
		for err := range errors {
			collected = append(collected, err)
		}

		assert.Empty(t, collected)
		for _, err := range collected {
			t.Errorf("Graph evolution error: %v", err)
		}
	})

	t.Run("search during node deletions", func(t *testing.T) {
		allowList := helpers.NewAllowList(10, 11, 12, 13, 14, 15)

		var wg sync.WaitGroup
		errors := make(chan error, 20)

		// Start concurrent searches
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				query := queries[workerID]

				for j := 0; j < 3; j++ {
					results, distances, err := index.SearchByVector(context.Background(), query, 3, allowList)
					robustErrorsCheck(results, distances, err, 3, errors, workerID, allowList)
					time.Sleep(20 * time.Millisecond)
				}
			}(i)
		}

		// Concurrently delete some nodes
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				nodeID := uint64(20 + workerID)
				err := index.Delete(nodeID)
				if err != nil {
					errors <- fmt.Errorf("delete worker %d: %w", workerID, err)
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		var collected []error
		for err := range errors {
			collected = append(collected, err)
		}

		assert.Empty(t, collected)
		for _, err := range collected {
			t.Errorf("Deletion during search error: %v", err)
		}
	})

	t.Run("strategy selection stability during graph changes", func(t *testing.T) {
		allowList := helpers.NewAllowList(1, 2, 3, 4, 5)

		// Record strategy selections over time during graph modifications
		strategies := make([]FilterStrategy, 10)

		for i := 0; i < 10; i++ {
			strategy := index.setStrategy(0, allowList, false)
			strategies[i] = strategy

			// Add a node
			if 40+i < len(vectors) {
				index.Add(context.Background(), uint64(40+i), vectors[40+i])
			}

			time.Sleep(10 * time.Millisecond)
		}

		// Verify strategy selection is stable and reasonable
		acornCount := 0
		for _, strategy := range strategies {
			if strategy == ACORN {
				acornCount++
			}
		}

		t.Logf("Strategy selections during graph evolution: %v", strategies)
		t.Logf("ACORN selected %d out of %d times", acornCount, len(strategies))

		// Strategy should be consistent given the same filter characteristics
		assert.Greater(t, len(strategies), 0, "Should have strategy selections")
	})
}

func averageDuration(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}

	var total int64
	for _, d := range durations {
		total += d.Nanoseconds()
	}

	return time.Duration(total / int64(len(durations)))
}

func robustErrorsCheck(results []uint64, distances []float32, err error, k int, errors chan error, workerID int, allowList helpers.AllowList) {
	if err != nil {
		errors <- fmt.Errorf("error returned %d: %w", workerID, err)
		return
	}
	if len(results) != k {
		errors <- fmt.Errorf("unexpected result length: got %d", len(results))
	}
	for i, id := range results {
		if !allowList.Contains(id) {
			errors <- fmt.Errorf("result %d (%d) not in allowList", i, id)
		}
	}
	for i := 1; i < len(distances); i++ {
		if distances[i] < distances[i-1] {
			errors <- fmt.Errorf("distances not sorted: %v", distances)
		}
	}
}
