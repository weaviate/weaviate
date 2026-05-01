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

package hnsw

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// BenchmarkReassignNeighborsOf benchmarks the reassignNeighborsOf function
// which is the core of tombstone cleanup in HNSW.
//
// Run with: go test -bench=BenchmarkReassignNeighborsOf -benchmem -benchtime=3s
//
// For CPU profiling:
//
//	go test -bench=BenchmarkReassignNeighborsOf_Profile -cpuprofile=cpu.prof -benchtime=10s
//	go tool pprof -http=:8080 cpu.prof
//
// For memory profiling:
//
//	go test -bench=BenchmarkReassignNeighborsOf_Profile -memprofile=mem.prof -benchtime=10s
//	go tool pprof -http=:8080 mem.prof
//
// For allocation profiling:
//
//	go test -bench=BenchmarkReassignNeighborsOf_Profile -memprofile=mem.prof -memprofilerate=1 -benchtime=10s
//	go tool pprof -alloc_space -http=:8080 mem.prof
func BenchmarkReassignNeighborsOf(b *testing.B) {
	scenarios := []struct {
		name             string
		vectorCount      int
		dimensions       int
		tombstonePercent float64
	}{
		{"Small_1K_5pct", 1000, 128, 0.05},
		{"Small_1K_10pct", 1000, 128, 0.10},
		{"Small_1K_20pct", 1000, 128, 0.20},
		{"Medium_10K_5pct", 10000, 128, 0.05},
		{"Medium_10K_10pct", 10000, 128, 0.10},
		{"Medium_10K_20pct", 10000, 128, 0.20},
		{"Large_100K_5pct", 100000, 128, 0.05},
		{"Large_100K_10pct", 100000, 128, 0.10},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			benchmarkReassignNeighborsOfScenario(b, scenario.vectorCount, scenario.dimensions, scenario.tombstonePercent)
		})
	}
}

// BenchmarkReassignNeighborsOf_Sizes benchmarks different graph sizes
func BenchmarkReassignNeighborsOf_Sizes(b *testing.B) {
	sizes := []int{1000, 5000, 10000, 50000, 100000}
	tombstonePercent := 0.10 // 10% tombstones

	for _, size := range sizes {
		b.Run(fmt.Sprintf("%dK_vectors", size/1000), func(b *testing.B) {
			benchmarkReassignNeighborsOfScenario(b, size, 128, tombstonePercent)
		})
	}
}

// BenchmarkReassignNeighborsOf_TombstoneRatio benchmarks different tombstone ratios
func BenchmarkReassignNeighborsOf_TombstoneRatio(b *testing.B) {
	vectorCount := 10000
	dimensions := 128
	tombstonePercents := []float64{0.01, 0.10, 0.30, 0.50}

	for _, pct := range tombstonePercents {
		b.Run(fmt.Sprintf("%.0fpct_tombstones", pct*100), func(b *testing.B) {
			benchmarkReassignNeighborsOfScenario(b, vectorCount, dimensions, pct)
		})
	}
}

// BenchmarkReassignNeighborsOf_Dimensions benchmarks different vector dimensions
func BenchmarkReassignNeighborsOf_Dimensions(b *testing.B) {
	vectorCount := 10000
	tombstonePercent := 0.10
	dimensions := []int{64, 128, 256, 512, 1024, 1536}

	for _, dim := range dimensions {
		b.Run(fmt.Sprintf("%d_dimensions", dim), func(b *testing.B) {
			benchmarkReassignNeighborsOfScenario(b, vectorCount, dim, tombstonePercent)
		})
	}
}

// BenchmarkReassignNeighborsOf_Profile is optimized for profiling
// Run this to generate clean CPU and memory profiles:
//
// CPU Profile:
//
//	go test -bench=BenchmarkReassignNeighborsOf_Profile$ -cpuprofile=cpu.prof -benchtime=10s
//	go tool pprof -http=:8080 cpu.prof
//
// Memory Profile:
//
//	go test -bench=BenchmarkReassignNeighborsOf_Profile$ -memprofile=mem.prof -benchtime=10s
//	go tool pprof -http=:8080 mem.prof
//
// Allocation Profile:
//
//	go test -bench=BenchmarkReassignNeighborsOf_Profile$ -memprofile=alloc.prof -memprofilerate=1 -benchtime=10s
//	go tool pprof -alloc_space -http=:8080 alloc.prof
//
// Trace (for visualizing goroutine scheduling):
//
//	go test -bench=BenchmarkReassignNeighborsOf_Profile$ -trace=trace.out -benchtime=5s
//	go tool trace trace.out
func BenchmarkReassignNeighborsOf_Profile(b *testing.B) {
	// Use a realistic scenario for profiling: 10K vectors, 10% tombstones
	benchmarkReassignNeighborsOfScenario(b, 10000, 128, 0.10)
}

func benchmarkReassignNeighborsOfScenario(b *testing.B, vectorCount int, dimensions int, tombstonePercent float64) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(42)) // deterministic for reproducibility

	// Generate test vectors
	vectors := generateRandomVectors(rng, vectorCount, dimensions)

	// Create HNSW index
	store := testinghelpers.NewDummyStore(b)
	defer store.Shutdown(context.Background())

	index, err := New(Config{
		RootPath:              b.TempDir(),
		ID:                    "benchmark-delete",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) >= len(vectors) {
				return nil, fmt.Errorf("id %d out of range", id)
			}
			return vectors[int(id)], nil
		},
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
	}, ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        128,
		VectorCacheMaxObjects: vectorCount + 1000,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(b, err)
	defer index.Drop(context.Background(), false)

	// Add all vectors to index
	for i := 0; i < vectorCount; i++ {
		err := index.Add(ctx, uint64(i), vectors[i])
		require.NoError(b, err)
	}

	// Create tombstones for a percentage of nodes
	tombstoneCount := int(float64(vectorCount) * tombstonePercent)
	tombstoneIDs := make([]uint64, 0, tombstoneCount)

	// Select tombstone candidates (prefer middle of range to ensure graph structure)
	for i := 0; i < tombstoneCount; i++ {
		// Spread tombstones throughout the graph
		id := uint64(rng.Intn(vectorCount))
		tombstoneIDs = append(tombstoneIDs, id)
	}

	// Add tombstones to the index
	for _, id := range tombstoneIDs {
		err := index.Delete(id)
		require.NoError(b, err)
	}

	// Create deleteList from tombstones
	deleteList := helpers.NewAllowList()
	for _, id := range tombstoneIDs {
		deleteList.Insert(id)
	}

	// Prepare breakCleanUpTombstonedNodes function (never break)
	breakFunc := func() bool {
		return false
	}

	// Report metrics
	b.ReportMetric(float64(vectorCount), "vectors")
	b.ReportMetric(float64(tombstoneCount), "tombstones")
	b.ReportMetric(tombstonePercent*100, "tombstone_%")
	b.ReportMetric(float64(dimensions), "dimensions")

	// Reset timer and benchmark
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Benchmark the core function
		ok, err := index.reassignNeighborsOf(ctx, deleteList, breakFunc)
		if err != nil {
			b.Fatalf("reassignNeighborsOf failed: %v", err)
		}
		if !ok {
			b.Fatal("reassignNeighborsOf returned not ok")
		}
	}
}

// BenchmarkReassignNeighbor benchmarks individual neighbor reassignment
func BenchmarkReassignNeighbor(b *testing.B) {
	scenarios := []struct {
		name             string
		vectorCount      int
		tombstonePercent float64
	}{
		{"1K_5pct", 1000, 0.05},
		{"10K_10pct", 10000, 0.10},
		{"100K_10pct", 100000, 0.10},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			benchmarkReassignNeighborScenario(b, scenario.vectorCount, scenario.tombstonePercent)
		})
	}
}

// BenchmarkReassignNeighbor_Profile is optimized for profiling the inner reassignNeighbor function
// Run with: go test -bench=BenchmarkReassignNeighbor_Profile$ -cpuprofile=cpu.prof -benchtime=10s
func BenchmarkReassignNeighbor_Profile(b *testing.B) {
	benchmarkReassignNeighborScenario(b, 10000, 0.10)
}

func benchmarkReassignNeighborScenario(b *testing.B, vectorCount int, tombstonePercent float64) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))
	dimensions := 128

	// Generate test vectors
	vectors := generateRandomVectors(rng, vectorCount, dimensions)

	// Create HNSW index
	store := testinghelpers.NewDummyStore(b)
	defer store.Shutdown(context.Background())

	index, err := New(Config{
		RootPath:              b.TempDir(),
		ID:                    "benchmark-delete",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) >= len(vectors) {
				return nil, fmt.Errorf("id %d out of range", id)
			}
			return vectors[int(id)], nil
		},
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
	}, ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        128,
		VectorCacheMaxObjects: vectorCount + 1000,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(b, err)
	defer index.Drop(context.Background(), false)

	// Add all vectors
	for i := 0; i < vectorCount; i++ {
		err := index.Add(ctx, uint64(i), vectors[i])
		require.NoError(b, err)
	}

	// Create tombstones
	tombstoneCount := int(float64(vectorCount) * tombstonePercent)
	tombstoneIDs := make([]uint64, 0, tombstoneCount)
	for i := 0; i < tombstoneCount; i++ {
		id := uint64(rng.Intn(vectorCount))
		tombstoneIDs = append(tombstoneIDs, id)
	}

	for _, id := range tombstoneIDs {
		err := index.Delete(id)
		require.NoError(b, err)
	}

	deleteList := helpers.NewAllowList()
	for _, id := range tombstoneIDs {
		deleteList.Insert(id)
	}

	breakFunc := func() bool { return false }

	// Pick a valid neighbor node to reassign (not deleted, not entrypoint)
	var neighborID uint64
	for i := 0; i < vectorCount; i++ {
		if !deleteList.Contains(uint64(i)) && uint64(i) != index.entryPointID {
			neighborID = uint64(i)
			break
		}
	}

	// Use sync.Map for processedIDs tracking
	processedIDs := &sync.Map{}

	b.ReportMetric(float64(vectorCount), "vectors")
	b.ReportMetric(float64(tombstoneCount), "tombstones")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Benchmark single neighbor reassignment
		_, err := index.reassignNeighbor(ctx, neighborID, deleteList, breakFunc, processedIDs)
		if err != nil {
			b.Fatalf("reassignNeighbor failed: %v", err)
		}
	}
}

// BenchmarkConnectionsPointTo benchmarks the helper function
func BenchmarkConnectionsPointTo(b *testing.B) {
	scenarios := []struct {
		name        string
		vectorCount int
		deleteCount int
		hasMatches  bool
	}{
		{"Small_NoMatches", 1000, 50, false},
		{"Small_WithMatches", 1000, 50, true},
		{"Medium_NoMatches", 10000, 100, false},
		{"Medium_WithMatches", 10000, 100, true},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			benchmarkConnectionsPointToScenario(b, scenario.vectorCount, scenario.deleteCount, scenario.hasMatches)
		})
	}
}

func benchmarkConnectionsPointToScenario(b *testing.B, vectorCount int, deleteCount int, hasMatches bool) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))
	dimensions := 128

	vectors := generateRandomVectors(rng, vectorCount, dimensions)

	store := testinghelpers.NewDummyStore(b)
	defer store.Shutdown(context.Background())

	index, err := New(Config{
		RootPath:              b.TempDir(),
		ID:                    "benchmark-delete",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
	}, ent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        128,
		VectorCacheMaxObjects: vectorCount + 1000,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(b, err)
	defer index.Drop(context.Background(), false)

	// Add vectors
	for i := 0; i < vectorCount; i++ {
		err := index.Add(ctx, uint64(i), vectors[i])
		require.NoError(b, err)
	}

	// Create deleteList
	deleteList := helpers.NewAllowList()
	if hasMatches {
		// Add IDs that will be in the node's connections
		for i := 0; i < deleteCount; i++ {
			deleteList.Insert(uint64(i))
		}
	} else {
		// Add IDs that won't be in connections (very high IDs)
		for i := 0; i < deleteCount; i++ {
			deleteList.Insert(uint64(vectorCount + i + 1000))
		}
	}

	// Get a node with connections
	var node *vertex
	for _, n := range index.nodes {
		if n != nil && n.connections.Layers() > 0 {
			node = n
			break
		}
	}

	if node == nil {
		b.Fatal("no node with connections found")
	}

	b.ReportMetric(float64(node.connections.Layers()), "layers")
	totalConns := 0
	for layer := uint8(0); layer < node.connections.Layers(); layer++ {
		totalConns += node.connections.LenAtLayer(layer)
	}
	b.ReportMetric(float64(totalConns), "connections")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = connectionsPointTo(node.connections, deleteList)
	}
}

// BenchmarkCleanUpTombstonedNodes benchmarks the full cleanup cycle
func BenchmarkCleanUpTombstonedNodes(b *testing.B) {
	scenarios := []struct {
		name             string
		vectorCount      int
		tombstonePercent float64
	}{
		{"1K_5pct", 1000, 0.05},
		{"1K_10pct", 1000, 0.10},
		{"10K_5pct", 10000, 0.05},
		{"10K_10pct", 10000, 0.10},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			benchmarkCleanUpTombstonedNodesScenario(b, scenario.vectorCount, scenario.tombstonePercent)
		})
	}
}

// BenchmarkCleanUpTombstonedNodes_Profile is optimized for profiling the full cleanup cycle
// Run with: go test -bench=BenchmarkCleanUpTombstonedNodes_Profile$ -cpuprofile=cpu.prof -benchtime=10s
func BenchmarkCleanUpTombstonedNodes_Profile(b *testing.B) {
	benchmarkCleanUpTombstonedNodesScenario(b, 10000, 0.10)
}

func benchmarkCleanUpTombstonedNodesScenario(b *testing.B, vectorCount int, tombstonePercent float64) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))
	dimensions := 128

	vectors := generateRandomVectors(rng, vectorCount, dimensions)

	b.ReportMetric(float64(vectorCount), "vectors")
	b.ReportMetric(tombstonePercent*100, "tombstone_%")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Setup for each iteration
		store := testinghelpers.NewDummyStore(b)

		index, err := New(Config{
			RootPath:              b.TempDir(),
			ID:                    "benchmark-delete",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewCosineDistanceProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				if int(id) >= len(vectors) {
					return nil, fmt.Errorf("id %d out of range", id)
				}
				return vectors[int(id)], nil
			},
			GetViewThunk:                 GetViewThunk,
			TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		}, ent.UserConfig{
			MaxConnections:        30,
			EFConstruction:        128,
			VectorCacheMaxObjects: vectorCount + 1000,
		}, cyclemanager.NewCallbackGroupNoop(), store)
		require.NoError(b, err)

		// Add vectors
		for j := 0; j < vectorCount; j++ {
			err := index.Add(ctx, uint64(j), vectors[j])
			require.NoError(b, err)
		}

		// Create tombstones
		tombstoneCount := int(float64(vectorCount) * tombstonePercent)
		for j := 0; j < tombstoneCount; j++ {
			id := uint64(rng.Intn(vectorCount))
			err := index.Delete(id)
			require.NoError(b, err)
		}

		b.StartTimer()

		// Benchmark the full cleanup
		err = index.CleanUpTombstonedNodes(neverStop)
		if err != nil {
			b.Fatalf("CleanUpTombstonedNodes failed: %v", err)
		}

		b.StopTimer()
		store.Shutdown(context.Background())
		index.Drop(context.Background(), false)
	}
}

// Helper functions

func generateRandomVectors(rng *rand.Rand, count, dimensions int) [][]float32 {
	vectors := make([][]float32, count)
	for i := 0; i < count; i++ {
		vec := make([]float32, dimensions)
		for j := 0; j < dimensions; j++ {
			vec[j] = rng.Float32()
		}
		// Normalize
		var norm float32
		for _, v := range vec {
			norm += v * v
		}
		norm = float32(1.0 / (float64(norm) + 1e-10))
		for j := range vec {
			vec[j] *= norm
		}
		vectors[i] = vec
	}
	return vectors
}
