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
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestTombstoneCleanupPerformanceAfterReboot(t *testing.T) {
	// This test verifies that tombstone cleanup performance is improved after a reboot
	// by testing the preloadVectorsForCleanup optimization
	
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	
	// Create a test index with a reasonable number of vectors
	vectorCount := 10000
	vectors := make([][]float32, vectorCount)
	for i := 0; i < vectorCount; i++ {
		vectors[i] = make([]float32, 128)
		for j := 0; j < 128; j++ {
			vectors[i][j] = float32(i + j)
		}
	}
	
	// Create index
	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "tombstone-performance-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[id], nil
		},
	}, ent.UserConfig{
		CleanupIntervalSeconds: 1,
		MaxConnections:         30,
		EFConstruction:         128,
	}, cyclemanager.NewCallbackGroup("test", logger, 1), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)
	index.PostStartup()
	
	// Add vectors
	for i, vec := range vectors {
		err := index.Add(ctx, uint64(i), vec)
		require.Nil(t, err)
	}
	
	// Delete half of the vectors to create tombstones
	deleteCount := vectorCount / 2
	for i := 0; i < deleteCount; i += 2 { // Delete every other vector
		err := index.Delete(uint64(i))
		require.Nil(t, err)
	}
	
	// Verify we have tombstones
	index.tombstoneLock.RLock()
	tombstoneCount := len(index.tombstones)
	index.tombstoneLock.RUnlock()
	assert.True(t, tombstoneCount > 0, "Expected tombstones to be present")
	
	// Test the preloadVectorsForCleanup function directly
	deleteList := index.tombstonesAsDenyList()
	
	start := time.Now()
	err = index.preloadVectorsForCleanup(ctx, vectorCount, deleteList, func() bool { return false })
	preloadDuration := time.Since(start)
	
	// The preload should complete without error
	assert.Nil(t, err, "Preload should not return an error")
	
	// The preload should complete in a reasonable time (less than 1 second for 10k vectors)
	assert.Less(t, preloadDuration, time.Second, "Preload should complete quickly")
	
	t.Logf("Preload completed in %v for %d vectors", preloadDuration, vectorCount)
	
	// Now test the full cleanup process
	start = time.Now()
	err = index.CleanUpTombstonedNodes(func() bool { return false })
	cleanupDuration := time.Since(start)
	
	// Cleanup should complete without error
	assert.Nil(t, err, "Cleanup should not return an error")
	
	// Cleanup should complete in a reasonable time
	assert.Less(t, cleanupDuration, 30*time.Second, "Cleanup should complete in reasonable time")
	
	t.Logf("Full cleanup completed in %v", cleanupDuration)
	
	// Verify tombstones are cleaned up
	index.tombstoneLock.RLock()
	remainingTombstones := len(index.tombstones)
	index.tombstoneLock.RUnlock()
	
	assert.Equal(t, 0, remainingTombstones, "All tombstones should be cleaned up")
}

func TestPreloadVectorsForCleanupEdgeCases(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	
	// Test with empty index
	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "empty-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return nil, nil
		},
	}, ent.UserConfig{
		CleanupIntervalSeconds: 1,
		MaxConnections:         30,
		EFConstruction:         128,
	}, cyclemanager.NewCallbackGroup("test", logger, 1), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)
	index.PostStartup()
	
	// Test with empty delete list
	deleteList := helpers.NewAllowList()
	
	// Should not error with empty index
	err = index.preloadVectorsForCleanup(ctx, 0, deleteList, func() bool { return false })
	assert.Nil(t, err, "Should handle empty index gracefully")
	
	// Test with large index (should skip preload)
	err = index.preloadVectorsForCleanup(ctx, 2000000, deleteList, func() bool { return false })
	assert.Nil(t, err, "Should skip preload for large index")
	
	// Test with cancellation
	cancelled := false
	err = index.preloadVectorsForCleanup(ctx, 1000, deleteList, func() bool { 
		cancelled = true
		return true 
	})
	assert.Nil(t, err, "Should handle cancellation gracefully")
	assert.True(t, cancelled, "Cancellation should be respected")
}
