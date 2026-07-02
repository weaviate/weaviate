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

// Tests for PR #11956: Compressed index termination bug
// The sync repair loop ping-pongs between dead nodes on compressed indexes
// because distToNode doesn't tombstone on the compressed branch.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// TestCompressedIndex_TerminationBug_AllDeleted tests that the sync repair loop
// terminates on a compressed index when all nodes are deleted.
//
// BUG: On compressed indexes, distToNode (index.go:664-671) returns ErrNotFound
// WITHOUT tombstoning the dead node. The repair loop then ping-pongs between
// dead nodes forever because findNewGlobalEntrypoint only skips tombstoned nodes.
//
// Expected: Loop terminates within O(n) iterations with errNoUsableEntrypoint
// Actual (BUG): Loop spins forever, never terminates
func TestCompressedIndex_TerminationBug_AllDeleted(t *testing.T) {
	ctx := context.Background()
	const dimensions = 8
	const numVectors = 1000 // Enough for k-means training

	// Generate random vectors for k-means training (using testinghelpers pattern)
	vectors, _ := testinghelpers.RandomVecs(numVectors, 1, dimensions)

	// Track which vectors are "deleted"
	var allDeleted atomic.Bool
	var countingEnabled atomic.Bool // Only count iterations after setup is complete
	var iterationCount atomic.Int32
	maxIterations := int32(numVectors * 2) // Hard cap - should terminate way before this

	var mu sync.Mutex

	logger, _ := test.NewNullLogger()
	tempDir := t.TempDir()
	store := testinghelpers.NewDummyStoreFromFolder(tempDir, t)
	defer store.Shutdown(ctx)

	vectorForIDThunk := func(ctx context.Context, id uint64) ([]float32, error) {
		if countingEnabled.Load() {
			count := iterationCount.Add(1)
			if count > maxIterations {
				panic(fmt.Sprintf("LOOP NOT BOUNDED ON COMPRESSED INDEX: exceeded %d iterations (at node %d)",
					maxIterations, id))
			}
		}
		mu.Lock()
		defer mu.Unlock()
		if allDeleted.Load() {
			return nil, storobj.NewErrNotFoundf(id, "deleted")
		}
		if int(id) < len(vectors) {
			return vectors[id], nil
		}
		return nil, storobj.NewErrNotFoundf(id, "not found")
	}

	tempVectorThunk := func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
		mu.Lock()
		defer mu.Unlock()
		if allDeleted.Load() {
			return nil, storobj.NewErrNotFoundf(id, "deleted")
		}
		if int(id) < len(vectors) {
			copy(container.Slice, vectors[id])
			return vectors[id], nil
		}
		return nil, storobj.NewErrNotFoundf(id, "not found")
	}

	uc := ent.UserConfig{
		MaxConnections:        16,
		EFConstruction:        64,
		EF:                    32,
		VectorCacheMaxObjects: 10000,
		PQ: ent.PQConfig{
			Enabled: true,
			Encoder: ent.PQEncoder{
				Type:         ent.PQEncoderTypeKMeans,
				Distribution: ent.PQEncoderDistributionLogNormal,
			},
			TrainingLimit: numVectors,
			Segments:      dimensions / 4, // 2 segments
			Centroids:     5,              // Small for testing
		},
	}

	index, err := New(Config{
		RootPath:                     tempDir,
		ID:                           "compressed-termination-test",
		MakeCommitLoggerThunk:        MakeNoopCommitLogger,
		DistanceProvider:             distancer.NewL2SquaredProvider(),
		VectorForIDThunk:             vectorForIDThunk,
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: tempVectorThunk,
		MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
		AllocChecker:                 memwatch.NewDummyMonitor(),
		Logger:                       logger,
	}, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer index.Drop(ctx, false)

	// Insert vectors
	for i := 0; i < numVectors; i++ {
		err := index.Add(ctx, uint64(i), vectors[i])
		require.NoError(t, err)
	}

	// Compress the index (enables PQ)
	err = index.compress(uc)
	require.NoError(t, err)
	require.True(t, index.compressed.Load(), "index should be compressed")

	// Now delete ALL vectors
	allDeleted.Store(true)
	// Clear the compressed cache for all vectors
	for i := 0; i < numVectors; i++ {
		index.compressor.Delete(ctx, uint64(i))
	}

	// Enable iteration counting NOW (after compression is done)
	iterationCount.Store(0)
	countingEnabled.Store(true)

	// Search with timeout - if bug exists, this will either:
	// 1. Panic with "LOOP NOT BOUNDED" (hit iteration cap)
	// 2. Timeout (infinite loop without hitting cap fast enough)
	done := make(chan struct{})
	go func() {
		query := make([]float32, dimensions)
		for i := range query {
			query[i] = float32(i)
		}
		_, _, _ = index.SearchByVector(ctx, query, 3, nil)
		close(done)
	}()

	select {
	case <-done:
		// Search completed - either terminated correctly or hit our panic cap
		finalCount := iterationCount.Load()
		t.Logf("Search completed with %d iterations (cap was %d)", finalCount, maxIterations)
	case <-time.After(5 * time.Second):
		t.Fatal("TIMEOUT: Search did not terminate - infinite loop in compressed repair path")
	}
}

// TestCompressedIndex_TerminationBug_TwoDeadNodes tests the minimal ping-pong case:
// two dead nodes A and B where repair alternates between them forever.
func TestCompressedIndex_TerminationBug_TwoDeadNodes(t *testing.T) {
	ctx := context.Background()
	const dimensions = 8
	const numVectors = 1000 // Enough for k-means training

	// Generate random vectors for k-means training
	vectors, _ := testinghelpers.RandomVecs(numVectors, 1, dimensions)

	// Only nodes 0 and 1 will be deleted (entrypoint and one other)
	// This is the minimal ping-pong case
	deletedNodes := map[uint64]bool{0: true, 1: true}
	var countingEnabled atomic.Bool
	var iterationCount atomic.Int32
	maxIterations := int32(10) // Small cap - should find live node quickly or loop forever

	var mu sync.Mutex

	logger, _ := test.NewNullLogger()
	tempDir := t.TempDir()
	store := testinghelpers.NewDummyStoreFromFolder(tempDir, t)
	defer store.Shutdown(ctx)

	vectorForIDThunk := func(ctx context.Context, id uint64) ([]float32, error) {
		if countingEnabled.Load() {
			count := iterationCount.Add(1)
			if count > maxIterations {
				panic(fmt.Sprintf("LOOP NOT BOUNDED (ping-pong between dead nodes): exceeded %d iterations (at node %d)",
					maxIterations, id))
			}
		}
		mu.Lock()
		defer mu.Unlock()
		if deletedNodes[id] {
			return nil, storobj.NewErrNotFoundf(id, "deleted")
		}
		if int(id) < len(vectors) {
			return vectors[id], nil
		}
		return nil, storobj.NewErrNotFoundf(id, "not found")
	}

	tempVectorThunk := func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
		mu.Lock()
		defer mu.Unlock()
		if deletedNodes[id] {
			return nil, storobj.NewErrNotFoundf(id, "deleted")
		}
		if int(id) < len(vectors) {
			copy(container.Slice, vectors[id])
			return vectors[id], nil
		}
		return nil, storobj.NewErrNotFoundf(id, "not found")
	}

	uc := ent.UserConfig{
		MaxConnections:        16,
		EFConstruction:        64,
		EF:                    32,
		VectorCacheMaxObjects: 10000,
		PQ: ent.PQConfig{
			Enabled: true,
			Encoder: ent.PQEncoder{
				Type:         ent.PQEncoderTypeKMeans,
				Distribution: ent.PQEncoderDistributionLogNormal,
			},
			TrainingLimit: numVectors,
			Segments:      dimensions / 4,
			Centroids:     5,
		},
	}

	index, err := New(Config{
		RootPath:                     tempDir,
		ID:                           "compressed-pingpong-test",
		MakeCommitLoggerThunk:        MakeNoopCommitLogger,
		DistanceProvider:             distancer.NewL2SquaredProvider(),
		VectorForIDThunk:             vectorForIDThunk,
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: tempVectorThunk,
		MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
		AllocChecker:                 memwatch.NewDummyMonitor(),
		Logger:                       logger,
	}, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer index.Drop(ctx, false)

	// Insert vectors
	for i := 0; i < numVectors; i++ {
		err := index.Add(ctx, uint64(i), vectors[i])
		require.NoError(t, err)
	}

	// Compress
	err = index.compress(uc)
	require.NoError(t, err)
	require.True(t, index.compressed.Load())

	// Delete nodes 0 and 1 from compressor cache
	index.compressor.Delete(ctx, 0)
	index.compressor.Delete(ctx, 1)

	// Enable iteration counting NOW (after compression is done)
	iterationCount.Store(0)
	countingEnabled.Store(true)

	done := make(chan struct{})
	go func() {
		query := make([]float32, dimensions)
		for i := range query {
			query[i] = float32(i)
		}
		results, _, _ := index.SearchByVector(ctx, query, 3, nil)
		t.Logf("Search returned %d results", len(results))
		close(done)
	}()

	select {
	case <-done:
		finalCount := iterationCount.Load()
		t.Logf("Search completed with %d iterations", finalCount)
		// Should terminate finding nodes 2 or 3
	case <-time.After(5 * time.Second):
		t.Fatal("TIMEOUT: Ping-pong infinite loop between dead nodes 0 and 1")
	}
}
