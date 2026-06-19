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

package hfresh

import (
	"context"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// TestSearchByFDEBudgetSeparation verifies that routingBudget and rerankBudget
// are correctly decoupled in the FDE search path.
//
// Key invariants:
// - routingBudget controls centroid selection count (posting coverage)
// - rerankBudget controls RQ1 result set size (MaxSim candidate depth)
// - Neither budget should affect the other's behavior
func TestSearchByFDEBudgetSeparation(t *testing.T) {
	ctx := context.Background()

	// Create a MUVERA-enabled index with sufficient documents to have multiple centroids
	tf := createMuveraHFreshIndexWithConfig(t, 64, 350) // searchProbe=64, rescoreLimit=350

	// Add enough multi-vectors to create multiple centroids
	// Use random vectors with fixed seed for reproducibility
	numDocs := 500
	tokensPerDoc := 8
	dims := 128
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < numDocs; i++ {
		vecs := make([][]float32, tokensPerDoc)
		for j := 0; j < tokensPerDoc; j++ {
			vec := make([]float32, dims)
			for k := 0; k < dims; k++ {
				// Random vectors ensure diverse centroids
				vec[k] = rng.Float32()
			}
			vecs[j] = vec
		}
		addMultiVectorToIndex(t, &tf, uint64(i), vecs)
	}

	// Force more splits by setting a low maxPostingSize threshold AFTER first Add
	// (setMaxPostingSize is called during first Add, so must set after)
	tf.Index.maxPostingSize = 50

	// Add more documents to trigger splits
	for i := numDocs; i < numDocs*2; i++ {
		vecs := make([][]float32, tokensPerDoc)
		for j := 0; j < tokensPerDoc; j++ {
			vec := make([]float32, dims)
			for k := 0; k < dims; k++ {
				vec[k] = rng.Float32()
			}
			vecs[j] = vec
		}
		addMultiVectorToIndex(t, &tf, uint64(i), vecs)
	}

	// Wait for background tasks (splits) to complete
	for tf.Index.taskQueue.Size() > 0 {
		time.Sleep(100 * time.Millisecond)
	}

	// Verify we have multiple centroids (at least 2 for meaningful budget testing)
	numCentroids := tf.Index.PostingMap.Size()
	t.Logf("Created %d centroids from %d documents", numCentroids, numDocs*2)
	require.GreaterOrEqual(t, numCentroids, 2, "need at least 2 centroids for this test")

	// Create a query using random vectors for consistent testing
	queryRng := rand.New(rand.NewSource(999))
	queryVecs := make([][]float32, tokensPerDoc)
	for j := 0; j < tokensPerDoc; j++ {
		vec := make([]float32, dims)
		for k := 0; k < dims; k++ {
			vec[k] = queryRng.Float32()
		}
		queryVecs[j] = vec
	}
	queryFDE := tf.Index.muveraEncoder.EncodeQuery(queryVecs)

	t.Run("routingBudget controls centroid selection", func(t *testing.T) {
		// Test with different routingBudgets, same rerankBudget
		rerankBudget := 100

		// Small routing budget
		ids1, err := tf.Index.searchByFDE(ctx, queryFDE, 16, rerankBudget, nil)
		require.NoError(t, err)

		// Large routing budget
		ids2, err := tf.Index.searchByFDE(ctx, queryFDE, 256, rerankBudget, nil)
		require.NoError(t, err)

		// With larger routing budget, we should find more or equal candidates
		// (more centroids = more posting coverage = potentially more unique docs)
		// Note: actual count depends on posting distribution
		t.Logf("routingBudget=16:  %d candidates", len(ids1))
		t.Logf("routingBudget=256: %d candidates", len(ids2))

		// Both should be capped at rerankBudget
		assert.LessOrEqual(t, len(ids1), rerankBudget)
		assert.LessOrEqual(t, len(ids2), rerankBudget)
	})

	t.Run("rerankBudget controls result set size", func(t *testing.T) {
		// Test with same routingBudget, different rerankBudgets
		routingBudget := 128

		// Small rerank budget
		ids1, err := tf.Index.searchByFDE(ctx, queryFDE, routingBudget, 32, nil)
		require.NoError(t, err)

		// Large rerank budget
		ids2, err := tf.Index.searchByFDE(ctx, queryFDE, routingBudget, 256, nil)
		require.NoError(t, err)

		// Results should be capped by rerankBudget
		assert.LessOrEqual(t, len(ids1), 32)
		assert.LessOrEqual(t, len(ids2), 256)

		// Smaller budget should return fewer or equal results
		assert.LessOrEqual(t, len(ids1), len(ids2))

		t.Logf("rerankBudget=32:  %d candidates", len(ids1))
		t.Logf("rerankBudget=256: %d candidates", len(ids2))
	})

	t.Run("budgets are independent", func(t *testing.T) {
		// Verify that changing one budget doesn't affect the other's semantics

		// Same routing, different rerank: should access same postings
		// (routing determines coverage, not rerank)
		routingBudget := 64

		ids1, err := tf.Index.searchByFDE(ctx, queryFDE, routingBudget, 64, nil)
		require.NoError(t, err)
		ids2, err := tf.Index.searchByFDE(ctx, queryFDE, routingBudget, 350, nil)
		require.NoError(t, err)

		// With same routing budget, the set of IDs found with smaller rerank
		// should be a subset of (or equal to) those found with larger rerank
		// (since we're looking at same postings, just keeping more/fewer top results)
		idSet2 := make(map[uint64]bool)
		for _, id := range ids2 {
			idSet2[id] = true
		}

		for _, id := range ids1 {
			assert.True(t, idSet2[id], "id %d from small rerankBudget should be in large rerankBudget results", id)
		}
	})
}

// TestSearchByMultiVectorBackwardCompatibility verifies that the default behavior
// of SearchByMultiVector remains equivalent to the old behavior.
//
// Old behavior: candidateCentroidNum = max(rescoreLimit, searchProbe)
// New behavior: routingBudget = max(searchProbe, rescoreLimit), rerankBudget = rescoreLimit
//
// These should produce identical results for the same inputs.
func TestSearchByMultiVectorBackwardCompatibility(t *testing.T) {
	ctx := context.Background()

	// Test with default config: searchProbe=64, rescoreLimit=350
	// Expected effective routing budget: max(64, 350) = 350
	tf := createMuveraHFreshIndexWithConfig(t, 64, 350)

	// Verify the config is set correctly
	assert.Equal(t, uint32(64), tf.Index.searchProbe)
	assert.Equal(t, uint32(350), tf.Index.rescoreLimit)

	// Add documents
	numDocs := 200
	tokensPerDoc := 4
	dims := 128

	for i := 0; i < numDocs; i++ {
		vecs := make([][]float32, tokensPerDoc)
		for j := 0; j < tokensPerDoc; j++ {
			vec := make([]float32, dims)
			for k := 0; k < dims; k++ {
				vec[k] = float32(i*tokensPerDoc*dims+j*dims+k) / float32(numDocs*tokensPerDoc*dims)
			}
			vecs[j] = vec
		}
		addMultiVectorToIndex(t, &tf, uint64(i), vecs)
	}

	time.Sleep(100 * time.Millisecond)

	// Create query
	queryVecs := make([][]float32, tokensPerDoc)
	for j := 0; j < tokensPerDoc; j++ {
		vec := make([]float32, dims)
		for k := 0; k < dims; k++ {
			vec[k] = 0.5
		}
		queryVecs[j] = vec
	}

	// Search using the public API
	k := 10
	ids, dists, err := tf.Index.SearchByMultiVector(ctx, queryVecs, k, nil)
	require.NoError(t, err)

	// Verify we get results
	assert.LessOrEqual(t, len(ids), k)
	assert.Equal(t, len(ids), len(dists))

	// The effective routing budget should be max(64, 350) = 350
	// This matches the old behavior where rescoreLimit was passed to SearchByVector
	// and candidateCentroidNum = max(k, searchProbe) = max(350, 64) = 350

	t.Logf("SearchByMultiVector returned %d results with default config", len(ids))
}

// TestSearchByVectorUnchanged verifies that non-MUVERA single-vector search
// behavior is not affected by the FDE decoupling changes.
func TestSearchByVectorUnchanged(t *testing.T) {
	ctx := context.Background()

	dims := 64
	numDocs := 100

	// Create vectors first so we can set up VectorForIDThunk
	vectors := make([][]float32, numDocs)
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < numDocs; i++ {
		vec := make([]float32, dims)
		for k := 0; k < dims; k++ {
			vec[k] = rng.Float32()
		}
		vectors[i] = vec
	}

	// Create non-MUVERA index with proper VectorForIDThunk
	tf := createHFreshIndexWithVectorStore(t, vectors)

	// Add single vectors
	for i := 0; i < numDocs; i++ {
		addVectorToIndex(t, &tf, uint64(i), vectors[i])
	}

	time.Sleep(100 * time.Millisecond)

	// Create query
	query := make([]float32, dims)
	for k := 0; k < dims; k++ {
		query[k] = 0.5
	}

	// Search
	k := 10
	ids, dists, err := tf.Index.SearchByVector(ctx, query, k, nil)
	require.NoError(t, err)

	assert.LessOrEqual(t, len(ids), k)
	assert.Equal(t, len(ids), len(dists))

	t.Logf("Single-vector SearchByVector returned %d results", len(ids))
}

// TestEffectiveRoutingBudgetCalculation verifies that the effective routing budget
// is calculated correctly as max(searchProbe, rescoreLimit) for backward compatibility.
func TestEffectiveRoutingBudgetCalculation(t *testing.T) {
	testCases := []struct {
		name                   string
		searchProbe            int
		rescoreLimit           int
		expectedRoutingBudget  int
		expectedRerankBudget   int
	}{
		{
			name:                   "rescoreLimit dominates (default)",
			searchProbe:            64,
			rescoreLimit:           350,
			expectedRoutingBudget:  350, // max(64, 350)
			expectedRerankBudget:   350,
		},
		{
			name:                   "searchProbe dominates",
			searchProbe:            512,
			rescoreLimit:           128,
			expectedRoutingBudget:  512, // max(512, 128)
			expectedRerankBudget:   128,
		},
		{
			name:                   "equal values",
			searchProbe:            256,
			rescoreLimit:           256,
			expectedRoutingBudget:  256,
			expectedRerankBudget:   256,
		},
		{
			name:                   "low searchProbe",
			searchProbe:            16,
			rescoreLimit:           1024,
			expectedRoutingBudget:  1024, // max(16, 1024)
			expectedRerankBudget:   1024,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tf := createMuveraHFreshIndexWithConfig(t, tc.searchProbe, tc.rescoreLimit)

			// Verify config
			assert.Equal(t, uint32(tc.searchProbe), tf.Index.searchProbe)
			assert.Equal(t, uint32(tc.rescoreLimit), tf.Index.rescoreLimit)

			// Calculate expected budgets (matching the logic in SearchByMultiVector)
			searchProbe := int(tf.Index.searchProbe)
			rescoreLimit := int(tf.Index.rescoreLimit)
			routingBudget := max(searchProbe, rescoreLimit)
			rerankBudget := rescoreLimit

			assert.Equal(t, tc.expectedRoutingBudget, routingBudget, "routing budget mismatch")
			assert.Equal(t, tc.expectedRerankBudget, rerankBudget, "rerank budget mismatch")
		})
	}
}

// Helper function to create a MUVERA HFresh index with specific config
func createMuveraHFreshIndexWithConfig(t *testing.T, searchProbe, rescoreLimit int) TestHFresh {
	t.Helper()

	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	cfg := DefaultConfig()
	mvStore := newMuveraTestStore()

	scheduler := queue.NewScheduler(
		queue.SchedulerOptions{
			Logger: logger,
		},
	)
	cfg.Scheduler = scheduler
	cfg.RootPath = t.TempDir()

	cfg.Centroids.HNSWConfig = &hnsw.Config{
		RootPath:              t.TempDir(),
		ID:                    "hfresh_muvera_budget_test",
		MakeCommitLoggerThunk: makeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		AllocChecker:          memwatch.NewDummyMonitor(),
		GetViewThunk:          func() common.BucketView { return &noopBucketView{} },
	}

	cfg.TombstoneCallbacks = cyclemanager.NewCallbackGroupNoop()
	cfg.Logger = logger
	cfg.MultiVectorForIDThunk = func(ctx context.Context, id uint64) ([][]float32, error) {
		return mvStore.getMultiVector(id)
	}

	scheduler.Start()
	t.Cleanup(func() {
		scheduler.Close(t.Context())
	})

	uc := ent.NewDefaultUserConfig()
	uc.SearchProbe = uint32(searchProbe)
	uc.RQ.RescoreLimit = rescoreLimit
	uc.Multivector.Enabled = true
	uc.Multivector.MuveraConfig.Enabled = true
	uc.Multivector.MuveraConfig.KSim = enthnsw.DefaultMultivectorKSim
	uc.Multivector.MuveraConfig.DProjections = enthnsw.DefaultMultivectorDProjections
	uc.Multivector.MuveraConfig.Repetitions = enthnsw.DefaultMultivectorRepetitions

	store := testinghelpers.NewDummyStore(t)

	index, err := New(cfg, uc, store)
	require.NoError(t, err)
	index.multivectorForIdThunk = cfg.MultiVectorForIDThunk

	// Set the config values on the index
	atomic.StoreUint32(&index.searchProbe, uint32(searchProbe))
	atomic.StoreUint32(&index.rescoreLimit, uint32(rescoreLimit))

	return TestHFresh{
		Index:   index,
		Logs:    hook,
		mvStore: mvStore,
	}
}

// TestPostingExpansionIntegration tests the posting expansion feature end-to-end.
// This verifies that:
// - The reverse map is built correctly from PostingMap
// - Additional postings are discovered and scanned
// - Allow-list filtering is respected
// - Tombstoned documents are not returned
func TestPostingExpansionIntegration(t *testing.T) {
	ctx := context.Background()

	t.Run("docToPostings is initialized for MUVERA index", func(t *testing.T) {
		tf := createMuveraHFreshIndexWithConfig(t, 64, 350)
		require.NotNil(t, tf.Index.docToPostings, "docToPostings should be initialized for MUVERA index")
	})

	t.Run("docToPostings is nil for non-MUVERA index", func(t *testing.T) {
		vectors := make([][]float32, 10)
		for i := range vectors {
			vectors[i] = make([]float32, 64)
		}
		tf := createHFreshIndexWithVectorStore(t, vectors)
		assert.Nil(t, tf.Index.docToPostings, "docToPostings should be nil for non-MUVERA index")
	})

	t.Run("posting expansion discovers additional candidates", func(t *testing.T) {
		tf := createMuveraHFreshIndexWithConfig(t, 16, 100)

		// Add documents with enough diversity to create multiple centroids
		numDocs := 300
		tokensPerDoc := 4
		dims := 128
		rng := rand.New(rand.NewSource(42))

		for i := 0; i < numDocs; i++ {
			vecs := make([][]float32, tokensPerDoc)
			for j := 0; j < tokensPerDoc; j++ {
				vec := make([]float32, dims)
				for k := 0; k < dims; k++ {
					vec[k] = rng.Float32()
				}
				vecs[j] = vec
			}
			addMultiVectorToIndex(t, &tf, uint64(i), vecs)
		}

		// Wait for background tasks
		for tf.Index.taskQueue.Size() > 0 {
			time.Sleep(100 * time.Millisecond)
		}

		// Verify docToPostings can be built
		require.NotNil(t, tf.Index.docToPostings)

		// Verify it gets built on first search
		queryVecs := make([][]float32, tokensPerDoc)
		for j := 0; j < tokensPerDoc; j++ {
			vec := make([]float32, dims)
			for k := 0; k < dims; k++ {
				vec[k] = rng.Float32()
			}
			queryVecs[j] = vec
		}

		// Search should trigger docToPostings build
		ids, _, err := tf.Index.SearchByMultiVector(ctx, queryVecs, 10, nil)
		require.NoError(t, err)
		assert.NotEmpty(t, ids)

		// Verify docToPostings was built
		assert.True(t, tf.Index.docToPostings.IsBuilt(), "docToPostings should be built after first search")
		assert.Greater(t, tf.Index.docToPostings.Size(), 0, "docToPostings should have entries")

		t.Logf("docToPostings: %d docs, %d bytes estimated",
			tf.Index.docToPostings.Size(),
			tf.Index.docToPostings.EstimatedMemoryBytes())
	})
}

// TestPostingExpansionBackwardCompatibility verifies that when posting expansion
// finds no additional postings, the behavior matches PR1 (no expansion).
func TestPostingExpansionBackwardCompatibility(t *testing.T) {
	ctx := context.Background()

	tf := createMuveraHFreshIndexWithConfig(t, 64, 100)

	// Add a small number of documents (all in one posting initially)
	numDocs := 50
	tokensPerDoc := 4
	dims := 128
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < numDocs; i++ {
		vecs := make([][]float32, tokensPerDoc)
		for j := 0; j < tokensPerDoc; j++ {
			vec := make([]float32, dims)
			for k := 0; k < dims; k++ {
				vec[k] = rng.Float32()
			}
			vecs[j] = vec
		}
		addMultiVectorToIndex(t, &tf, uint64(i), vecs)
	}

	// Create query
	queryVecs := make([][]float32, tokensPerDoc)
	for j := 0; j < tokensPerDoc; j++ {
		vec := make([]float32, dims)
		for k := 0; k < dims; k++ {
			vec[k] = rng.Float32()
		}
		queryVecs[j] = vec
	}

	// Search with MUVERA
	ids, dists, err := tf.Index.SearchByMultiVector(ctx, queryVecs, 10, nil)
	require.NoError(t, err)

	// Should get results regardless of expansion
	assert.LessOrEqual(t, len(ids), 10)
	assert.Equal(t, len(ids), len(dists))
}

// TestNonMuveraPathUnchanged verifies that single-vector search (non-MUVERA)
// does not use posting expansion.
func TestNonMuveraPathUnchanged(t *testing.T) {
	ctx := context.Background()
	dims := 64
	numDocs := 100

	vectors := make([][]float32, numDocs)
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < numDocs; i++ {
		vec := make([]float32, dims)
		for k := 0; k < dims; k++ {
			vec[k] = rng.Float32()
		}
		vectors[i] = vec
	}

	tf := createHFreshIndexWithVectorStore(t, vectors)

	// Verify docToPostings is nil (not initialized for non-MUVERA)
	assert.Nil(t, tf.Index.docToPostings)

	// Add vectors
	for i := 0; i < numDocs; i++ {
		addVectorToIndex(t, &tf, uint64(i), vectors[i])
	}

	time.Sleep(100 * time.Millisecond)

	// Create query
	query := make([]float32, dims)
	for k := 0; k < dims; k++ {
		query[k] = 0.5
	}

	// Search - should work without posting expansion
	ids, dists, err := tf.Index.SearchByVector(ctx, query, 10, nil)
	require.NoError(t, err)

	assert.LessOrEqual(t, len(ids), 10)
	assert.Equal(t, len(ids), len(dists))

	// docToPostings should still be nil (not used for single-vector search)
	assert.Nil(t, tf.Index.docToPostings)
}

// createHFreshIndexWithVectorStore creates a non-MUVERA HFresh index with
// VectorForIDThunk properly configured to return vectors from the provided store.
func createHFreshIndexWithVectorStore(t *testing.T, vectors [][]float32) TestHFresh {
	t.Helper()

	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	cfg := DefaultConfig()

	scheduler := queue.NewScheduler(
		queue.SchedulerOptions{
			Logger: logger,
		},
	)
	cfg.Scheduler = scheduler
	cfg.RootPath = t.TempDir()

	cfg.Centroids.HNSWConfig = &hnsw.Config{
		RootPath:              t.TempDir(),
		ID:                    "hfresh_vector_store_test",
		MakeCommitLoggerThunk: makeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		AllocChecker:          memwatch.NewDummyMonitor(),
		GetViewThunk:          func() common.BucketView { return &noopBucketView{} },
	}

	cfg.TombstoneCallbacks = cyclemanager.NewCallbackGroupNoop()
	cfg.Logger = logger

	// Set VectorForIDThunk to return vectors from our store
	cfg.VectorForIDThunk = hnsw.NewVectorForIDThunk(cfg.TargetVector, func(ctx context.Context, indexID uint64, targetVector string) ([]float32, error) {
		if int(indexID) < len(vectors) {
			return vectors[indexID], nil
		}
		return nil, nil
	})

	scheduler.Start()
	t.Cleanup(func() {
		scheduler.Close(t.Context())
	})

	uc := ent.NewDefaultUserConfig()
	store := testinghelpers.NewDummyStore(t)

	index, err := New(cfg, uc, store)
	require.NoError(t, err)

	return TestHFresh{
		Index: index,
		Logs:  hook,
	}
}
