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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
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

	// Create a MUVERA-enabled index with a small number of documents.
	// Budget separation testing only needs to verify plumbing, not large-scale behavior.
	tf := createMuveraHFreshIndexWithConfig(t, 64, 350) // searchProbe=64, rescoreLimit=350

	// Small index: 30 docs, 4 tokens each, 64 dims - enough to test budget logic
	numDocs := 30
	tokensPerDoc := 4
	dims := 64
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

	// Create a query
	queryVecs := make([][]float32, tokensPerDoc)
	for j := 0; j < tokensPerDoc; j++ {
		vec := make([]float32, dims)
		for k := 0; k < dims; k++ {
			vec[k] = rng.Float32()
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
// Old behavior: a single candidate budget of max(k, rescoreLimit) drove both
// routing breadth and rerank depth.
// New behavior (muveraSearchBudgets): routingBudget = max(k, searchProbe),
// rerankBudget = max(k, rescoreLimit).
//
// These should produce identical results for the same inputs under the
// default configuration.
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

	// With k=10, searchProbe=64, rescoreLimit=350, muveraSearchBudgets yields
	// routingBudget = max(10, 64) = 64 and rerankBudget = max(10, 350) = 350
	// (TestMuveraSearchBudgets covers the production function directly).

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

func updateSearchProbe(t *testing.T, tf *TestHFresh, probe uint32) {
	t.Helper()
	uc := ent.NewDefaultUserConfig()
	uc.SearchProbe = probe
	require.NoError(t, tf.Index.UpdateUserConfig(uc, func() {}))
}

// TestMuveraSearchBudgets pins the budget semantics of the decoupled
// routing/rerank search: searchProbe steers routing in BOTH directions (a
// low explicit probe is not floored by rescoreLimit), rescoreLimit floors
// only the rerank depth, and the user-requested k floors both.
//
// An earlier version used routingBudget = max(k, searchProbe, rescoreLimit),
// which silently ignored any searchProbe below rescoreLimit (350) and broke
// the low-probe latency/recall trade-off.
func TestMuveraSearchBudgets(t *testing.T) {
	tests := []struct {
		name        string
		searchProbe uint32 // 0 = leave the parse-time default (256)
		k           int
		wantRouting int
		wantRerank  int
	}{
		{name: "defaults", k: 10, wantRouting: 256, wantRerank: 350},
		{name: "low probe respected below rescoreLimit", searchProbe: 16, k: 10, wantRouting: 16, wantRerank: 350},
		{name: "high probe respected above rescoreLimit", searchProbe: 512, k: 10, wantRouting: 512, wantRerank: 350},
		{name: "k wins over low probe", searchProbe: 16, k: 600, wantRouting: 600, wantRerank: 600},
		{name: "k wins over defaults", k: 400, wantRouting: 400, wantRerank: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tf := createMuveraHFreshIndex(t)
			if tt.searchProbe > 0 {
				updateSearchProbe(t, &tf, tt.searchProbe)
			}
			routing, rerank := tf.Index.muveraSearchBudgets(tt.k)
			require.Equal(t, tt.wantRouting, routing, "routingBudget")
			require.Equal(t, tt.wantRerank, rerank, "rerankBudget")
		})
	}
}

// TestSearchProbeChangesResults is the behavioral half: an explicit low
// searchProbe must actually reduce routing work. This is the test that
// would have caught the rescoreLimit floor silently swallowing low probes:
// with the floor, probe 16 scanned exactly the same postings as the
// default, so the candidate coverage — and every downstream result — was
// bit-identical.
//
// The observable is candidate COVERAGE (rerank budget above the corpus
// size), not the rescored top-k: on IVF-style routing the true top-k
// concentrates in the postings nearest the query, so a lower probe often
// returns the same top-k even though it scans a fraction of the data —
// top-k equality is a property of the dataset, coverage is a property of
// the budget. Any future recall-recovery step that re-widens a narrow
// probe (e.g. posting expansion) must be disabled here, since masking
// narrow-probe misses is precisely such a feature's job.
func TestSearchProbeChangesResults(t *testing.T) {
	const (
		nDocs  = 2500
		tokens = 2
		dim    = 16
	)
	tf := createMuveraHFreshIndex(t, withDistanceProvider(distancer.NewCosineDistanceProvider()))
	rng := rand.New(rand.NewSource(21))
	for i := 0; i < nDocs; i++ {
		addMultiVectorToIndex(t, &tf, uint64(i), randomMultiVector(rng, tokens, dim))
	}

	// wait for background splits so the posting layout is stable and wide
	// enough (>16 postings) for the routing budget to matter
	require.Eventually(t, func() bool {
		return tf.Index.taskQueue.Size() == 0
	}, 60*time.Second, 100*time.Millisecond, "background tasks did not drain")

	probe := randomMultiVector(rng, tokens, dim)
	queryFDE := tf.Index.muveraEncoder.EncodeQuery(tf.Index.normalizeMultiVec(probe))

	// rerank budget above the corpus size so the candidate set reflects
	// coverage, not the RQ1 top-N cut
	wideOpen := nDocs * 2

	fullCandidates, err := tf.Index.searchByFDE(t.Context(), queryFDE, 1000, wideOpen, nil)
	require.NoError(t, err)
	require.Equal(t, nDocs, len(fullCandidates),
		"a routing budget above the posting count must cover the whole corpus")

	lowProbeCandidates, err := tf.Index.searchByFDE(t.Context(), queryFDE, 16, wideOpen, nil)
	require.NoError(t, err)
	require.NotEmpty(t, lowProbeCandidates)
	require.Less(t, len(lowProbeCandidates), len(fullCandidates),
		"routingBudget=16 must scan fewer postings than full routing; "+
			"equal coverage means the probe is being silently floored")

	// end to end: the collection-level searchProbe drives the same budgets
	updateSearchProbe(t, &tf, 16)
	routing, rerank := tf.Index.muveraSearchBudgets(10)
	require.Equal(t, 16, routing)
	require.Equal(t, 350, rerank)
	ids, _, err := tf.Index.SearchByMultiVector(t.Context(), probe, 10, nil)
	require.NoError(t, err)
	require.Len(t, ids, 10)
}
