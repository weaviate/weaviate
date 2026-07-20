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

package db

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
)

func strfmtUUID(i int) strfmt.UUID {
	return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i))
}

// resultsFromVecs builds a fused result set sorted by descending score (earlier = more relevant).
func resultsFromVecs(targetVector string, vecs [][]float32) []search.Result {
	out := make([]search.Result, len(vecs))
	for i, v := range vecs {
		out[i] = search.Result{
			ID:    strfmtUUID(i),
			Score: float32(len(vecs) - i),
		}
		if v == nil {
			continue
		}
		if targetVector == "" {
			out[i].Vector = v
		} else {
			out[i].Vectors = models.Vectors{targetVector: v}
		}
	}
	return out
}

func ids(results []search.Result) []string {
	out := make([]string, len(results))
	for i, r := range results {
		out[i] = string(r.ID)
	}
	return out
}

func mmrSelection(limit uint32, balance float32) *searchparams.Selection {
	return &searchparams.Selection{MMR: &searchparams.SelectionMMR{Limit: limit, Balance: balance}}
}

func TestDiversifyResults(t *testing.T) {
	ctx := context.Background()
	prov := distancer.NewCosineDistanceProvider()

	// Three tight cluster-A vectors and one far cluster-B vector.
	clusterA1 := []float32{1, 0, 0}
	clusterA2 := []float32{0.99, 0.01, 0}
	clusterA3 := []float32{0.98, 0.02, 0}
	clusterB := []float32{0, 0, 1}

	// diversifyResults returns the full ordering, so output length always == input length.
	t.Run("balance=0 pulls the diverse far candidate into the top results", func(t *testing.T) {
		results := resultsFromVecs("", [][]float32{clusterA1, clusterA2, clusterA3, clusterB})
		out, err := diversifyResults(ctx, mmrSelection(2, 0), "", prov, results, false)
		require.NoError(t, err)
		require.Len(t, out, 4)
		// Second slot should be far cluster-B, not the near-duplicate A2.
		assert.Equal(t, strfmtUUID(0), out[0].ID)
		assert.Equal(t, strfmtUUID(3), out[1].ID, "expected diverse candidate, got %v", ids(out))
	})

	t.Run("balance=1 preserves fused relevance order", func(t *testing.T) {
		results := resultsFromVecs("", [][]float32{clusterA1, clusterA2, clusterA3, clusterB})
		out, err := diversifyResults(ctx, mmrSelection(3, 1), "", prov, results, false)
		require.NoError(t, err)
		require.Len(t, out, 4)
		assert.Equal(t, []string{
			string(strfmtUUID(0)), string(strfmtUUID(1)),
			string(strfmtUUID(2)), string(strfmtUUID(3)),
		}, ids(out))
	})

	t.Run("vectorless candidate keeps its fused rank", func(t *testing.T) {
		// Position 1 has no vector — a BM25-only hit that must keep its rank.
		results := resultsFromVecs("", [][]float32{clusterA1, nil, clusterA2, clusterB})
		out, err := diversifyResults(ctx, mmrSelection(4, 0), "", prov, results, false)
		require.NoError(t, err)
		require.Len(t, out, 4)
		assert.Equal(t, strfmtUUID(1), out[1].ID, "vectorless hit lost its rank: %v", ids(out))
	})

	t.Run("all candidates vectorless keeps fused order", func(t *testing.T) {
		results := resultsFromVecs("", [][]float32{nil, nil, nil})
		out, err := diversifyResults(ctx, mmrSelection(2, 0), "", prov, results, false)
		require.NoError(t, err)
		require.Len(t, out, 3)
		assert.Equal(t, []string{string(strfmtUUID(0)), string(strfmtUUID(1)), string(strfmtUUID(2))}, ids(out))
	})

	t.Run("named target vector is read from Result.Vectors", func(t *testing.T) {
		results := resultsFromVecs("my_vec", [][]float32{clusterA1, clusterA2, clusterA3, clusterB})
		out, err := diversifyResults(ctx, mmrSelection(2, 0), "my_vec", prov, results, false)
		require.NoError(t, err)
		require.Len(t, out, 4)
		assert.Equal(t, strfmtUUID(3), out[1].ID)
	})

	t.Run("nil selection is a no-op", func(t *testing.T) {
		results := resultsFromVecs("", [][]float32{clusterA1, clusterB})
		out, err := diversifyResults(ctx, nil, "", prov, results, false)
		require.NoError(t, err)
		assert.Equal(t, results, out)
	})

	t.Run("empty input", func(t *testing.T) {
		out, err := diversifyResults(ctx, mmrSelection(5, 0), "", prov, nil, false)
		require.NoError(t, err)
		assert.Empty(t, out)
	})

	t.Run("slots past the page keep relevance order", func(t *testing.T) {
		// limit=2 means only the first two slots need MMR order; the rest must
		// be the unselected candidates in their original relevance order.
		results := resultsFromVecs("", [][]float32{clusterA1, clusterA2, clusterA3, clusterB})
		out, err := diversifyResults(ctx, mmrSelection(2, 0), "", prov, results, false)
		require.NoError(t, err)
		require.Len(t, out, 4)
		assert.Equal(t, strfmtUUID(0), out[0].ID)
		assert.Equal(t, strfmtUUID(3), out[1].ID, "page slots must be MMR-ordered")
		assert.Equal(t, []string{string(strfmtUUID(1)), string(strfmtUUID(2))},
			ids(out[2:]), "tail must keep relevance order")
	})

	t.Run("vectorless slot inside the page still gets enough MMR candidates", func(t *testing.T) {
		// Positions 1 and 3 are vectorless and keep their slots, so a page of 3
		// only consumes two diversified candidates.
		results := resultsFromVecs("", [][]float32{clusterA1, nil, clusterA2, nil, clusterA3, clusterB})
		out, err := diversifyResults(ctx, mmrSelection(3, 0), "", prov, results, false)
		require.NoError(t, err)
		require.Len(t, out, 6)
		assert.Equal(t, strfmtUUID(0), out[0].ID)
		assert.Equal(t, strfmtUUID(1), out[1].ID, "vectorless hit lost its rank: %v", ids(out))
		assert.Equal(t, strfmtUUID(5), out[2].ID, "expected the diverse candidate in the page: %v", ids(out))
		assert.Equal(t, strfmtUUID(3), out[3].ID, "vectorless hit lost its rank: %v", ids(out))
	})
}

// countingDistProvider wraps a distancer.Provider and counts SingleDist calls.
type countingDistProvider struct {
	distancer.Provider
	calls *int
}

func (c countingDistProvider) SingleDist(a, b []float32) (float32, error) {
	*c.calls++
	return c.Provider.SingleDist(a, b)
}

// TestDiversifyResultsBoundedWork pins that MMR only orders the page, not the
// whole window: a full ordering of a window of n candidates costs O(n²)
// distance evaluations, page-bounded selection costs O(page × n).
func TestDiversifyResultsBoundedWork(t *testing.T) {
	const window, page = 200, 10

	vecs := make([][]float32, window)
	for i := range vecs {
		theta := float64(i) / float64(window) * 2 * math.Pi
		vecs[i] = []float32{float32(math.Cos(theta)), float32(math.Sin(theta))}
	}
	results := resultsFromVecs("", vecs)

	calls := 0
	prov := countingDistProvider{Provider: distancer.NewCosineDistanceProvider(), calls: &calls}

	out, err := diversifyResults(context.Background(), mmrSelection(page, 0.5), "", prov, results, false)
	require.NoError(t, err)
	require.Len(t, out, window, "full-length output contract must hold")
	assert.Equal(t, strfmtUUID(0), out[0].ID, "most relevant candidate leads the page")

	// page-bounded: ≤ (page-1) rounds × window scans. A full ordering would
	// need ~window²/2 ≈ 20000 evaluations.
	assert.LessOrEqual(t, calls, page*window,
		"MMR must not compute a full ordering of the window")
}
