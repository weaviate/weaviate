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

// resultsFromVecs builds a fused result set sorted by descending score, where
// score is assigned by position (earlier = more relevant). targetVector ==
// "" stores vectors on Result.Vector, otherwise on Result.Vectors[target].
func resultsFromVecs(targetVector string, vecs [][]float32) []search.Result {
	out := make([]search.Result, len(vecs))
	for i, v := range vecs {
		out[i] = search.Result{
			ID:    strfmtUUID(i),
			Score: float32(len(vecs) - i), // descending
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

	// Three tight cluster-A vectors and one far cluster-B vector. By fused
	// score the top of the list is dominated by cluster A.
	clusterA1 := []float32{1, 0, 0}
	clusterA2 := []float32{0.99, 0.01, 0}
	clusterA3 := []float32{0.98, 0.02, 0}
	clusterB := []float32{0, 0, 1}

	t.Run("balance=0 pulls the diverse far candidate into the top results", func(t *testing.T) {
		// Fused order: A1, A2, A3, B. A plain truncation to 2 yields [A1, A2].
		results := resultsFromVecs("", [][]float32{clusterA1, clusterA2, clusterA3, clusterB})
		out, err := diversifyResults(ctx, mmrSelection(2, 0), "", prov, results)
		require.NoError(t, err)
		require.Len(t, out, 2)
		// Most relevant stays first; second slot should be the far cluster-B
		// candidate, not the near-duplicate A2.
		assert.Equal(t, strfmtUUID(0), out[0].ID)
		assert.Equal(t, strfmtUUID(3), out[1].ID, "expected diverse candidate, got %v", ids(out))
	})

	t.Run("balance=1 preserves fused relevance order", func(t *testing.T) {
		results := resultsFromVecs("", [][]float32{clusterA1, clusterA2, clusterA3, clusterB})
		out, err := diversifyResults(ctx, mmrSelection(3, 1), "", prov, results)
		require.NoError(t, err)
		require.Len(t, out, 3)
		assert.Equal(t, []string{string(strfmtUUID(0)), string(strfmtUUID(1)), string(strfmtUUID(2))}, ids(out))
	})

	t.Run("vectorless candidate keeps its fused rank", func(t *testing.T) {
		// Position 1 (second-most relevant) has no vector — a BM25-only hit.
		results := resultsFromVecs("", [][]float32{clusterA1, nil, clusterA2, clusterB})
		out, err := diversifyResults(ctx, mmrSelection(4, 0), "", prov, results)
		require.NoError(t, err)
		require.Len(t, out, 4)
		// The vectorless doc must remain at index 1, never dropped or demoted.
		assert.Equal(t, strfmtUUID(1), out[1].ID, "vectorless hit lost its rank: %v", ids(out))
	})

	t.Run("all candidates vectorless falls back to fused truncation", func(t *testing.T) {
		results := resultsFromVecs("", [][]float32{nil, nil, nil})
		out, err := diversifyResults(ctx, mmrSelection(2, 0), "", prov, results)
		require.NoError(t, err)
		require.Len(t, out, 2)
		assert.Equal(t, []string{string(strfmtUUID(0)), string(strfmtUUID(1))}, ids(out))
	})

	t.Run("limit larger than candidate pool returns all", func(t *testing.T) {
		results := resultsFromVecs("", [][]float32{clusterA1, clusterB})
		out, err := diversifyResults(ctx, mmrSelection(10, 0), "", prov, results)
		require.NoError(t, err)
		assert.Len(t, out, 2)
	})

	t.Run("named target vector is read from Result.Vectors", func(t *testing.T) {
		results := resultsFromVecs("my_vec", [][]float32{clusterA1, clusterA2, clusterA3, clusterB})
		out, err := diversifyResults(ctx, mmrSelection(2, 0), "my_vec", prov, results)
		require.NoError(t, err)
		require.Len(t, out, 2)
		assert.Equal(t, strfmtUUID(3), out[1].ID)
	})

	t.Run("nil selection is a no-op", func(t *testing.T) {
		results := resultsFromVecs("", [][]float32{clusterA1, clusterB})
		out, err := diversifyResults(ctx, nil, "", prov, results)
		require.NoError(t, err)
		assert.Equal(t, results, out)
	})

	t.Run("empty input", func(t *testing.T) {
		out, err := diversifyResults(ctx, mmrSelection(5, 0), "", prov, nil)
		require.NoError(t, err)
		assert.Empty(t, out)
	})
}
