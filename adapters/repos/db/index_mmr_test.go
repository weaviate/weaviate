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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
)

// These tests exercise diversifyResults in its raw-distance relevance mode
// (relevanceFromDist=true), used by the pure-vector path where MMR's relevance
// term is the vector distance rather than a fused/boosted score. The
// score-based mode is covered in selection_hybrid_test.go.

// resultFromVecDist builds a Result carrying the default vector and its query
// distance (used as the MMR relevance term when relevanceFromDist=true).
func resultFromVecDist(id int, vector []float32, dist float32) search.Result {
	return search.Result{ID: strfmtUUID(id), Vector: vector, Dist: dist}
}

func resultFromNamedVecDist(id int, name string, vector []float32, dist float32) search.Result {
	return search.Result{ID: strfmtUUID(id), Vectors: models.Vectors{name: vector}, Dist: dist}
}

func TestDiversifyResultsFromDist_NilSelection(t *testing.T) {
	prov := distancer.NewL2SquaredProvider()
	results := []search.Result{resultFromVecDist(0, []float32{1, 0}, 0.1)}

	out, err := diversifyResults(context.Background(), nil, "", prov, results, true)
	require.NoError(t, err)
	assert.Equal(t, results, out)
}

func TestDiversifyResultsFromDist_EmptyInput(t *testing.T) {
	prov := distancer.NewL2SquaredProvider()
	sel := mmrSelection(3, 0.5)

	out, err := diversifyResults(context.Background(), sel, "", prov, nil, true)
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestDiversifyResultsFromDist_DiversityWithDefaultVector(t *testing.T) {
	// Two clusters: close cluster near (1,0,0), far cluster near (0,1,0).
	// With balance=0 (pure diversity) MMR should select from both clusters.
	prov := distancer.NewL2SquaredProvider()
	results := []search.Result{
		resultFromVecDist(0, []float32{1, 0, 0}, 0.01),
		resultFromVecDist(1, []float32{1.01, 0, 0}, 0.02),
		resultFromVecDist(2, []float32{0.99, 0, 0}, 0.03),
		resultFromVecDist(3, []float32{0, 1, 0}, 0.9),
		resultFromVecDist(4, []float32{0, 1.01, 0}, 0.91),
		resultFromVecDist(5, []float32{0, 0.99, 0}, 0.92),
	}

	sel := mmrSelection(4, 0)
	out, err := diversifyResults(context.Background(), sel, "", prov, results, true)
	require.NoError(t, err)
	require.Len(t, out, len(results))

	// First pick: most relevant (closest distance).
	assert.Equal(t, strfmtUUID(0), out[0].ID)
	// Second pick: from the far cluster (diverse from the first pick).
	assert.Contains(t, []string{string(strfmtUUID(3)), string(strfmtUUID(4)), string(strfmtUUID(5))},
		string(out[1].ID), "second pick should be from the far cluster for diversity")

	closeCount, farCount := 0, 0
	for _, r := range out {
		if r.Vector[0] > 0.5 {
			closeCount++
		} else {
			farCount++
		}
	}
	assert.True(t, closeCount > 0 && farCount > 0,
		"MMR should select from both clusters, got close=%d far=%d", closeCount, farCount)
}

func TestDiversifyResultsFromDist_PureRelevance(t *testing.T) {
	// With balance=1 (pure relevance), MMR returns in ascending distance order.
	prov := distancer.NewCosineDistanceProvider()
	results := []search.Result{
		resultFromVecDist(0, []float32{1, 0}, 0.1),
		resultFromVecDist(1, []float32{0, 1}, 0.9),
		resultFromVecDist(2, []float32{0.5, 0.5}, 0.5),
	}

	sel := mmrSelection(3, 1)
	out, err := diversifyResults(context.Background(), sel, "", prov, results, true)
	require.NoError(t, err)
	require.Len(t, out, 3)

	assert.Equal(t, []string{
		string(strfmtUUID(0)), string(strfmtUUID(2)), string(strfmtUUID(1)),
	}, ids(out))
}

func TestDiversifyResultsFromDist_NamedVector(t *testing.T) {
	prov := distancer.NewL2SquaredProvider()
	results := []search.Result{
		resultFromNamedVecDist(0, "title", []float32{1, 0}, 0.1),
		resultFromNamedVecDist(1, "title", []float32{0, 1}, 0.8),
	}

	sel := mmrSelection(2, 0.5)
	out, err := diversifyResults(context.Background(), sel, "title", prov, results, true)
	require.NoError(t, err)
	require.Len(t, out, 2)
	assert.Equal(t, strfmtUUID(0), out[0].ID)
}

func TestDiversifyResultsFromDist_FullOrderingRegardlessOfLimit(t *testing.T) {
	// MMR.Limit does not truncate here: diversifyResults emits the full
	// diversified ordering and the caller paginates.
	prov := distancer.NewCosineDistanceProvider()
	results := []search.Result{
		resultFromVecDist(0, []float32{1, 0}, 0.1),
		resultFromVecDist(1, []float32{0.9, 0.1}, 0.15),
		resultFromVecDist(2, []float32{0, 1}, 0.9),
		resultFromVecDist(3, []float32{0.5, 0.5}, 0.5),
	}

	sel := mmrSelection(2, 0)
	out, err := diversifyResults(context.Background(), sel, "", prov, results, true)
	require.NoError(t, err)
	require.Len(t, out, len(results))

	assert.Equal(t, strfmtUUID(0), out[0].ID)
	assert.Equal(t, strfmtUUID(2), out[1].ID)
}
