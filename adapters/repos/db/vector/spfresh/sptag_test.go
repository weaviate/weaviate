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

package spfresh

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

func TestBruteForceSPTAG_Search(t *testing.T) {
	dim := 512
	q := compressionhelpers.NewRotationalQuantizer(dim, 42, 8, distancer.NewCosineDistanceProvider())
	sptag := NewBruteForceSPTAG(q)

	// Seed vectors
	vectors := map[uint64][]float32{
		1: make([]float32, dim),
		2: make([]float32, dim),
		3: make([]float32, dim),
	}

	// Populate with some deterministic values for reproducibility
	for i := 0; i < dim; i++ {
		vectors[1][i] = float32(i)
		vectors[2][i] = float32(i + 1)
		vectors[3][i] = float32(2*i + 5)
	}

	// Encode and upsert
	for id, v := range vectors {
		encoded := q.Encode(v)
		err := sptag.Upsert(id, encoded)
		require.NoError(t, err)
	}

	// Use a query close to vector 2
	query := make([]float32, dim)
	for i := 0; i < dim; i++ {
		query[i] = float32(i + 1) // same as vector 2
	}
	encodedQuery := q.Encode(query)

	results, err := sptag.Search(encodedQuery, 2)
	require.NoError(t, err)
	require.True(t, len(results) >= 1)

	// Vector 2 should be one of the closest
	require.Contains(t, results, uint64(2))

	// Delete vector 2 and search again
	err = sptag.Delete(2)
	require.NoError(t, err)

	results, err = sptag.Search(encodedQuery, 2)
	require.NoError(t, err)
	require.NotContains(t, results, uint64(2))

	// Ensure other vectors are still present
	require.Contains(t, results, uint64(1))
	require.Contains(t, results, uint64(3))

	// Test with an empty search
	results, err = sptag.Search(encodedQuery, 0)
	require.NoError(t, err)
	require.Empty(t, results)
}
