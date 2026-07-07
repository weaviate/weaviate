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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// Regression tests for issue #277: the number of returned results must be
// min(limit, matching docs). rescoreLimit (default 350) is a quality floor
// for the RQ1 candidate depth, not a cap — limits above it were silently
// truncated to rescoreLimit results.
func TestSearchLimitNotCappedByRescoreLimit(t *testing.T) {
	const (
		nDocs = 500
		dim   = 32
	)
	limits := []int{10, 350, 400, 500}

	t.Run("single vector path", func(t *testing.T) {
		tf := createHFreshIndex(t)
		// the non-muvera path rescores against the original vectors, which
		// the shard normally provides via VectorForIDThunk
		stored := make(map[uint64][]float32, nDocs)
		tf.Index.vectorForId = func(_ context.Context, id uint64) ([]float32, error) {
			return stored[id], nil
		}
		rng := rand.New(rand.NewSource(42))
		for i := 0; i < nDocs; i++ {
			vec := make([]float32, dim)
			for j := range vec {
				vec[j] = rng.Float32()
			}
			stored[uint64(i)] = vec
			addVectorToIndex(t, &tf, uint64(i), vec)
		}

		probe := make([]float32, dim)
		for j := range probe {
			probe[j] = rng.Float32()
		}
		for _, limit := range limits {
			ids, dists, err := tf.Index.SearchByVector(t.Context(), probe, limit, nil)
			require.NoError(t, err)
			require.Len(t, ids, min(limit, nDocs), "limit=%d", limit)
			require.Len(t, dists, min(limit, nDocs), "limit=%d", limit)
		}
	})

	t.Run("multi vector path", func(t *testing.T) {
		tf := createMuveraHFreshIndex(t, withDistanceProvider(distancer.NewCosineDistanceProvider()))
		rng := rand.New(rand.NewSource(42))
		for i := 0; i < nDocs; i++ {
			addMultiVectorToIndex(t, &tf, uint64(i), randomMultiVector(rng, 2, dim))
		}

		probe := randomMultiVector(rng, 2, dim)
		for _, limit := range limits {
			ids, dists, err := tf.Index.SearchByMultiVector(t.Context(), probe, limit, nil)
			require.NoError(t, err)
			require.Len(t, ids, min(limit, nDocs), "limit=%d", limit)
			require.Len(t, dists, min(limit, nDocs), "limit=%d", limit)
		}
	})
}
