//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build !race

package flat

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	flattenent "github.com/weaviate/weaviate/entities/vectorindex/flat"
)

// Test_Flat_CosineLargeStoredVectorRanking is a regression test for
// https://github.com/weaviate/weaviate/issues/13746.
//
// A large but finite stored vector ([1e20, 0]) must not lose its cosine
// ranking against a normally scaled query ([1, 0]): its square overflows
// float32 during normalization, and on affected builds the stored vector
// collapsed to zero, producing a strict ranking reversal (distance 1.0
// instead of 0.0). Positive scaling of a nonzero vector must preserve its
// cosine distance, so the large-vector case must match the control case
// where A is replaced by [1, 0].
func Test_Flat_CosineLargeStoredVectorRanking(t *testing.T) {
	ctx := context.Background()
	provider := distancer.NewCosineDistanceProvider()

	newIndex := func(t *testing.T) (*flat, func()) {
		t.Helper()
		store, dirName := createTestStore(t)
		index, err := New(Config{
			ID:                "test-13746",
			RootPath:          dirName,
			DistanceProvider:  provider,
			MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		}, flattenent.UserConfig{}, store)
		require.NoError(t, err)
		return index, func() { index.Shutdown(context.Background()) }
	}

	run := func(t *testing.T, largeA bool) ([]uint64, []float32) {
		t.Helper()
		index, cleanup := newIndex(t)
		defer cleanup()

		vectorA := []float32{1, 0}
		if largeA {
			vectorA = []float32{1e20, 0}
		}
		// issue #13746 step 2: two objects, cosine, 2 dimensions
		require.NoError(t, index.Add(ctx, 1, vectorA))
		require.NoError(t, index.Add(ctx, 2, []float32{0.5, 0.8660254}))

		// issue #13746 step 3: nearVector query [1, 0], limit 2
		ids, dists, err := index.SearchByVector(ctx, []float32{1, 0}, 2, nil)
		require.NoError(t, err)
		require.Len(t, ids, 2)
		require.Len(t, dists, 2)
		return ids, dists
	}

	t.Run("large stored vector ranks like its scaled-down control", func(t *testing.T) {
		ids, dists := run(t, true)

		// expected behavior from the issue: A first with distance ~0,
		// B second with distance ~0.5
		assert.Equal(t, uint64(1), ids[0], "aligned large vector A must rank first, got ids %v with distances %v", ids, dists)
		assert.InDelta(t, float32(0), dists[0], 1e-5,
			"A's cosine distance must be ~0, got %v (ids %v)", dists[0], ids)
		assert.Equal(t, uint64(2), ids[1])
		assert.InDelta(t, float32(0.5), dists[1], 1e-4)

		// actual behavior on affected builds: B first, A second with
		// distance 1.0 (strict ranking reversal)
	})

	t.Run("control: A replaced by [1, 0] leaves the result unchanged", func(t *testing.T) {
		ids, dists := run(t, false)

		assert.Equal(t, uint64(1), ids[0], "control: A must rank first, got ids %v with distances %v", ids, dists)
		assert.InDelta(t, float32(0), dists[0], 1e-5)
		assert.Equal(t, uint64(2), ids[1])
		assert.InDelta(t, float32(0.5), dists[1], 1e-4)
	})
}
