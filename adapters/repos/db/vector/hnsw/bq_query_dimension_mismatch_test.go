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

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Regression test for https://github.com/weaviate/weaviate/issues/12207:
// a nearVector query with an under-dimensioned vector against a
// BQ-compressed HNSW index used to silently return an empty result set
// instead of erroring. This is because BQ's compressed byte-length only
// depends on how many 64-bit blocks a vector packs into: a query whose
// dimension is smaller than the index dimension, but which still packs into
// the same number of blocks, produces a validly-shaped (but meaningless)
// compressed code that passes every length check downstream. (An
// over-dimensioned query that crosses a block boundary already errors
// correctly, since it changes the compressed byte length.)
func TestSearchByVector_BQUnderDimensionedQueryErrors(t *testing.T) {
	ctx := context.Background()
	dimensions := 64 // exactly one 64-bit block
	vectors, _ := testinghelpers.RandomVecs(50, 0, dimensions)

	cfg := createVectorHnswIndexTestConfig()
	cfg.DistanceProvider = distancer.NewCosineDistanceProvider()
	cfg.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
		return vectors[int(id)], nil
	}
	cfg.TempVectorForIDWithViewThunk = func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
		copy(container.Slice, vectors[int(id)])
		return container.Slice, nil
	}

	uc := ent.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
		EF:             36,
		BQ:             ent.BQConfig{Enabled: true},
	}

	index, err := New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.Nil(t, err)
	t.Cleanup(func() {
		_ = index.Shutdown(ctx)
	})

	for i, vec := range vectors {
		require.Nil(t, index.Add(ctx, uint64(i), vec))
	}

	t.Run("matching dimension succeeds", func(t *testing.T) {
		_, _, err := index.SearchByVector(ctx, vectors[0], 3, nil)
		assert.NoError(t, err)
	})

	t.Run("under-dimensioned query errors instead of returning empty results", func(t *testing.T) {
		underDim := make([]float32, dimensions-32)
		copy(underDim, vectors[0])

		res, _, err := index.SearchByVector(ctx, underDim, 3, nil)
		assert.Error(t, err, "expected a dimension-mismatch error, got a silent result: %v", res)
		if err != nil {
			assert.ErrorIs(t, err, distancer.ErrVectorLength)
		}
	})

	t.Run("over-dimensioned query also errors", func(t *testing.T) {
		overDim := make([]float32, dimensions+32)
		copy(overDim, vectors[0])

		res, _, err := index.SearchByVector(ctx, overDim, 3, nil)
		assert.Error(t, err, "expected a dimension-mismatch error, got a silent result: %v", res)
	})
}
