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

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Distance cutoffs smaller than the old absolute 1e-6 tolerance returned every
// vector regardless of its distance (gh-13315).
func TestHnswSearchByVectorDistance_SmallCutoffs(t *testing.T) {
	for _, tc := range []struct {
		name    string
		scale   float32 // vectors are [scale*k, 0, 0, 0] for k = 1, 3, 10
		cutoffs map[float32][]uint64
	}{
		{
			// squared distances 1e-40, 9e-40, 1e-38 (subnormal), the issue's repro
			name:  "subnormal",
			scale: 1e-20,
			cutoffs: map[float32][]uint64{
				4e-40: {0}, 5e-40: {0}, 2e-39: {0, 1}, 1e-38: {0, 1, 2},
			},
		},
		{
			// squared distances 1e-8, 9e-8, 1e-6: normal floats below 1e-6
			name:  "normal but below 1e-6",
			scale: 1e-4,
			cutoffs: map[float32][]uint64{
				5e-9: {}, 5e-8: {0}, 5e-7: {0, 1}, 2e-6: {0, 1, 2},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			vectors := [][]float32{}
			for _, k := range []float32{1, 3, 10} {
				vectors = append(vectors, []float32{tc.scale * k, 0, 0, 0})
			}

			cfg := createVectorHnswIndexTestConfig()
			cfg.DistanceProvider = distancer.NewL2SquaredProvider()
			cfg.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[id], nil
			}
			index, err := New(cfg, ent.UserConfig{MaxConnections: 30, EFConstruction: 60, EF: 36},
				cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
			require.NoError(t, err)

			for id, v := range vectors {
				require.NoError(t, index.Add(ctx, uint64(id), v))
			}

			for cutoff, want := range tc.cutoffs {
				ids, _, err := index.SearchByVectorDistance(ctx, []float32{0, 0, 0, 0}, cutoff, 100, nil)
				require.NoError(t, err)
				assert.ElementsMatch(t, want, ids, "cutoff %g", cutoff)
			}
		})
	}
}

// Cosine distances carry absolute float32 noise: a duplicate of
// [0.123, 0.456] computes as ~6e-8, and must still match distance 0.
func TestHnswSearchByVectorDistance_CosineDuplicateAtZero(t *testing.T) {
	ctx := context.Background()
	vectors := [][]float32{
		distancer.Normalize([]float32{0.123, 0.456}),
		distancer.Normalize([]float32{0.456, 0.123}),
	}
	cfg := createVectorHnswIndexTestConfig()
	cfg.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
		return vectors[id], nil
	}
	index, err := New(cfg, ent.UserConfig{MaxConnections: 30, EFConstruction: 60, EF: 36},
		cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	for id, v := range vectors {
		require.NoError(t, index.Add(ctx, uint64(id), v))
	}

	ids, _, err := index.SearchByVectorDistance(ctx, []float32{0.123, 0.456}, 0, 100, nil)
	require.NoError(t, err)
	assert.Equal(t, []uint64{0}, ids)
}
