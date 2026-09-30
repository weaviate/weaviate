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

//go:build !race

package flat

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
)

// Distance cutoffs smaller than the old absolute 1e-6 tolerance returned every
// vector regardless of its distance (gh-13315).
func TestFlatSearchByVectorDistance_SmallCutoffs(t *testing.T) {
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
			ctx := t.Context()
			index, err := New(Config{
				ID:                "id",
				RootPath:          t.TempDir(),
				DistanceProvider:  distancer.NewL2SquaredProvider(),
				MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
			}, flatent.UserConfig{}, loadTestStore(t, t.TempDir()))
			require.NoError(t, err)

			for id, k := range []float32{1, 3, 10} {
				require.NoError(t, index.Add(ctx, uint64(id), []float32{tc.scale * k, 0, 0, 0}))
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
func TestFlatSearchByVectorDistance_CosineDuplicateAtZero(t *testing.T) {
	ctx := t.Context()
	index, err := New(Config{
		ID:                "id",
		RootPath:          t.TempDir(),
		DistanceProvider:  distancer.NewCosineDistanceProvider(),
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, flatent.UserConfig{}, loadTestStore(t, t.TempDir()))
	require.NoError(t, err)

	require.NoError(t, index.Add(ctx, 0, []float32{0.123, 0.456}))
	require.NoError(t, index.Add(ctx, 1, []float32{0.456, 0.123}))

	ids, _, err := index.SearchByVectorDistance(ctx, []float32{0.123, 0.456}, 0, 100, nil)
	require.NoError(t, err)
	assert.Equal(t, []uint64{0}, ids)
}
