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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// Distance cutoffs smaller than the old absolute 1e-6 tolerance returned every
// vector regardless of its distance (gh-13315).
func TestHFreshSearchByVectorDistance_SmallCutoffs(t *testing.T) {
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
			tf := createHFreshIndex(t, withDistanceProvider(distancer.NewL2SquaredProvider()))
			t.Cleanup(func() { require.NoError(t, tf.Index.Shutdown(context.Background())) })
			for id, k := range []float32{1, 3, 10} {
				addVectorToIndex(t, &tf, uint64(id), []float32{tc.scale * k, 0, 0, 0})
			}
			tf.Index.waitForMaintenance(t)

			for cutoff, want := range tc.cutoffs {
				ids, _, err := tf.Index.SearchByVectorDistance(t.Context(), []float32{0, 0, 0, 0}, cutoff, 100, nil)
				require.NoError(t, err)
				assert.ElementsMatch(t, want, ids, "cutoff %g", cutoff)
			}
		})
	}
}

// Cosine distances carry absolute float32 noise: a duplicate of
// [0.123, 0.456] computes as ~6e-8, and must still match distance 0.
func TestHFreshSearchByVectorDistance_CosineDuplicateAtZero(t *testing.T) {
	tf := createHFreshIndex(t, withDistanceProvider(distancer.NewCosineDistanceProvider()))
	t.Cleanup(func() { require.NoError(t, tf.Index.Shutdown(context.Background())) })
	addVectorToIndex(t, &tf, 0, []float32{0.123, 0.456})
	addVectorToIndex(t, &tf, 1, []float32{0.456, 0.123})
	tf.Index.waitForMaintenance(t)

	ids, _, err := tf.Index.SearchByVectorDistance(t.Context(), []float32{0.123, 0.456}, 0, 100, nil)
	require.NoError(t, err)
	assert.Equal(t, []uint64{0}, ids)
}
