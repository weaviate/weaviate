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
	"testing"

	"github.com/stretchr/testify/require"
)

// Regression tests for issue #278: single vectors must never reach a muvera
// index. Before this guard, a single vector inserted into an empty muvera
// index initialized dims with the token dimensionality instead of the FDE
// dimensionality, corrupting every subsequent AddMulti.
func TestSingleVectorRejectedOnMuveraIndex(t *testing.T) {
	tf := createMuveraHFreshIndex(t)
	vec := []float32{0.1, 0.2, 0.3}

	t.Run("Add", func(t *testing.T) {
		err := tf.Index.Add(t.Context(), 0, vec)
		require.ErrorContains(t, err, "single vectors are not supported")
	})

	t.Run("AddBatch", func(t *testing.T) {
		err := tf.Index.AddBatch(t.Context(), []uint64{0}, [][]float32{vec})
		require.ErrorContains(t, err, "single vectors are not supported")
	})

	t.Run("ValidateBeforeInsert", func(t *testing.T) {
		err := tf.Index.ValidateBeforeInsert(vec)
		require.ErrorContains(t, err, "single vectors are not supported")
	})

	t.Run("index left untouched", func(t *testing.T) {
		// the rejected inserts must not have initialized dimensions
		require.Zero(t, tf.Index.dims)

		// and a proper multi-vector insert still works afterwards
		require.NoError(t, tf.Index.AddMulti(t.Context(), 1, [][]float32{{0.1, 0.2, 0.3, 0.4}}))
	})
}

// Regression tests for issue #280: ValidateMultiBeforeInsert must mirror
// AddMulti's empty checks so that async-indexing enqueue rejects the same
// payloads the sync path does.
func TestValidateMultiBeforeInsertEmpty(t *testing.T) {
	tests := []struct {
		name   string
		vec    [][]float32
		expErr string
	}{
		{name: "no tokens", vec: [][]float32{}, expErr: "cannot be empty"},
		{name: "nil", vec: nil, expErr: "cannot be empty"},
		{name: "empty token", vec: [][]float32{{}}, expErr: "cannot be empty"},
		{name: "empty tokens", vec: [][]float32{{}, {}}, expErr: "cannot be empty"},
		{name: "inconsistent dims", vec: [][]float32{{0.1, 0.2}, {0.3}}, expErr: "inconsistent dimensions"},
		{name: "valid", vec: [][]float32{{0.1, 0.2}, {0.3, 0.4}}},
	}

	tf := createMuveraHFreshIndex(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tf.Index.ValidateMultiBeforeInsert(tt.vec)
			if tt.expErr != "" {
				require.ErrorContains(t, err, tt.expErr)
				return
			}
			require.NoError(t, err)
		})
	}

	t.Run("multi-vector rejected on single-vector index", func(t *testing.T) {
		single := createHFreshIndex(t)
		err := single.Index.ValidateMultiBeforeInsert([][]float32{{0.1, 0.2}})
		require.ErrorContains(t, err, "muvera is not enabled")
	})
}
