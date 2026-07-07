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

// Regression test for issue #275 (single-node manifestation): a multi-vector
// search on a collection that never indexed any multi-vector data must
// return a clean error. Before the guard, EncodeQuery ran on the
// uninitialized muvera encoder (nil projection matrices) and panicked with
// "index out of range [0] with length 0".
func TestSearchByMultiVectorOnEmptyCollection(t *testing.T) {
	tf := createMuveraHFreshIndex(t)
	probe := [][]float32{{0.1, 0.2, 0.3, 0.4}}

	t.Run("search errors cleanly before first insert", func(t *testing.T) {
		_, _, err := tf.Index.SearchByMultiVector(t.Context(), probe, 3, nil)
		require.ErrorIs(t, err, ErrMuveraNotInitialized)
	})

	t.Run("distance search errors cleanly before first insert", func(t *testing.T) {
		_, _, err := tf.Index.SearchByMultiVectorDistance(t.Context(), probe, 0.5, 100, nil)
		require.ErrorIs(t, err, ErrMuveraNotInitialized)
	})

	t.Run("search works after first insert", func(t *testing.T) {
		addMultiVectorToIndex(t, &tf, 0, [][]float32{{0.1, 0.2, 0.3, 0.4}, {0.5, 0.6, 0.7, 0.8}})
		ids, _, err := tf.Index.SearchByMultiVector(t.Context(), probe, 3, nil)
		require.NoError(t, err)
		require.Equal(t, []uint64{0}, ids)
	})
}
