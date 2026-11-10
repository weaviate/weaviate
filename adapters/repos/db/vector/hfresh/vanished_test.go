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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMultiVectorSearchSkipsVanishedObject removes a document's object from
// the store without deleting it from the index, as a shard does for a moment
// while it deletes an object. A multi-vector search that rescores the
// document must leave it out and not fail.
func TestMultiVectorSearchSkipsVanishedObject(t *testing.T) {
	const docs, gone = 50, 7

	tf := createMuveraHFreshIndexWithConfig(t, 64, 350)
	rng := rand.New(rand.NewSource(3))
	vectors := make([][][]float32, docs)
	for i := range vectors {
		vectors[i] = make([][]float32, 4)
		for j := range vectors[i] {
			vectors[i][j] = make([]float32, 32)
			for k := range vectors[i][j] {
				vectors[i][j][k] = rng.Float32()
			}
		}
		addMultiVectorToIndex(t, &tf, uint64(i), vectors[i])
	}
	tf.Index.waitForMaintenance(t)

	tf.mvStore.deleteMultiVector(gone)
	ids, dists, err := tf.Index.SearchByMultiVector(t.Context(), vectors[gone], 10, nil)
	require.NoError(t, err)
	require.NotContains(t, ids, uint64(gone))
	require.NotEmpty(t, ids)
	require.Len(t, dists, len(ids))
}
