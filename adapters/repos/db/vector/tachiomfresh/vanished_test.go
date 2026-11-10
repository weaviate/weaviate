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

package tachiomfresh

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// TestSearchSkipsVanishedObject removes a document's object from the store
// without deleting it from the index, as a shard does for a moment while it
// deletes an object. A search that scores the document must leave it out and
// not fail, on the posting path and on the flat path of a filtered search,
// with the buffered read and with the plain one.
func TestSearchSkipsVanishedObject(t *testing.T) {
	const docs, gone = 50, 7

	tests := []struct {
		name  string
		allow helpers.AllowList
		opts  []testIndexOption
	}{
		{name: "posting path"},
		{name: "flat path", allow: helpers.NewAllowList(1, 2, 3, gone, 9)},
		{name: "posting path, plain read", opts: []testIndexOption{withoutTempThunk()}},
		{name: "flat path, plain read", allow: helpers.NewAllowList(1, 2, 3, gone, 9), opts: []testIndexOption{withoutTempThunk()}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := append([]testIndexOption{withDistanceProvider(distancer.NewDotProductProvider())}, tt.opts...)
			tf := newTestIndex(t, testMaxPostingSize, testKCentroids, testKDocsToScore, hfreshEFConstruction, opts...)
			rng := rand.New(rand.NewSource(3))
			vectors := make([][][]float32, docs)
			for i := range vectors {
				vectors[i] = randomMultiVector(rng, testTokens, testDim)
				addMultiVectorToIndex(t, &tf, uint64(i), vectors[i])
			}
			tf.Index.waitForMaintenance(t)

			tf.mvStore.deleteMultiVector(gone)
			ids, dists, err := tf.Index.SearchByMultiVector(t.Context(), vectors[gone], 10, tt.allow)
			require.NoError(t, err)
			require.NotContains(t, ids, uint64(gone))
			require.NotEmpty(t, ids)
			require.Len(t, dists, len(ids))
		})
	}
}
