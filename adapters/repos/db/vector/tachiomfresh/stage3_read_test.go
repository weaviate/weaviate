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
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// TestStage3BufferedReads: stage 3 through the buffered thunk
// returns the ids and distances the plain document thunk returns, a
// candidate deleted meanwhile is skipped as before, and after the first
// candidate the thunk allocates nothing per candidate.
func TestStage3BufferedReads(t *testing.T) {
	opts := []testIndexOption{withDistanceProvider(distancer.NewDotProductProvider())}
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction, opts...)
	require.NotNil(t, tf.Index.tempMultiVectorForID)
	ctx := context.Background()

	// documents of varying length, so the container grows and shrinks
	rng := rand.New(rand.NewSource(1))
	const dims = 8
	var ids []uint64
	for doc := uint64(0); doc < 40; doc++ {
		n := 1 + rng.Intn(12)
		tokens := make([][]float32, n)
		for i := range tokens {
			tokens[i] = make([]float32, dims)
			for j := range tokens[i] {
				tokens[i][j] = rng.Float32() + 0.1
			}
		}
		tf.mvStore.storeMultiVector(doc, tokens)
		ids = append(ids, doc)
	}
	require.NoError(t, tf.Index.initDimensions(make([]float32, dims)))
	query := [][]float32{tf.mvStore.multiVectors[3][0], tf.mvStore.multiVectors[17][0]}

	var container common.VectorSlice
	gotIDs, gotDists, err := tf.Index.computeLateInteraction(ctx, query, 10, ids, &container)
	require.NoError(t, err)
	wantIDs, wantDists, err := tf.Index.computeLateInteraction(ctx, query, 10, ids, nil)
	require.NoError(t, err)
	require.Equal(t, wantIDs, gotIDs)
	require.InDeltaSlice(t, wantDists, gotDists, 1e-6)
	require.NotEmpty(t, container.Buff8)
	require.NotEmpty(t, container.Slice, "the container holds the last document's tokens")

	// the thunk allocates nothing once the container has grown
	allocs := testing.AllocsPerRun(20, func() {
		if _, err := tf.Index.tempMultiVectorForID(ctx, 5, &container); err != nil {
			t.Fatal(err)
		}
	})
	require.LessOrEqual(t, allocs, 1.0, "the [][]float32 headers at most")

	// a candidate deleted meanwhile is not a result
	tf.mvStore.deleteMultiVector(ids[0])
	_, err = tf.Index.VersionMap.MarkDeleted(ctx, ids[0])
	require.NoError(t, err)
	gotIDs, _, err = tf.Index.computeLateInteraction(ctx, query, 10, ids, &container)
	require.NoError(t, err)
	require.NotContains(t, gotIDs, ids[0])
	require.Len(t, gotIDs, 10)

	// an index without the buffered thunk
	// serves the same candidates through the plain thunk, container
	// or not
	plain := newTestIndex(t, 64, 8, 8, hfreshEFConstruction, append(opts, withoutTempThunk())...)
	require.Nil(t, plain.Index.tempMultiVectorForID)
	for doc, tokens := range tf.mvStore.multiVectors {
		plain.mvStore.storeMultiVector(doc, tokens)
	}
	require.NoError(t, plain.Index.initDimensions(make([]float32, dims)))
	plainIDs, plainDists, err := plain.Index.computeLateInteraction(ctx, query, 10, ids[1:], &common.VectorSlice{})
	require.NoError(t, err)
	require.Equal(t, gotIDs, plainIDs)
	noContainerIDs, _, err := plain.Index.computeLateInteraction(ctx, query, 10, ids[1:], nil)
	require.NoError(t, err)
	require.Equal(t, gotIDs, noContainerIDs)
	_, gotDists, err = tf.Index.computeLateInteraction(ctx, query, 10, ids[1:], &container)
	require.NoError(t, err)
	require.InDeltaSlice(t, gotDists, plainDists, 1e-6)
}
