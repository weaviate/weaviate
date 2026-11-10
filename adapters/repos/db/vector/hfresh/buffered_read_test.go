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
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// The exact scoring gives the same ids and distances whether it reads the
// candidates through the buffered thunk or the plain one, also when several
// searches run at once.
func TestBufferedMultiVectorReadMatchesPlainRead(t *testing.T) {
	tf := createMuveraHFreshIndexWithConfig(t, 64, 350)
	rng := rand.New(rand.NewSource(5))
	const docs, tokens, dims = 60, 4, 32
	random := func() [][]float32 {
		doc := make([][]float32, tokens)
		for i := range doc {
			doc[i] = make([]float32, dims)
			for j := range doc[i] {
				doc[i][j] = rng.Float32()
			}
		}
		return doc
	}
	for i := range docs {
		addMultiVectorToIndex(t, &tf, uint64(i), random())
	}
	tf.Index.waitForMaintenance(t)

	queries := make([][][]float32, 8)
	for i := range queries {
		queries[i] = random()
	}
	require.Nil(t, tf.Index.tempMultiVectorForID)
	wantIDs := make([][]uint64, len(queries))
	wantDists := make([][]float32, len(queries))
	for i, q := range queries {
		ids, dists, err := tf.Index.SearchByMultiVector(t.Context(), q, 10, nil)
		require.NoError(t, err)
		require.NotEmpty(t, ids)
		wantIDs[i], wantDists[i] = ids, dists
	}

	var reads sync.Map
	tf.Index.tempMultiVectorForID = func(_ context.Context, id uint64, container *common.VectorSlice) ([][]float32, error) {
		reads.Store(id, true)
		vecs, err := tf.mvStore.getMultiVector(id)
		if err != nil {
			return nil, err
		}
		total := 0
		for _, v := range vecs {
			total += len(v)
		}
		if cap(container.Slice) < total {
			container.Slice = make([]float32, total)
		}
		flat := container.Slice[:total]
		out := make([][]float32, len(vecs))
		off := 0
		for i, v := range vecs {
			copy(flat[off:off+len(v)], v)
			out[i] = flat[off : off+len(v) : off+len(v)]
			off += len(v)
		}
		return out, nil
	}

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			for i, q := range queries {
				ids, dists, err := tf.Index.SearchByMultiVector(t.Context(), q, 10, nil)
				assert.NoError(t, err)
				assert.Equal(t, wantIDs[i], ids)
				assert.Equal(t, wantDists[i], dists)
			}
		}, tf.Index.logger)
	}
	wg.Wait()

	_, read := reads.Load(wantIDs[0][0])
	require.True(t, read, "the buffered thunk served the search")
}
