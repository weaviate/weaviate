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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

func TestDocSet(t *testing.T) {
	tests := []struct {
		name    string
		add     []uint64
		remove  []uint64
		want    []uint64
		absent  []uint64
		stopAt  int
		wantGot int
	}{
		{name: "empty", absent: []uint64{0, 1, 64}},
		{name: "one", add: []uint64{5}, want: []uint64{5}, absent: []uint64{4, 6, 69}},
		{name: "across words", add: []uint64{0, 63, 64, 127, 128, 1000}, want: []uint64{0, 63, 64, 127, 128, 1000}},
		{name: "removed", add: []uint64{1, 2, 3}, remove: []uint64{2, 70}, want: []uint64{1, 3}, absent: []uint64{2, 70}},
		{name: "added twice", add: []uint64{9, 9}, want: []uint64{9}},
		{name: "stopped", add: []uint64{1, 2, 3, 4}, want: []uint64{1, 2, 3, 4}, stopAt: 2, wantGot: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var d docSet
			for _, id := range tt.add {
				d.add(id)
			}
			for _, id := range tt.remove {
				d.remove(id)
			}
			for _, id := range tt.want {
				require.True(t, d.contains(id), "doc %d", id)
			}
			for _, id := range tt.absent {
				require.False(t, d.contains(id), "doc %d", id)
			}
			require.Equal(t, len(tt.want), d.count())
			var got []uint64
			d.iterate(func(id uint64) bool {
				got = append(got, id)
				return tt.stopAt == 0 || len(got) < tt.stopAt
			})
			if tt.stopAt == 0 {
				require.Equal(t, tt.want, got)
			} else {
				require.Equal(t, tt.want[:tt.wantGot], got)
			}
		})
	}

	t.Run("iterate may change the set", func(t *testing.T) {
		var d docSet
		for id := uint64(0); id < 10; id++ {
			d.add(id)
		}
		var seen int
		d.iterate(func(id uint64) bool {
			d.remove(id)
			seen++
			return true
		})
		require.Equal(t, 10, seen)
		require.Zero(t, d.count())
	})
}

// TestContainsDocAndIterate: the doc set follows inserts and deletes, and
// comes back from the postings after a restart with the deleted documents
// left out, through the shard's interface methods.
func TestContainsDocAndIterate(t *testing.T) {
	dir := t.TempDir()
	open := func() testIndex {
		return newTestIndexAt(t, dir, testMaxPostingSize, testKCentroids, testKDocsToScore, hfreshEFConstruction,
			withDistanceProvider(distancer.NewDotProductProvider()))
	}
	tf := open()
	const docs = 50
	rng := rand.New(rand.NewSource(3))
	for i := range uint64(docs) {
		addMultiVectorToIndex(t, &tf, i, randomMultiVector(rng, testTokens, testDim))
	}
	tf.Index.waitForMaintenance(t)
	deleted := map[uint64]bool{3: true, 17: true, 49: true}
	for id := range deleted {
		require.NoError(t, tf.Index.DeleteMulti(id))
	}

	check := func(t *testing.T, index *TachiomFresh) {
		for id := range uint64(docs) {
			require.Equal(t, !deleted[id], index.ContainsDoc(id), "doc %d", id)
		}
		require.False(t, index.ContainsDoc(docs), "never inserted")
		require.False(t, index.ContainsDoc(1<<20), "never inserted, past the set")
		var got []uint64
		index.Iterate(func(id uint64) bool {
			got = append(got, id)
			return true
		})
		var want []uint64
		for id := range uint64(docs) {
			if !deleted[id] {
				want = append(want, id)
			}
		}
		require.Equal(t, want, got)
	}
	check(t, tf.Index)
	tf.close(t)

	reopened := open()
	check(t, reopened.Index)
	reopened.close(t)
}

// TestIndexInterface: the constant answers of the shard's interface and
// the refusal of the single-vector methods.
func TestIndexInterface(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction)
	index := tf.Index
	require.Equal(t, common.IndexType(common.IndexTypeTachiomFresh), index.Type())
	require.True(t, index.Multivector())
	require.False(t, index.Compressed())
	require.InDelta(t, 1.0, index.CompressionStats().CompressionRatio(128), 1e-9)

	ctx := t.Context()
	require.ErrorIs(t, index.Add(ctx, 1, []float32{1}), ErrMultiVectorOnly)
	require.ErrorIs(t, index.AddBatch(ctx, []uint64{1}, [][]float32{{1}}), ErrMultiVectorOnly)
	require.ErrorIs(t, index.ValidateBeforeInsert([]float32{1}), ErrMultiVectorOnly)
	_, _, err := index.SearchByVector(ctx, []float32{1}, 1, nil)
	require.ErrorIs(t, err, ErrMultiVectorOnly)
	_, _, err = index.SearchByVectorDistance(ctx, []float32{1}, 1, 10, nil)
	require.ErrorIs(t, err, ErrMultiVectorOnly)
	_, err = index.QueryVectorDistancer([]float32{1}).DistanceFunc(1)
	require.ErrorIs(t, err, ErrMultiVectorOnly)

	// the multi-vector distancer and the distance search score as the search
	rng := rand.New(rand.NewSource(5))
	docs := make([][][]float32, 20)
	for i := range docs {
		docs[i] = randomMultiVector(rng, testTokens, testDim)
		addMultiVectorToIndex(t, &tf, uint64(i), docs[i])
	}
	query := randomMultiVector(rng, 4, testDim)
	ids, dists, err := index.SearchByMultiVector(ctx, query, 20, nil)
	require.NoError(t, err)
	require.Len(t, ids, 20)
	distancer := index.QueryMultiVectorDistancer(query)
	for i, id := range ids {
		d, err := distancer.DistanceFunc(id)
		require.NoError(t, err)
		require.InDelta(t, dists[i], d, 1e-5, "doc %d", id)
	}
	within, withinDists, err := index.SearchByMultiVectorDistance(ctx, query, dists[9], 100, nil)
	require.NoError(t, err)
	require.Equal(t, ids[:10], within)
	require.Equal(t, dists[:10], withinDists)
}
