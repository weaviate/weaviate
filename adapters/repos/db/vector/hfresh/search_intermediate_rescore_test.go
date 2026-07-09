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
	"encoding/binary"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
)

// exactFDETop1 computes the exact-FDE cosine argmin over all stored FDEs —
// the ground truth for what the intermediate rescore stage must produce.
func exactFDETop1(t *testing.T, tf *TestHFresh, queryFDE []float32) uint64 {
	t.Helper()
	bucket := tf.Index.store.Bucket(tf.Index.id + "_muvera_vectors")
	require.NotNil(t, bucket)
	dotP := distancer.NewDotProductProvider()

	best := uint64(0)
	bestDist := float32(2)
	c := bucket.Cursor()
	defer c.Close()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if len(k) != 8 || len(v) == 0 {
			continue
		}
		id := binary.BigEndian.Uint64(k)
		fde := make([]float32, len(v)/4)
		for i := range fde {
			fde[i] = float32frombitsLETest(v[i*4:])
		}
		fdeN := distancer.Normalize(fde)
		nd, err := dotP.SingleDist(queryFDE, fdeN)
		require.NoError(t, err)
		dist := 1 + nd
		if dist < bestDist || (dist == bestDist && id < best) {
			bestDist = dist
			best = id
		}
	}
	return best
}

func float32frombitsLETest(b []byte) float32 {
	bits := uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
	return math.Float32frombits(bits)
}

// TestIntermediateRescoreCorrectsRQ1 is the category-C case by construction:
// with rerankBudget=1 and k=1, the old pipeline returns the RQ1 (1-bit)
// argmin while the new stage must return the exact-FDE argmin. The test
// finds a probe where the two genuinely disagree (a real RQ1 ranking error)
// and asserts the stage corrects it.
func TestIntermediateRescoreCorrectsRQ1(t *testing.T) {
	const (
		nDocs  = 500
		tokens = 2
		dim    = 32
	)
	tf := createMuveraHFreshIndex(t, withDistanceProvider(distancer.NewCosineDistanceProvider()))
	rng := rand.New(rand.NewSource(11))
	for i := 0; i < nDocs; i++ {
		addMultiVectorToIndex(t, &tf, uint64(i), randomMultiVector(rng, tokens, dim))
	}

	// rerankBudget = max(k=1, rescoreLimit=1) = 1
	uc := ent.NewDefaultUserConfig()
	uc.RQ.RescoreLimit = 1
	require.NoError(t, tf.Index.UpdateUserConfig(uc, func() {}))

	ctx := context.Background()
	corrected := 0
	for probeSeed := 0; probeSeed < 200; probeSeed++ {
		probe := randomMultiVector(rand.New(rand.NewSource(int64(1000+probeSeed))), tokens, dim)

		tf.Index.disableIntermediateRescore = true
		oldIDs, _, err := tf.Index.SearchByMultiVector(ctx, probe, 1, nil)
		require.NoError(t, err)
		require.Len(t, oldIDs, 1)

		tf.Index.disableIntermediateRescore = false
		newIDs, _, err := tf.Index.SearchByMultiVector(ctx, probe, 1, nil)
		require.NoError(t, err)
		require.Len(t, newIDs, 1)

		if oldIDs[0] == newIDs[0] {
			continue // RQ1 and exact-FDE agree on this probe
		}

		// the two pipelines disagree: the new one must match the exact-FDE
		// ground truth (this is precisely a category-C event corrected)
		queryFDE := tf.Index.muveraEncoder.EncodeQuery(tf.Index.normalizeMultiVec(probe))
		queryFDE = distancer.Normalize(queryFDE)
		want := exactFDETop1(t, &tf, queryFDE)
		require.Equal(t, want, newIDs[0],
			"probe %d: intermediate rescore must return the exact-FDE argmin", probeSeed)
		corrected++
	}
	require.Greater(t, corrected, 0,
		"no probe produced an RQ1/exact-FDE disagreement in 200 attempts; the C-case was not exercised")
	t.Logf("category-C events corrected: %d/200 probes", corrected)
}

// TestIntermediateRescoreOrderingMatchesExactFDE pins the fold math: with a
// rerank budget covering the whole corpus, the MaxSim input order produced
// by the stage must equal the offline exact-cosine-over-normalized-FDEs
// ranking. Verified through searchByFDE directly.
func TestIntermediateRescoreOrderingMatchesExactFDE(t *testing.T) {
	const (
		nDocs  = 50
		tokens = 2
		dim    = 16
	)
	tf := createMuveraHFreshIndex(t, withDistanceProvider(distancer.NewCosineDistanceProvider()))
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < nDocs; i++ {
		addMultiVectorToIndex(t, &tf, uint64(i), randomMultiVector(rng, tokens, dim))
	}

	probe := randomMultiVector(rng, tokens, dim)
	queryFDE := tf.Index.muveraEncoder.EncodeQuery(tf.Index.normalizeMultiVec(probe))

	ids, err := tf.Index.searchByFDE(context.Background(), queryFDE, 16, nDocs, nil)
	require.NoError(t, err)
	require.Len(t, ids, nDocs)

	// offline: exact cosine over normalized FDEs, production tie semantics
	queryFDEn := distancer.Normalize(queryFDE)
	dotP := distancer.NewDotProductProvider()
	bucket := tf.Index.store.Bucket(tf.Index.id + "_muvera_vectors")
	dists := make(map[uint64]float32, nDocs)
	for i := uint64(0); i < nDocs; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		raw, err := bucket.Get(key)
		require.NoError(t, err)
		fde := make([]float32, len(raw)/4)
		for j := range fde {
			fde[j] = float32frombitsLETest(raw[j*4:])
		}
		nd, err := dotP.SingleDist(queryFDEn, distancer.Normalize(fde))
		require.NoError(t, err)
		dists[i] = 1 + nd
	}
	for pos := 1; pos < len(ids); pos++ {
		prev, cur := ids[pos-1], ids[pos]
		require.True(t, dists[prev] < dists[cur] ||
			(dists[prev] == dists[cur] && prev < cur),
			"position %d: order %d before %d contradicts exact FDE distances (%.6f vs %.6f)",
			pos, prev, cur, dists[prev], dists[cur])
	}
}

// TestIntermediateRescoreSkipsMissingFDE pins the graceful handling of a
// candidate whose FDE disappeared between the posting scan and the rescore
// (crash-window analog of the single-vector path's ErrNotFound skip).
func TestIntermediateRescoreSkipsMissingFDE(t *testing.T) {
	const dim = 16
	tf := createMuveraHFreshIndex(t, withDistanceProvider(distancer.NewCosineDistanceProvider()))
	onehot := func(i int) []float32 {
		v := make([]float32, dim)
		v[i] = 1
		return v
	}
	addMultiVectorToIndex(t, &tf, 0, [][]float32{onehot(0)})
	addMultiVectorToIndex(t, &tf, 1, [][]float32{onehot(1)})

	// remove doc1's FDE from the bucket only (version map still live)
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 1)
	require.NoError(t, tf.Index.store.Bucket(tf.Index.id+"_muvera_vectors").Delete(key))

	ids, _, err := tf.Index.SearchByMultiVector(context.Background(), [][]float32{onehot(0)}, 2, nil)
	require.NoError(t, err, "a missing FDE must be skipped, not fail the search")
	require.Equal(t, []uint64{0}, ids)
}

// TestSingleVectorPathUnaffectedByIntermediateRescore pins that the
// single-vector pipeline does not change: the stage lives exclusively in
// searchByFDE (multivector), and flipping the seam must not alter results.
func TestSingleVectorPathUnaffectedByIntermediateRescore(t *testing.T) {
	const (
		nDocs = 300
		dim   = 32
	)
	tf := createHFreshIndex(t)
	stored := make(map[uint64][]float32, nDocs)
	tf.Index.vectorForId = func(_ context.Context, id uint64) ([]float32, error) {
		return stored[id], nil
	}
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < nDocs; i++ {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rng.Float32()
		}
		stored[uint64(i)] = vec
		addVectorToIndex(t, &tf, uint64(i), vec)
	}
	probe := make([]float32, dim)
	for j := range probe {
		probe[j] = rng.Float32()
	}

	tf.Index.disableIntermediateRescore = false
	on, onD, err := tf.Index.SearchByVector(context.Background(), probe, 20, nil)
	require.NoError(t, err)
	tf.Index.disableIntermediateRescore = true
	off, offD, err := tf.Index.SearchByVector(context.Background(), probe, 20, nil)
	require.NoError(t, err)

	require.Equal(t, on, off)
	require.Equal(t, onD, offD)
}
