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

package lsmkv

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/varenc"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/schema"
)

// A posting list of a single document takes the ENCODE_AS_FULL_BYTES fast path in
// decodeBlock. That path must still seed currentBlockImpact/currentBlockMaxId: the
// WAND loop sums currentBlockImpact into the upper bound that gates pruning, so a
// zero impact lets a single-doc term's document be dropped once the heap is full.
func TestDecodeBlockSinglePostingInitializesBlockImpact(t *testing.T) {
	config := schema.BM25Config{K1: 1.2, B: 0.75}
	codecs := []varenc.VarEncDataType{varenc.DeltaVarIntUint64, varenc.VarIntUint64}

	cases := []struct {
		name                string
		maxID               uint64
		maxImpactTf         uint32
		maxImpactPropLength uint32
		idf                 float64
		propertyBoost       float32
		avgPropLength       float64
	}{
		{"unit boost", 7, 3, 2, 2.0, 1.0, 4.0},
		{"high tf, boosted", 42, 9, 1, 1.5, 2.0, 5.0},
		{"max id at zero offset", 1, 1, 1, 0.8, 1.0, 1.0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			blockEntries := []terms.BlockEntry{{
				MaxId:               tc.maxID,
				Offset:              0,
				MaxImpactTf:         tc.maxImpactTf,
				MaxImpactPropLength: tc.maxImpactPropLength,
			}}

			sbm := NewSegmentBlockMaxTest(1, blockEntries, nil, nil, []byte("term"), 0,
				tc.idf, tc.propertyBoost, nil, nil, tc.avgPropLength, config, codecs)
			require.NotNil(t, sbm)

			// mirror computeCurrentBlockImpact for the single block we passed in
			freq := float64(tc.maxImpactTf)
			propLen := float64(tc.maxImpactPropLength)
			want := float32(tc.idf *
				(freq / (freq + config.K1*(1-config.B+config.B*(propLen/tc.avgPropLength)))) *
				float64(tc.propertyBoost))

			require.Equal(t, want, sbm.CurrentBlockImpact(),
				"single-posting fast path must set currentBlockImpact; a zero upper bound silently prunes the doc")
			require.Equal(t, tc.maxID, sbm.CurrentBlockMaxId(),
				"single-posting fast path must set currentBlockMaxId")
		})
	}
}

// End-to-end against a real flushed segment: a rare, high-idf term that matches a
// single document must not be pruned from a limited multi-term query. Before the
// fix the single-doc term's block impact stayed 0, so once the result heap filled
// with the lower-scoring common docs, the rare (and highest-scoring) doc was
// dropped — a silent missing result.
func TestBlockMaxWandSinglePostingNotPruned(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()

	bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })

	const (
		commonTerm = "common"
		rareTerm   = "rare"
		rareDocID  = uint64(99) // highest id => processed last, after the heap fills
	)

	// common: many low-tf docs -> multi-doc posting, low idf
	commonDocs := []uint64{10, 11, 12, 13, 14}
	for _, id := range commonDocs {
		require.NoError(t, bucket.MapSet([]byte(commonTerm),
			NewMapPairFromDocIdAndTf(id, 1, 1, false)))
	}
	// rare: exactly one doc -> single-block fast path, high idf, high tf
	require.NoError(t, bucket.MapSet([]byte(rareTerm),
		NewMapPairFromDocIdAndTf(rareDocID, 8, 1, false)))

	require.NoError(t, bucket.FlushAndSwitch())

	corpusN := len(commonDocs) + 1 // distinct documents
	limit := len(commonDocs)       // forces the heap to fill before the rare doc

	got := runBlockMaxWand(t, bucket, []string{commonTerm, rareTerm}, nil, corpusN, limit)

	_, found := got[rareDocID]
	require.True(t, found,
		"rare single-doc term's document was pruned from the top-%d; got result ids %v", limit, keysOf(got))

	// it is the highest-idf match, so it should also outrank every common doc
	for id, score := range got {
		if id == rareDocID {
			continue
		}
		require.Greater(t, got[rareDocID], score,
			"rare doc should outscore common doc %d", id)
	}
}

// runBlockMaxWand runs the block-max WAND search the same way createDiskTermFromCV
// feeds it in production, under an optional filter, and returns docID -> score
// across all segments/memtables.
func runBlockMaxWand(t *testing.T, bucket *Bucket, queries []string, filter helpers.AllowList, corpusN, limit int) map[uint64]float32 {
	t.Helper()
	ctx := context.Background()

	view := bucket.GetConsistentView()
	defer view.ReleaseView()

	config := schema.BM25Config{K1: 1.2, B: 0.75}
	boosts := make([]int, len(queries))
	for i := range boosts {
		boosts[i] = 1
	}

	diskTerms, _, _, err := bucket.createDiskTermFromCV(ctx, view, float64(corpusN), filter, queries, "", 1, boosts, config)
	require.NoError(t, err)

	got := make(map[uint64]float32)
	for _, segTerms := range diskTerms {
		if len(segTerms) == 0 {
			continue
		}
		heap, err := DoBlockMaxWand(ctx, limit, segTerms, 1.0, false, len(queries), 1, bucket.logger)
		require.NoError(t, err)
		for heap.Len() > 0 {
			item := heap.Pop()
			got[item.ID] = item.Dist
		}
	}
	return got
}

func keysOf(m map[uint64]float32) []uint64 {
	ids := make([]uint64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	return ids
}

// Covers the regimes the WAND hot path feeds sortByID: already-sorted,
// nearly-sorted, reverse, all-equal, and short inputs.
func TestTermsSortByID(t *testing.T) {
	mk := func(ids ...uint64) Terms {
		out := make(Terms, len(ids))
		for i, id := range ids {
			out[i] = &SegmentBlockMax{idPointer: id}
		}
		return out
	}
	idsOf := func(in Terms) []uint64 {
		out := make([]uint64, len(in))
		for i, x := range in {
			out[i] = x.idPointer
		}
		return out
	}

	cases := []struct {
		name string
		in   Terms
		want []uint64
	}{
		{"empty", mk(), []uint64{}},
		{"single", mk(7), []uint64{7}},
		{"already sorted", mk(1, 2, 3, 4), []uint64{1, 2, 3, 4}},
		{"reverse", mk(4, 3, 2, 1), []uint64{1, 2, 3, 4}},
		{"all equal", mk(5, 5, 5), []uint64{5, 5, 5}},
		{"nearly sorted (hot-path regime)", mk(1, 2, 4, 3, 5), []uint64{1, 2, 3, 4, 5}},
		{"with duplicates", mk(3, 1, 2, 1, 3), []uint64{1, 1, 2, 3, 3}},
		{"smallest at end", mk(2, 3, 4, 5, 1), []uint64{1, 2, 3, 4, 5}},
		{"largest at start", mk(9, 1, 2, 3, 4), []uint64{1, 2, 3, 4, 9}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.in.sortByID()
			require.Equal(t, tc.want, idsOf(tc.in))
		})
	}
}
