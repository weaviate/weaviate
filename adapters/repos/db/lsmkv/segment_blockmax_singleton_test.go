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

//go:build integrationTest
// +build integrationTest

package lsmkv

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/schema"
)

// A single-document disk term takes decodeBlock's docCount<=ENCODE_AS_FULL_BYTES
// early return, which used to skip initializing currentBlockImpact (the term's
// WAND upper bound) and currentBlockMaxId, leaving them at 0. A 0 upper bound
// makes BlockMax-WAND prune away documents that match the term — and a single-
// document term is exactly a rare, high-idf term whose matches should rank high.
//
// TestSingletonTerm_BlockImpactInitialized checks the field directly;
// TestSingletonTerm_NotPrunedFromTopK checks the user-visible consequence.
func TestSingletonTerm_BlockImpactInitialized(t *testing.T) {
	ctx := context.Background()
	bm25 := schema.BM25Config{K1: 1.2, B: 0.75}
	const N = 1_000_000

	bucket := newInvertedBucket(t, ctx)
	defer bucket.Shutdown(ctx)

	require.NoError(t, bucket.MapSet([]byte("s"), NewMapPairFromDocIdAndTf(7, 5, 1, false))) // docCount==1
	for d := uint64(1); d <= 50; d++ {
		require.NoError(t, bucket.MapSet([]byte("m"), NewMapPairFromDocIdAndTf(d, 3, 1, false)))
	}
	require.NoError(t, bucket.FlushAndSwitch())

	view := bucket.GetConsistentView()
	defer view.ReleaseView()
	diskTerms, _, _, err := bucket.createDiskTermFromCV(ctx, view, N, nil,
		[]string{"s", "m"}, "", 1, []int{1, 1}, bm25)
	require.NoError(t, err)

	var singleton *SegmentBlockMax
	for _, seg := range diskTerms {
		for _, term := range seg {
			if term.QueryTermIndex() == 0 {
				singleton = term
			}
		}
	}
	require.NotNil(t, singleton)
	require.EqualValues(t, 1, singleton.docCount)

	// impact must equal the block's true max impact (idf * tf/(tf+k1) at propLen==1),
	// not 0; block-max id must be the doc id, not 0.
	require.NotZero(t, singleton.currentBlockImpact, "single-doc currentBlockImpact must be initialized")
	require.InDelta(t, singleton.computeCurrentBlockImpact(), singleton.currentBlockImpact, 1e-6)
	require.EqualValues(t, 7, singleton.currentBlockMaxId)
}

func TestSingletonTerm_NotPrunedFromTopK(t *testing.T) {
	ctx := context.Background()
	bm25 := schema.BM25Config{K1: 1.2, B: 0.75}
	const N = 1_000_000
	const avgPropLen = 1.0

	bucket := newInvertedBucket(t, ctx)
	defer bucket.Shutdown(ctx)

	// "d": common term, docs 1..300 tf 1 (low idf). "s": rare term, doc 7 only,
	// tf 5 (high idf). doc 7 matches both, so it is the unique true top-1.
	for d := uint64(1); d <= 300; d++ {
		require.NoError(t, bucket.MapSet([]byte("d"), NewMapPairFromDocIdAndTf(d, 1, 1, false)))
	}
	require.NoError(t, bucket.MapSet([]byte("s"), NewMapPairFromDocIdAndTf(7, 5, 1, false)))
	require.NoError(t, bucket.FlushAndSwitch())

	view := bucket.GetConsistentView()
	defer view.ReleaseView()
	diskTerms, _, _, err := bucket.createDiskTermFromCV(ctx, view, N, nil,
		[]string{"d", "s"}, "", 1, []int{1, 1}, bm25)
	require.NoError(t, err)

	topID, topScore := uint64(0), float32(-1e30)
	for _, segTerms := range diskTerms {
		if len(segTerms) == 0 {
			continue
		}
		heap, err := DoBlockMaxWand(ctx, 1, segTerms, avgPropLen, false, len(segTerms), 1, bucket.logger)
		require.NoError(t, err)
		for heap.Len() > 0 {
			it := heap.Pop()
			if it.Dist > topScore {
				topScore, topID = it.Dist, it.ID
			}
		}
	}
	require.Equalf(t, uint64(7), topID,
		"BlockMax-WAND must return doc 7 (matched by the rare term) as top-1, got doc %d @ %.4f", topID, topScore)
}

func newInvertedBucket(t *testing.T, ctx context.Context) *Bucket {
	t.Helper()
	dir := t.TempDir()
	b, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	return b
}
