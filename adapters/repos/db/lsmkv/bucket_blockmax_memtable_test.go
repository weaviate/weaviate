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

	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/schema"
)

// SetIdf() recomputes currentBlockImpact from a term's BlockEntry max-impact
// pair. A memtable term that leaves those fields at zero gets a zero WAND bound,
// so its unflushed matches are silently pruned once the result heap fills.
func TestBlockMaxWandMemtableTermNotPruned(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()

	bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })

	const rareDocID = uint64(99) // highest id => processed last, after the heap fills
	commonDocs := []uint64{10, 11, 12, 13, 14}
	for _, id := range commonDocs {
		require.NoError(t, bucket.MapSet([]byte("common"), NewMapPairFromDocIdAndTf(id, 1, 1, false)))
	}
	require.NoError(t, bucket.MapSet([]byte("rare"), NewMapPairFromDocIdAndTf(rareDocID, 8, 1, false)))

	// deliberately NOT flushed: the terms stay memtable-resident.

	view := bucket.GetConsistentView()
	defer view.ReleaseView()

	config := schema.BM25Config{K1: 1.2, B: 0.75}
	corpusN := len(commonDocs) + 1 // distinct documents
	limit := len(commonDocs)       // forces the heap to fill before the rare doc

	diskTerms, _, _, err := bucket.createDiskTermFromCV(ctx, view, float64(corpusN), nil,
		[]string{"common", "rare"}, "", 1, []int{1, 1}, config)
	require.NoError(t, err)

	got := make(map[uint64]float32)
	for _, segTerms := range diskTerms {
		if len(segTerms) == 0 {
			continue
		}
		// mirror bm25_searcher_block.go, which calls SetIdf on every term.
		for _, term := range segTerms {
			require.Greater(t, term.CurrentBlockImpact(), float32(0),
				"memtable term should have a positive block impact before SetIdf")
			term.SetIdf(term.Idf())
			require.Greater(t, term.CurrentBlockImpact(), float32(0),
				"SetIdf must not zero a memtable term's block impact")
		}
		heap, err := DoBlockMaxWand(ctx, limit, segTerms, 1.0, false, len(segTerms), 1, logger)
		require.NoError(t, err)
		for heap.Len() > 0 {
			item := heap.Pop()
			got[item.ID] = item.Dist
		}
	}

	_, found := got[rareDocID]
	require.True(t, found,
		"memtable rare term's document was pruned from the top-%d; got result ids %v", limit, got)
}
