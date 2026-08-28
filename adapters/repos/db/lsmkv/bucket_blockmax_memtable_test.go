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
	"encoding/binary"
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

// TestInvertedMapCursorSeekOnDiskSegment drives the seek path of the inverted
// segment cursor, which positions itself from the disk index and then takes the
// key and values from the record it parses. Nothing else reaches it: the
// compactors that use this cursor only walk it with first and next.
func TestInvertedMapCursorSeekOnDiskSegment(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()

	bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })

	// terms leave gaps, so a probe can land between two of them
	terms := []string{"term-02", "term-04", "term-06", "term-08"}
	for i, term := range terms {
		require.NoError(t, bucket.MapSet([]byte(term),
			NewMapPairFromDocIdAndTf(uint64(i)+1, 1, 1, false)))
	}
	// everything must live on disk: a memtable hit would not reach the cursor
	require.NoError(t, bucket.FlushAndSwitch())

	tests := []struct {
		name     string
		seek     string
		wantTerm string
		wantDoc  uint64
		wantNone bool
	}{
		{name: "exact match on the first term", seek: "term-02", wantTerm: "term-02", wantDoc: 1},
		{name: "between two terms", seek: "term-05", wantTerm: "term-06", wantDoc: 3},
		{name: "below the smallest term", seek: "term-00", wantTerm: "term-02", wantDoc: 1},
		{name: "exact match on the last term", seek: "term-08", wantTerm: "term-08", wantDoc: 4},
		{name: "past the highest term", seek: "term-99", wantNone: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c, err := bucket.MapCursor()
			require.NoError(t, err)
			defer c.Close()

			key, pairs := c.Seek(ctx, []byte(test.seek))
			if test.wantNone {
				require.Nil(t, key)
				return
			}

			require.Equal(t, test.wantTerm, string(key))
			// the pairs have to belong to that term, which a mis-positioned
			// cursor would get wrong
			require.Len(t, pairs, 1)
			require.Equal(t, test.wantDoc, binary.BigEndian.Uint64(pairs[0].Key))
		})
	}

	// walking on from a seek proves the cursor left its next offset on the node
	// boundary, not somewhere inside the record it just parsed
	t.Run("seek then walk to the end", func(t *testing.T) {
		c, err := bucket.MapCursor()
		require.NoError(t, err)
		defer c.Close()

		var seen []string
		for key, _ := c.Seek(ctx, []byte("term-05")); key != nil; key, _ = c.Next(ctx) {
			seen = append(seen, string(key))
		}
		require.Equal(t, []string{"term-06", "term-08"}, seen)
	})
}
