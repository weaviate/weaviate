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

package lsmkv

import (
	"context"
	"strconv"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/schema"
)

// TestBlockMaxWandPropertyBoost exercises the block-max WAND pivot
// (DoBlockMaxWand) and the per-block impact bounds for propertyBoost
// consistency; see requireWandTopK in search_segment_dowand_boost_test.go. The
// "flushed" case covers the disk segment (computeCurrentBlockImpact), "memtable"
// the unflushed active/flushing bounds. Fails on any bare-idf bound at boost > 1.
func TestBlockMaxWandPropertyBoost(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	bm25 := schema.BM25Config{K1: 1.2, B: 0.75}

	const nDocs, limit = 4000, 10
	postings, _ := zipfCorpus(7, nDocs, 800, 20)
	queryIDs := topTermsByDF(postings, 40)
	keyFor := func(id uint64) string { return "t" + strconv.FormatUint(id, 36) }
	query := make([]string, len(queryIDs))
	for i, id := range queryIDs {
		query[i] = keyFor(id)
	}
	dupBoosts := make([]int, len(query))
	for i := range dupBoosts {
		dupBoosts[i] = 1
	}

	for _, flush := range []bool{true, false} {
		name := "memtable"
		if flush {
			name = "flushed"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(StrategyInverted))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })
			bucket.SetMemtableThreshold(1 << 30) // single segment / one memtable

			for tid, dps := range postings {
				key := keyFor(tid)
				for _, dp := range dps {
					require.NoError(t, bucket.MapSet([]byte(key),
						NewMapPairFromDocIdAndTf(dp.Id, dp.Frequency, dp.PropLength, false)))
				}
			}
			if flush {
				require.NoError(t, bucket.FlushAndSwitch())
			}
			avgPropLen, _ := bucket.GetAveragePropertyLength()

			bruteForce := func(boost float32) map[uint64]float64 {
				view := bucket.GetConsistentView()
				defer view.ReleaseView()
				diskTerms, _, _, err := bucket.createDiskTermFromCV(ctx, view, float64(nDocs), nil,
					query, "", boost, dupBoosts, bm25)
				require.NoError(t, err)
				scores := map[uint64]float64{}
				for _, segTerms := range diskTerms {
					for _, term := range segTerms {
						for !term.Exhausted() {
							id, s, _ := term.Score(avgPropLen, false)
							scores[id] += s
							term.Advance()
						}
					}
				}
				return scores
			}

			runWand := func(boost float32) map[uint64]float32 {
				view := bucket.GetConsistentView()
				defer view.ReleaseView()
				diskTerms, _, _, err := bucket.createDiskTermFromCV(ctx, view, float64(nDocs), nil,
					query, "", boost, dupBoosts, bm25)
				require.NoError(t, err)
				out := map[uint64]float32{}
				for _, segTerms := range diskTerms {
					if len(segTerms) == 0 {
						continue
					}
					h, err := DoBlockMaxWand(ctx, limit, segTerms, avgPropLen, false, len(query), 1, logger)
					require.NoError(t, err)
					for h != nil && h.Len() > 0 {
						it := h.Pop()
						out[it.ID] = it.Dist
					}
				}
				return out
			}

			for _, boost := range []float32{1, 5} {
				t.Run(strconv.FormatFloat(float64(boost), 'g', -1, 32), func(t *testing.T) {
					requireWandTopK(t, bruteForce(boost), runWand(boost), limit)
				})
			}
		})
	}
}
