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
	"math"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestBlockMaxWandMergeFilterIdentity proves the tiered merged-filter is a
// bit-identical no-op on results: for a filtered query over a corpus with
// tombstones, forcing the merge on (fold tombstones into the filter, one
// membership check per candidate) must return exactly the same doc ids and
// scores as forcing it off (filter + tombstones checked separately). The
// bm25MergeGateRatio toggle isolates the code path from the workload's actual
// sumDf/cardinality ratio so both branches are exercised deterministically.
func TestBlockMaxWandMergeFilterIdentity(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()

	// doc ids across three sroar container bands so the merge's AndNot spans
	// containers, not just one array.
	const perBand = 60
	bands := []uint64{0, 1 << 16, 2 << 16}
	var docIDs []uint64
	for _, base := range bands {
		for i := uint64(0); i < perBand; i++ {
			docIDs = append(docIDs, base+i*7)
		}
	}
	queries := []string{"alpha", "beta", "gamma"}
	corpusN := len(docIDs)
	limit := 10 // a real top-K cut so scoring/pruning is exercised, not "return all"

	// wantMerge pins that the merge path fired (or didn't): a merged term carries
	// a fresh folded filter (pointer != the input) with both tombstone bitmaps
	// nil'd, so the identity assertion below can't pass vacuously on a merge that
	// silently no-op'd.
	search := func(t *testing.T, bucket *Bucket, filter helpers.AllowList, wantMerge bool) map[uint64]float32 {
		got, err := queryBlockMaxWand(bucket, queries, filter, corpusN, limit, func(diskTerms [][]*SegmentBlockMax) {
			folded := 0
			for _, seg := range diskTerms {
				for _, tm := range seg {
					if tm != nil && tm.filterDocIds != filter && tm.tombstones == nil && tm.memTombstones == nil {
						folded++
					}
				}
			}
			if wantMerge {
				require.Positive(t, folded, "merge path did not fire: no disk term carries a folded filter")
			} else {
				require.Zero(t, folded, "merge fired when the gate should have skipped it")
			}
		})
		require.NoError(t, err)
		return got
	}

	cases := []struct {
		name             string
		flushAfterDelete bool // flush the first deletes into a segment (segment tombstones)
		extraMemDeletes  bool // after flushing, tombstone more docs in the active memtable
	}{
		{name: "memtable_tombstones", flushAfterDelete: false},
		{name: "segment_tombstones", flushAfterDelete: true},
		// Both at once: the only case that reaches the two-level fold
		// (sroar.AndNot(filter, segTomb) then .AndNot(memTomb)); the other two
		// each exercise a single tombstone source.
		{name: "segment_and_memtable_tombstones", flushAfterDelete: true, extraMemDeletes: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(StrategyInverted))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })

			// three overlapping posting lists with varied tf so scores separate
			for i, id := range docIDs {
				require.NoError(t, bucket.MapSet([]byte("alpha"),
					NewMapPairFromDocIdAndTf(id, float32(1+i%5), 1, false)))
				if i%2 == 0 {
					require.NoError(t, bucket.MapSet([]byte("beta"),
						NewMapPairFromDocIdAndTf(id, float32(2+i%3), 1, false)))
				}
				if i%3 == 0 {
					require.NoError(t, bucket.MapSet([]byte("gamma"),
						NewMapPairFromDocIdAndTf(id, 4, 1, false)))
				}
			}
			require.NoError(t, bucket.FlushAndSwitch())

			// delete every 4th doc, from every band, so tombstones hit all containers
			for i := 0; i < len(docIDs); i += 4 {
				mapKey := make([]byte, 8)
				binary.BigEndian.PutUint64(mapKey, docIDs[i])
				require.NoError(t, bucket.MapDeleteKey([]byte("alpha"), mapKey))
			}
			if tc.flushAfterDelete {
				require.NoError(t, bucket.FlushAndSwitch())
			}

			// A second, disjoint delete stride left unflushed in the active
			// memtable, so this run carries a segment tombstone set (i%4==0) and a
			// memtable tombstone set (i%8==2) at once — both intersecting the
			// filter, disjoint, and leaving the i%8==6 docs alive so the result is
			// non-empty.
			if tc.extraMemDeletes {
				for i := 2; i < len(docIDs); i += 8 {
					mapKey := make([]byte, 8)
					binary.BigEndian.PutUint64(mapKey, docIDs[i])
					require.NoError(t, bucket.MapDeleteKey([]byte("alpha"), mapKey))
				}
			}

			// bitmap filter keeping every other doc, so the merge's AndNot has both
			// filter bits and tombstone bits to combine.
			bm := sroar.NewBitmap()
			for i, id := range docIDs {
				if i%2 == 0 {
					bm.Set(id)
				}
			}
			filter := helpers.NewAllowListFromBitmap(bm)

			prev := bm25MergeGateRatio
			t.Cleanup(func() { bm25MergeGateRatio = prev })

			bm25MergeGateRatio = math.Inf(1) // never merge: filter + tombstones checked separately
			unmerged := search(t, bucket, filter, false)
			bm25MergeGateRatio = 0 // always merge: tombstones folded into the filter
			merged := search(t, bucket, filter, true)

			require.NotEmpty(t, unmerged, "baseline returned nothing; workload is vacuous")
			require.Equal(t, len(unmerged), len(merged), "merge changed the result count")
			for id, score := range unmerged {
				ms, ok := merged[id]
				require.Truef(t, ok, "merge dropped doc %d", id)
				require.Equalf(t, score, ms, "merge changed doc %d score", id)
			}
		})
	}
}
