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
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestBlockMaxWandCursorAdmissibility pins the admissibility contract of the
// cursor-backed probes in SegmentBlockMax (tombstoned, filterContains): the
// result set must be exactly the written docs minus tombstoned ones,
// intersected with the filter — and surviving docs must keep the scores of an
// unrestricted pre-delete baseline (neither filters nor tombstones change the
// posting lists the scores are computed from). Doc ids span three sroar
// container keys so the cursors cross containers, not just walk one array.
func TestBlockMaxWandCursorAdmissibility(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()

	const perBand = 60
	bands := []uint64{0, 1 << 16, 2 << 16}
	var docIDs []uint64
	for _, base := range bands {
		for i := uint64(0); i < perBand; i++ {
			docIDs = append(docIDs, base+i*7)
		}
	}
	queries := []string{"alpha", "beta"}
	corpusN := len(docIDs)
	limit := corpusN + 10 // return every admissible doc

	writeCorpus := func(t *testing.T, bucket *Bucket) {
		t.Helper()
		for i, id := range docIDs {
			tf := float32(1 + i%5)
			require.NoError(t, bucket.MapSet([]byte("alpha"),
				NewMapPairFromDocIdAndTf(id, tf, 1, false)))
			if i%2 == 0 {
				require.NoError(t, bucket.MapSet([]byte("beta"),
					NewMapPairFromDocIdAndTf(id, 2, 1, false)))
			}
		}
		require.NoError(t, bucket.FlushAndSwitch())
	}

	deleteDocs := func(t *testing.T, bucket *Bucket, ids []uint64) {
		t.Helper()
		for _, id := range ids {
			mapKey := make([]byte, 8)
			binary.BigEndian.PutUint64(mapKey, id)
			require.NoError(t, bucket.MapDeleteKey([]byte("alpha"), mapKey))
		}
	}

	search := func(t *testing.T, bucket *Bucket, filter helpers.AllowList) map[uint64]float32 {
		return runBlockMaxWand(t, bucket, queries, filter, corpusN, limit, nil)
	}

	// every 4th doc, from every band, so deletions hit all containers
	var deleted []uint64
	for i := 0; i < len(docIDs); i += 4 {
		deleted = append(deleted, docIDs[i])
	}

	cases := []struct {
		name             string
		tombstone        bool
		flushAfterDelete bool // segment tombstones instead of memtable ones
		filterEvery      int  // keep every n-th doc; 0 = no filter
	}{
		{name: "bitmap_filter", filterEvery: 2},
		{name: "memtable_tombstones", tombstone: true},
		{name: "segment_tombstones", tombstone: true, flushAfterDelete: true},
		{name: "filter_and_memtable_tombstones", tombstone: true, filterEvery: 2},
		{name: "filter_and_segment_tombstones", tombstone: true, flushAfterDelete: true, filterEvery: 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(StrategyInverted))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })

			writeCorpus(t, bucket)
			baseline := search(t, bucket, nil)
			require.Len(t, baseline, corpusN, "baseline must return the whole corpus")

			admissible := make(map[uint64]bool, corpusN)
			for _, id := range docIDs {
				admissible[id] = true
			}
			if tc.tombstone {
				deleteDocs(t, bucket, deleted)
				if tc.flushAfterDelete {
					require.NoError(t, bucket.FlushAndSwitch())
				}
				for _, id := range deleted {
					admissible[id] = false
				}
			}
			var filter helpers.AllowList
			if tc.filterEvery > 0 {
				bm := sroar.NewBitmap()
				for i, id := range docIDs {
					if i%tc.filterEvery == 0 {
						bm.Set(id)
					} else {
						admissible[id] = false
					}
				}
				filter = helpers.NewAllowListFromBitmap(bm)
			}

			got := search(t, bucket, filter)

			for _, id := range docIDs {
				score, returned := got[id]
				if admissible[id] {
					require.Truef(t, returned, "admissible doc %d missing from results", id)
					require.Equalf(t, baseline[id], score,
						"doc %d score drifted from the unrestricted baseline", id)
				} else {
					require.Falsef(t, returned, "inadmissible doc %d returned", id)
				}
			}
		})
	}
}
