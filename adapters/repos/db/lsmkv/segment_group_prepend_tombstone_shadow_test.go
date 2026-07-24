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

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
)

// TestSegmentGroup_PrependedAddShadowedByNewerDelete pins the convergence
// invariant unconditional-scan reindexing depends on (weaviate/weaviate#11692):
// after PrependSegmentsFromBucket, a mirrored ingest-bucket delete must
// shadow an add sitting in the older prepended segments, at read time and
// after compaction, for every migration-target strategy. Companion:
// TestReindexDeleteConvergence_IngestTombstoneShadowsScannedPosting (db).
func TestSegmentGroup_PrependedAddShadowedByNewerDelete(t *testing.T) {
	newBucket := func(t *testing.T, ctx context.Context, dir, strategy string, keepTombstones bool) *Bucket {
		t.Helper()
		logger, _ := test.NewNullLogger()
		opts := []BucketOption{
			WithStrategy(strategy),
			WithKeepTombstones(keepTombstones),
		}
		if strategy == StrategyRoaringSet {
			opts = append(opts, WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
		}
		b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.NoError(t, err)
		b.SetMemtableThreshold(1e9)
		return b
	}

	compactFully := func(t *testing.T, ctx context.Context, b *Bucket) {
		t.Helper()
		for {
			compacted, err := b.disk.compactOnce(ctx)
			require.NoError(t, err)
			if !compacted {
				break
			}
		}
	}

	cases := []struct {
		strategy string
		// add writes the victim posting (plus a survivor control) into the
		// reindex-side bucket.
		add func(t *testing.T, b *Bucket)
		// del records the mirror-side delete of the victim posting in the
		// ingest bucket.
		del func(t *testing.T, b *Bucket)
		// assertShadowed fails if the victim posting is readable (the
		// survivor control must remain readable).
		assertShadowed func(t *testing.T, b *Bucket)
	}{
		{
			strategy: StrategyRoaringSet,
			add: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.RoaringSetAddList([]byte("victim-key"), []uint64{7}))
				require.NoError(t, b.RoaringSetAddList([]byte("survivor-key"), []uint64{8}))
			},
			del: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.RoaringSetRemoveOne([]byte("victim-key"), 7))
			},
			assertShadowed: func(t *testing.T, b *Bucket) {
				ctx := context.Background()
				bm, release, err := b.RoaringSetGet(ctx, []byte("victim-key"))
				require.NoError(t, err)
				require.Falsef(t, bm.Contains(7),
					"RoaringSet: deleted docID 7 resurrected from the prepended reindex segment")
				release()
				bm, release, err = b.RoaringSetGet(ctx, []byte("survivor-key"))
				require.NoError(t, err)
				require.Truef(t, bm.Contains(8), "RoaringSet: survivor posting must remain readable")
				release()
			},
		},
		{
			strategy: StrategyRoaringSetRange,
			add: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.RoaringSetRangeAdd(42, 7))
				require.NoError(t, b.RoaringSetRangeAdd(43, 8))
			},
			del: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.RoaringSetRangeRemove(42, 7))
			},
			assertShadowed: func(t *testing.T, b *Bucket) {
				read := func(key uint64) []uint64 {
					reader := b.ReaderRoaringSetRange()
					defer reader.Close()
					bm, release, err := reader.Read(context.Background(), key, filters.OperatorEqual)
					require.NoError(t, err)
					if release != nil {
						defer release()
					}
					if bm == nil {
						return nil
					}
					return bm.ToArray()
				}
				require.Emptyf(t, read(42),
					"RoaringSetRange: deleted docID 7 resurrected from the prepended reindex segment")
				require.Equalf(t, []uint64{8}, read(43),
					"RoaringSetRange: survivor posting must remain readable")
			},
		},
		{
			strategy: StrategyMapCollection,
			add: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.MapSet([]byte("victim-row"),
					MapPair{Key: []byte("doc-7"), Value: []byte("v")}))
				require.NoError(t, b.MapSet([]byte("survivor-row"),
					MapPair{Key: []byte("doc-8"), Value: []byte("v")}))
			},
			del: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.MapDeleteKey([]byte("victim-row"), []byte("doc-7")))
			},
			assertShadowed: func(t *testing.T, b *Bucket) {
				pairs, err := b.MapList(context.Background(), []byte("victim-row"))
				require.NoError(t, err)
				require.Emptyf(t, pairs,
					"MapCollection: deleted map key resurrected from the prepended reindex segment")
				pairs, err = b.MapList(context.Background(), []byte("survivor-row"))
				require.NoError(t, err)
				require.Lenf(t, pairs, 1, "MapCollection: survivor pair must remain readable")
			},
		},
		{
			strategy: StrategyInverted,
			add: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.MapSet([]byte("victimterm"),
					NewMapPairFromDocIdAndTf(7, 1, 1, false)))
				require.NoError(t, b.MapSet([]byte("survivorterm"),
					NewMapPairFromDocIdAndTf(8, 1, 1, false)))
			},
			del: func(t *testing.T, b *Bucket) {
				pair := NewMapPairFromDocIdAndTf(7, 1, 1, false)
				require.NoError(t, b.MapDeleteKey([]byte("victimterm"), pair.Key))
			},
			assertShadowed: func(t *testing.T, b *Bucket) {
				pairs, err := b.MapList(context.Background(), []byte("victimterm"))
				require.NoError(t, err)
				require.Emptyf(t, pairs,
					"Inverted: deleted posting resurrected from the prepended reindex segment")
				pairs, err = b.MapList(context.Background(), []byte("survivorterm"))
				require.NoError(t, err)
				require.Lenf(t, pairs, 1, "Inverted: survivor posting must remain readable")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.strategy, func(t *testing.T) {
			ctx := context.Background()

			// Reindex-side bucket: holds the scanned (stale) posting.
			srcDir := t.TempDir()
			src := newBucket(t, ctx, srcDir, tc.strategy, false)
			tc.add(t, src)
			require.NoError(t, src.FlushAndSwitch())
			require.NoError(t, src.Shutdown(ctx))

			// Ingest-side bucket: holds the mirrored delete, tombstones kept
			// (production: loadIngestBuckets keepTombstones=!isMerged).
			tgtDir := t.TempDir()
			tgt := newBucket(t, ctx, tgtDir, tc.strategy, true)
			defer tgt.Shutdown(ctx)
			tc.del(t, tgt)
			require.NoError(t, tgt.FlushAndSwitch())

			// The runtimeSwap merge: reindex segments become the OLDER
			// segments of the ingest bucket.
			require.NoError(t, tgt.PrependSegmentsFromBucket(ctx, srcDir))

			// Read-time shadowing.
			tc.assertShadowed(t, tgt)

			// And across compaction: merging the prepended add with the
			// newer tombstone must not resurrect the posting.
			compactFully(t, ctx, tgt)
			tc.assertShadowed(t, tgt)
		})
	}
}
