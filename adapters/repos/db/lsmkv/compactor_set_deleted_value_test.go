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
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
)

// A value that was deleted from a set stays deleted through a compaction.
//
// A value and its tombstone cancel each other out in the set decoder. The
// compaction that takes the oldest segment in also drops the tombstones it
// is left with: nothing older can hold what they delete. When a key lives in
// only one of the two segments, its values used to skip the decoder and go
// straight to that cleanup. A value and the tombstone that deleted it, both
// written between two flushes, then lost the tombstone and kept the value:
// the deleted value was back, for good.
//
// The shard deletes doc ids from Set buckets this way (SetDeleteSingle): from
// the _id property bucket of every class when an object is deleted or gets a
// new doc id, and from filterable, property length and null buckets still in
// the Set strategy of before roaring sets. The old doc id came back in an id
// filter; TestIDFilterKeepsOldDocIDsOutThroughCompaction in package db shows
// it through a shard.
func TestCompactionSetKeepsDeletedValuesDeleted(t *testing.T) {
	x, y := []byte("value-x"), []byte("value-y")
	add := func(vals ...[]byte) func(*testing.T, *Bucket, []byte) {
		return func(t *testing.T, b *Bucket, key []byte) { require.NoError(t, b.SetAdd(key, vals)) }
	}
	del := func(val []byte) func(*testing.T, *Bucket, []byte) {
		return func(t *testing.T, b *Bucket, key []byte) { require.NoError(t, b.SetDeleteSingle(key, val)) }
	}
	type write = func(*testing.T, *Bucket, []byte)

	tests := []struct {
		name string
		// segments are the writes to the key between two flushes, oldest
		// first. A segment without any holds other keys only.
		segments [][]write
		want     [][]byte
	}{
		{
			name:     "added and deleted in the newer segment, the key not in the older",
			segments: [][]write{nil, {add(x), del(x)}},
		},
		{
			name:     "added, deleted and another added in the newer segment",
			segments: [][]write{nil, {add(x), del(x), add(y)}},
			want:     [][]byte{y},
		},
		{
			name:     "added and deleted in the older segment, the key not in the newer",
			segments: [][]write{{add(x, y), del(x)}, nil},
			want:     [][]byte{y},
		},
		{
			name:     "deleted and added again in one segment",
			segments: [][]write{nil, {add(x), del(x), add(x)}},
			want:     [][]byte{x},
		},
		{
			name:     "added and deleted in the middle one of three segments",
			segments: [][]write{nil, {add(x), del(x), add(y)}, nil},
			want:     [][]byte{y},
		},
		{
			name:     "added and deleted in the last one of three segments",
			segments: [][]write{nil, nil, {add(x), del(x), add(y)}},
			want:     [][]byte{y},
		},
		{
			name:     "added in one segment and deleted in the next",
			segments: [][]write{{add(x)}, {del(x), add(y)}},
			want:     [][]byte{y},
		},
		{
			name:     "added in one segment, deleted two segments later",
			segments: [][]write{{add(x)}, nil, {del(x), add(y)}},
			want:     [][]byte{y},
		},
		{
			name:     "nothing deleted",
			segments: [][]write{nil, {add(x, y)}},
			want:     [][]byte{x, y},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			logger, _ := test.NewNullLogger()
			bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(StrategySetCollection), WithForceCompaction(true))
			require.NoError(t, err)
			defer func() { require.NoError(t, bucket.Shutdown(ctx)) }()
			// so big it never triggers as part of this test
			bucket.SetMemtableThreshold(1e9)

			key := []byte("key")
			for i, writes := range tt.segments {
				// every segment holds something, so that it is written
				require.NoError(t, bucket.SetAdd(otherKey(i), [][]byte{[]byte("z")}))
				for _, write := range writes {
					write(t, bucket, key)
				}
				require.NoError(t, bucket.FlushAndSwitch())
			}
			got, err := bucket.SetList(key)
			require.NoError(t, err)
			require.ElementsMatch(t, tt.want, got, "before any compaction")

			rounds := 0
			for {
				compacted, err := bucket.disk.compactOnce(ctx)
				require.NoError(t, err)
				if !compacted {
					break
				}
				rounds++
				got, err := bucket.SetList(key)
				require.NoError(t, err)
				require.ElementsMatchf(t, tt.want, got, "after compaction %d", rounds)
			}
			require.Equal(t, len(tt.segments)-1, rounds, "the segments end up as one")
		})
	}
}

// Every strategy keeps what was deleted deleted through the compactions: an
// entry written and deleted between two flushes, under a key that only one of
// the segments holds, the others holding other keys only.
func TestCompactionKeepsDeletedEntriesDeletedInEveryStrategy(t *testing.T) {
	ctx := context.Background()
	key, kept, gone := []byte("key"), []byte("kept"), []byte("gone")
	const rangeKey, keptDoc, goneDoc = uint64(5), uint64(7), uint64(8)
	strategies := []struct {
		name     string
		strategy string
		// other writes a key of its own to segment i, so that it is written
		other func(t *testing.T, b *Bucket, i int)
		// add writes the entry that stays and the one that goes, del deletes
		// the one that goes
		add, del func(t *testing.T, b *Bucket)
		check    func(t *testing.T, b *Bucket, at string)
	}{
		{
			name: "set", strategy: StrategySetCollection,
			other: func(t *testing.T, b *Bucket, i int) {
				require.NoError(t, b.SetAdd(otherKey(i), [][]byte{[]byte("z")}))
			},
			add: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.SetAdd(key, [][]byte{kept, gone}))
			},
			del: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.SetDeleteSingle(key, gone))
			},
			check: func(t *testing.T, b *Bucket, at string) {
				got, err := b.SetList(key)
				require.NoError(t, err)
				require.Equal(t, [][]byte{kept}, got, at)
			},
		},
		{
			name: "map", strategy: StrategyMapCollection,
			other: func(t *testing.T, b *Bucket, i int) {
				require.NoError(t, b.MapSet(otherKey(i), MapPair{Key: []byte("z"), Value: []byte("z")}))
			},
			add: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.MapSet(key, MapPair{Key: kept, Value: []byte("1")}))
				require.NoError(t, b.MapSet(key, MapPair{Key: gone, Value: []byte("2")}))
			},
			del: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.MapDeleteKey(key, gone))
			},
			check: func(t *testing.T, b *Bucket, at string) {
				got, err := b.MapList(ctx, key)
				require.NoError(t, err)
				require.Len(t, got, 1, at)
				require.Equal(t, kept, got[0].Key, at)
			},
		},
		{
			name: "roaring set", strategy: StrategyRoaringSet,
			other: func(t *testing.T, b *Bucket, i int) {
				require.NoError(t, b.RoaringSetAddOne(otherKey(i), 1))
			},
			add: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.RoaringSetAddList(key, []uint64{keptDoc, goneDoc}))
			},
			del: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.RoaringSetRemoveOne(key, goneDoc))
			},
			check: func(t *testing.T, b *Bucket, at string) {
				got, release, err := b.RoaringSetGet(ctx, key)
				require.NoError(t, err)
				defer release()
				require.Equal(t, []uint64{keptDoc}, got.ToArray(), at)
			},
		},
		{
			name: "roaring set range", strategy: StrategyRoaringSetRange,
			other: func(t *testing.T, b *Bucket, i int) {
				require.NoError(t, b.RoaringSetRangeAdd(100+uint64(i), 100+uint64(i)))
			},
			add: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.RoaringSetRangeAdd(rangeKey, keptDoc, goneDoc))
			},
			del: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.RoaringSetRangeRemove(rangeKey, goneDoc))
			},
			check: func(t *testing.T, b *Bucket, at string) {
				reader := b.ReaderRoaringSetRange()
				defer reader.Close()
				for _, op := range []filters.Operator{filters.OperatorEqual, filters.OperatorLessThanEqual} {
					got, release, err := reader.Read(ctx, rangeKey, op)
					require.NoError(t, err)
					require.Equal(t, []uint64{keptDoc}, got.ToArray(), "%s: %s", at, op.Name())
					release()
				}
			},
		},
		{
			name: "inverted", strategy: StrategyInverted,
			other: func(t *testing.T, b *Bucket, i int) {
				require.NoError(t, b.InvertedSet(otherKey(i), 100+uint64(i), 1, 1))
			},
			add: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.InvertedSet(key, keptDoc, 1, 1))
				require.NoError(t, b.InvertedSet(key, goneDoc, 1, 1))
			},
			del: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.InvertedDeleteDoc(key, goneDoc))
			},
			check: func(t *testing.T, b *Bucket, at string) {
				got, err := b.DocPointerWithScoreList(ctx, key, 1)
				require.NoError(t, err)
				ids := make([]uint64, len(got))
				for i := range got {
					ids[i] = got[i].Id
				}
				require.Equal(t, []uint64{keptDoc}, ids, at)
			},
		},
		{
			name: "replace", strategy: StrategyReplace,
			other: func(t *testing.T, b *Bucket, i int) {
				require.NoError(t, b.Put(otherKey(i), []byte("z")))
			},
			add: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.Put(kept, []byte("1")))
				require.NoError(t, b.Put(gone, []byte("2")))
			},
			del: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.Delete(gone))
			},
			check: func(t *testing.T, b *Bucket, at string) {
				got, err := b.Get(kept)
				require.NoError(t, err)
				require.Equal(t, []byte("1"), got, at)
				got, err = b.Get(gone)
				require.NoError(t, err)
				require.Nil(t, got, at)
			},
		},
	}
	layouts := []struct {
		name string
		// segments is how many segments are written; the entries are added
		// in segment addAt and deleted in segment delAt, counted from the
		// oldest
		segments, addAt, delAt int
	}{
		{name: "added and deleted in the newer of two segments", segments: 2, addAt: 1, delAt: 1},
		{name: "added and deleted in the older of two segments", segments: 2, addAt: 0, delAt: 0},
		{name: "added and deleted in the middle one of three segments", segments: 3, addAt: 1, delAt: 1},
		{name: "added and deleted in the last one of three segments", segments: 3, addAt: 2, delAt: 2},
		{name: "added in one segment and deleted in the next", segments: 2, addAt: 0, delAt: 1},
		{name: "added in one segment and deleted two segments later", segments: 3, addAt: 0, delAt: 2},
	}
	for _, st := range strategies {
		for _, layout := range layouts {
			t.Run(st.name+"/"+layout.name, func(t *testing.T) {
				dir := t.TempDir()
				logger, _ := test.NewNullLogger()
				bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
					WithStrategy(st.strategy), WithForceCompaction(true), WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
				require.NoError(t, err)
				defer func() { require.NoError(t, bucket.Shutdown(ctx)) }()
				bucket.SetMemtableThreshold(1e9)

				for i := 0; i < layout.segments; i++ {
					st.other(t, bucket, i)
					if i == layout.addAt {
						st.add(t, bucket)
					}
					if i == layout.delAt {
						st.del(t, bucket)
						st.check(t, bucket, "in the memtable")
					}
					require.NoError(t, bucket.FlushAndSwitch())
				}
				st.check(t, bucket, "flushed")

				rounds := 0
				for {
					compacted, err := bucket.disk.compactOnce(ctx)
					require.NoError(t, err)
					if !compacted {
						break
					}
					rounds++
					st.check(t, bucket, fmt.Sprintf("after compaction %d", rounds))
				}
				require.Equal(t, layout.segments-1, rounds, "the segments end up as one")
			})
		}
	}
}

func otherKey(i int) []byte {
	return []byte{'o', 't', 'h', 'e', 'r', byte('0' + i)}
}
