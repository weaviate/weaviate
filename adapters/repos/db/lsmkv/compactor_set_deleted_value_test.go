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
// The inverted index deletes the doc id of a deleted object from the sets of
// its property values this way (SetDeleteSingle), so an object added and
// deleted between two flushes could come back in a filter.
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
				require.NoError(t, bucket.SetAdd([]byte{'o', 't', 'h', 'e', 'r', byte('0' + i)}, [][]byte{[]byte("z")}))
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

// The other strategies keep what was deleted deleted through the same
// compaction: an entry written and deleted between two flushes, under a key
// that only the newer of two segments holds.
func TestCompactionKeepsDeletedEntriesDeletedInEveryStrategy(t *testing.T) {
	ctx := context.Background()
	key, kept, gone := []byte("key"), []byte("kept"), []byte("gone")
	tests := []struct {
		name     string
		strategy string
		write    func(t *testing.T, b *Bucket)
		check    func(t *testing.T, b *Bucket, at string)
	}{
		{
			name: "set", strategy: StrategySetCollection,
			write: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.SetAdd(key, [][]byte{kept, gone}))
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
			write: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.MapSet(key, MapPair{Key: kept, Value: []byte("1")}))
				require.NoError(t, b.MapSet(key, MapPair{Key: gone, Value: []byte("2")}))
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
			write: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.RoaringSetAddList(key, []uint64{7, 8}))
				require.NoError(t, b.RoaringSetRemoveOne(key, 8))
			},
			check: func(t *testing.T, b *Bucket, at string) {
				got, release, err := b.RoaringSetGet(ctx, key)
				require.NoError(t, err)
				defer release()
				require.Equal(t, []uint64{7}, got.ToArray(), at)
			},
		},
		{
			name: "replace", strategy: StrategyReplace,
			write: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.Put(kept, []byte("1")))
				require.NoError(t, b.Put(gone, []byte("2")))
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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			logger, _ := test.NewNullLogger()
			bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(tt.strategy), WithForceCompaction(true), WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
			require.NoError(t, err)
			defer func() { require.NoError(t, bucket.Shutdown(ctx)) }()
			bucket.SetMemtableThreshold(1e9)

			// the older segment, with another key
			other := []byte("another")
			switch tt.strategy {
			case StrategySetCollection:
				require.NoError(t, bucket.SetAdd(other, [][]byte{[]byte("z")}))
			case StrategyMapCollection:
				require.NoError(t, bucket.MapSet(other, MapPair{Key: []byte("z"), Value: []byte("z")}))
			case StrategyRoaringSet:
				require.NoError(t, bucket.RoaringSetAddOne(other, 1))
			case StrategyReplace:
				require.NoError(t, bucket.Put(other, []byte("z")))
			}
			require.NoError(t, bucket.FlushAndSwitch())
			tt.write(t, bucket)
			tt.check(t, bucket, "in the memtable")
			require.NoError(t, bucket.FlushAndSwitch())
			tt.check(t, bucket, "flushed")

			compacted, err := bucket.disk.compactOnce(ctx)
			require.NoError(t, err)
			require.True(t, compacted)
			tt.check(t, bucket, "compacted")
		})
	}
}
