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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestRoaringSetEachDistinctKey(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	newBucket := func(t *testing.T, strategy string) *Bucket {
		t.Helper()
		opts := []BucketOption{WithStrategy(strategy), WithUseBloomFilter(true)}
		if strategy == StrategyRoaringSet {
			opts = append(opts, WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
		}
		b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.NoError(t, err)
		t.Cleanup(func() { _ = b.Shutdown(ctx) })
		b.SetMemtableThreshold(1e9)
		return b
	}

	// two segments plus a memtable, with cross-layer additions and deletions:
	// a lives in both segments, b and dead are fully deleted by the second
	// segment, c spans the second segment and the memtable, d is
	// memtable-only. Index-distinct is 5 (a, b, c, d, dead), live is 3.
	newPopulated := func(t *testing.T) *Bucket {
		t.Helper()
		b := newBucket(t, StrategyRoaringSet)
		require.NoError(t, b.RoaringSetAddList([]byte("a"), []uint64{1, 2, 3}))
		require.NoError(t, b.RoaringSetAddList([]byte("b"), []uint64{4}))
		require.NoError(t, b.RoaringSetAddList([]byte("dead"), []uint64{9}))
		require.NoError(t, b.FlushAndSwitch())
		require.NoError(t, b.RoaringSetAddList([]byte("a"), []uint64{5}))
		require.NoError(t, b.RoaringSetAddList([]byte("c"), []uint64{6}))
		require.NoError(t, b.RoaringSetRemoveOne([]byte("b"), 4))
		require.NoError(t, b.RoaringSetRemoveOne([]byte("dead"), 9))
		require.NoError(t, b.FlushAndSwitch())
		require.NoError(t, b.RoaringSetAddList([]byte("c"), []uint64{7}))
		require.NoError(t, b.RoaringSetAddList([]byte("d"), []uint64{8}))
		return b
	}

	collect := func(t *testing.T, b *Bucket, maxDistinct int) (map[string]int, bool) {
		t.Helper()
		got := map[string]int{}
		exceeded, err := b.RoaringSetEachDistinctKey(ctx, maxDistinct,
			func(key []byte, liveCount int) error {
				got[string(key)] = liveCount
				return nil
			})
		require.NoError(t, err)
		return got, exceeded
	}

	t.Run("live counts merge across segments and memtable", func(t *testing.T) {
		got, exceeded := collect(t, newPopulated(t), 10)
		require.False(t, exceeded)
		assert.Equal(t, map[string]int{"a": 4, "c": 2, "d": 1}, got)
	})

	t.Run("index-distinct exactly at the limit passes", func(t *testing.T) {
		got, exceeded := collect(t, newPopulated(t), 5)
		require.False(t, exceeded)
		assert.Len(t, got, 3)
	})

	t.Run("deleted keys count toward the limit", func(t *testing.T) {
		// only 3 keys are live, but the index holds 5
		_, exceeded := collect(t, newPopulated(t), 4)
		require.True(t, exceeded)
	})

	t.Run("far over the limit rejects without emitting", func(t *testing.T) {
		b := newBucket(t, StrategyRoaringSet)
		for i := 0; i < 3000; i++ {
			require.NoError(t, b.RoaringSetAddList(
				[]byte(fmt.Sprintf("value-%06d", i)), []uint64{uint64(i)}))
		}
		require.NoError(t, b.FlushAndSwitch())

		calls := 0
		exceeded, err := b.RoaringSetEachDistinctKey(ctx, 100,
			func([]byte, int) error {
				calls++
				return nil
			})
		require.NoError(t, err)
		require.True(t, exceeded)
		assert.Zero(t, calls)
	})

	t.Run("wrong strategy errors", func(t *testing.T) {
		b := newBucket(t, StrategyReplace)
		_, err := b.RoaringSetEachDistinctKey(ctx, 10, func([]byte, int) error { return nil })
		require.Error(t, err)
	})

	t.Run("cancelled context stops the walk", func(t *testing.T) {
		b := newPopulated(t)
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, err := b.RoaringSetEachDistinctKey(cancelled, 10, func([]byte, int) error { return nil })
		require.ErrorIs(t, err, context.Canceled)
	})
}
