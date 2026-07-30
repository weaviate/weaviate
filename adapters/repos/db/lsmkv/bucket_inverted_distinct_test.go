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

func TestInvertedEachDistinctKey(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	newInverted := func(t *testing.T) *Bucket {
		t.Helper()
		b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			WithStrategy(StrategyInverted), WithUseBloomFilter(true))
		require.NoError(t, err)
		t.Cleanup(func() { _ = b.Shutdown(ctx) })
		b.SetMemtableThreshold(1e9)
		return b
	}
	addDocs := func(t *testing.T, b *Bucket, key string, docIDs ...uint64) {
		t.Helper()
		for _, id := range docIDs {
			require.NoError(t, b.MapSet([]byte(key), NewMapPairFromDocIdAndTf(id, 1, 1, false)))
		}
	}
	collect := func(t *testing.T, b *Bucket, maxDistinct int) (map[string]int, bool, bool) {
		t.Helper()
		got := map[string]int{}
		exceeded, exact, err := b.InvertedEachDistinctKey(ctx, maxDistinct,
			func(key []byte, docCount int) error {
				got[string(key)] = docCount
				return nil
			})
		require.NoError(t, err)
		return got, exceeded, exact
	}

	t.Run("stored counts sum across segments", func(t *testing.T) {
		b := newInverted(t)
		addDocs(t, b, "a", 1, 2, 3)
		addDocs(t, b, "b", 4)
		require.NoError(t, b.FlushAndSwitch())
		addDocs(t, b, "a", 5)
		addDocs(t, b, "c", 6)
		require.NoError(t, b.FlushAndSwitch())

		got, exceeded, exact := collect(t, b, 10)
		require.False(t, exceeded)
		require.True(t, exact)
		assert.Equal(t, map[string]int{"a": 4, "b": 1, "c": 1}, got)
	})

	t.Run("exceeded past maxDistinct", func(t *testing.T) {
		b := newInverted(t)
		addDocs(t, b, "a", 1)
		addDocs(t, b, "b", 2)
		addDocs(t, b, "c", 3)
		require.NoError(t, b.FlushAndSwitch())

		_, exceeded, exact := collect(t, b, 2)
		require.True(t, exceeded)
		require.False(t, exact)
	})

	t.Run("segment tombstones void the guarantee", func(t *testing.T) {
		b := newInverted(t)
		addDocs(t, b, "a", 1, 2)
		require.NoError(t, b.FlushAndSwitch())
		pair := NewMapPairFromDocIdAndTf(2, 1, 1, false)
		require.NoError(t, b.MapDeleteKey([]byte("a"), pair.Key))
		require.NoError(t, b.FlushAndSwitch())

		got, exceeded, exact := collect(t, b, 10)
		require.False(t, exceeded)
		require.False(t, exact)
		assert.Empty(t, got, "fn must not run without the exactness guarantee")
	})

	t.Run("unflushed memtable state voids the guarantee", func(t *testing.T) {
		b := newInverted(t)
		addDocs(t, b, "a", 1)
		require.NoError(t, b.FlushAndSwitch())
		addDocs(t, b, "b", 2) // unflushed

		got, exceeded, exact := collect(t, b, 10)
		require.False(t, exceeded)
		require.False(t, exact)
		assert.Empty(t, got)
	})

	t.Run("non-inverted strategy is not exact", func(t *testing.T) {
		b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			WithStrategy(StrategyRoaringSet),
			WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
			WithUseBloomFilter(true))
		require.NoError(t, err)
		t.Cleanup(func() { _ = b.Shutdown(ctx) })

		got, exceeded, exact := collect(t, b, 10)
		require.False(t, exceeded)
		require.False(t, exact)
		assert.Empty(t, got)
	})

	t.Run("far over the limit rejects via the bloom bound", func(t *testing.T) {
		b := newInverted(t)
		for i := 0; i < 3000; i++ {
			addDocs(t, b, fmt.Sprintf("term-%06d", i), uint64(i))
		}
		require.NoError(t, b.FlushAndSwitch())

		_, exceeded, exact := collect(t, b, 100)
		require.True(t, exceeded)
		require.False(t, exact)
	})
}
