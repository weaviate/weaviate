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

	t.Run("unflushed additions are counted", func(t *testing.T) {
		b := newInverted(t)
		addDocs(t, b, "a", 1)
		addDocs(t, b, "b", 2)
		require.NoError(t, b.FlushAndSwitch())
		addDocs(t, b, "a", 3) // unflushed, on a key the segment holds
		addDocs(t, b, "c", 4) // unflushed, memtable-only key

		got, exceeded, exact := collect(t, b, 10)
		require.False(t, exceeded)
		require.True(t, exact)
		assert.Equal(t, map[string]int{"a": 2, "b": 1, "c": 1}, got)
	})

	t.Run("a doc written twice for a key counts once", func(t *testing.T) {
		b := newInverted(t)
		addDocs(t, b, "a", 1)
		addDocs(t, b, "a", 1)

		got, exceeded, exact := collect(t, b, 10)
		require.False(t, exceeded)
		require.True(t, exact)
		assert.Equal(t, map[string]int{"a": 1}, got)
	})

	t.Run("unflushed tombstone voids the guarantee", func(t *testing.T) {
		b := newInverted(t)
		addDocs(t, b, "a", 1, 2)
		require.NoError(t, b.FlushAndSwitch())
		pair := NewMapPairFromDocIdAndTf(2, 1, 1, false)
		require.NoError(t, b.MapDeleteKey([]byte("a"), pair.Key)) // unflushed

		got, exceeded, exact := collect(t, b, 10)
		require.False(t, exceeded)
		require.False(t, exact)
		assert.Empty(t, got, "fn must not run without the exactness guarantee")
	})

	t.Run("unflushed keys count toward the limit", func(t *testing.T) {
		b := newInverted(t)
		addDocs(t, b, "a", 1)
		addDocs(t, b, "b", 2)
		require.NoError(t, b.FlushAndSwitch())
		addDocs(t, b, "c", 3) // unflushed, third distinct key

		_, exceeded, exact := collect(t, b, 2)
		require.True(t, exceeded)
		require.False(t, exact)
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

	// a key set the bloom estimate overshoots on: a bucket sitting exactly at
	// the limit must not be rejected on the estimate alone
	t.Run("overshooting estimate at the limit still walks", func(t *testing.T) {
		const keys = 432
		b := newInverted(t)
		for i := 0; i < keys; i++ {
			addDocs(t, b, fmt.Sprintf("term-%06d", i), uint64(i))
		}
		require.NoError(t, b.FlushAndSwitch())

		estimate, err := b.GetKeysCount()
		require.NoError(t, err)
		require.Greater(t, estimate, uint32(keys),
			"key set no longer overshoots; pick another that does")

		got, exceeded, exact := collect(t, b, keys)
		require.False(t, exceeded)
		require.True(t, exact)
		require.Len(t, got, keys)
		for i := 0; i < keys; i++ {
			assert.Equal(t, 1, got[fmt.Sprintf("term-%06d", i)])
		}
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

// Ingest is the state the walk now reads through rather than bailing on, so it
// has to hold up while writes land in the memtable it is walking and flushes
// and compactions move them underneath it.
func TestInvertedEachDistinctKeyConcurrentIngest(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted), WithUseBloomFilter(true))
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Shutdown(ctx) })
	b.SetMemtableThreshold(1e9)

	const keys = 12
	key := func(i int) []byte { return []byte(fmt.Sprintf("key-%02d", i)) }

	for i := range keys {
		require.NoError(t, b.MapSet(key(i), NewMapPairFromDocIdAndTf(uint64(i), 1, 1, false)))
	}
	require.NoError(t, b.FlushAndSwitch())

	highest := map[string]int{}
	walkUnderChurn(t, b, func(n int) error {
		// every doc id is written once, for one key: nothing is ever deleted or
		// rewritten, so counts only ever grow
		docID := uint64(keys + n)
		return b.MapSet(key(n%keys), NewMapPairFromDocIdAndTf(docID, 1, 1, false))
	}, func() {
		seen := map[string]int{}
		exceeded, exact, err := b.InvertedEachDistinctKey(ctx, keys*4,
			func(k []byte, docCount int) error {
				if _, dup := seen[string(k)]; dup {
					return fmt.Errorf("key %q emitted twice in one walk", k)
				}
				seen[string(k)] = docCount
				return nil
			})
		require.NoError(t, err)
		require.False(t, exceeded)
		require.True(t, exact, "nothing is deleted, so the counts stay provable")
		require.Len(t, seen, keys)
		for i := range keys {
			count := seen[string(key(i))]
			require.GreaterOrEqualf(t, count, highest[string(key(i))],
				"key %s lost docs it had already reported", key(i))
			highest[string(key(i))] = count
		}
	})

	total := 0
	for _, count := range highest {
		total += count
	}
	require.Greater(t, total, keys, "the writer never got ahead of the reader")
}
