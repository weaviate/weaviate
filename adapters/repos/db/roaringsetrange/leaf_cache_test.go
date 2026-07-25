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

package roaringsetrange

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

func TestParseLeafCacheMaxMemory(t *testing.T) {
	tests := []struct {
		name     string
		env      string
		expected int
	}{
		{name: "unset uses default", env: "", expected: DefaultLeafCacheMaxMemory},
		{name: "zero disables", env: "0", expected: 0},
		{name: "plain bytes", env: "1048576", expected: 1 << 20},
		{name: "binary unit", env: "16MiB", expected: 16 << 20},
		{name: "decimal unit", env: "100MB", expected: 100_000_000},
		{name: "garbage falls back to default", env: "sixteen", expected: DefaultLeafCacheMaxMemory},
		{name: "negative falls back to default", env: "-1", expected: DefaultLeafCacheMaxMemory},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, parseLeafCacheMaxMemory(tt.env))
		})
	}
}

func TestNewLeafCacheDisabled(t *testing.T) {
	for _, maxBytes := range []int{0, -1} {
		c := newLeafCache(maxBytes)
		require.Nil(t, c)

		// a nil cache must be usable without nil checks at every call site
		bm, admit := c.probe(0, leafKey{})
		assert.Nil(t, bm)
		assert.False(t, admit)
		c.store(0, leafKey{}, roaringset.NewBitmap(1))
	}
}

func TestLeafCacheAdmitsOnSecondSight(t *testing.T) {
	c := newLeafCache(1 << 20)
	key := leafKey{kind: leafGreaterThanEqual, valueMin: 42}
	bm := roaringset.NewBitmap(1, 2, 3)

	bmOut, admit := c.probe(0, key)
	require.Nil(t, bmOut)
	require.False(t, admit, "first sight must not be admitted")

	// a store that was never admitted is still accepted; the point of the
	// filter is that the read path does not ask for one
	bmOut, admit = c.probe(0, key)
	require.Nil(t, bmOut)
	require.True(t, admit, "second sight must be admitted")

	c.store(0, key, bm)

	bmOut, admit = c.probe(0, key)
	assert.Same(t, bm, bmOut)
	assert.False(t, admit)
}

func TestLeafCacheDoesNotRetainOneShotKeys(t *testing.T) {
	c := newLeafCache(1 << 20)

	// every key distinct: the long-tail workload the cache must not tax
	for i := uint64(0); i < 10_000; i++ {
		bm, admit := c.probe(0, leafKey{kind: leafGreaterThanEqual, valueMin: i})
		require.Nil(t, bm)
		require.False(t, admit)
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	assert.Empty(t, c.entries)
	assert.Zero(t, c.bytes)
}

func TestLeafCacheEnforcesByteBudget(t *testing.T) {
	// three values per bitmap, so every entry has the same size
	sized := roaringset.NewBitmap(1, 2, 3)
	entrySize := sized.LenInBytes()

	c := newLeafCache(2 * entrySize)
	for i := uint64(0); i < 5; i++ {
		key := leafKey{kind: leafGreaterThanEqual, valueMin: i}
		c.probe(0, key)
		c.probe(0, key)
		c.store(0, key, roaringset.NewBitmap(1, 2, 3))
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	assert.LessOrEqual(t, c.bytes, 2*entrySize)
	assert.Len(t, c.entries, 2)
	// FIFO: the two most recent survive
	assert.Equal(t, uint64(3), c.entries[0].key.valueMin)
	assert.Equal(t, uint64(4), c.entries[1].key.valueMin)
}

func TestLeafCacheRejectsOversizedEntry(t *testing.T) {
	small := roaringset.NewBitmap(1)
	// one value per sroar container, so the bitmap is genuinely wider
	bigValues := make([]uint64, 10)
	for i := range bigValues {
		bigValues[i] = uint64(i) << 16
	}
	big := roaringset.NewBitmap(bigValues...)
	require.Greater(t, big.LenInBytes(), small.LenInBytes())

	c := newLeafCache(small.LenInBytes())

	smallKey := leafKey{kind: leafGreaterThanEqual, valueMin: 1}
	c.probe(0, smallKey)
	c.probe(0, smallKey)
	c.store(0, smallKey, small)

	bigKey := leafKey{kind: leafGreaterThanEqual, valueMin: 2}
	c.probe(0, bigKey)
	c.probe(0, bigKey)
	c.store(0, bigKey, big)

	c.lock.Lock()
	defer c.lock.Unlock()
	require.Len(t, c.entries, 1, "an oversized leaf must not evict the cache to fit itself")
	assert.Equal(t, smallKey, c.entries[0].key)
}

func TestLeafCacheGenerations(t *testing.T) {
	c := newLeafCache(1 << 20)
	key := leafKey{kind: leafGreaterThanEqual, valueMin: 7}
	bm := roaringset.NewBitmap(1, 2, 3)

	c.probe(0, key)
	c.probe(0, key)
	c.store(0, key, bm)

	t.Run("newer generation drops the entry", func(t *testing.T) {
		cached, admit := c.probe(1, key)
		assert.Nil(t, cached)
		// the admission filter survives generation changes, so a hot key is
		// re-admitted immediately rather than after another miss
		assert.True(t, admit)
	})

	t.Run("store against a superseded generation is ignored", func(t *testing.T) {
		c.store(0, key, bm)

		c.lock.Lock()
		defer c.lock.Unlock()
		assert.Empty(t, c.entries)
	})

	t.Run("older reader is served nothing", func(t *testing.T) {
		c.store(1, key, bm)

		cached, admit := c.probe(0, key)
		assert.Nil(t, cached)
		assert.False(t, admit)

		// and the newer generation's entry is untouched
		cached, _ = c.probe(1, key)
		assert.Same(t, bm, cached)
	})
}

func TestLeafCacheKeySeparatesBetweenRanges(t *testing.T) {
	c := newLeafCache(1 << 20)
	a := leafKey{kind: leafBetween, valueMin: 1, valueMax: 2}
	b := leafKey{kind: leafBetween, valueMin: 1, valueMax: 100}
	gte := leafKey{kind: leafGreaterThanEqual, valueMin: 1}

	bmA := roaringset.NewBitmap(1)
	c.probe(0, a)
	c.probe(0, a)
	c.store(0, a, bmA)

	cached, _ := c.probe(0, b)
	assert.Nil(t, cached, "a different upper bound must not share an entry")

	cached, _ = c.probe(0, gte)
	assert.Nil(t, cached, "a different merge shape must not share an entry")
}
