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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/filters"
)

func TestParseLeafCacheMaxMemory(t *testing.T) {
	tests := []struct {
		name     string
		env      string
		expected int
		wantErr  bool
	}{
		{name: "unset uses default", env: "", expected: DefaultLeafCacheMaxMemory},
		{name: "zero disables", env: "0", expected: 0},
		{name: "plain bytes", env: "1048576", expected: 1 << 20},
		{name: "binary unit", env: "16MiB", expected: 16 << 20},
		{name: "decimal unit", env: "100MB", expected: 100_000_000},
		// the reason this reports rather than swallows: a doubled unit, a stray
		// space and a transposed unit are all things an operator actually types,
		// and every one of them used to look exactly like the variable being unset
		{name: "garbage", env: "sixteen", expected: DefaultLeafCacheMaxMemory, wantErr: true},
		{name: "negative", env: "-1", expected: DefaultLeafCacheMaxMemory, wantErr: true},
		{name: "doubled unit", env: "64MiBB", expected: DefaultLeafCacheMaxMemory, wantErr: true},
		// humanize tolerates a trailing space but not a leading one
		{name: "trailing space", env: "64 MiB ", expected: 64 << 20},
		{name: "leading space", env: " 64MiB", expected: DefaultLeafCacheMaxMemory, wantErr: true},
		{name: "transposed unit", env: "64iMB", expected: DefaultLeafCacheMaxMemory, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseLeafCacheMaxMemory(tt.env)
			assert.Equal(t, tt.expected, got)
			if !tt.wantErr {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), LeafCacheMaxMemoryEnv, "the operator cannot act on an unnamed variable")
			assert.Contains(t, err.Error(), tt.env, "the offending value must be quoted back")
		})
	}
}

// A budget that fell back to the default has to say so. Silence makes a
// mistyped budget indistinguishable from an unset one, and the operator then
// believes they configured something they did not get.
func TestLogLeafCacheConfig(t *testing.T) {
	tests := []struct {
		name      string
		envValue  string
		envErr    error
		maxBytes  int
		wantLevel logrus.Level
		wantIn    []string
	}{
		{
			name:     "unparseable warns and names both the variable and the value",
			envValue: "64MiBB", envErr: fmt.Errorf("%s: %q: invalid size", LeafCacheMaxMemoryEnv, "64MiBB"),
			maxBytes:  DefaultLeafCacheMaxMemory,
			wantLevel: logrus.WarnLevel,
			wantIn:    []string{LeafCacheMaxMemoryEnv, "64MiBB", "16 MiB"},
		},
		{
			name:     "zero reports the kill switch, which no counter can show",
			envValue: "0", maxBytes: 0,
			wantLevel: logrus.InfoLevel,
			wantIn:    []string{LeafCacheMaxMemoryEnv, "disables"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prevValue, prevErr, prevMax := leafCacheEnvValue, leafCacheEnvErr, leafCacheMaxMemory
			prevLogged := leafCacheLogged.Swap(false)
			t.Cleanup(func() {
				leafCacheEnvValue, leafCacheEnvErr, leafCacheMaxMemory = prevValue, prevErr, prevMax
				leafCacheLogged.Store(prevLogged)
			})
			leafCacheEnvValue, leafCacheEnvErr, leafCacheMaxMemory = tt.envValue, tt.envErr, tt.maxBytes

			logger, hook := test.NewNullLogger()
			logLeafCacheConfig(logger)

			require.Len(t, hook.Entries, 1)
			assert.Equal(t, tt.wantLevel, hook.LastEntry().Level)
			for _, want := range tt.wantIn {
				assert.Contains(t, hook.LastEntry().Message, want)
			}
		})
	}
}

// A valid budget stays quiet: a line on every healthy boot trains operators to
// ignore the one that matters.
func TestLogLeafCacheConfigStaysQuietWhenValid(t *testing.T) {
	prevErr, prevMax := leafCacheEnvErr, leafCacheMaxMemory
	prevLogged := leafCacheLogged.Swap(false)
	t.Cleanup(func() {
		leafCacheEnvErr, leafCacheMaxMemory = prevErr, prevMax
		leafCacheLogged.Store(prevLogged)
	})
	leafCacheEnvErr, leafCacheMaxMemory = nil, 32<<20

	logger, hook := test.NewNullLogger()
	logLeafCacheConfig(logger)
	assert.Empty(t, hook.Entries)
}

func TestNewLeafCacheDisabled(t *testing.T) {
	for _, maxBytes := range []int{0, -1} {
		c := newLeafCache(maxBytes)
		require.Nil(t, c)

		// a nil cache must be usable without nil checks at every call site
		bm, admit := c.probe(0, leafKey{}, 0)
		assert.Nil(t, bm)
		assert.False(t, admit)
		c.store(0, leafKey{}, roaringset.NewBitmap(1))
	}
}

func TestLeafCacheAdmitsOnSecondSight(t *testing.T) {
	c := newLeafCache(1 << 20)
	key := leafKey{kind: leafGreaterThanEqual, valueMin: 42}
	bm := roaringset.NewBitmap(1, 2, 3)

	bmOut, admit := c.probe(0, key, 0)
	require.Nil(t, bmOut)
	require.False(t, admit, "first sight must not be admitted")

	// store never checks the admission filter itself, only probe does
	bmOut, admit = c.probe(0, key, 0)
	require.Nil(t, bmOut)
	require.True(t, admit, "second sight must be admitted")

	c.store(0, key, bm)

	bmOut, admit = c.probe(0, key, 0)
	assert.Same(t, bm, bmOut)
	assert.False(t, admit)
}

func TestLeafCacheDoesNotRetainOneShotKeys(t *testing.T) {
	c := newLeafCache(1 << 20)

	// every key distinct: the long-tail workload the cache must not tax
	for i := uint64(0); i < 10_000; i++ {
		bm, admit := c.probe(0, leafKey{kind: leafGreaterThanEqual, valueMin: i}, 0)
		require.Nil(t, bm)
		require.False(t, admit)
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	assert.Empty(t, c.entries)
	assert.Zero(t, c.bytes)
}

func TestLeafCacheStopsAdmittingWhenFull(t *testing.T) {
	// three values per bitmap, so every entry has the same size
	entrySize := roaringset.NewBitmap(1, 2, 3).LenInBytes()

	c := newLeafCache(2 * entrySize)
	for i := uint64(0); i < 5; i++ {
		key := leafKey{kind: leafGreaterThanEqual, valueMin: i}
		c.probe(0, key, entrySize)
		if _, admit := c.probe(0, key, entrySize); admit {
			c.store(0, key, roaringset.NewBitmap(1, 2, 3))
		}
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	assert.LessOrEqual(t, c.bytes, 2*entrySize)
	require.Len(t, c.entries, 2)
	// the first two admitted keep their slots; a wider working set must not
	// churn them via a clone-and-discard per query
	assert.Equal(t, uint64(0), c.entries[0].key.valueMin)
	assert.Equal(t, uint64(1), c.entries[1].key.valueMin)
}

func TestLeafCacheDoesNotAdmitWhenTheEntryCannotFit(t *testing.T) {
	entrySize := roaringset.NewBitmap(1, 2, 3).LenInBytes()
	c := newLeafCache(entrySize)
	key := leafKey{kind: leafGreaterThanEqual, valueMin: 1}

	c.probe(0, key, 10*entrySize)
	_, admit := c.probe(0, key, 10*entrySize)
	assert.False(t, admit, "admission must be declined before the caller pays for a clone")

	// and store refuses too, in case the caller's bound was optimistic
	c.store(0, key, roaringset.NewBitmap(bigBitmapValues(10)...))

	c.lock.Lock()
	defer c.lock.Unlock()
	assert.Empty(t, c.entries)
}

// bigBitmapValues returns values one sroar container apart, so the resulting
// bitmap is genuinely wide rather than a single array container.
func bigBitmapValues(n int) []uint64 {
	out := make([]uint64, n)
	for i := range out {
		out[i] = uint64(i) << 16
	}
	return out
}

func TestLeafCacheGenerations(t *testing.T) {
	c := newLeafCache(1 << 20)
	key := leafKey{kind: leafGreaterThanEqual, valueMin: 7}
	bm := roaringset.NewBitmap(1, 2, 3)

	c.probe(0, key, 0)
	c.probe(0, key, 0)
	c.store(0, key, bm)

	t.Run("newer generation drops the entry", func(t *testing.T) {
		cached, admit := c.probe(1, key, 0)
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

		cached, admit := c.probe(0, key, 0)
		assert.Nil(t, cached)
		assert.False(t, admit)

		// and the newer generation's entry is untouched
		cached, _ = c.probe(1, key, 0)
		assert.Same(t, bm, cached)
	})
}

func TestLeafCacheKeySeparatesBetweenRanges(t *testing.T) {
	c := newLeafCache(1 << 20)
	a := leafKey{kind: leafBetween, valueMin: 1, valueMax: 2}
	b := leafKey{kind: leafBetween, valueMin: 1, valueMax: 100}
	gte := leafKey{kind: leafGreaterThanEqual, valueMin: 1}

	bmA := roaringset.NewBitmap(1)
	c.probe(0, a, 0)
	c.probe(0, a, 0)
	c.store(0, a, bmA)

	cached, _ := c.probe(0, b, 0)
	assert.Nil(t, cached, "a different upper bound must not share an entry")

	cached, _ = c.probe(0, gte, 0)
	assert.Nil(t, cached, "a different merge shape must not share an entry")
}

// TestLeafCacheDropDuringClone pins that probe's returned pointer stays valid
// even if a concurrent generation change drops the entry before the caller
// clones it: dropping only releases the reference, never the buffer.
func TestLeafCacheDropDuringClone(t *testing.T) {
	c := newLeafCache(1 << 20)
	key := leafKey{kind: leafGreaterThanEqual, valueMin: 7}
	ref := roaringset.NewBitmap(bigBitmapValues(64)...)
	want := ref.ToArray()

	var (
		generation atomic.Uint64
		wg         sync.WaitGroup
		clones     atomic.Int64
	)
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(1); ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			generation.Store(i)
			// first probe drops the previous generation, second admits
			c.probe(i, key, 1<<20)
			if _, admit := c.probe(i, key, 1<<20); admit {
				c.store(i, key, ref.Clone())
			}
		}
	}()

	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				bm, _ := c.probe(generation.Load(), key, 1<<20)
				if bm == nil {
					continue
				}
				assert.Equal(t, want, bm.Clone().ToArray())
				clones.Add(1)
			}
		}()
	}

	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()

	require.Greater(t, clones.Load(), int64(0), "no entry was ever served, the test would be vacuous")
}

// The three operator states have to be three readings. Before this counter,
// a disabled cache and an unexercised one both showed zero hits and zero
// misses, so a dashboard or a gate arming on them could not tell "off" from
// "never hit" — and QA has armed gates on exactly these counters.
func TestLeafCacheDisabledIsObservable(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mt1, mt2, mt3 := createTestMemtables(logger)

	seg := newCachedSegment(logger, 0)
	require.Nil(t, seg.leafCache, "this test is about the nil-cache path")

	seg.MergeMemtableEventually(mt1)
	seg.MergeMemtableEventually(mt2)
	seg.MergeMemtableEventually(mt3)
	waitUntilMemtablesMerged(t, seg)

	before := map[string]float64{
		"disabled": testutil.ToFloat64(leafCacheDisabled),
		"hit":      testutil.ToFloat64(leafCacheHits),
		"miss":     testutil.ToFloat64(leafCacheMisses),
	}

	for round := 0; round < 3; round++ {
		query(t, seg, 13, filters.OperatorGreaterThanEqual)
	}

	assert.Greater(t, testutil.ToFloat64(leafCacheDisabled), before["disabled"],
		"queries flowed through a disabled cache and nothing recorded it")
	assert.Equal(t, before["hit"], testutil.ToFloat64(leafCacheHits))
	assert.Equal(t, before["miss"], testutil.ToFloat64(leafCacheMisses),
		"a disabled cache must not look like a cache that is missing")
}
