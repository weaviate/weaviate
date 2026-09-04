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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
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
		{name: "garbage falls back to default", env: "sixteen", expected: DefaultLeafCacheMaxMemory, wantErr: true},
		{name: "negative falls back to default", env: "-1", expected: DefaultLeafCacheMaxMemory, wantErr: true},
		{name: "doubled unit falls back to default", env: "64MiBB", expected: DefaultLeafCacheMaxMemory, wantErr: true},
		{name: "transposed unit falls back to default", env: "64iMB", expected: DefaultLeafCacheMaxMemory, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseLeafCacheMaxMemory(tt.env)
			assert.Equal(t, tt.expected, got)
			if tt.wantErr {
				assert.Error(t, err, "a budget that cannot be parsed must be reported, not swallowed")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// withLeafCacheEnv restages the package-level budget the way a process that
// booted with env would see it, and re-arms the once-guard so each case starts
// from an unwarned process.
func withLeafCacheEnv(t *testing.T, env string) {
	t.Helper()

	prevValue, prevMax, prevErr := leafCacheEnvValue, leafCacheMaxMemory, leafCacheEnvErr
	t.Cleanup(func() {
		leafCacheEnvValue, leafCacheMaxMemory, leafCacheEnvErr = prevValue, prevMax, prevErr
		leafCacheEnvWarned.Store(false)
	})

	leafCacheEnvValue = env
	leafCacheMaxMemory, leafCacheEnvErr = parseLeafCacheMaxMemory(env)
	leafCacheEnvWarned.Store(false)
}

// TestLeafCacheBudgetIsLoudWhenIgnored covers the signal an operator gets when
// their budget is dropped. The silent cases carry the information: a warning
// that fires for a good value tells nobody anything.
func TestLeafCacheBudgetIsLoudWhenIgnored(t *testing.T) {
	tests := []struct {
		name     string
		env      string
		wantWarn bool
	}{
		{name: "unparseable warns", env: "sixtyfour", wantWarn: true},
		{name: "doubled unit warns", env: "64MiBB", wantWarn: true},
		{name: "parseable is silent", env: "64MiB"},
		{name: "zero kill switch is silent", env: "0"},
		{name: "unset is silent", env: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withLeafCacheEnv(t, tt.env)

			logger, hook := test.NewNullLogger()
			// the real production call site, reached per segment
			NewSegmentInMemory(logger)

			if !tt.wantWarn {
				assert.Empty(t, hook.AllEntries(),
					"a budget that was honoured must not log")
				return
			}

			require.Len(t, hook.AllEntries(), 1)
			entry := hook.LastEntry()
			assert.Equal(t, logrus.WarnLevel, entry.Level)
			assert.Contains(t, entry.Message, LeafCacheMaxMemoryEnv,
				"the operator must be able to tell which setting was dropped")
			assert.Contains(t, entry.Message, humanize.IBytes(DefaultLeafCacheMaxMemory),
				"the message must name the budget actually in force")
			assert.Equal(t, tt.env, entry.Data["value"],
				"the rejected value must be echoed back")
			assert.NotNil(t, entry.Data[logrus.ErrorKey])
		})
	}
}

func TestLeafCacheBudgetWarnsOncePerProcess(t *testing.T) {
	withLeafCacheEnv(t, "sixtyfour")

	logger, hook := test.NewNullLogger()
	for range 3 {
		NewSegmentInMemory(logger)
	}

	assert.Len(t, hook.AllEntries(), 1,
		"one segment per rangeable property per shard would flood the log")
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
