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

package roaringset

import (
	"flag"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// benchMaxId controls the initial doc ID ceiling for BitmapFactory benchmarks,
// simulating a shard of that size. Override with -bench-max-id=<n>.
//
// From the package directory:
//
//	go test -bench=BenchmarkBitmapFactory_GetBitmap -benchtime=5s \
//	  -cpu=1,4,8,16 -bench-max-id=5000000
//
// From the repo root (package path must precede -args):
//
//	go test -bench=BenchmarkBitmapFactory_GetBitmap -benchtime=5s \
//	  -cpu=1,4,8,16 ./adapters/repos/db/roaringset/ -args -bench-max-id=5000000
var benchMaxId = flag.Uint64("bench-max-id", 1_000_000, "initial max doc ID for BitmapFactory benchmarks")

// BenchmarkBitmapFactory_GetBitmap measures GetBitmap throughput
// with and without concurrent RemoveIds calls (simulating deletes/updates).
func BenchmarkBitmapFactory_GetBitmap(b *testing.B) {
	startMaxId := *benchMaxId

	b.Run("without_concurrent_removes", func(b *testing.B) {
		bmf := NewBitmapFactory(NewBitmapBufPoolNoop(), func() uint64 { return startMaxId })

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				bm, release := bmf.GetBitmap()
				_ = bm.GetCardinality()
				release()
			}
		})
	})

	b.Run("with_concurrent_removes", func(b *testing.B) {
		bmf := NewBitmapFactory(NewBitmapBufPoolNoop(), func() uint64 { return startMaxId })

		var stopped atomic.Bool
		go func() {
			id := uint64(0)
			for !stopped.Load() {
				bmf.RemoveIds(id % startMaxId)
				id++
			}
		}()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				bm, release := bmf.GetBitmap()
				_ = bm.GetCardinality()
				release()
			}
		})

		stopped.Store(true)
	})

	// with_increasing_maxid simulates concurrent inserts: maxId grows, periodically
	// pushing past the prefilled threshold and triggering the write-lock expansion
	// path inside GetBitmap itself (FillUp). Capped at 2×startMaxId to prevent
	// unbounded bitmap growth from dominating clone latency.
	b.Run("with_increasing_maxid", func(b *testing.B) {
		var maxId atomic.Uint64
		maxId.Store(startMaxId)
		bmf := NewBitmapFactory(NewBitmapBufPoolNoop(), func() uint64 { return maxId.Load() })

		var stopped atomic.Bool
		go func() {
			for !stopped.Load() {
				if maxId.Load() < 2*startMaxId {
					maxId.Add(1)
				}
			}
		}()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				bm, release := bmf.GetBitmap()
				_ = bm.GetCardinality()
				release()
			}
		})

		stopped.Store(true)
	})

	// with_concurrent_removes_and_increasing_maxid combines both write-lock sources:
	// RemoveIds (from deletes/updates) and FillUp expansions (from inserts).
	b.Run("with_concurrent_removes_and_increasing_maxid", func(b *testing.B) {
		var maxId atomic.Uint64
		maxId.Store(startMaxId)
		bmf := NewBitmapFactory(NewBitmapBufPoolNoop(), func() uint64 { return maxId.Load() })

		var stopped atomic.Bool
		go func() {
			for !stopped.Load() {
				if maxId.Load() < 2*startMaxId {
					maxId.Add(1)
				}
			}
		}()
		go func() {
			id := uint64(0)
			for !stopped.Load() {
				bmf.RemoveIds(id % startMaxId)
				id++
			}
		}()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				bm, release := bmf.GetBitmap()
				_ = bm.GetCardinality()
				release()
			}
		})

		stopped.Store(true)
	})
}

// BenchmarkBitmapFactory_RemoveIds measures isolated RemoveIds throughput under
// parallel write contention. A concurrent-gets variant is intentionally omitted:
// RemoveIds (~13 ns/op) is ~1000× faster than GetBitmap (~18 µs/op), so a
// background reader goroutine starves under write pressure, causing the benchmark
// framework to calibrate b.N to ~385M iterations which then takes hundreds of
// seconds at contended rates. The complementary view — how reads are slowed by
// concurrent writes — is already covered by BenchmarkBitmapFactory_GetBitmap/with_concurrent_removes.
func BenchmarkBitmapFactory_RemoveIds(b *testing.B) {
	startMaxId := *benchMaxId

	bmf := NewBitmapFactory(NewBitmapBufPoolNoop(), func() uint64 { return startMaxId })

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := uint64(0)
		for pb.Next() {
			bmf.RemoveIds(id % startMaxId)
			id++
		}
	})
}

func TestBitmap_Condense(t *testing.T) {
	t.Run("And with itself (internal array)", func(t *testing.T) {
		bm := NewBitmap(slice(0, 1000)...)
		for i := 0; i < 10; i++ {
			bm.And(bm)
		}
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		// As of sroar 0.0.5 "And" merge is optimized not to expand
		// existing bitmap when not needed. Therefore calling Condense
		// does not guarantee decreasing bitmap size
		assert.GreaterOrEqual(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})

	t.Run("And with itself (internal bitmap)", func(t *testing.T) {
		bm := NewBitmap(slice(0, 5000)...)
		for i := 0; i < 10; i++ {
			bm.And(bm)
		}
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		// As of sroar 0.0.5 "And" merge is optimized not to expand
		// existing bitmap when not needed. Therefore calling Condense
		// does not guarantee decreasing bitmap size
		assert.GreaterOrEqual(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})

	t.Run("And (internal arrays)", func(t *testing.T) {
		bm1 := NewBitmap(slice(0, 1000)...)
		bm2 := NewBitmap(slice(500, 1500)...)
		bm := bm1.Clone()
		bm.And(bm2)
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		assert.Greater(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})

	t.Run("And (internal bitmaps)", func(t *testing.T) {
		bm1 := NewBitmap(slice(0, 5000)...)
		bm2 := NewBitmap(slice(1000, 6000)...)
		bm := bm1.Clone()
		bm.And(bm2)
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		// As of sroar 0.0.5 "And" merge is optimized not to expand
		// existing bitmap when not needed. Therefore calling Condense
		// does not guarantee decreasing bitmap size
		assert.GreaterOrEqual(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})

	t.Run("And (internal bitmaps to bitmap with few elements)", func(t *testing.T) {
		bm1 := NewBitmap(slice(0, 5000)...)
		bm2 := NewBitmap(slice(4000, 9000)...)
		bm := bm1.Clone()
		bm.And(bm2)
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		// As of sroar 0.0.5 "And" merge is optimized not to expand
		// existing bitmap when not needed. Therefore calling Condense
		// does not guarantee decreasing bitmap size
		assert.GreaterOrEqual(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})

	t.Run("Remove (array)", func(t *testing.T) {
		bm := NewBitmap(slice(0, 1000)...)
		for i := uint64(2); i < 1000; i++ {
			bm.Remove(i)
		}
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		assert.Greater(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})

	t.Run("Remove (bitmap)", func(t *testing.T) {
		bm := NewBitmap(slice(0, 100_000)...)
		for i := uint64(10_000); i < 100_000; i++ {
			bm.Remove(i)
		}
		bmLen := len(bm.ToBuffer())

		condensed := Condense(bm)
		condensedLen := len(condensed.ToBuffer())

		assert.Greater(t, bmLen, condensedLen)
		assert.ElementsMatch(t, bm.ToArray(), condensed.ToArray())
	})
}

func slice(from, to uint64) []uint64 {
	len := to - from
	s := make([]uint64, len)
	for i := uint64(0); i < len; i++ {
		s[i] = from + i
	}
	return s
}

func TestDocIDCount(t *testing.T) {
	// The shard wires its index counter straight in, so the getter reports how
	// many doc IDs have been allocated. Exclusive, so zero is a shard holding
	// nothing rather than one holding doc ID 0 — see TestBitmapFactoryUniverse
	// for why that distinction is load-bearing.
	tests := []struct {
		name  string
		count uint64
		// wantPrefilled pins what NewBitmapFactory built from the same getter
		wantPrefilled uint64
	}{
		{name: "a shard holding nothing", count: 0, wantPrefilled: defaultIdIncrement},
		{name: "one object, doc ID 0", count: 1, wantPrefilled: 1 + defaultIdIncrement},
		{name: "a written shard", count: 5001, wantPrefilled: 5001 + defaultIdIncrement},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bmf := NewBitmapFactory(NewBitmapBufPoolNoop(), func() uint64 { return tc.count })

			assert.Equal(t, tc.count, bmf.DocIDCount())
			assert.Equal(t, tc.wantPrefilled, bmf.prefilledMaxId)
		})
	}

	t.Run("tracks the getter rather than the prefilled ceiling", func(t *testing.T) {
		count := uint64(11)
		bmf := NewBitmapFactory(NewBitmapBufPoolNoop(), func() uint64 { return count })

		assert.Equal(t, uint64(11), bmf.DocIDCount())
		count = 5001
		assert.Equal(t, uint64(5001), bmf.DocIDCount())
	})
}

// TestBitmapFactoryUniverse pins what a deny-list filter is inverted against.
// The universe is the half-open range [0, count), so a shard that has allocated
// no doc ID answers with nothing.
//
// The distinction is load-bearing twice over. A phantom ID reaches the caller as
// a matching document — filteredAggregator reports len(allowList) as the meta
// count without looking an object up — and it does not stay a read-only error:
// objectsByDocID finds no object for it, classifies it deleted, and prunes it
// from the shared prefilled bitmap, which FillUp never restores. The shard's
// first real object would then be missing from every deny-list filter for the
// life of the process.
func TestBitmapFactoryUniverse(t *testing.T) {
	universe := func(t *testing.T, bmf *BitmapFactory) []uint64 {
		t.Helper()
		bm, release := bmf.GetBitmap()
		defer release()
		return bm.ToArray()
	}

	t.Run("a shard that has allocated no doc ID has an empty universe", func(t *testing.T) {
		bmf := NewBitmapFactory(NewBitmapBufPoolNoop(), func() uint64 { return 0 })

		assert.Empty(t, universe(t, bmf),
			"nothing has been written, so no doc ID exists to deny")
	})

	t.Run("one object is doc ID 0", func(t *testing.T) {
		bmf := NewBitmapFactory(NewBitmapBufPoolNoop(), func() uint64 { return 1 })

		assert.Equal(t, []uint64{0}, universe(t, bmf))
	})

	t.Run("the first object survives a query against the empty shard", func(t *testing.T) {
		count := uint64(0)
		bmf := NewBitmapFactory(NewBitmapBufPoolNoop(), func() uint64 { return count })

		// a deny-list filter runs before anything is written. Whatever it
		// returns, objectsByDocID prunes every ID it cannot resolve.
		for _, id := range universe(t, bmf) {
			bmf.RemoveIds(id)
		}

		count = 1
		assert.Equal(t, []uint64{0}, universe(t, bmf),
			"the shard's first object must not have been pruned before it existed")

		count = 2
		assert.Equal(t, []uint64{0, 1}, universe(t, bmf))
	})
}

func TestBitmapFactory(t *testing.T) {
	count := uint64(11) // doc IDs 0..10
	countGetter := func() uint64 { return count }
	bmf := NewBitmapFactory(NewBitmapBufPoolNoop(), countGetter)

	t.Run("prefilled bitmap includes increment", func(t *testing.T) {
		expPrefilledMaxId := count + defaultIdIncrement
		expPrefilledCardinality := int(count + defaultIdIncrement + 1)

		bm, release := bmf.GetBitmap()
		defer release()

		require.NotNil(t, bm)
		assert.Equal(t, expPrefilledMaxId, bmf.prefilled.Maximum())
		assert.Equal(t, expPrefilledCardinality, bmf.prefilled.GetCardinality())
		assert.Equal(t, count-1, bm.Maximum())
		assert.Equal(t, int(count), bm.GetCardinality())
	})

	t.Run("count increased up to increment threshold does not change internal bitmap", func(t *testing.T) {
		expPrefilledMaxId := bmf.prefilled.Maximum()

		count += 10
		bm1, release1 := bmf.GetBitmap()
		defer release1()

		require.NotNil(t, bm1)
		assert.Equal(t, expPrefilledMaxId, bmf.prefilled.Maximum())
		assert.Equal(t, int(expPrefilledMaxId)+1, bmf.prefilled.GetCardinality())
		assert.Equal(t, count-1, bm1.Maximum())
		assert.Equal(t, int(count), bm1.GetCardinality())

		count += (defaultIdIncrement - 10)
		bm2, release2 := bmf.GetBitmap()
		defer release2()

		require.NotNil(t, bm2)
		assert.Equal(t, expPrefilledMaxId, bmf.prefilled.Maximum())
		assert.Equal(t, int(expPrefilledMaxId)+1, bmf.prefilled.GetCardinality())
		assert.Equal(t, count-1, bm2.Maximum())
		assert.Equal(t, int(count), bm2.GetCardinality())
	})

	t.Run("count surpasses increment threshold changes internal bitmap", func(t *testing.T) {
		count += 1
		expPrefilledMaxId := count + defaultIdIncrement

		bm, release := bmf.GetBitmap()
		defer release()

		require.NotNil(t, bm)
		assert.Equal(t, expPrefilledMaxId, bmf.prefilled.Maximum())
		assert.Equal(t, int(expPrefilledMaxId)+1, bmf.prefilled.GetCardinality())
		assert.Equal(t, count-1, bm.Maximum())
		assert.Equal(t, int(count), bm.GetCardinality())
	})
}

// TestIterator covers roaringset's own iterator wrapper, whose Next reports a
// zero value and exhaustion separately — the "bitmap with only 0" case is what
// tells the two apart.
func TestIterator(t *testing.T) {
	testCases := []struct {
		name string
		vals []uint64
	}{
		{
			name: "empty bitmap",
			vals: []uint64{},
		},
		{
			name: "bitmap with only 0",
			vals: []uint64{0},
		},
		{
			name: "bitmap with few values including 0",
			vals: []uint64{0, 3, 5, 10},
		},
		{
			name: "bitmap with few values excluding 0",
			vals: []uint64{3, 5, 9, 10},
		},
	}

	t.Run("returns ok when value is returned", func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				bm := NewBitmap(tc.vals...)
				it := NewIterator(bm)

				for _, expVal := range tc.vals {
					val, ok := it.Next()
					assert.Equal(t, expVal, val)
					assert.True(t, ok)
				}
				for range 5 {
					val, ok := it.Next()
					assert.Equal(t, uint64(0), val)
					assert.False(t, ok)
				}
			})
		}
	})

	t.Run("resets iterator", func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				bm := NewBitmap(tc.vals...)
				it := NewIterator(bm)

				for range 7 {
					it.Next()
				}
				it.Reset()

				for _, expVal := range tc.vals {
					val, ok := it.Next()
					assert.Equal(t, expVal, val)
					assert.True(t, ok)
				}
				for range 5 {
					val, ok := it.Next()
					assert.Equal(t, uint64(0), val)
					assert.False(t, ok)
				}
			})
		}
	})

	t.Run("runs in loop", func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				bm := NewBitmap(tc.vals...)
				it := NewIterator(bm)

				vals := make([]uint64, 0, len(tc.vals))
				for val, ok := it.Next(); ok; val, ok = it.Next() {
					vals = append(vals, val)
				}

				assert.Equal(t, tc.vals, vals)
			})
		}
	})
}
