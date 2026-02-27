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

func TestBitmapFactory(t *testing.T) {
	maxId := uint64(10)
	maxIdGetter := func() uint64 { return maxId }
	bmf := NewBitmapFactory(NewBitmapBufPoolNoop(), maxIdGetter)

	t.Run("prefilled bitmap includes increment", func(t *testing.T) {
		expPrefilledMaxId := maxId + defaultIdIncrement
		expPrefilledCardinality := int(maxId + defaultIdIncrement + 1)

		bm, release := bmf.GetBitmap()
		defer release()

		require.NotNil(t, bm)
		assert.Equal(t, expPrefilledMaxId, bmf.prefilled.Maximum())
		assert.Equal(t, expPrefilledCardinality, bmf.prefilled.GetCardinality())
		assert.Equal(t, maxId, bm.Maximum())
		assert.Equal(t, int(maxId)+1, bm.GetCardinality())
	})

	t.Run("maxId increased up to increment threshold does not change internal bitmap", func(t *testing.T) {
		expPrefilledMaxId := bmf.prefilled.Maximum()

		maxId += 10
		bm1, release1 := bmf.GetBitmap()
		defer release1()

		require.NotNil(t, bm1)
		assert.Equal(t, expPrefilledMaxId, bmf.prefilled.Maximum())
		assert.Equal(t, int(expPrefilledMaxId)+1, bmf.prefilled.GetCardinality())
		assert.Equal(t, maxId, bm1.Maximum())
		assert.Equal(t, int(maxId)+1, bm1.GetCardinality())

		maxId += (defaultIdIncrement - 10)
		bm2, release2 := bmf.GetBitmap()
		defer release2()

		require.NotNil(t, bm2)
		assert.Equal(t, expPrefilledMaxId, bmf.prefilled.Maximum())
		assert.Equal(t, int(expPrefilledMaxId)+1, bmf.prefilled.GetCardinality())
		assert.Equal(t, maxId, bm2.Maximum())
		assert.Equal(t, int(maxId)+1, bm2.GetCardinality())
	})

	t.Run("maxId surpasses increment threshold changes internal bitmap", func(t *testing.T) {
		maxId += 1
		expPrefilledMaxId := maxId + defaultIdIncrement

		bm, release := bmf.GetBitmap()
		defer release()

		require.NotNil(t, bm)
		assert.Equal(t, expPrefilledMaxId, bmf.prefilled.Maximum())
		assert.Equal(t, int(expPrefilledMaxId)+1, bmf.prefilled.GetCardinality())
		assert.Equal(t, maxId, bm.Maximum())
		assert.Equal(t, int(maxId)+1, bm.GetCardinality())
	})
}
