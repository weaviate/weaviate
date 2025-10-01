//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package common

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
)

func TestPagedArray(t *testing.T) {
	arr := NewPagedArray[uint64](10, 10)

	setN := func(n int) {
		t.Helper()

		for i := 0; i < n; i++ {
			p, slot := arr.EnsurePageFor(uint64(i))
			atomic.StoreUint64(&p[slot], uint64(i))
		}
	}

	checkN := func(n int) {
		for i := 0; i < n; i++ {
			p, slot := arr.GetPageFor(uint64(i))
			v := atomic.LoadUint64(&p[slot])
			if v != uint64(i) {
				t.Errorf("expected %d, got %d", i, v)
			}
		}
	}

	setN(10)
	checkN(10)

	arr = NewPagedArray[uint64](1000, 10)
	setN(1000)
	checkN(1000)
}

func BenchmarkArraySparse(b *testing.B) {
	keys := make([]uint64, 10_000)
	values := make([]int, 10_000)
	for i := range 10_000 {
		keys[i] = uint64(rand.Int31n(500_000_000))
		values[i] = rand.Int()
	}

	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buf := NewPagedArray[uint64](512, 1000_000)

			for j := 0; j < 1000; j++ {
				p, slot := buf.EnsurePageFor(keys[j])
				atomic.StoreUint64(&p[slot], uint64(values[j]))
			}
		}
	})

	pbbuf := NewPagedArray[uint64](512, 1000_000)

	for j := 0; j < 10_000; j++ {
		p, slot := pbbuf.EnsurePageFor(keys[j])
		atomic.StoreUint64(&p[slot], uint64(values[j]))
	}

	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < 10_000; j++ {
				p, slot := pbbuf.GetPageFor(keys[j])
				_ = atomic.LoadUint64(&p[slot])
			}
		}
	})
}

func BenchmarkArrayMonotonic(b *testing.B) {
	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buf := NewPagedArray[uint64](20, 512)

			for j := 0; j < 10_000; j++ {
				p, slot := buf.EnsurePageFor(uint64(j))
				atomic.StoreUint64(&p[slot], uint64(j))
			}
		}
	})

	pbbuf := NewPagedArray[uint64](20, 512)

	for j := 0; j < 10_000; j++ {
		p, slot := pbbuf.EnsurePageFor(uint64(j))
		atomic.StoreUint64(&p[slot], uint64(j))
	}

	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < 10_000; j++ {
				p, slot := pbbuf.GetPageFor(uint64(j))
				_ = atomic.LoadUint64(&p[slot])
			}
		}
	})
}

func TestPagedArrayConcurrentSetAndGet(t *testing.T) {
	buf := NewPagedArray[uint64](20, 512)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {

		for j := uint64(0); j < 1000; j++ {
			buf.EnsurePageFor(j)
		}

		wg.Add(2)
		go func(i int) {
			defer wg.Done()

			for j := uint64(0); j < 1000; j++ {
				p, slot := buf.GetPageFor(j)
				atomic.StoreUint64(&p[slot], uint64(i))
			}
		}(i)
		go func(i int) {
			defer wg.Done()

			for j := uint64(0); j < 1000; j++ {
				p, slot := buf.GetPageFor(j)
				_ = atomic.LoadUint64(&p[slot])
			}
		}(i)
	}
	wg.Wait()
}
