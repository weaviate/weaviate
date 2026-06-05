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

func TestGroupedPagedArray(t *testing.T) {
	const maxPages = 1024
	const pageSize = 64

	arr := NewGroupedPagedArray[uint64](maxPages, pageSize)

	// Test that unallocated pages return nil
	page, idx := arr.GetPageFor(0)
	if page != nil {
		t.Fatal("expected nil page for unallocated ID")
	}
	if idx != -1 {
		t.Fatal("expected -1 index for unallocated ID")
	}

	// Test EnsurePageFor allocates correctly
	page, idx = arr.EnsurePageFor(0)
	if page == nil {
		t.Fatal("expected non-nil page after EnsurePageFor")
	}
	if idx != 0 {
		t.Fatalf("expected index 0, got %d", idx)
	}

	// Write and read back
	page[idx] = 42
	page2, idx2 := arr.GetPageFor(0)
	if page2[idx2] != 42 {
		t.Fatalf("expected 42, got %d", page2[idx2])
	}

	// Test that IDs in different groups work
	farID := uint64(200 * pageSize) // should be in group 1+
	page, idx = arr.EnsurePageFor(farID)
	if page == nil {
		t.Fatal("expected page for far ID")
	}
	page[idx] = 7
	page3, idx3 := arr.GetPageFor(farID)
	if page3[idx3] != 7 {
		t.Fatalf("expected 7, got %d", page3[idx3])
	}

	// Test out-of-bounds panics
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for out-of-bounds ID")
		}
	}()
	arr.EnsurePageFor(uint64(maxPages+1) * pageSize)
}

// BenchmarkArrayComparison compares PagedArray vs GroupedPagedArray with identical capacity
func BenchmarkArrayComparison(b *testing.B) {
	const maxPages = 16 * 1024
	const pageSize = 64 * 1024
	const numOps = 10_000

	// Generate test data once
	keys := make([]uint64, numOps)
	values := make([]uint64, numOps)
	for i := range numOps {
		keys[i] = uint64(rand.Int31n(500_000_000))
		values[i] = uint64(rand.Int())
	}

	b.Run("PagedArray/Set/Sparse", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			arr := NewPagedArray[uint64](maxPages, pageSize)
			for j := 0; j < numOps; j++ {
				p, slot := arr.EnsurePageFor(keys[j])
				atomic.StoreUint64(&p[slot], values[j])
			}
		}
	})

	b.Run("GroupedPagedArray/Set/Sparse", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			arr := NewGroupedPagedArray[uint64](maxPages, pageSize)
			for j := 0; j < numOps; j++ {
				p, slot := arr.EnsurePageFor(keys[j])
				atomic.StoreUint64(&p[slot], values[j])
			}
		}
	})

	// Pre-populate for Get benchmarks
	pagedArr := NewPagedArray[uint64](maxPages, pageSize)
	groupedArr := NewGroupedPagedArray[uint64](maxPages, pageSize)
	for j := 0; j < numOps; j++ {
		p, slot := pagedArr.EnsurePageFor(keys[j])
		atomic.StoreUint64(&p[slot], values[j])
		p, slot = groupedArr.EnsurePageFor(keys[j])
		atomic.StoreUint64(&p[slot], values[j])
	}

	b.Run("PagedArray/Get/Sparse", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < numOps; j++ {
				p, slot := pagedArr.GetPageFor(keys[j])
				_ = atomic.LoadUint64(&p[slot])
			}
		}
	})

	b.Run("GroupedPagedArray/Get/Sparse", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < numOps; j++ {
				p, slot := groupedArr.GetPageFor(keys[j])
				_ = atomic.LoadUint64(&p[slot])
			}
		}
	})

	// Monotonic access pattern
	b.Run("PagedArray/Set/Monotonic", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			arr := NewPagedArray[uint64](maxPages, pageSize)
			for j := 0; j < numOps; j++ {
				p, slot := arr.EnsurePageFor(uint64(j))
				atomic.StoreUint64(&p[slot], values[j])
			}
		}
	})

	b.Run("GroupedPagedArray/Set/Monotonic", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			arr := NewGroupedPagedArray[uint64](maxPages, pageSize)
			for j := 0; j < numOps; j++ {
				p, slot := arr.EnsurePageFor(uint64(j))
				atomic.StoreUint64(&p[slot], values[j])
			}
		}
	})

	// Pre-populate for monotonic Get benchmarks
	pagedArrMono := NewPagedArray[uint64](maxPages, pageSize)
	groupedArrMono := NewGroupedPagedArray[uint64](maxPages, pageSize)
	for j := 0; j < numOps; j++ {
		p, slot := pagedArrMono.EnsurePageFor(uint64(j))
		atomic.StoreUint64(&p[slot], values[j])
		p, slot = groupedArrMono.EnsurePageFor(uint64(j))
		atomic.StoreUint64(&p[slot], values[j])
	}

	b.Run("PagedArray/Get/Monotonic", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < numOps; j++ {
				p, slot := pagedArrMono.GetPageFor(uint64(j))
				_ = atomic.LoadUint64(&p[slot])
			}
		}
	})

	b.Run("GroupedPagedArray/Get/Monotonic", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < numOps; j++ {
				p, slot := groupedArrMono.GetPageFor(uint64(j))
				_ = atomic.LoadUint64(&p[slot])
			}
		}
	})
}
