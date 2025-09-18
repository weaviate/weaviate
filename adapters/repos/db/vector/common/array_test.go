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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPagedArray(t *testing.T) {
	arr := NewPagedArray[int](10, 10)
	require.Equal(t, arr.Cap(), 0, "wrong initial cap")

	setN := func(n int) {
		t.Helper()

		for i := 0; i < n; i++ {
			arr.AllocPageFor(uint64(i))
			arr.Set(uint64(i), i)
		}
	}

	checkN := func(n int) {
		for i := 0; i < n; i++ {
			v := arr.Get(uint64(i))
			if v != i {
				t.Errorf("expected %d, got %d", i, v)
			}
		}
	}

	setN(10)
	checkN(10)

	arr.Grow(1000)
	setN(1000)
	checkN(1000)

	arr.Reset()

	setN(1000)
	checkN(1000)

	arr.Reset()

	setN(100)
	require.Equal(t, 10, arr.Get(10))
	require.Zero(t, arr.Get(140))

	arr.Reset()
	for i := 0; i < 100; i += 2 {
		arr.Set(uint64(i), i)
	}
	for i := 0; i < 100; i += 2 {
		require.Equal(t, i, arr.Get(uint64(i)))
	}
	for i := 1; i < 100; i += 2 {
		require.Zero(t, arr.Get(uint64(i)))
	}
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
			buf := NewPagedArray[int](512, 1000_000)

			for j := 0; j < 1000; j++ {
				buf.AllocPageFor(keys[j])
				buf.Set(keys[j], values[j])
			}
		}
	})

	pbbuf := NewPagedArray[int](512, 1000_000)

	for j := 0; j < 10_000; j++ {
		pbbuf.AllocPageFor(keys[j])
		pbbuf.Set(keys[j], values[j])
	}

	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < 10_000; j++ {
				_ = pbbuf.Get(keys[j])
			}
		}
	})
}

func BenchmarkArrayMonotonic(b *testing.B) {
	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buf := NewPagedArray[int](20, 512)

			for j := 0; j < 10_000; j++ {
				buf.AllocPageFor(uint64(j))
				buf.Set(uint64(j), j)
			}
		}
	})

	pbbuf := NewPagedArray[int](20, 512)

	for j := 0; j < 10_000; j++ {
		pbbuf.AllocPageFor(uint64(j))
		pbbuf.Set(uint64(j), j)
	}

	b.Run("Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < 10_000; j++ {
				_ = pbbuf.Get(uint64(j))
			}
		}
	})
}

func TestPagedArrayConcurrentSetAndGet(t *testing.T) {
	buf := NewPagedArray[uint64](20, 512)
	locks := NewShardedRWLocks(10)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()

			for j := uint64(0); j < 1000; j++ {
				locks.Lock(j)
				buf.AllocPageFor(j)
				buf.Set(j, j)
				locks.Unlock(j)
			}
		}(i)
		go func(i int) {
			defer wg.Done()

			for j := uint64(0); j < 1000; j++ {
				locks.RLock(j)
				_ = buf.Get(j)
				locks.RUnlock(j)
			}
		}(i)
	}
	wg.Wait()
}
