//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package common

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPagedCache(t *testing.T) {
	cache := NewPagedCacheWith[int](10, 2)
	require.Len(t, cache.cache, 2, "wrong initial number of pages")

	setN := func(n int) {
		for i := 0; i < n; i++ {
			cache.Set(i, &i)
		}
	}

	checkN := func(n int) {
		for i := 0; i < n; i++ {
			v := cache.Get(i)
			if *v != i {
				t.Errorf("expected %d, got %d", i, *v)
			}
		}
	}

	setN(10)
	checkN(10)

	setN(1000)
	checkN(1000)

	cache.Reset()

	setN(1000)
	checkN(1000)

	cache.Reset()

	setN(100)
	require.Equal(t, 10, *cache.Get(10))
	require.Nil(t, cache.Get(140))

	cache.Reset()
	for i := 0; i < 100; i += 2 {
		cache.Set(i, &i)
	}
	for i := 0; i < 100; i += 2 {
		require.Equal(t, i, *cache.Get(i))
	}
	for i := 1; i < 100; i += 2 {
		require.Nil(t, cache.Get(i))
	}
}

func BenchmarkPagedCache(b *testing.B) {
	pageSize := 512
	keys := make([]int, 10000)
	values := make([]int, 10000)
	for i := range 10000 {
		keys[i] = int(rand.Int31n(500_000_000))
		values[i] = rand.Int()
	}

	b.Run("PagedCache/Set", func(b *testing.B) {
		cache := NewPagedCacheWith[int](pageSize, 10)

		for i := 0; i < b.N; i++ {
			cache.Reset()

			for j := 0; j < 10000; j++ {
				cache.Set(keys[j], &values[j])
			}
		}
	})

	b.Run("FlatCache/Set", func(b *testing.B) {
		cache := make([]*int, 500_000_000)

		for i := 0; i < b.N; i++ {
			clear(cache)

			for j := 0; j < 10000; j++ {
				cache[keys[j]] = &values[j]
			}
		}
	})

	b.Run("PagedCache/Get", func(b *testing.B) {
		cache := NewPagedCacheWith[int](pageSize, 10)

		for j := 0; j < 10000; j++ {
			cache.Set(keys[j], &values[j])
		}

		for i := 0; i < b.N; i++ {
			for j := 0; j < 10000; j++ {
				_ = cache.Get(keys[j])
			}
		}
	})

	b.Run("FlatCache/Get", func(b *testing.B) {
		cache := make([]*int, 500_000_000)

		for j := 0; j < 10000; j++ {
			cache[keys[j]] = &values[j]
		}

		for i := 0; i < b.N; i++ {
			for j := 0; j < 10000; j++ {
				_ = cache[keys[j]]
			}
		}
	})
}
