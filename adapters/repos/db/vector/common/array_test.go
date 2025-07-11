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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPagedArray(t *testing.T) {
	arr := NewPagedArrayWith[int](10, 2)
	require.Len(t, arr.array, 2, "wrong initial number of pages")

	setN := func(n int) {
		for i := 0; i < n; i++ {
			arr.Set(i, i)
		}
	}

	checkN := func(n int) {
		for i := 0; i < n; i++ {
			v := arr.Get(i)
			if v != i {
				t.Errorf("expected %d, got %d", i, v)
			}
		}
	}

	setN(10)
	checkN(10)

	setN(1000)
	checkN(1000)

	arr.Reset()

	setN(1000)
	checkN(1000)

	arr.Reset()

	setN(100)
	require.Equal(t, 10, arr.Get(10))
	require.Nil(t, arr.Get(140))

	arr.Reset()
	for i := 0; i < 100; i += 2 {
		arr.Set(i, i)
	}
	for i := 0; i < 100; i += 2 {
		require.Equal(t, i, arr.Get(i))
	}
	for i := 1; i < 100; i += 2 {
		require.Nil(t, arr.Get(i))
	}
}

func BenchmarkPagedArray(b *testing.B) {
	pageSize := 512
	keys := make([]int, 10000)
	values := make([]int, 10000)
	for i := range 10000 {
		keys[i] = int(rand.Int31n(500_000_000))
		values[i] = rand.Int()
	}

	b.Run("PagedArray/Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			arr := NewPagedArrayWith[int](pageSize, 10)
			b.StartTimer()

			for j := 0; j < 1000; j++ {
				arr.Set(keys[j], values[j])
			}
		}
	})

	arr := NewPagedArrayWith[int](pageSize, 10)

	for j := 0; j < 10000; j++ {
		arr.Set(keys[j], values[j])
	}

	b.Run("PagedArray/Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < 10000; j++ {
				_ = arr.Get(keys[j])
			}
		}
	})
}

func TestFlatArray(t *testing.T) {
	arr := NewFlatArray[int](10)

	require.Len(t, arr.array, 10, "wrong initial size of arr")

	setN := func(n int) {
		for i := 0; i < n; i++ {
			arr.Set(i, i)
		}
	}

	checkN := func(n int) {
		for i := 0; i < n; i++ {
			v := arr.Get(i)
			if v != i {
				t.Errorf("expected %d, got %d", i, v)
			}
		}
	}

	setN(10)
	checkN(10)

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
		arr.Set(i, i)
	}
	for i := 0; i < 100; i += 2 {
		require.Equal(t, i, arr.Get(i))
	}
	for i := 1; i < 100; i += 2 {
		require.Zero(t, arr.Get(i))
	}
}

func BenchmarkFlatArray(b *testing.B) {
	keys := make([]int, 10000)
	values := make([]int, 10000)
	for i := range 10000 {
		keys[i] = int(rand.Int31n(500_000_000))
		values[i] = rand.Int()
	}

	b.Run("FlatArray/Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			arr := NewFlatArray[int](10)
			b.StartTimer()

			for j := 0; j < 1000; j++ {
				arr.Set(keys[j], values[j])
			}
		}
	})

	arr := NewFlatArray[int](10)

	for j := 0; j < 10000; j++ {
		arr.Set(keys[j], values[j])
	}

	b.Run("FlatArray/Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < 10000; j++ {
				_ = arr.Get(keys[j])
			}
		}
	})
}
