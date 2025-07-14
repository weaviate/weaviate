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

func TestPagedBuffer(t *testing.T) {
	var buf PagedBuffer[int]
	require.Equal(t, buf.Cap(), 0, "wrong initial cap")

	setN := func(n int) {
		for i := 0; i < n; i++ {
			buf.Set(uint64(i), i)
		}
	}

	checkN := func(n int) {
		for i := 0; i < n; i++ {
			v := buf.Get(uint64(i))
			if v != i {
				t.Errorf("expected %d, got %d", i, v)
			}
		}
	}

	setN(10)
	checkN(10)

	setN(1000)
	checkN(1000)

	buf.Reset()

	setN(1000)
	checkN(1000)

	buf.Reset()

	setN(100)
	require.Equal(t, 10, buf.Get(10))
	require.Zero(t, buf.Get(140))

	buf.Reset()
	for i := 0; i < 100; i += 2 {
		buf.Set(uint64(i), i)
	}
	for i := 0; i < 100; i += 2 {
		require.Equal(t, i, buf.Get(uint64(i)))
	}
	for i := 1; i < 100; i += 2 {
		require.Zero(t, buf.Get(uint64(i)))
	}
}

func TestFlatBuffer(t *testing.T) {
	var buf FlatBuffer[int]
	require.Equal(t, buf.Cap(), 0, "wrong initial cap")

	setN := func(n int) {
		for i := 0; i < n; i++ {
			buf.Set(uint64(i), i)
		}
	}

	checkN := func(n int) {
		for i := 0; i < n; i++ {
			v := buf.Get(uint64(i))
			if v != i {
				t.Errorf("expected %d, got %d", i, v)
			}
		}
	}

	setN(10)
	checkN(10)

	setN(1000)
	checkN(1000)

	buf.Reset()

	setN(1000)
	checkN(1000)

	buf.Reset()

	setN(100)
	require.Equal(t, 10, buf.Get(10))
	require.Zero(t, buf.Get(140))

	buf.Reset()
	for i := 0; i < 100; i += 2 {
		buf.Set(uint64(i), i)
	}
	for i := 0; i < 100; i += 2 {
		require.Equal(t, i, buf.Get(uint64(i)))
	}
	for i := 1; i < 100; i += 2 {
		require.Zero(t, buf.Get(uint64(i)))
	}
}

func BenchmarkBufferSparse(b *testing.B) {
	keys := make([]uint64, 10_000)
	values := make([]int, 10_000)
	for i := range 10_000 {
		keys[i] = uint64(rand.Int31n(500_000_000))
		values[i] = rand.Int()
	}

	b.Run("PagedBuffer/Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var buf PagedBuffer[int]

			for j := 0; j < 1000; j++ {
				buf.Set(keys[j], values[j])
			}
		}
	})

	var pbbuf PagedBuffer[int]

	for j := 0; j < 10_000; j++ {
		pbbuf.Set(keys[j], values[j])
	}

	b.Run("PagedBuffer/Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < 10_000; j++ {
				_ = pbbuf.Get(keys[j])
			}
		}
	})

	b.Run("FlatArray/Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var buf FlatBuffer[int]

			for j := 0; j < 1000; j++ {
				buf.Set(uint64(keys[j]), values[j])
			}
		}
	})

	var fbuf FlatBuffer[int]

	for j := 0; j < 10_000; j++ {
		fbuf.Set(uint64(keys[j]), values[j])
	}

	b.Run("FlatArray/Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < 10_000; j++ {
				_ = fbuf.Get(uint64(keys[j]))
			}
		}
	})
}

func BenchmarkBufferMonotonic(b *testing.B) {
	b.Run("PagedBuffer/Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var buf PagedBuffer[int]

			for j := 0; j < 10_000; j++ {
				buf.Set(uint64(j), j)
			}
		}
	})

	var pbbuf PagedBuffer[int]

	for j := 0; j < 10_000; j++ {
		pbbuf.Set(uint64(j), j)
	}

	b.Run("PagedBuffer/Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < 10_000; j++ {
				_ = pbbuf.Get(uint64(j))
			}
		}
	})

	b.Run("FlatArray/Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var buf FlatBuffer[int]

			for j := 0; j < 10_000; j++ {
				buf.Set(uint64(j), j)
			}
		}
	})

	var fbuf FlatBuffer[int]

	for j := 0; j < 10_000; j++ {
		fbuf.Set(uint64(j), j)
	}

	b.Run("FlatArray/Get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < 10_000; j++ {
				_ = fbuf.Get(uint64(j))
			}
		}
	})
}
