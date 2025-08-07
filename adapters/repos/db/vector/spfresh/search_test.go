package spfresh

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
)

func TestKSmallest(t *testing.T) {
	k := 64

	values := make([]uint64, 0, 10_000)
	for i := 0; i < 10_000; i++ {
		values = append(values, uint64(rand.Int32N(1_000_000)))
	}

	ks := NewKSmallest(k)
	q := priorityqueue.NewMax[uint64](k)
	for i, v := range values {
		ks.Insert(uint64(i), float32(v))
		q.Insert(uint64(i), float32(v))
		if q.Len() > k {
			q.Pop()
		}
	}

	A := ks.GetAll()
	B := make([]KSmallestItem, k)
	i := k - 1
	for q.Len() > 0 {
		item := q.Pop()
		B[i] = KSmallestItem{ID: item.ID, Dist: item.Dist}
		i--
	}

	require.Equal(t, A, B)
}

func BenchmarkKSmallest(b *testing.B) {
	k := 512

	values := make([]uint64, 0, 10_000)
	for i := 0; i < 10_000; i++ {
		values = append(values, uint64(rand.Int32N(1_000_000)))
	}

	b.Run("Basic", func(b *testing.B) {
		for b.Loop() {
			ks := NewKSmallest(k)
			for _, v := range values {
				ks.Insert(v, float32(v))
			}
		}
	})

	b.Run("Heap", func(b *testing.B) {
		for b.Loop() {
			q := priorityqueue.NewMax[uint64](k)
			for _, v := range values {
				q.Insert(v, float32(v))
				if q.Len() > k {
					q.Pop()
				}
			}
		}
	})
}
