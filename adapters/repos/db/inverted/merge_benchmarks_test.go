package inverted

import (
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/semi-technologies/weaviate/entities/filters"
)

func BenchmarkAnd10k1m_Old(b *testing.B) {
	b.StopTimer()

	list1 := propValuePair{
		docIDs: docPointers{
			docIDs:   randomIDs(1e4),
			checksum: []byte{0x01},
		},
		operator: filters.OperatorEqual,
	}

	list2 := propValuePair{
		docIDs: docPointers{
			docIDs:   randomIDs(1e6),
			checksum: []byte{0x02},
		},
		operator: filters.OperatorEqual,
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mergeAnd([]*propValuePair{&list1, &list2})
	}
}

func BenchmarkAnd10k1m_Optimized(b *testing.B) {
	b.StopTimer()

	list1 := propValuePair{
		docIDs: docPointers{
			docIDs:   randomIDs(1e4),
			checksum: []byte{0x01},
		},
		operator: filters.OperatorEqual,
	}

	list2 := propValuePair{
		docIDs: docPointers{
			docIDs:   randomIDs(1e6),
			checksum: []byte{0x02},
		},
		operator: filters.OperatorEqual,
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mergeAndOptimized([]*propValuePair{&list1, &list2})
	}
}

func BenchmarkMultipleListsOf20k_Old(b *testing.B) {
	b.StopTimer()

	lists := make([]*propValuePair, 10)
	for i := range lists {
		lists[i] = &propValuePair{
			docIDs: docPointers{
				docIDs:   randomIDs(2e4),
				checksum: []byte{uint8(i)},
			},
			operator: filters.OperatorEqual,
		}
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mergeAnd(lists)
	}
}

func BenchmarkMultipleListsOf20k_Optimized(b *testing.B) {
	b.StopTimer()

	lists := make([]*propValuePair, 10)
	for i := range lists {
		lists[i] = &propValuePair{
			docIDs: docPointers{
				docIDs:   randomIDs(2e4),
				checksum: []byte{uint8(i)},
			},
			operator: filters.OperatorEqual,
		}
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mergeAndOptimized(lists)
	}
}

func BenchmarkSort10k(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		list := randomIDs(1e5)
		b.StartTimer()

		sort.Slice(list, func(a, b int) bool {
			return list[a].id < list[b].id
		})
	}
}

func BenchmarkUnsortedLinearSearch(b *testing.B) {
	searchTargets := randomIDs(1e5)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		list := randomIDs(1e5)
		b.StartTimer()

		for i := range searchTargets {
			linearSearchUnsorted(list, searchTargets[i].id)
		}
	}
}

func BenchmarkSortedBinarySearch(b *testing.B) {
	searchTargets := randomIDs(1e6)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		list := randomIDs(1e4)
		b.StartTimer()

		sort.Slice(list, func(a, b int) bool {
			return list[a].id < list[b].id
		})

		for i := range searchTargets {
			binarySearch(list, searchTargets[i].id)
		}
	}
}

func BenchmarkHashmap(b *testing.B) {
	searchTargets := randomIDs(1e6)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		list := randomIDs(1e4)
		b.StartTimer()

		lookup := make(map[uint64]struct{}, len(list))
		for i := range list {
			lookup[list[i].id] = struct{}{}
		}

		for i := range searchTargets {
			_, ok := lookup[searchTargets[i].id]
			_ = ok
		}
	}
}

func randomIDs(count int) []docPointer {
	out := make([]docPointer, count)
	for i := range out {
		out[i] = docPointer{id: rand.Uint64()}
	}

	return out
}

func linearSearchUnsorted(in []docPointer, needle uint64) bool {
	for i := range in {
		if in[i].id == needle {
			return true
		}
	}

	return false
}

// function binary_search(A, n, T) is
//     L := 0
//     R := n − 1
//     while L ≤ R do
//         m := floor((L + R) / 2)
//         if A[m] < T then
//             L := m + 1
//         else if A[m] > T then
//             R := m − 1
//         else:
//             return m
//     return unsuccessful

func binarySearch(in []docPointer, needle uint64) bool {
	left := 0
	right := len(in) - 1

	for left <= right {
		m := int(math.Floor(float64((left + right)) / float64(2)))
		if in[m].id < needle {
			left = m + 1
		} else if in[m].id > needle {
			right = m - 1
		} else {
			return true
		}
	}

	return false
}
