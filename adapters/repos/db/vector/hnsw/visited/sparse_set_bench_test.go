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

package visited

import (
	"math/rand"
	"testing"
)

// scatteredNodes returns count pseudo-random node ids in [0, size). Fixed seed
// so runs are comparable across benchmark invocations.
func scatteredNodes(size, count int) []uint64 {
	r := rand.New(rand.NewSource(1))
	nodes := make([]uint64, count)
	for i := range nodes {
		nodes[i] = uint64(r.Intn(size))
	}
	return nodes
}

// BenchmarkSparseSet_FreshSet reproduces the allocation pattern behind
// 0-weaviate-issues#357: the outer sync.Pool drops sets on every GC cycle, so
// under load sets are constantly reborn and every fresh set pays the lazy
// segment allocations again as a search touches segments scattered across the
// graph. collisionRate matches the production value in NewPool.
func BenchmarkSparseSet_FreshSet(b *testing.B) {
	benches := []struct {
		name   string
		size   int
		visits int
	}{
		{"1M-ids_2k-visits", 1_000_000, 2_000},
		{"10M-ids_5k-visits", 10_000_000, 5_000},
		{"100M-ids_5k-visits", 100_000_000, 5_000},
	}
	for _, bc := range benches {
		b.Run(bc.name, func(b *testing.B) {
			nodes := scatteredNodes(bc.size, bc.visits)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s := NewSparseSet(bc.size, 4096)
				for _, n := range nodes {
					s.CheckAndVisit(n)
				}
			}
		})
	}
}

// BenchmarkSparseSet_ReusedSet is the steady state: a set kept alive by the
// outer pool, reset between queries. Segment storage is retained across
// queries, so this should stay at ~0 allocs/op.
func BenchmarkSparseSet_ReusedSet(b *testing.B) {
	const size = 10_000_000
	nodes := scatteredNodes(size, 5_000)
	s := NewSparseSet(size, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Reset()
		for _, n := range nodes {
			s.CheckAndVisit(n)
		}
	}
}
