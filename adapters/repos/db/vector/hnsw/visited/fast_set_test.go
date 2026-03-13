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
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFastSet_BasicOperations(t *testing.T) {
	f := NewFastSet(100)

	// Initially nothing is visited
	assert.False(t, f.Visited(0))
	assert.False(t, f.Visited(100))
	assert.False(t, f.Visited(1000000))

	// Visit some nodes
	f.Visit(0)
	f.Visit(100)
	f.Visit(1000000)

	// Check they are visited
	assert.True(t, f.Visited(0))
	assert.True(t, f.Visited(100))
	assert.True(t, f.Visited(1000000))

	// Check others are not
	assert.False(t, f.Visited(1))
	assert.False(t, f.Visited(99))
	assert.False(t, f.Visited(999999))
}

func TestFastSet_Reset(t *testing.T) {
	f := NewFastSet(100)

	f.Visit(1)
	f.Visit(2)
	f.Visit(3)

	assert.True(t, f.Visited(1))
	assert.True(t, f.Visited(2))
	assert.True(t, f.Visited(3))

	f.Reset()

	// After reset, nothing should be visited
	assert.False(t, f.Visited(1))
	assert.False(t, f.Visited(2))
	assert.False(t, f.Visited(3))

	// Can visit again
	f.Visit(2)
	assert.False(t, f.Visited(1))
	assert.True(t, f.Visited(2))
	assert.False(t, f.Visited(3))
}

func TestFastSet_ResetOverflow(t *testing.T) {
	f := NewFastSet(100)

	// Simulate 255 resets (marker goes 1->255, then overflows to 0->1)
	for i := range 255 {
		f.Visit(uint64(i))
		f.Reset()
	}

	// After 255 resets, marker overflows and markers are cleared
	// Visit something new
	f.Visit(999)
	assert.True(t, f.Visited(999))
	assert.False(t, f.Visited(0)) // Old entries should not be visited
}

func TestFastSet_Grow(t *testing.T) {
	f := NewFastSet(16) // Small initial size

	// Insert enough to trigger growth
	for i := range 100 {
		f.Visit(uint64(i * 1000))
	}

	// Verify all are still visited
	for i := range 100 {
		assert.True(t, f.Visited(uint64(i*1000)), "node %d should be visited", i*1000)
	}

	// Verify non-visited are not visited
	assert.False(t, f.Visited(1))
	assert.False(t, f.Visited(500))
}

func TestFastPool_BorrowReturn(t *testing.T) {
	pool := NewFastPool(2, 100, 10)

	// Borrow all initial sets
	s1 := pool.Borrow()
	s2 := pool.Borrow()

	// Pool should be empty, next borrow creates new
	s3 := pool.Borrow()

	assert.Equal(t, 0, pool.Len())

	// Use the sets
	s1.Visit(1)
	s2.Visit(2)
	s3.Visit(3)

	// Return them
	pool.Return(s1)
	pool.Return(s2)
	pool.Return(s3)

	assert.Equal(t, 3, pool.Len())

	// Borrow again - should get reset sets
	s4 := pool.Borrow()
	assert.False(t, s4.Visited(1)) // Should be reset
	assert.False(t, s4.Visited(2))
	assert.False(t, s4.Visited(3))
}

// Benchmarks comparing FastSet vs ListSet

func BenchmarkFastSet_Visit_Sparse(b *testing.B) {
	// Scenario: 2M max node ID, visiting 500 nodes
	f := NewFastSet(500)
	rng := rand.New(rand.NewSource(42))
	nodeIDs := make([]uint64, 500)
	for i := range nodeIDs {
		nodeIDs[i] = uint64(rng.Intn(2_000_000))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, id := range nodeIDs {
			f.Visit(id)
		}
		f.Reset()
	}
}

func BenchmarkListSet_Visit_Sparse(b *testing.B) {
	// Scenario: 2M max node ID, visiting 500 nodes
	l := NewList(2_000_000)
	rng := rand.New(rand.NewSource(42))
	nodeIDs := make([]uint64, 500)
	for i := range nodeIDs {
		nodeIDs[i] = uint64(rng.Intn(2_000_000))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, id := range nodeIDs {
			l.Visit(id)
		}
		l.Reset()
	}
}

func BenchmarkFastSet_Visit_Dense(b *testing.B) {
	// Scenario: 50K max node ID, visiting 10K nodes
	f := NewFastSet(10_000)
	rng := rand.New(rand.NewSource(42))
	nodeIDs := make([]uint64, 10_000)
	for i := range nodeIDs {
		nodeIDs[i] = uint64(rng.Intn(50_000))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, id := range nodeIDs {
			f.Visit(id)
		}
		f.Reset()
	}
}

func BenchmarkListSet_Visit_Dense(b *testing.B) {
	// Scenario: 50K max node ID, visiting 10K nodes
	l := NewList(50_000)
	rng := rand.New(rand.NewSource(42))
	nodeIDs := make([]uint64, 10_000)
	for i := range nodeIDs {
		nodeIDs[i] = uint64(rng.Intn(50_000))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, id := range nodeIDs {
			l.Visit(id)
		}
		l.Reset()
	}
}

func BenchmarkFastSet_Visited(b *testing.B) {
	f := NewFastSet(500)
	rng := rand.New(rand.NewSource(42))

	// Pre-visit some nodes
	for range 500 {
		f.Visit(uint64(rng.Intn(2_000_000)))
	}

	// Generate lookup IDs (mix of visited and not visited)
	lookupIDs := make([]uint64, 1000)
	for i := range lookupIDs {
		lookupIDs[i] = uint64(rng.Intn(2_000_000))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, id := range lookupIDs {
			_ = f.Visited(id)
		}
	}
}

func BenchmarkListSet_Visited(b *testing.B) {
	l := NewList(2_000_000)
	rng := rand.New(rand.NewSource(42))

	// Pre-visit some nodes
	for range 500 {
		l.Visit(uint64(rng.Intn(2_000_000)))
	}

	// Generate lookup IDs (mix of visited and not visited)
	lookupIDs := make([]uint64, 1000)
	for i := range lookupIDs {
		lookupIDs[i] = uint64(rng.Intn(2_000_000))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, id := range lookupIDs {
			_ = l.Visited(id)
		}
	}
}

// Memory benchmark - this shows the allocation difference
func BenchmarkFastSet_Memory_2M_Sparse(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		f := NewFastSet(500)
		for j := range 500 {
			f.Visit(uint64(j * 4000)) // Spread across 2M range
		}
	}
}

func BenchmarkListSet_Memory_2M_Sparse(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l := NewList(2_000_000)
		for j := range 500 {
			l.Visit(uint64(j * 4000)) // Spread across 2M range
		}
	}
}

func BenchmarkFastSet_Visit_Clustered(b *testing.B) {
	f := NewFastSet(500)
	// Clustered: 500 nodes in range 0-5000 (neighbors are close in ID space)
	rng := rand.New(rand.NewSource(42))
	nodeIDs := make([]uint64, 500)
	base := uint64(rng.Intn(2_000_000))
	for i := range nodeIDs {
		nodeIDs[i] = base + uint64(rng.Intn(5000))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, id := range nodeIDs {
			f.Visit(id)
		}
		f.Reset()
	}
}

func BenchmarkListSet_Visit_Clustered(b *testing.B) {
	l := NewList(2_000_000)
	// Clustered: 500 nodes in range 0-5000 (neighbors are close in ID space)
	rng := rand.New(rand.NewSource(42))
	nodeIDs := make([]uint64, 500)
	base := uint64(rng.Intn(2_000_000))
	for i := range nodeIDs {
		nodeIDs[i] = base + uint64(rng.Intn(5000))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, id := range nodeIDs {
			l.Visit(id)
		}
		l.Reset()
	}
}

func BenchmarkVisit_1M(b *testing.B) {
	// 1M max node ID, varying number of visited nodes
	for _, visitCount := range []int{1_000, 10_000, 100_000} {
		rng := rand.New(rand.NewSource(42))
		nodeIDs := make([]uint64, visitCount)
		for i := range nodeIDs {
			nodeIDs[i] = uint64(rng.Intn(1_000_000))
		}

		name := fmt.Sprintf("%dk", visitCount/1000)

		b.Run("FastSet/"+name, func(b *testing.B) {
			f := NewFastSet(visitCount)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for _, id := range nodeIDs {
					f.Visit(id)
				}
				f.Reset()
			}
		})

		b.Run("ListSet/"+name, func(b *testing.B) {
			l := NewList(1_000_000)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for _, id := range nodeIDs {
					l.Visit(id)
				}
				l.Reset()
			}
		})
	}
}
