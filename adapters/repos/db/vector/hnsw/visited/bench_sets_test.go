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

package visited

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

const (
	totalSize     = 500_000_000
	rate          = 0.00001
	collisionRate = 8192
)

func BenchmarkSparseRandom(b *testing.B) {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	s := NewSparseSet(totalSize, collisionRate)
	for i := 0; i < b.N; i++ {
		if rand.Float32() < rate {
			s.Visit(uint64(500067347))
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&m2)
	fmt.Println("total memory:", m2.TotalAlloc-m1.TotalAlloc)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Visited(uint64(i))
	}
}

func BenchmarkListRandom(b *testing.B) {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	s := NewList(totalSize)
	for i := 0; i < b.N; i++ {
		if rand.Float32() < rate {
			s.Visit(uint64(i))
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&m2)
	fmt.Println("total memory:", m2.TotalAlloc-m1.TotalAlloc)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Visited(uint64(i))
	}
}

func BenchmarkAllowRandom(b *testing.B) {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	stemp := make([]uint64, 0, totalSize)
	for i := 0; i < b.N; i++ {
		if rand.Float32() < rate {
			stemp = append(stemp, uint64(i))
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&m2)
	fmt.Println("total memory:", m2.TotalAlloc-m1.TotalAlloc)
	s := helpers.NewAllowList(stemp...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Contains(uint64(i))
	}
}

func BenchmarkSparseTail(b *testing.B) {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	s := NewSparseSet(totalSize, collisionRate)
	for i := 0; i < b.N; i++ {
		if i < rate*totalSize {
			s.Visit(uint64(i))
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&m2)
	fmt.Println("total memory:", m2.TotalAlloc-m1.TotalAlloc)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Visited(uint64(i))
	}
}

func BenchmarkListTail(b *testing.B) {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	s := NewList(totalSize)
	for i := 0; i < b.N; i++ {
		if i < rate*totalSize {
			s.Visit(uint64(i))
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&m2)
	fmt.Println("total memory:", m2.TotalAlloc-m1.TotalAlloc)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Visited(uint64(i))
	}
}

func BenchmarkAllowTail(b *testing.B) {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	stemp := make([]uint64, 0, totalSize)
	for i := 0; i < b.N; i++ {
		if i < rate*totalSize {
			stemp = append(stemp, uint64(i))
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&m2)
	fmt.Println("total memory:", m2.TotalAlloc-m1.TotalAlloc)
	s := helpers.NewAllowList(stemp...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Contains(uint64(i))
	}
}

func BenchmarkSparseReset(b *testing.B) {
	s := NewSparseSet(500_000_000, 8192)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Reset()
	}
}

func BenchmarkList(b *testing.B) {
	s := NewList(500_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Reset()
	}
}

func BenchmarkSets(b *testing.B) {
	b.Run("uniform distribution", func(b *testing.B) {
		b.Run("sparse ", func(b *testing.B) { BenchmarkSparseRandom(b) })
		b.Run("visited ", func(b *testing.B) { BenchmarkListRandom(b) })
		b.Run("allow ", func(b *testing.B) { BenchmarkAllowRandom(b) })
	})
	b.Run("tailing distribution", func(b *testing.B) {
		b.Run("sparse ", func(b *testing.B) { BenchmarkSparseTail(b) })
		b.Run("visited ", func(b *testing.B) { BenchmarkListTail(b) })
		b.Run("allow ", func(b *testing.B) { BenchmarkAllowTail(b) })
	})
}
