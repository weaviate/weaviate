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

func BenchmarkSparse(b *testing.B) {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	s := NewSparseSet(500_000_000, 8192)
	for i := 0; i < b.N; i++ {
		if rand.Float32() < 0.00001 {
			s.Visit(uint64(i))
		}
	}
	runtime.ReadMemStats(&m2)
	fmt.Println("total memory:", m2.TotalAlloc-m1.TotalAlloc)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Visited(uint64(i))
	}
}

func BenchmarkList(b *testing.B) {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	s := NewList(500_000_000)
	for i := 0; i < b.N; i++ {
		if rand.Float32() < 0.00001 {
			s.Visit(uint64(i))
		}
	}
	runtime.ReadMemStats(&m2)
	fmt.Println("total memory:", m2.TotalAlloc-m1.TotalAlloc)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Visited(uint64(i))
	}
}

func BenchmarkAllow(b *testing.B) {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	stemp := make([]uint64, 0, 500_000_000)
	for i := 0; i < b.N; i++ {
		if rand.Float32() < 0.0001 {
			stemp = append(stemp, uint64(i))
		}
	}
	runtime.ReadMemStats(&m2)
	fmt.Println("total memory:", m2.TotalAlloc-m1.TotalAlloc)
	s := helpers.NewAllowList(stemp...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Contains(uint64(i))
	}
}
