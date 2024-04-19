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

package distancer

import (
	"fmt"
	"testing"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
)

func benchmarkDot(b *testing.B, dims int, dotFn func(a, b []float32) float32) {
	r := getRandomSeed()

	vec1 := make([]float32, dims)
	vec2 := make([]float32, dims)
	for i := range vec1 {
		vec1[i] = r.Float32()
		vec2[i] = r.Float32()
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		dotFn(vec1, vec2)
	}
}

func BenchmarkDot(b *testing.B) {
	dims := []int{2, 4, 6, 8, 10, 12, 16, 24, 30, 32, 128, 256, 300, 384, 512, 768, 1024, 1536}
	for _, dim := range dims {
		b.Run(fmt.Sprintf("%d dimensions", dim), func(b *testing.B) {
			b.Run("pure go", func(b *testing.B) { benchmarkDot(b, dim, DotProductFloatGo) })
			b.Run("avx", func(b *testing.B) { benchmarkDot(b, dim, asm.Dot) })
			b.Run("avx512", func(b *testing.B) { benchmarkDot(b, dim, asm.DotAVX512) })
		})
	}
}

func benchmarkHamming(b *testing.B, dims int, hammingFn func(a, b []float32) float32) {
	r := getRandomSeed()

	vec1 := make([]float32, dims)
	vec2 := make([]float32, dims)
	for i := range vec1 {
		vec1[i] = r.Float32()
		vec2[i] = r.Float32()
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		hammingFn(vec1, vec2)
	}
}

func BenchmarkHamming(b *testing.B) {
	dims := []int{2, 4, 6, 8, 10, 12, 16, 24, 30, 32, 128, 256, 300, 384, 512, 768, 1024, 1536}

	for _, dim := range dims {
		b.Run(fmt.Sprintf("%d dimensions", dim), func(b *testing.B) {
			b.Run("pure go", func(b *testing.B) { benchmarkHamming(b, dim, HammingDistanceGo) })
			b.Run("avx256", func(b *testing.B) { benchmarkHamming(b, dim, asm.DotAVX256) })
			b.Run("avx512", func(b *testing.B) { benchmarkHamming(b, dim, asm.DotAVX512) })
		})
	}
}
