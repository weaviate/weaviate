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
	"golang.org/x/sys/cpu"
)

func benchmarkDotGo(b *testing.B, dims int) {
	r := getRandomSeed()

	vec1 := make([]float32, dims)
	vec2 := make([]float32, dims)
	for i := range vec1 {
		vec1[i] = r.Float32()
		vec2[i] = r.Float32()
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		DotProductFloatGo(vec1, vec2)
	}
}

func benchmarkDotNeon(b *testing.B, dims int) {
	r := getRandomSeed()

	vec1 := make([]float32, dims)
	vec2 := make([]float32, dims)
	for i := range vec1 {
		vec1[i] = r.Float32()
		vec2[i] = r.Float32()
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		if cpu.ARM64.HasSVE {
			asm.Dot_SVE(vec1, vec2)
		} else {
			asm.Dot_Neon(vec1, vec2)
		}
	}
}

func BenchmarkDot(b *testing.B) {
	dims := []int{30, 32, 128, 256, 300, 384, 600, 768, 1024, 1536}
	for _, dim := range dims {
		b.Run(fmt.Sprintf("%d dimensions", dim), func(b *testing.B) {
			b.Run("pure go", func(b *testing.B) { benchmarkDotGo(b, dim) })
			b.Run("avx", func(b *testing.B) { benchmarkDotNeon(b, dim) })
		})
	}
}

func benchmarkHammingGo(b *testing.B, dims int) {
	r := getRandomSeed()

	vec1 := make([]float32, dims)
	vec2 := make([]float32, dims)
	for i := range vec1 {
		vec1[i] = r.Float32()
		vec2[i] = r.Float32()
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		HammingDistanceGo(vec1, vec2)
	}
}

func benchmarkHammingNeon(b *testing.B, dims int) {
	r := getRandomSeed()

	vec1 := make([]float32, dims)
	vec2 := make([]float32, dims)
	for i := range vec1 {
		vec1[i] = r.Float32()
		vec2[i] = r.Float32()
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		asm.Hamming(vec1, vec2)
	}
}

func BenchmarkHamming(b *testing.B) {
	dims := []int{
		1,
		2,
		3,
		4,
		5,
		6,
		8,
		10,
		12,
		16,
		24,
		30,
		31,
		32,
		64,
		67,
		128,
		256,
		260,
		299,
		300,
		384,
		390,
		600,
		768,
		777,
		784,
		1024,
		1536,
	}

	for _, dim := range dims {
		b.Run(fmt.Sprintf("%d dimensions", dim), func(b *testing.B) {
			b.Run("pure go", func(b *testing.B) { benchmarkHammingGo(b, dim) })
			b.Run("avx", func(b *testing.B) { benchmarkHammingNeon(b, dim) })
		})
	}
}
