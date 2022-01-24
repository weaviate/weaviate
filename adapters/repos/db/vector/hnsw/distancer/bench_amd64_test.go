//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package distancer

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
)

func benchmarkDotGo(b *testing.B, dims int) {
	rand.Seed(time.Now().UnixNano())

	vec1 := make([]float32, dims)
	vec2 := make([]float32, dims)
	for i := range vec1 {
		vec1[i] = rand.Float32()
		vec2[i] = rand.Float32()
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		DotProductGo(vec1, vec2)
	}
}

func benchmarkDotAVX(b *testing.B, dims int) {
	rand.Seed(time.Now().UnixNano())

	vec1 := make([]float32, dims)
	vec2 := make([]float32, dims)
	for i := range vec1 {
		vec1[i] = rand.Float32()
		vec2[i] = rand.Float32()
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		asm.Dot(vec1, vec2)
	}
}

func BenchmarkDot(b *testing.B) {
	dims := []int{256, 300, 600, 768, 1024}
	for _, dim := range dims {
		b.Run(fmt.Sprintf("%d dimensions", dim), func(b *testing.B) {
			b.Run("pure go", func(b *testing.B) { benchmarkDotGo(b, dim) })
			b.Run("avx", func(b *testing.B) { benchmarkDotAVX(b, dim) })
		})
	}
}
