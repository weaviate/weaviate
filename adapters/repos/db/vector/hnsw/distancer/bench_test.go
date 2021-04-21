package distancer

import (
	"math/rand"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
)

var dims = 256

func BenchmarkDotGo(b *testing.B) {
	rand.Seed(time.Now().UnixNano())

	vec1 := make([]float32, dims)
	vec2 := make([]float32, dims)
	for i := range vec1 {
		vec1[i] = rand.Float32()
		vec2[i] = rand.Float32()
	}

	b.ResetTimer()
	// run the Fib function b.N times
	for n := 0; n < b.N; n++ {
		DotProductGo(vec1, vec2)
	}
}

func BenchmarkDotAVX(b *testing.B) {
	rand.Seed(time.Now().UnixNano())

	vec1 := make([]float32, dims)
	vec2 := make([]float32, dims)
	for i := range vec1 {
		vec1[i] = rand.Float32()
		vec2[i] = rand.Float32()
		// vec1[i] = 3
		// vec2[i] = 2
	}

	b.ResetTimer()
	// run the Fib function b.N times
	for n := 0; n < b.N; n++ {
		// _ = 1 - DotProductAVXScratch(&vec1[0], &vec2[0], size)
		asm.Dot(vec1, vec2)
	}
}
