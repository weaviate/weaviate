package compressionhelpers

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

func genVector2(r *rand.Rand, dimensions int) []float32 {
	vector := make([]float32, 0, dimensions)
	for i := 0; i < dimensions; i++ {
		// Some distances like dot could produce negative values when the vectors have negative values
		// This change will not affect anything when using a distance like l2, but will cover some bugs
		// when using distances like dot
		vector = append(vector, r.Float32()*2-1)
	}
	return vector
}

func getFixedSeed2() *rand.Rand {
	seed := int64(425812)
	return rand.New(rand.NewSource(seed))
}

func randomVecsFixedSeed(size int, queriesSize int, dimensions int) ([][]float32, [][]float32) {
	fmt.Printf("generating %d vectors...\n", size+queriesSize)
	r := getFixedSeed2()
	vectors := make([][]float32, 0, size)
	queries := make([][]float32, 0, queriesSize)
	for i := 0; i < size; i++ {
		vectors = append(vectors, genVector2(r, dimensions))
	}
	for i := 0; i < queriesSize; i++ {
		queries = append(queries, genVector2(r, dimensions))
	}
	return vectors, queries
}

func BenchmarkPartialLASQDotSpeedNoFetching(b *testing.B) {
	vSize := 100_000
	dims := 1536
	data, _ := randomVecsFixedSeed(vSize, 0, dims)
	lasq := NewLocallyAdaptiveScalarQuantizer(data, distancer.NewCosineDistanceProvider())
	compressed := make([][]byte, vSize)
	for i := 0; i < vSize; i++ {
		compressed[i] = lasq.Encode(data[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LAQDotExpImpl(lasq.means, compressed[0][:lasq.dims], compressed[1][:lasq.dims], 1, 1)
	}
}
