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

package compressionhelpers_test

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	testinghelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

var logger, _ = test.NewNullLogger()

func TestBinaryQuantizerRecall(t *testing.T) {
	k := 10
	distanceProvider := distancer.NewCosineDistanceProvider()
	vectors, queryVecs := testinghelpers.RandomVecsFixedSeed(10_000, 100, 1536)
	compressionhelpers.Concurrently(logger, uint64(len(vectors)), func(i uint64) {
		vectors[i] = distancer.Normalize(vectors[i])
	})
	compressionhelpers.Concurrently(logger, uint64(len(queryVecs)), func(i uint64) {
		queryVecs[i] = distancer.Normalize(queryVecs[i])
	})
	bq := compressionhelpers.NewBinaryQuantizer(nil)

	codes := make([][]uint64, len(vectors))
	compressionhelpers.Concurrently(logger, uint64(len(vectors)), func(i uint64) {
		codes[i] = bq.Encode(vectors[i])
	})
	neighbors := make([][]uint64, len(queryVecs))
	compressionhelpers.Concurrently(logger, uint64(len(queryVecs)), func(i uint64) {
		neighbors[i], _ = testinghelpers.BruteForce(logger, vectors, queryVecs[i], k, func(f1, f2 []float32) float32 {
			d, _ := distanceProvider.SingleDist(f1, f2)
			return d
		})
	})
	correctedK := 200
	hits := uint64(0)
	mutex := sync.Mutex{}
	duration := time.Duration(0)
	compressionhelpers.Concurrently(logger, uint64(len(queryVecs)), func(i uint64) {
		before := time.Now()
		query := bq.Encode(queryVecs[i])
		heap := priorityqueue.NewMax[any](correctedK)
		for j := range codes {
			d, _ := bq.DistanceBetweenCompressedVectors(codes[j], query)
			if heap.Len() < correctedK || heap.Top().Dist > d {
				if heap.Len() == correctedK {
					heap.Pop()
				}
				heap.Insert(uint64(j), d)
			}
		}
		ids := make([]uint64, correctedK)
		for j := range ids {
			ids[j] = heap.Pop().ID
		}
		mutex.Lock()
		duration += time.Since(before)
		hits += testinghelpers.MatchesInLists(neighbors[i][:k], ids)
		mutex.Unlock()
	})
	recall := float32(hits) / float32(k*len(queryVecs))
	latency := float32(duration.Microseconds()) / float32(len(queryVecs))
	fmt.Println(recall, latency)
	assert.True(t, recall > 0.7)

	// Currently BQ does not expose any stats so just check struct exists
	_, ok := bq.Stats().(compressionhelpers.BQStats)
	assert.True(t, ok)
}

func TestBinaryQuantizerChecksSize(t *testing.T) {
	bq := compressionhelpers.NewBinaryQuantizer(nil)
	_, err := bq.DistanceBetweenCompressedVectors(make([]uint64, 3), make([]uint64, 4))
	assert.NotNil(t, err)
}

func extractBit(code []uint64, idx int) bool {
	return code[idx/64]&(uint64(1)<<(idx%64)) != 0
}

func TestBinaryQuantizerBitAssignmenFixedValues(t *testing.T) {
	test_bits := []struct {
		value           float32
		quantized_value bool
	}{
		{-1.0, true},
		{1.0, false},
		{0.0, false},
		{float32(math.NaN()), false},
		{float32(math.Inf(1)), false},
		{float32(math.Inf(-1)), true},
	}

	bq := compressionhelpers.NewBinaryQuantizer(nil)
	for _, b := range test_bits {
		y := []float32{b.value}
		code := bq.Encode(y)
		assert.True(t, extractBit(code, 0) == b.quantized_value,
			"Unexpected quantized value: %f should quantize to %t",
			b.value, b.quantized_value)
	}
}

func TestBinaryQuantizerBitAssignmenRandomVector(t *testing.T) {
	const seed = 42
	r := rand.New(rand.NewSource(seed))
	x := make([]float32, 1000)
	for i := range len(x) {
		x[i] = 2.0*r.Float32() - 1.0
	}

	bq := compressionhelpers.NewBinaryQuantizer(nil)
	code := bq.Encode(x)
	for i, v := range x {
		assert.True(t, extractBit(code, i) == (v < 0),
			"Unexpected quantized value: %f should quantize to %t", v, v < 0)
	}

	for i := 1000; i < 1024; i++ {
		assert.True(t, extractBit(code, i) == false,
			"Remaining bits should be set to zero.")
	}
}

// Verify that using bit shifts produces the same results as the previous
// approach of computing powers of floats and converting to uint64.
func TestBinaryQuantizerBitShiftBackwardsCompatibility(t *testing.T) {
	for j := range 64 {
		pow_bit := uint64(math.Pow(2, float64(j%64)))
		shift_bit := uint64(1) << (j % 64)
		assert.True(t, pow_bit == shift_bit)
	}
}

func BenchmarkBinaryQuantization(b *testing.B) {
	bq := compressionhelpers.NewBinaryQuantizer(nil)
	const seed = 42
	r := rand.New(rand.NewSource(seed))
	for _, d := range []int{32, 64, 100, 256, 500, 1024, 4096} {
		x := make([]float32, d)
		for i := range d {
			x[i] = 2.0*r.Float32() - 1.0
		}
		b.Run(fmt.Sprintf("BinaryQuantization-dim-%d", d), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// Even though we do not use the output of bq.Encode() the call
				// does not seem to be optimized away by the compiler.
				// TODO: Use b.Loop() instead when we move to Go 1.24.
				bq.Encode(x)
			}
		})
	}
}
