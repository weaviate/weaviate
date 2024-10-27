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

//go:build !race

package compressionhelpers_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	testinghelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func Test_NoRaceRandomLASQDistanceByteToByte(t *testing.T) {
	distancers := []distancer.Provider{distancer.NewL2SquaredProvider(), distancer.NewDotProductProvider(), distancer.NewCosineDistanceProvider()}
	vSize := 100
	qSize := 10
	dims := 150
	k := 10
	data, queries := testinghelpers.RandomVecsFixedSeed(vSize, qSize, dims)
	testinghelpers.Normalize(data)
	testinghelpers.Normalize(queries)
	for _, distancer := range distancers {
		sq := compressionhelpers.NewLocallyAdaptiveScalarQuantizer(data, distancer)
		neighbors := make([][]uint64, qSize)
		for j, y := range queries {
			neighbors[j], _ = testinghelpers.BruteForce(logrus.New(), data, y, k, distancerWrapper(distancer))
		}
		xCompressed := make([][]byte, vSize)
		for i, x := range data {
			xCompressed[i] = sq.Encode(x)
		}
		var relevant uint64
		mutex := sync.Mutex{}
		ellapsed := time.Duration(0)
		compressionhelpers.Concurrently(logger, uint64(len(queries)), func(i uint64) {
			heap := priorityqueue.NewMax[any](k)
			cQuery := sq.Encode(queries[i])
			for j := range xCompressed {
				before := time.Now()
				d, _ := sq.DistanceBetweenCompressedVectors(xCompressed[j], cQuery)
				ell := time.Since(before)
				mutex.Lock()
				ellapsed += ell
				mutex.Unlock()
				if heap.Len() < k || heap.Top().Dist > d {
					if heap.Len() == k {
						heap.Pop()
					}
					heap.Insert(uint64(j), d)
				}
			}
			results := make([]uint64, 0, k)
			for heap.Len() > 0 {
				results = append(results, heap.Pop().ID)
			}
			hits := matchesInLists(neighbors[i][:k], results)
			mutex.Lock()
			relevant += hits
			mutex.Unlock()
		})

		recall := float32(relevant) / float32(k*len(queries))
		latency := float32(ellapsed.Microseconds()) / float32(len(queries))
		fmt.Println(distancer.Type(), recall, latency)
		assert.GreaterOrEqual(t, recall, float32(0.98), distancer.Type())
	}
}

func BenchmarkLASQDotSpeed(b *testing.B) {
	vSize := 100_000
	dims := 1536
	data, _ := testinghelpers.RandomVecsFixedSeed(vSize, 0, dims)
	lasq := compressionhelpers.NewLocallyAdaptiveScalarQuantizer(data, distancer.NewCosineDistanceProvider())
	compressed := make([][]byte, vSize)
	for i := 0; i < vSize; i++ {
		compressed[i] = lasq.Encode(data[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		i0 := i * 100 % vSize
		i1 := i % vSize
		lasq.DistanceBetweenCompressedVectors(compressed[i0], compressed[i1])
	}
}

func BenchmarkFloatDotSpeed(b *testing.B) {
	vSize := 100_000
	dims := 1536
	data, _ := testinghelpers.RandomVecsFixedSeed(vSize, 0, dims)
	l2Dist := distancer.NewDotProductProvider()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		i0 := i * 100 % vSize
		i1 := i % vSize
		l2Dist.SingleDist(data[i0], data[i1])
	}
}

func BenchmarkLASQDotSpeedNoFetching(b *testing.B) {
	vSize := 100_000
	dims := 1536
	data, _ := testinghelpers.RandomVecsFixedSeed(vSize, 0, dims)
	lasq := compressionhelpers.NewLocallyAdaptiveScalarQuantizer(data, distancer.NewCosineDistanceProvider())
	compressed := make([][]byte, vSize)
	for i := 0; i < vSize; i++ {
		compressed[i] = lasq.Encode(data[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lasq.DistanceBetweenCompressedVectors(compressed[0], compressed[1])
	}
}

func BenchmarkFloatDotSpeedNoFetching(b *testing.B) {
	vSize := 100_000
	dims := 1536
	data, _ := testinghelpers.RandomVecsFixedSeed(vSize, 0, dims)
	l2Dist := distancer.NewDotProductProvider()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l2Dist.SingleDist(data[0], data[1])
	}
}

func BenchmarkLASQL2Speed(b *testing.B) {
	vSize := 100
	dims := 1536
	data, _ := testinghelpers.RandomVecsFixedSeed(vSize, 0, dims)
	lasq := compressionhelpers.NewLocallyAdaptiveScalarQuantizer(data, distancer.NewL2SquaredProvider())
	v1 := lasq.Encode(data[1])
	v2 := lasq.Encode(data[2])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lasq.DistanceBetweenCompressedVectors(v1, v2)
		//compressionhelpers.DotByteImpl(v1[:dims], v2[:dims])
	}
}

func BenchmarkSQL2Speed(b *testing.B) {
	vSize := 100
	dims := 1536
	data, _ := testinghelpers.RandomVecsFixedSeed(vSize, 0, dims)
	sq := compressionhelpers.NewScalarQuantizer(data, distancer.NewL2SquaredProvider())
	v1 := sq.Encode(data[1])
	v2 := sq.Encode(data[2])
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sq.DistanceBetweenCompressedVectors(v1, v2)
		//compressionhelpers.DotByteImpl(v1[:dims], v2[:dims])
	}
}

func BenchmarkFloatL2Speed(b *testing.B) {
	vSize := 2
	dims := 1536
	data, _ := testinghelpers.RandomVecsFixedSeed(vSize, 0, dims)
	l2Dist := distancer.NewL2SquaredProvider()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l2Dist.SingleDist(data[0], data[1])
	}
}
