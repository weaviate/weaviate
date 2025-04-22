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
	"math"
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

func Test_NoRaceSQEncode(t *testing.T) {
	sq := compressionhelpers.NewScalarQuantizer([][]float32{
		{1, 0, 0, 0},
		{1, 1, 1, 5},
	}, distancer.NewCosineDistanceProvider())
	vec := []float32{0.5, 1, 0, 2}
	code := sq.Encode(vec)
	assert.NotNil(t, code)
	assert.Equal(t, byte(25), code[0])
	assert.Equal(t, byte(51), code[1])
	assert.Equal(t, byte(0), code[2])
	assert.Equal(t, byte(102), code[3])
}

func Test_NoRaceSQDistance(t *testing.T) {
	distancers := []distancer.Provider{distancer.NewL2SquaredProvider(), distancer.NewCosineDistanceProvider(), distancer.NewDotProductProvider()}
	for _, distancer := range distancers {
		sq := compressionhelpers.NewScalarQuantizer([][]float32{
			{1, 0, 0, 0},
			{1, 1, 1, 5},
		}, distancer)
		vec1 := []float32{0.217, 0.435, 0, 0.348}
		vec2 := []float32{0.241, 0.202, 0.257, 0.300}

		dist, err := sq.DistanceBetweenCompressedVectors(sq.Encode(vec1), sq.Encode(vec2))
		expectedDist, _ := distancer.SingleDist(vec1, vec2)
		assert.Nil(t, err)
		if err == nil {
			assert.True(t, math.Abs(float64(expectedDist-dist)) < 0.0112)
			fmt.Println(expectedDist-dist, expectedDist, dist)
		}
	}
}

func distancerWrapper(dp distancer.Provider) func(x, y []float32) float32 {
	return func(x, y []float32) float32 {
		dist, _ := dp.SingleDist(x, y)
		return dist
	}
}

func Test_NoRaceRandomSQDistanceFloatToByte(t *testing.T) {
	distancers := []distancer.Provider{distancer.NewL2SquaredProvider(), distancer.NewCosineDistanceProvider(), distancer.NewDotProductProvider()}
	vSize := 100
	qSize := 10
	dims := 150
	k := 10
	data, queries := testinghelpers.RandomVecs(vSize, qSize, dims)
	testinghelpers.Normalize(data)
	testinghelpers.Normalize(queries)
	for _, distancer := range distancers {
		sq := compressionhelpers.NewScalarQuantizer(data, distancer)
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
			cd := sq.NewDistancer(queries[i])
			for j := range xCompressed {
				before := time.Now()
				d, _ := cd.Distance(xCompressed[j])
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
		assert.GreaterOrEqual(t, recall, float32(0.95), distancer.Type())

		sqStats := sq.Stats().(compressionhelpers.SQStats)
		assert.GreaterOrEqual(t, sqStats.A, float32(-1))
		assert.GreaterOrEqual(t, sqStats.A, sqStats.B)
		assert.LessOrEqual(t, sqStats.B, float32(1))
	}
}

func Test_NoRaceRandomSQDistanceByteToByte(t *testing.T) {
	distancers := []distancer.Provider{distancer.NewL2SquaredProvider(), distancer.NewCosineDistanceProvider(), distancer.NewDotProductProvider()}
	vSize := 100
	qSize := 10
	dims := 150
	k := 10
	data, queries := testinghelpers.RandomVecsFixedSeed(vSize, qSize, dims)
	testinghelpers.Normalize(data)
	testinghelpers.Normalize(queries)
	for _, distancer := range distancers {
		sq := compressionhelpers.NewScalarQuantizer(data, distancer)
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
			cd := sq.NewCompressedQuantizerDistancer(sq.Encode(queries[i]))
			for j := range xCompressed {
				before := time.Now()
				d, _ := cd.Distance(xCompressed[j])
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
		assert.GreaterOrEqual(t, recall, float32(0.95), distancer.Type())
	}
}

func matchesInLists(control []uint64, results []uint64) uint64 {
	desired := map[uint64]struct{}{}
	for _, relevant := range control {
		desired[relevant] = struct{}{}
	}

	var matches uint64
	for _, candidate := range results {
		_, ok := desired[candidate]
		if ok {
			matches++
		}
	}

	return matches
}
