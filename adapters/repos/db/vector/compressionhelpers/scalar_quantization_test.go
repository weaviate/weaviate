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
	"github.com/stretchr/testify/require"
)

func FuzzScalarQuantizer(f *testing.F) {
	const codes = 255.0
    f.Add(float32(0), float32(1), float32(2), float32(3), float32(4), float32(5), float32(6), float32(7))

    f.Fuzz(func(t *testing.T, v1, v2, v3, v4, v5, v6, v7, v8 float32) {
        data1 := []float32{v1, v2, v3, v4}
        data2 := []float32{v5, v6, v7, v8}

        sq :=compressionhelpers. NewScalarQuantizer([][]float32{data1, data2}, distancer.NewCosineDistanceProvider())
        if sq == nil {
            t.Skip("Invalid input resulted in nil ScalarQuantizer")
        }

        for _, vec := range [][]float32{data1, data2} {
            encoded := sq.Encode(vec)
            decoded := sq.Decode(encoded)

            require.Equal(t, len(vec)+8, len(encoded), "Encoded length should be input length + 8")
            require.Equal(t, len(vec), len(decoded), "Decoded length should match input length")

            for i, decodedVal := range decoded {
                if encoded[i] == 255 {  // Upper bound case
                    assert.GreaterOrEqual(t, decodedVal, sq.B()+sq.A(), 
                        "Decoded value(%v) should be greater than or equal to B+A for upper bound(%v)", decodedVal, sq.B()+sq.A())
                } else {
                    lowerBound := sq.B() + (sq.A() * float32(encoded[i]) / codes)
                    upperBound := sq.B() + (sq.A() * float32(encoded[i]+1) / codes)

                    assert.GreaterOrEqual(t, decodedVal, lowerBound, 
                        "Decoded value should be greater than or equal to lower bound")
                    assert.Less(t, decodedVal, upperBound, 
                        "Decoded value should be less than upper bound")
                }
            }

            encoded2 := sq.Encode(vec)
            assert.Equal(t, encoded, encoded2, "Encoding should be deterministic")
        }

        encoded1 := sq.Encode(data1)
        encoded2 := sq.Encode(data2)
        dist, err := sq.DistanceBetweenCompressedVectors(encoded1, encoded2)
        assert.NoError(t, err, "Distance calculation should not error")
        assert.NotNil(t, dist, "Distance should not be nil")

        distancer := sq.NewDistancer(data1)
        assert.NotNil(t, distancer, "NewDistancer should not return nil")

        dist2, ok, err := distancer.Distance(encoded2)
        assert.True(t, ok, "Distance calculation should be successful")
        assert.NoError(t, err, "Distance calculation should not error")
        assert.Equal(t, dist, dist2, "Distances should match")
    })
}

func Test_NoRaceSQEncode(t *testing.T) {
	sq := compressionhelpers.NewScalarQuantizer([][]float32{
		{0, 0, 0, 0},
		{1, 1, 1, 1},
	}, distancer.NewCosineDistanceProvider())
	vec := []float32{0.5, 1, 0, 2}
	code := sq.Encode(vec)
	assert.NotNil(t, code)
	assert.Equal(t, byte(127), code[0])
	assert.Equal(t, byte(255), code[1])
	assert.Equal(t, byte(0), code[2])
	assert.Equal(t, byte(255), code[3])
}

func Test_NoRaceSQDistance(t *testing.T) {
	distancers := []distancer.Provider{distancer.NewL2SquaredProvider(), distancer.NewCosineDistanceProvider(), distancer.NewDotProductProvider()}
	for _, distancer := range distancers {
		sq := compressionhelpers.NewScalarQuantizer([][]float32{
			{0, 0, 0, 0},
			{1, 1, 1, 1},
		}, distancer)
		vec1 := []float32{0.217, 0.435, 0, 0.348}
		vec2 := []float32{0.241, 0.202, 0.257, 0.300}

		dist, err := sq.DistanceBetweenCompressedVectors(sq.Encode(vec1), sq.Encode(vec2))
		expectedDist, _, _ := distancer.SingleDist(vec1, vec2)
		assert.Nil(t, err)
		if err == nil {
			assert.True(t, math.Abs(float64(expectedDist-dist)) < 0.01)
			fmt.Println(expectedDist-dist, expectedDist, dist)
		}
	}
}

func distancerWrapper(dp distancer.Provider) func(x, y []float32) float32 {
	return func(x, y []float32) float32 {
		dist, _, _ := dp.SingleDist(x, y)
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
				d, _, _ := cd.Distance(xCompressed[j])
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

func Test_NoRaceRandomSQDistanceByteToByte(t *testing.T) {
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
			cd := sq.NewCompressedQuantizerDistancer(sq.Encode(queries[i]))
			for j := range xCompressed {
				before := time.Now()
				d, _, _ := cd.Distance(xCompressed[j])
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
		assert.GreaterOrEqual(t, recall, float32(0.8), distancer.Type())
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
