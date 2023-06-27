//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build !race

package ssdhelpers_test

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	ssdhelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
	testinghelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

type IndexAndDistance struct {
	index    uint64
	distance float32
}

func distance(dp distancer.Provider) func(x, y []float32) float32 {
	return func(x, y []float32) float32 {
		dist, _, _ := dp.SingleDist(x, y)
		return dist
	}
}

func Test_NoRacePQSettings(t *testing.T) {
	distanceProvider := distancer.NewL2SquaredProvider()
	_, err := ssdhelpers.NewProductQuantizer(
		128,
		512,
		false,
		distanceProvider,
		128,
		ssdhelpers.UseKMeansEncoder,
		ssdhelpers.LogNormalEncoderDistribution,
	)
	assert.NotNil(t, err)
}

func Test_NoRacePQKMeans(t *testing.T) {
	dimensions := 128
	vectors_size := 1000
	queries_size := 100
	k := 100
	vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, int(dimensions))
	distanceProvider := distancer.NewL2SquaredProvider()

	pq, _ := ssdhelpers.NewProductQuantizer(
		dimensions,
		255,
		false,
		distanceProvider,
		dimensions,
		ssdhelpers.UseKMeansEncoder,
		ssdhelpers.LogNormalEncoderDistribution,
	)
	pq.Fit(vectors)
	encoded := make([][]byte, vectors_size)
	for i := 0; i < vectors_size; i++ {
		encoded[i] = pq.Encode(vectors[i])
	}

	var relevant uint64
	queries_size = 100
	for _, query := range queries {
		truth := testinghelpers.BruteForce(vectors, query, k, distance(distanceProvider))
		distances := make([]IndexAndDistance, len(vectors))

		lut := pq.CenterAt(query)
		for v := range vectors {
			distances[v] = IndexAndDistance{index: uint64(v), distance: pq.Distance(encoded[v], lut)}
		}
		sort.Slice(distances, func(a, b int) bool {
			return distances[a].distance < distances[b].distance
		})

		results := make([]uint64, 0, k)
		for i := 0; i < k; i++ {
			results = append(results, distances[i].index)
		}
		relevant += testinghelpers.MatchesInLists(truth, results)
	}
	recall := float32(relevant) / float32(k*queries_size)
	fmt.Println(recall)
	assert.True(t, recall > 0.99)
}

func Test_NoRacePQDecodeBytes(t *testing.T) {
	t.Run("extracts correctly on one code per byte", func(t *testing.T) {
		amount := 100
		values := make([]byte, 0, amount)
		for i := byte(0); i < byte(amount); i++ {
			values = append(values, i)
		}
		for i := 0; i < amount; i++ {
			code := ssdhelpers.ExtractCode8(values, i)
			assert.Equal(t, code, uint8(i))
		}
	})
}

func Test_NoRacePQEncodeBytes(t *testing.T) {
	t.Run("encodes correctly on one code per byte", func(t *testing.T) {
		amount := 100
		values := make([]byte, amount)
		for i := 0; i < amount; i++ {
			ssdhelpers.PutCode8(uint8(i), values, i)
		}
		for i := 0; i < amount; i++ {
			code := ssdhelpers.ExtractCode8(values, i)
			assert.Equal(t, code, uint8(i))
		}
	})
}
