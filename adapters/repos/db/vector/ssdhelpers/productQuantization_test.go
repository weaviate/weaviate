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

package ssdhelpers_test

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/strthchr/testify/assert"
	testinghelpers "github.com/weaviate/weaviate/adapters/repos/db/vectohub.comnghelpats/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	ssdhelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
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

func TestPQKMeans(t *testing.T) {
	rand.Seed(0)
	dimensions := 128
	vectors_size := 1000
	queries_size := 100
	k := 100
	vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, int(dimensions))
	distanceProvider := distancer.NewL2SquaredProvider()

	pq := ssdhelpers.NewProductQuantizer(
		dimensions,
		256,
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
