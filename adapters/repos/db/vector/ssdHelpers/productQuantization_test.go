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

package ssdhelpers_test

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdHelpers"
	testinghelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/testingHelpers"
	"github.com/stretchr/testify/assert"
)

func compare(x []byte, y []byte) bool {
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

type IndexAndDistance struct {
	index    uint64
	distance float32
}

func TestPQKMeans(t *testing.T) {
	rand.Seed(0)
	dimensions := 128
	vectors_size := 1000
	queries_size := 100
	k := 100
	vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, int(dimensions))
	distanceProvider := ssdhelpers.NewDistanceProvider(distancer.NewL2SquaredProvider())

	pq := ssdhelpers.NewProductQuantizer(
		dimensions,
		256,
		distanceProvider,
		dimensions,
		ssdhelpers.UseKMeansEncoder,
	)
	pq.Fit(vectors)
	encoded := make([][]byte, vectors_size)
	for i := 0; i < vectors_size; i++ {
		encoded[i] = pq.Encode(vectors[i])
	}

	var relevant uint64
	queries_size = 100
	for _, query := range queries {
		truth := testinghelpers.BruteForce(vectors, query, k, distanceProvider.Distance)
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
