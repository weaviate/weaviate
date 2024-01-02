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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	testinghelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func Test_NoRaceKMeansNNearest(t *testing.T) {
	distanceProvider := distancer.NewL2SquaredProvider()
	vectors := [][]float32{
		{0, 5},
		{0.1, 4.9},
		{0.01, 5.1},
		{10.1, 7},
		{5.1, 2},
		{5.0, 2.1},
	}
	kmeans := compressionhelpers.NewKMeans(
		3,
		2,
		0,
	)
	kmeans.Fit(vectors)
	centers := make([]byte, 6)
	for i := range centers {
		centers[i] = byte(kmeans.Nearest(vectors[i]))
	}
	for v := range vectors {
		min, _, _ := distanceProvider.SingleDist(vectors[v], kmeans.Centroid(centers[v]))
		for c := range centers {
			dist, _, _ := distanceProvider.SingleDist(vectors[v], kmeans.Centroid(centers[c]))
			assert.True(t, dist >= min)
		}
	}
}

func Test_NoRaceRandomData(t *testing.T) {
	vectorsSize := 10000
	vectors, _ := testinghelpers.RandomVecs(vectorsSize, 0, 128)
	before := time.Now()
	kmeans := compressionhelpers.NewKMeans(
		256,
		1,
		10,
	)
	kmeans.Fit(vectors)
	assert.True(t, time.Since(before).Seconds() < 50)
}
