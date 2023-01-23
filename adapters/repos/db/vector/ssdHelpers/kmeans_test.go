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
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	ssdhelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/ssdhelpers"
	testinghelpers "github.com/semi-technologies/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/stretchr/testify/assert"
)

func TestKMeansNNearest(t *testing.T) {
	distanceProvider := distancer.NewL2SquaredProvider()
	vectors := [][]float32{
		{0, 5},
		{0.1, 4.9},
		{0.01, 5.1},
		{10.1, 7},
		{5.1, 2},
		{5.0, 2.1},
	}
	kmeans := ssdhelpers.NewKMeans(
		3,
		distanceProvider,
		2,
	)
	kmeans.Fit(vectors)
	centers := make([]uint64, 6)
	for i := range centers {
		centers[i] = kmeans.Nearest(vectors[i])
	}
	for v := range vectors {
		min, _, _ := distanceProvider.SingleDist(vectors[v], kmeans.Centroid(byte(centers[v])))
		for c := range centers {
			dist, _, _ := distanceProvider.SingleDist(vectors[v], kmeans.Centroid(byte(centers[c])))
			assert.True(t, dist >= min)
		}
	}
}

func extractSegment(i int, v []float32) []float32 {
	return v[i*1 : (i+1)*1]
}

func TestRandomData(t *testing.T) {
	vectors_size := 10000
	vectors, _ := testinghelpers.RandomVecs(vectors_size, 0, 128)
	distanceProvider := ssdhelpers.NewDistanceProvider(distancer.NewL2SquaredProvider())
	before := time.Now()
	kmeans := ssdhelpers.NewKMeansWithFilter(
		256,
		distanceProvider.Provider,
		1,
		func(x []float32) []float32 {
			return extractSegment(int(10), x)
		},
	)
	kmeans.Fit(vectors)
	assert.True(t, time.Since(before).Seconds() < 50)
}
