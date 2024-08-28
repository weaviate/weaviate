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

//go:build cuvs

package cuvs_index

import (
	"context"
	"testing"

	cuvs "github.com/rapidsai/cuvs/go"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	cuvsEnt "github.com/weaviate/weaviate/entities/vectorindex/cuvs"
	"golang.org/x/exp/rand"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func distanceWrapper(provider distancer.Provider) func(x, y []float32) float32 {
	return func(x, y []float32) float32 {
		dist, _ := provider.SingleDist(x, y)
		return dist
	}
}

func TestCagra(t *testing.T) {

	// rand.Seed(time.Now().UnixNano())

	NDataPoints := 1024
	NFeatures := 128

	TestDataset := make([][]float32, NDataPoints)
	for i := range TestDataset {
		TestDataset[i] = make([]float32, NFeatures)
		for j := range TestDataset[i] {
			TestDataset[i][j] = rand.Float32()
		}
	}

	logger, _ := test.NewNullLogger()

	index, err := New(Config{"a", "vector", logger, cuvs.DistanceL2}, cuvsEnt.UserConfig{}, nil)
	println("here")
	if err != nil {
		panic(err)
	}

	ids := make([]uint64, 1000)
	for i := range ids {
		ids[i] = uint64(i)
	}

	err = index.AddBatch(context.Background(), ids, TestDataset[:1000])

	for i := 0; i < 100000; i++ {
		// err = index.Add(uint64(1001), TestDataset[1001])
		// if err != nil {
		// 	panic(err)
		// }
		err = index.AddBatch(context.Background(), ids, TestDataset[:1000])
		if err != nil {
			panic(err)
		}
		// 	println("adding")
	}

	// use the first 4 points from the dataset as queries : will test that we get them back
	// as their own nearest neighbor

	// NQueries := 4
	K := 1

	// NeighborsDataset := make([][]uint32, NQueries)
	// for i := range NeighborsDataset {
	// 	NeighborsDataset[i] = make([]uint32, K)
	// }
	// DistancesDataset := make([][]float32, NQueries)
	// for i := range DistancesDataset {
	// 	DistancesDataset[i] = make([]float32, K)
	// }

	ids, dists, err := index.SearchByVector(TestDataset[1], K, nil)

	if err != nil {
		panic(err)
	}

	for i := range ids {
		println(ids[i])
		println(dists[i])
	}

	// arr_dist, _ := distances.GetArray()
	// for i := range arr_dist {
	// 	if arr_dist[i][0] >= float32(0.001) || arr_dist[i][0] <= float32(-0.001) {
	// 		t.Error("wrong distance, expected", float32(i), "got", arr_dist[i][0])
	// 	}
	// }

}

func hasDuplicates(results []uint64) bool {
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[i] == results[j] {
				return true
			}
		}
	}
	return false
}

func createEmptyCuvsIndexForTests(t testing.TB, vecForIDFn common.VectorForID[float32]) *cuvs_index {
	// mock out commit logger before adding data so we don't leave a disk
	// footprint. Commit logging and deserializing from a (condensed) commit log
	// is tested in a separate integration test that takes care of providing and
	// cleaning up the correct place on disk to write test files

	store, err := lsmkv.New("lsmkv", "~", logrus.New(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	index, err := New(Config{
		ID:             "unittest",
		TargetVector:   "target",
		Logger:         logrus.New(),
		DistanceMetric: cuvs.DistanceL2,
	}, cuvsEnt.UserConfig{}, store)
	require.Nil(t, err)
	return index
}
