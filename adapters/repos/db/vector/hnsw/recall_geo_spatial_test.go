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

//go:build integrationTestSlow && !race
// +build integrationTestSlow,!race

package hnsw

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestRecallGeo(t *testing.T) {
	size := 10000
	queries := 100
	efConstruction := 128
	maxNeighbors := 64

	vectors := make([][]float32, size)
	queryVectors := make([][]float32, queries)
	var vectorIndex *hnsw

	t.Run("generate random vectors", func(t *testing.T) {
		fmt.Printf("generating %d vectors", size)
		for i := 0; i < size; i++ {
			lat, lon := randLatLon()
			vectors[i] = []float32{lat, lon}
		}
		fmt.Printf("done\n")

		fmt.Printf("generating %d search queries", queries)
		for i := 0; i < queries; i++ {
			lat, lon := randLatLon()
			queryVectors[i] = []float32{lat, lon}
		}
		fmt.Printf("done\n")
	})

	t.Run("importing into hnsw", func(t *testing.T) {
		fmt.Printf("importing into hnsw\n")
		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "recallbenchmark",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewGeoProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
		}, ent.UserConfig{
			MaxConnections: maxNeighbors,
			EFConstruction: efConstruction,
		}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))

		require.Nil(t, err)
		vectorIndex = index

		workerCount := runtime.GOMAXPROCS(0)
		jobsForWorker := make([][][]float32, workerCount)

		for i, vec := range vectors {
			workerID := i % workerCount
			jobsForWorker[workerID] = append(jobsForWorker[workerID], vec)
		}

		beforeImport := time.Now()
		wg := &sync.WaitGroup{}
		for workerID, jobs := range jobsForWorker {
			wg.Add(1)
			go func(workerID int, myJobs [][]float32) {
				defer wg.Done()
				for i, vec := range myJobs {
					originalIndex := (i * workerCount) + workerID
					err := vectorIndex.Add(uint64(originalIndex), vec)
					require.Nil(t, err)
				}
			}(workerID, jobs)
		}

		wg.Wait()
		fmt.Printf("import took %s\n", time.Since(beforeImport))
	})

	t.Run("with k=10", func(t *testing.T) {
		k := 10

		var relevant int
		var retrieved int

		var times time.Duration

		for i := 0; i < queries; i++ {
			controlList := bruteForce(vectors, queryVectors[i], k)
			before := time.Now()
			results, _, err := vectorIndex.knnSearchByVector(queryVectors[i], k, 800, nil)
			times += time.Since(before)

			require.Nil(t, err)

			retrieved += k
			relevant += matchesInLists(controlList, results)
		}

		recall := float32(relevant) / float32(retrieved)
		fmt.Printf("recall is %f\n", recall)
		fmt.Printf("avg search time for k=%d is %s\n", k, times/time.Duration(queries))
		assert.True(t, recall >= 0.99)
	})

	t.Run("with max dist set", func(t *testing.T) {
		distances := []float32{
			0.1,
			1,
			10,
			100,
			1000,
			2000,
			5000,
			7500,
			10000,
			12500,
			15000,
			20000,
			35000,
			100000, // larger than the circumference of the earth, should contain all
		}

		for _, maxDist := range distances {
			t.Run(fmt.Sprintf("with maxDist=%f", maxDist), func(t *testing.T) {
				var relevant int
				var retrieved int

				var times time.Duration

				for i := 0; i < queries; i++ {
					controlList := bruteForceMaxDist(vectors, queryVectors[i], maxDist)
					before := time.Now()
					results, err := vectorIndex.KnnSearchByVectorMaxDist(queryVectors[i], maxDist, 800, nil)
					times += time.Since(before)
					require.Nil(t, err)

					retrieved += len(results)
					relevant += matchesInLists(controlList, results)
				}

				if relevant == 0 {
					// skip, as we risk dividing by zero, if both relevant and retrieved
					// are zero, however, we want to fail with a divide-by-zero if only
					// retrieved is 0 and relevant was more than 0
					return
				}
				recall := float32(relevant) / float32(retrieved)
				fmt.Printf("recall is %f\n", recall)
				fmt.Printf("avg search time for maxDist=%f is %s\n", maxDist, times/time.Duration(queries))
				assert.True(t, recall >= 0.99)
			})
		}
	})
}

func matchesInLists(control []uint64, results []uint64) int {
	desired := map[uint64]struct{}{}
	for _, relevant := range control {
		desired[relevant] = struct{}{}
	}

	var matches int
	for _, candidate := range results {
		_, ok := desired[candidate]
		if ok {
			matches++
		}
	}

	return matches
}

func bruteForce(vectors [][]float32, query []float32, k int) []uint64 {
	type distanceAndIndex struct {
		distance float32
		index    uint64
	}

	distances := make([]distanceAndIndex, len(vectors))

	distancer := distancer.NewGeoProvider().New(query)
	for i, vec := range vectors {
		dist, _, _ := distancer.Distance(vec)
		distances[i] = distanceAndIndex{
			index:    uint64(i),
			distance: dist,
		}
	}

	sort.Slice(distances, func(a, b int) bool {
		return distances[a].distance < distances[b].distance
	})

	if len(distances) < k {
		k = len(distances)
	}

	out := make([]uint64, k)
	for i := 0; i < k; i++ {
		out[i] = distances[i].index
	}

	return out
}

func bruteForceMaxDist(vectors [][]float32, query []float32, maxDist float32) []uint64 {
	type distanceAndIndex struct {
		distance float32
		index    uint64
	}

	distances := make([]distanceAndIndex, len(vectors))

	distancer := distancer.NewGeoProvider().New(query)
	for i, vec := range vectors {
		dist, _, _ := distancer.Distance(vec)
		distances[i] = distanceAndIndex{
			index:    uint64(i),
			distance: dist,
		}
	}

	sort.Slice(distances, func(a, b int) bool {
		return distances[a].distance < distances[b].distance
	})

	out := make([]uint64, len(distances))
	i := 0
	for _, elem := range distances {
		if elem.distance > maxDist {
			break
		}
		out[i] = distances[i].index
		i++
	}

	return out[:i]
}

func randLatLon() (float32, float32) {
	maxLat := float32(90.0)
	minLat := float32(-90.0)
	maxLon := float32(180)
	minLon := float32(-180)

	lat := minLat + (maxLat-minLat)*rand.Float32()
	lon := minLon + (maxLon-minLon)*rand.Float32()
	return lat, lon
}
