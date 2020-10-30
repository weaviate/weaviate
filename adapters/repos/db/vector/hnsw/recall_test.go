//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// +build benchmarkRecall

package hnsw

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"testing"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecall(t *testing.T) {
	dimensions := 300
	size := 10000
	queries := 1000
	efConstruction := 256
	maxNeighbors := 120

	vectors := make([][]float32, size)
	queryVectors := make([][]float32, queries)
	var vectorIndex *hnsw

	t.Run("generate random vectors", func(t *testing.T) {
		fmt.Printf("generating %d vectors", size)
		for i := 0; i < size; i++ {
			vector := make([]float32, dimensions)
			for j := 0; j < dimensions; j++ {
				vector[j] = rand.Float32()
			}
			vectors[i] = vector
		}
		fmt.Printf("done\n")

		fmt.Printf("generating %d search queries", queries)
		for i := 0; i < queries; i++ {
			queryVector := make([]float32, dimensions)
			for j := 0; j < dimensions; j++ {
				queryVector[j] = rand.Float32()
			}
			queryVectors[i] = queryVector
		}
		fmt.Printf("done\n")
	})

	t.Run("importing into hnsw", func(t *testing.T) {
		fmt.Printf("importing into hnsw\n")
		cl := &noopCommitLogger{}
		makeCL := func() CommitLogger {
			return cl
		}

		index, err := New(
			"doesnt-matter-as-committlogger-is-mocked-out",
			"recallbenchmark",
			makeCL,
			maxNeighbors, efConstruction,
			func(ctx context.Context, id int32) ([]float32, error) {
				return vectors[int(id)], nil
			})
		require.Nil(t, err)
		vectorIndex = index

		workerCount := runtime.GOMAXPROCS(0)
		jobsForWorker := make([][][]float32, workerCount)

		for i, vec := range vectors {
			workerID := i % workerCount
			jobsForWorker[workerID] = append(jobsForWorker[workerID], vec)
		}

		wg := &sync.WaitGroup{}
		for workerID, jobs := range jobsForWorker {
			wg.Add(1)
			go func(workerID int, myJobs [][]float32) {
				defer wg.Done()
				for i, vec := range myJobs {
					originalIndex := (i * workerCount) + workerID
					err := vectorIndex.Add(originalIndex, vec)
					require.Nil(t, err)
				}
			}(workerID, jobs)
		}

		wg.Wait()
	})

	t.Run("with k=1", func(t *testing.T) {
		k := 1

		var relevant int
		var retrieved int

		for i := 0; i < queries; i++ {
			controlList := bruteForce(vectors, queryVectors[i], k)
			results, err := vectorIndex.SearchByVector(queryVectors[i], k, nil)
			require.Nil(t, err)

			retrieved += k
			relevant += matchesInLists(controlList, results)
		}

		recall := float32(relevant) / float32(retrieved)
		assert.True(t, recall >= 0.99)
	})
}

func matchesInLists(control []int, results []int) int {
	desired := map[int]struct{}{}
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

func bruteForce(vectors [][]float32, query []float32, k int) []int {
	type distanceAndIndex struct {
		distance float32
		index    int
	}

	distances := make([]distanceAndIndex, len(vectors))

	distancer := distancer.NewCosineProvider.New(query)
	for i, vec := range vectors {
		dist, _ := distancer.distance(vec)
		distances[i] = distanceAndIndex{
			index:    i,
			distance: dist,
		}
	}

	sort.Slice(distances, func(a, b int) bool {
		return distances[a].distance < distances[b].distance
	})

	if len(distances) < k {
		k = len(distances)
	}

	out := make([]int, k)
	for i := 0; i < k; i++ {
		out[i] = distances[i].index
	}

	return out
}
