//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build benchmarkRecall
// +build benchmarkRecall

package hnsw

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecall(t *testing.T) {
	efConstruction := 2000
	maxNeighbors := 100

	var vectors [][]float32
	var queries [][]float32
	var truths [][]uint64
	var vectorIndex *hnsw

	t.Run("generate random vectors", func(t *testing.T) {
		vectorsJSON, err := ioutil.ReadFile("recall_vectors.json")
		require.Nil(t, err)
		err = json.Unmarshal(vectorsJSON, &vectors)
		require.Nil(t, err)

		queriesJSON, err := ioutil.ReadFile("recall_queries.json")
		require.Nil(t, err)
		err = json.Unmarshal(queriesJSON, &queries)
		require.Nil(t, err)

		truthsJSON, err := ioutil.ReadFile("recall_truths.json")
		require.Nil(t, err)
		err = json.Unmarshal(truthsJSON, &truths)
		require.Nil(t, err)
	})

	t.Run("importing into hnsw", func(t *testing.T) {
		fmt.Printf("importing into hnsw\n")

		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "recallbenchmark",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewDotProductProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
		}, UserConfig{
			MaxConnections: maxNeighbors,
			EFConstruction: efConstruction,
		})
		require.Nil(t, err)
		vectorIndex = index

		workerCount := runtime.GOMAXPROCS(0)
		jobsForWorker := make([][][]float32, workerCount)

		before := time.Now()
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
					err := vectorIndex.Add(uint64(originalIndex), vec)
					require.Nil(t, err)
				}
			}(workerID, jobs)
		}

		wg.Wait()
		fmt.Printf("importing took %s\n", time.Since(before))
	})

	t.Run("inspect a query", func(t *testing.T) {
		k := 20

		hasDuplicates := 0

		for _, vec := range queries {
			results, err := vectorIndex.SearchByVector(vec, k, nil)
			require.Nil(t, err)
			if containsDuplicates(results) {
				hasDuplicates++
				panic("stop")
			}
		}

		fmt.Printf("%d out of %d searches contained duplicates", hasDuplicates, len(queries))
	})

	t.Run("with k=1", func(t *testing.T) {
		k := 1

		var relevant int
		var retrieved int

		for i := 0; i < len(queries); i++ {
			results, err := vectorIndex.SearchByVector(queries[i], k, nil)
			require.Nil(t, err)

			retrieved += k
			relevant += matchesInLists(truths[i], results)
		}

		recall := float32(relevant) / float32(retrieved)
		fmt.Printf("recall is %f\n", recall)
		assert.True(t, recall >= 0.99)
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

	distancer := distancer.NewDotProductProvider().New(query)
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

func containsDuplicates(in []uint64) bool {
	seen := map[uint64]struct{}{}

	for _, value := range in {
		if _, ok := seen[value]; ok {
			return true
		}
		seen[value] = struct{}{}
	}

	return false
}
