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

//go:build integrationTestBug
// +build integrationTestBug

package hnsw

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func Normalize(v []float32) []float32 {
	var norm float32
	for i := range v {
		norm += v[i] * v[i]
	}

	norm = float32(math.Sqrt(float64(norm)))
	for i := range v {
		v[i] = v[i] / norm
	}

	return v
}

func TestSlowDownBugAtHighEF(t *testing.T) {
	dimensions := 256
	size := 25000
	efConstruction := 2000
	maxNeighbors := 100

	vectors := make([][]float32, size)
	var vectorIndex *hnsw

	t.Run("generate random vectors", func(t *testing.T) {
		fmt.Printf("generating %d vectors", size)
		for i := 0; i < size; i++ {
			vector := make([]float32, dimensions)
			for j := 0; j < dimensions; j++ {
				vector[j] = rand.Float32()
			}
			vectors[i] = Normalize(vector)
		}
		fmt.Printf("done\n")
	})

	t.Run("importing into hnsw", func(t *testing.T) {
		fmt.Printf("importing into hnsw\n")
		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "recallbenchmark",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewDotProductProvider(),
			// DistanceProvider: distancer.NewCosineDistanceProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return nil, nil
			},
		}, UserConfig{
			MaxConnections: maxNeighbors,
			EFConstruction: efConstruction,
		}, testinghelpers.NewDummyStore(t))

		require.Nil(t, err)
		vectorIndex = index

		workerCount := runtime.GOMAXPROCS(0)
		// workerCount := 1
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
		// neighbor := bruteForceCosine(vectors, vectors[0], 2)
		// dist, _, _ := distancer.NewCosineDistanceProvider().SingleDist(vectors[0], vectors[neighbor[1]])
		// fmt.Printf("distance between 0 and %d is %f\n", neighbor[1], dist)
		fmt.Printf("import took %s\n", time.Since(beforeImport))
		// vectorIndex.Dump()

		t.Fail()
	})
}
