// +build integrationTestBug

package hnsw

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/stretchr/testify/require"
)

func TestSlowDownBugAtHighEF(t *testing.T) {
	dimensions := 256
	size := 10000
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
			vectors[i] = vector
		}
		fmt.Printf("done\n")
	})

	t.Run("importing into hnsw", func(t *testing.T) {
		fmt.Printf("importing into hnsw\n")
		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "recallbenchmark",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewCosineProvider(),
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
		// dist, _, _ := distancer.NewCosineProvider().SingleDist(vectors[0], vectors[neighbor[1]])
		// fmt.Printf("distance between 0 and %d is %f\n", neighbor[1], dist)
		fmt.Printf("import took %s\n", time.Since(beforeImport))

		// vectorIndex.Dump()

		t.Fail()
	})
}
