// +build integrationTestSlow

package hnsw

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGraphIntegrity(t *testing.T) {
	dimensions := 300
	size := 1000
	efConstruction := 128
	maxNeighbors := 64

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
	})

	t.Run("importing into hnsw", func(t *testing.T) {
		fmt.Printf("importing into hnsw\n")
		cl := &NoopCommitLogger{}
		makeCL := func() (CommitLogger, error) {
			return cl, nil
		}
		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "graphintegrity",
			MakeCommitLoggerThunk: makeCL,
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
			DistanceProvider: distancer.NewCosineProvider(),
		}, UserConfig{
			MaxConnections: maxNeighbors,
			EFConstruction: efConstruction,
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
					originalIndex := uint64(i*workerCount) + uint64(workerID)
					err := vectorIndex.Add(originalIndex, vec)
					require.Nil(t, err)
				}
			}(workerID, jobs)
		}

		wg.Wait()
	})

	for _, node := range vectorIndex.nodes {
		if node == nil {
			continue
		}

		conlen := len(node.connections[0])

		// why exactly half? There is no guarantee that every single node has the
		// maximum number of connections, thus checking for the exact maxNeighbors
		// amount would not be very helpful. However, if nodes don't even have half
		// the number of desired connections we have a good indication that
		// something is wrong
		requiredMinimum := maxNeighbors / 2
		assert.True(t, conlen >= requiredMinimum, fmt.Sprintf(
			"have %d connections, but want at least %d", conlen, requiredMinimum))
	}
}
