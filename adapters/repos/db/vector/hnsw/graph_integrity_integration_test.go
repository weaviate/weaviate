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

//go:build integrationTestSlow || !race
// +build integrationTestSlow !race

package hnsw

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	ent "github.com/semi-technologies/weaviate/entities/vectorindex/hnsw"
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
			DistanceProvider: distancer.NewDotProductProvider(),
		}, ent.UserConfig{
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

		// it is debatable how much value this test still adds. It used to check
		// that a lot of connections are present before we had the heurisitic. But
		// with the heuristic it's not uncommon that a node's connections get
		// reduced to a slow amount of key connections. We have thus set this value
		// to 1 to make sure that no nodes are entirely unconnected, but it's
		// questionable if this still adds any value at all
		requiredMinimum := 1
		assert.True(t, conlen >= requiredMinimum, fmt.Sprintf(
			"have %d connections, but want at least %d", conlen, requiredMinimum))
	}
}
