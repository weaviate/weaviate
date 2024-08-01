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

/*go:build integrationTest
+build integrationTest*/

package hnsw

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

var numNodes = 100

func TestUnreachablePoints(t *testing.T) {
	efConstruction := 0
	ef := 0
	maxNeighbors := 0

	var vectors [][]float32
	var vectorIndex *hnsw

	t.Run("generate random vectors", func(t *testing.T) {
		vectors = [][]float32{
			{0, 0, 0},
			{1, 1, 1},
			{-1, -1, -1},
			{-5, -5, -5},
			{5, 5, 5},
		}

	})

	t.Run("importing into hnsw", func(t *testing.T) {
		fmt.Printf("importing into hnsw\n")

		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "recallbenchmark",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewCosineDistanceProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
		}, ent.UserConfig{
			MaxConnections: maxNeighbors,
			EFConstruction: efConstruction,
			EF:             ef,
		}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), nil)
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

		for i := 1; i <= 5; i++ {

			vectorIndex.generateGraphConnections(i)
			/*println("The entrypoint is ", vectorIndex.entryPointID)
			for _, node := range vectorIndex.nodes {
				if node == nil {
					break
				}
				fmt.Println("node.id = ", node.id)
				for level, connection := range node.connections {
					fmt.Println("level = ", level)
					fmt.Println("node.connections = ", connection)
				}
				fmt.Println()
			}*/

			res := vectorIndex.calculateUnreachablePoints()
			fmt.Println("Unreachable points: ", res)
			vectorIndex.cleanConnections()
		}
	})

}

func (h *hnsw) generateGraphConnections(testCase int) {

	switch testCase {
	case 1:
		// CASE 1
		// Entrypoint
		h.entryPointID = 0
		// Current maximum layer
		h.currentMaximumLayer = 1
		// Node 0 connections
		h.nodes[0].upgradeToLevelNoLock(1)
		h.nodes[0].setConnectionsAtLevel(1, []uint64{1, 2})
		// Node 1 connections
		h.nodes[1].upgradeToLevelNoLock(1)
		h.nodes[1].setConnectionsAtLevel(1, []uint64{3})
		// Node 2 connections
		h.nodes[2].upgradeToLevelNoLock(1)
		h.nodes[2].setConnectionsAtLevel(1, []uint64{4})
		// Node 3 connections
		h.nodes[3].upgradeToLevelNoLock(1)
		// Node 4 connections
		h.nodes[4].upgradeToLevelNoLock(1)
	case 2:
		// CASE 2
		// Entrypoint
		h.entryPointID = 0
		// Current maximum layer
		h.currentMaximumLayer = 1
		// Node 0 connections
		h.nodes[0].upgradeToLevelNoLock(1)
		h.nodes[0].setConnectionsAtLevel(0, []uint64{1})
		// Node 1 connections
		h.nodes[1].upgradeToLevelNoLock(1)
		h.nodes[1].setConnectionsAtLevel(0, []uint64{3})
		h.nodes[1].setConnectionsAtLevel(1, []uint64{2})
		// Node 2 connections
		h.nodes[2].upgradeToLevelNoLock(1)
		// Node 3 connections
		h.nodes[3].setConnectionsAtLevel(0, []uint64{4})
	case 3:
		// Entrypoint
		h.entryPointID = 0
		// Current maximum layer
		h.currentMaximumLayer = 1
		// Node 0 connections
		h.nodes[0].upgradeToLevelNoLock(1)
		h.nodes[0].setConnectionsAtLevel(0, []uint64{1})
		// Node 1 connections
		h.nodes[1].setConnectionsAtLevel(0, []uint64{2})
		// Node 2 connections
		h.nodes[2].setConnectionsAtLevel(0, []uint64{3})
		// Node 3 connections
		h.nodes[3].setConnectionsAtLevel(0, []uint64{4})
	case 4:
		// Entrypoint
		h.entryPointID = 0
		// Current maximum layer
		h.currentMaximumLayer = 2
		// Node 0 connections
		h.nodes[0].upgradeToLevelNoLock(2)
	case 5:
		// Entrypoint
		h.entryPointID = 0
		// Current maximum layer
		h.currentMaximumLayer = 1
		// Node 0 connections
		h.nodes[0].upgradeToLevelNoLock(1)
		h.nodes[0].setConnectionsAtLevel(1, []uint64{1, 2})
		// Node 1 connections
		h.nodes[1].upgradeToLevelNoLock(1)
		h.nodes[1].setConnectionsAtLevel(0, []uint64{3})
		// Node 2 connections
		h.nodes[2].upgradeToLevelNoLock(1)
		// Node 3 connections
		h.nodes[3].setConnectionsAtLevel(0, []uint64{4})
	}

}

func (h *hnsw) cleanConnections() {
	for i := 0; i < len(h.nodes); i++ {
		if h.nodes[i] == nil {
			break
		}
		h.nodes[i].connections = make([][]uint64, 1)
		h.nodes[i].level = 0
	}
	fmt.Println("Connections cleaned")
}
