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

//go:build integrationTest
// +build integrationTest

package hnsw

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/graph"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestUnreachablePoints(t *testing.T) {
	ctx := context.Background()
	var vectors [][]float32
	var vectorIndex *hnsw

	t.Run("generate vectors", func(t *testing.T) {
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
			MaxConnections: 0,
			EFConstruction: 0,
			EF:             0,
		}, cyclemanager.NewCallbackGroupNoop(), nil)
		require.Nil(t, err)
		vectorIndex = index
		groundtruth := [][]uint64{{}, {2}, {}, {1, 2, 3, 4}, {}}

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
					err := vectorIndex.Add(ctx, uint64(originalIndex), vec)
					require.Nil(t, err)
				}
			}(workerID, jobs)
		}

		wg.Wait()
		fmt.Printf("importing took %s\n", time.Since(before))

		for i := 1; i <= 5; i++ {
			vectorIndex.generateGraphConnections(i)
			res := vectorIndex.calculateUnreachablePoints()
			assert.Equal(t, groundtruth[i-1], res)
			vectorIndex.cleanConnections()
		}
	})
}

func (h *hnsw) generateGraphConnections(testCase int) {
	switch testCase {
	case 1:
		h.entryPointID = 0
		h.currentMaximumLayer = 1
		// Node 0
		node0 := h.nodes.Get(0)
		node0.Edit(func(v *graph.VertexEditor) error {
			v.SetLevel(1)
			v.SetConnectionsAtLevel(1, []uint64{1, 2})
			return nil
		})
		// Node 1
		node1 := h.nodes.Get(1)
		node1.Edit(func(v *graph.VertexEditor) error {
			v.SetLevel(1)
			v.SetConnectionsAtLevel(1, []uint64{3})
			return nil
		})
		// Node 2
		node2 := h.nodes.Get(2)
		node2.Edit(func(v *graph.VertexEditor) error {
			v.SetLevel(1)
			v.SetConnectionsAtLevel(1, []uint64{4})
			return nil
		})
		// Node 3
		node3 := h.nodes.Get(3)
		node3.Edit(func(v *graph.VertexEditor) error {
			v.SetLevel(1)
			return nil
		})
		// Node 4
		node4 := h.nodes.Get(4)
		node4.Edit(func(v *graph.VertexEditor) error {
			v.SetLevel(1)
			return nil
		})
	case 2:
		h.entryPointID = 0
		h.currentMaximumLayer = 1
		// Node 0
		node0 := h.nodes.Get(0)
		node0.Edit(func(v *graph.VertexEditor) error {
			v.SetLevel(1)
			v.SetConnectionsAtLevel(0, []uint64{1})
			return nil
		})
		// Node 1
		node1 := h.nodes.Get(1)
		node1.Edit(func(v *graph.VertexEditor) error {
			v.SetLevel(1)
			v.SetConnectionsAtLevel(0, []uint64{3})
			v.SetConnectionsAtLevel(1, []uint64{2})
			return nil
		})
		// Node 2
		node2 := h.nodes.Get(2)
		node2.Edit(func(v *graph.VertexEditor) error {
			v.SetLevel(1)
			return nil
		})
		// Node 3
		node3 := h.nodes.Get(3)
		node3.Edit(func(v *graph.VertexEditor) error {
			v.SetConnectionsAtLevel(0, []uint64{4})
			return nil
		})
	case 3:
		h.entryPointID = 0
		h.currentMaximumLayer = 1
		// Node 0
		node0 := h.nodes.Get(0)
		node0.Edit(func(v *graph.VertexEditor) error {
			v.SetLevel(1)
			v.SetConnectionsAtLevel(0, []uint64{1})
			return nil
		})
		// Node 1
		node1 := h.nodes.Get(1)
		node1.Edit(func(v *graph.VertexEditor) error {
			v.SetConnectionsAtLevel(0, []uint64{2})
			return nil
		})
		// Node 2
		node2 := h.nodes.Get(2)
		node2.Edit(func(v *graph.VertexEditor) error {
			v.SetConnectionsAtLevel(0, []uint64{3})
			return nil
		})
		// Node 3
		node3 := h.nodes.Get(3)
		node3.Edit(func(v *graph.VertexEditor) error {
			v.SetConnectionsAtLevel(0, []uint64{4})
			return nil
		})
	case 4:
		h.entryPointID = 0
		h.currentMaximumLayer = 2
		// Node 0
		node0 := h.nodes.Get(0)
		node0.Edit(func(v *graph.VertexEditor) error {
			v.SetLevel(2)
			return nil
		})
	case 5:
		h.entryPointID = 0
		h.currentMaximumLayer = 1
		// Node 0
		node0 := h.nodes.Get(0)
		node0.Edit(func(v *graph.VertexEditor) error {
			v.SetLevel(1)
			v.SetConnectionsAtLevel(1, []uint64{1, 2})
			return nil
		})
		// Node 1
		node1 := h.nodes.Get(1)
		node1.Edit(func(v *graph.VertexEditor) error {
			v.SetLevel(1)
			v.SetConnectionsAtLevel(0, []uint64{3})
			return nil
		})
		// Node 2
		node2 := h.nodes.Get(2)
		node2.Edit(func(v *graph.VertexEditor) error {
			v.SetLevel(1)
			return nil
		})
		// Node 3
		node3 := h.nodes.Get(3)
		node3.Edit(func(v *graph.VertexEditor) error {
			v.SetConnectionsAtLevel(0, []uint64{4})
			return nil
		})
	}
}

func (h *hnsw) cleanConnections() {
	h.nodes.Iter(func(id uint64, node *graph.Vertex) bool {
		node.Edit(func(v *graph.VertexEditor) error {
			v.SetLevel(0)
			v.ResetConnectionsWith(make([][]uint64, 1))
			return nil
		})

		return true
	})
}
