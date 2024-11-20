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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestMultiVectorHnsw(t *testing.T) {

	var vectorIndex *hnsw
	ctx := context.Background()
	maxConnections := 32
	efConstruction := 256
	ef := 256

	// Define small 2D and 3D vectors
	vectors := [][][]float32{
		// Document ID: 0
		{
			{0.8, 0.0}, // Relative ID: 0
			{0.0, 1.0}, // Relative ID: 1
			{0.5, 0.5}, // Relative ID: 2
		},

		// Document ID: 1
		/*{
			{7.8, 9.0}, // Relative ID: 0
			{6.4, 8.2}, // Relative ID: 1
		},

		// Document ID: 2
		{
			{100, 47}, // Relative ID: 0
			{1, 0},    // Relative ID: 1
		},*/
	}

	queries := [][][]float32{
		// Query 0
		{
			{0.5, 0.5},
		},

		// Query 1
		/*{
			{50.7, 49.2},
			{20.9, 71.1},
		},*/
	}

	// Expected results for each query
	/*expectedResults := [][]uint64{
		{0, 1, 2},
		{2, 1, 0},
	}*/

	t.Run("importing into hnsw", func(t *testing.T) {

		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "recallbenchmark",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewDotProductProvider(),
			MultipleVectorForIDThunk: func(ctx context.Context, docID uint64, relativeVecID uint64) ([]float32, error) {
				return vectors[docID][relativeVecID], nil
			},
		}, ent.UserConfig{
			MaxConnections: maxConnections,
			EFConstruction: efConstruction,
			EF:             ef,
			Multivector:    true,
		}, cyclemanager.NewCallbackGroupNoop(), nil)
		require.Nil(t, err)
		vectorIndex = index
		fmt.Printf("hnsw created\n")
		//workerCount := runtime.GOMAXPROCS(0)
		workerCount := 1
		jobsForWorker := make([][][][]float32, workerCount)

		before := time.Now()
		for i, vec := range vectors {
			workerID := i % workerCount
			jobsForWorker[workerID] = append(jobsForWorker[workerID], vec)
		}

		wg := &sync.WaitGroup{}
		for workerID, jobs := range jobsForWorker {
			wg.Add(1)
			go func(workerID int, myJobs [][][]float32) {
				defer wg.Done()
				for i, vec := range myJobs {
					originalIndex := (i * workerCount) + workerID
					err := vectorIndex.AddMulti(ctx, uint64(originalIndex), vec)
					require.Nil(t, err)
				}
			}(workerID, jobs)
		}

		wg.Wait()
		fmt.Printf("importing took %s\n", time.Since(before))
	})

	t.Run("inspect a query", func(t *testing.T) {
		k := 100

		for _, query := range queries {
			_, _, err := vectorIndex.SearchByMultipleVector(ctx, query, k, nil)
			require.Nil(t, err)
			//require.Equal(t, expectedResults[i], ids)
		}

	})
}
