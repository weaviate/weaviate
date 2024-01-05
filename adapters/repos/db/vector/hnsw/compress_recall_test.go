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

//go:build !race

package hnsw_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func distanceWrapper(provider distancer.Provider) func(x, y []float32) float32 {
	return func(x, y []float32) float32 {
		dist, _, _ := provider.SingleDist(x, y)
		return dist
	}
}

func Test_NoRaceCompressionRecall(t *testing.T) {
	path := t.TempDir()

	efConstruction := 64
	ef := 64
	maxNeighbors := 32
	segments := 4
	dimensions := 64
	vectors_size := 10000
	queries_size := 100
	fmt.Println("Sift1M PQ")
	before := time.Now()
	vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, dimensions)
	testinghelpers.Normalize(vectors)
	testinghelpers.Normalize(queries)
	k := 100

	distancers := []distancer.Provider{
		distancer.NewL2SquaredProvider(),
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
	}

	for _, distancer := range distancers {
		truths := make([][]uint64, queries_size)
		compressionhelpers.Concurrently(uint64(len(queries)), func(i uint64) {
			truths[i], _ = testinghelpers.BruteForce(vectors, queries[i], k, distanceWrapper(distancer))
		})
		fmt.Printf("generating data took %s\n", time.Since(before))

		uc := ent.UserConfig{
			MaxConnections:        maxNeighbors,
			EFConstruction:        efConstruction,
			EF:                    ef,
			VectorCacheMaxObjects: 10e12,
		}
		index, _ := hnsw.New(hnsw.Config{
			RootPath:              path,
			ID:                    "recallbenchmark",
			MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
			ClassName:             "clasRecallBenchmark",
			ShardName:             "shardRecallBenchmark",
			DistanceProvider:      distancer,
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				if int(id) >= len(vectors) {
					return nil, storobj.NewErrNotFoundf(id, "out of range")
				}
				return vectors[int(id)], nil
			},
			TempVectorForIDThunk: func(ctx context.Context, id uint64, container *common.VectorSlice) ([]float32, error) {
				copy(container.Slice, vectors[int(id)])
				return container.Slice, nil
			},
		}, uc, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
		init := time.Now()
		compressionhelpers.Concurrently(uint64(vectors_size), func(id uint64) {
			index.Add(id, vectors[id])
		})
		before = time.Now()
		fmt.Println("Start compressing...")
		uc.PQ = ent.PQConfig{
			Enabled:   true,
			Segments:  dimensions / segments,
			Centroids: 256,
			Encoder:   ent.NewDefaultUserConfig().PQ.Encoder,
		}
		uc.EF = 256
		wg := sync.WaitGroup{}
		wg.Add(1)
		index.UpdateUserConfig(uc, func() {
			fmt.Printf("Time to compress: %s\n", time.Since(before))
			fmt.Printf("Building the index took %s\n", time.Since(init))

			var relevant uint64
			var retrieved int

			var querying time.Duration = 0
			compressionhelpers.Concurrently(uint64(len(queries)), func(i uint64) {
				before = time.Now()
				results, _, _ := index.SearchByVector(queries[i], k, nil)
				querying += time.Since(before)
				retrieved += k
				relevant += testinghelpers.MatchesInLists(truths[i], results)
			})

			recall := float32(relevant) / float32(retrieved)
			latency := float32(querying.Microseconds()) / float32(queries_size)
			fmt.Println(recall, latency)
			assert.True(t, recall > 0.9)

			err := os.RemoveAll(path)
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		})
		wg.Wait()
	}
}
