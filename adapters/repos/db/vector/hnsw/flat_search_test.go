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

package hnsw

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func Test_NoRaceCompressionRecall(t *testing.T) {
	for _, includeDeletes := range []bool{false, true} {
		for _, concurrency := range []int{1, 2, 4, 8} {
			t.Run(fmt.Sprintf("deletes=%v concurrencys= %d", includeDeletes, concurrency), func(t *testing.T) {
				path := t.TempDir()
				logger, _ := test.NewNullLogger()
				ctx := context.Background()

				efConstruction := 4
				ef := 64
				maxNeighbors := 4
				segments := 4
				dimensions := 16
				vectors_size := 10_000
				queries_size := 100
				before := time.Now()
				vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, dimensions)
				testinghelpers.Normalize(vectors)
				testinghelpers.Normalize(queries)
				k := 10

				distancer := distancer.NewCosineDistanceProvider()

				allowList := helpers.NewAllowList()
				allowList.Insert(makeRange(0, uint64(vectors_size))...)

				fmt.Printf("generating data took %s\n", time.Since(before))

				uc := ent.UserConfig{
					MaxConnections:        maxNeighbors,
					EFConstruction:        efConstruction,
					EF:                    ef,
					VectorCacheMaxObjects: 10e12,
				}
				rescored := false
				index, _ := New(Config{
					RootPath:              path,
					ID:                    "recallbenchmark",
					MakeCommitLoggerThunk: MakeNoopCommitLogger,
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
						rescored = true
						return container.Slice, nil
					},
				}, uc, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
				init := time.Now()
				compressionhelpers.Concurrently(logger, uint64(vectors_size), func(id uint64) {
					index.Add(ctx, id, vectors[id])
				})
				before = time.Now()
				fmt.Println("Start compressing...")
				uc.PQ = ent.PQConfig{
					Enabled:       true,
					Segments:      dimensions / segments,
					Centroids:     256,
					Encoder:       ent.NewDefaultUserConfig().PQ.Encoder,
					TrainingLimit: 5_000,
				}
				uc.EF = 256
				wg := sync.WaitGroup{}
				wg.Add(1)
				if includeDeletes {
					for i := uint64(0); i < uint64(vectors_size); i += 3 {
						vectors[i] = nil
					}
				}
				truths := make([][]uint64, queries_size)
				compressionhelpers.Concurrently(logger, uint64(len(queries)), func(i uint64) {
					truths[i], _ = testinghelpers.BruteForce(logger, vectors, queries[i], k, testinghelpers.DistanceWrapper(distancer))
				})
				index.UpdateUserConfig(uc, func() {
					fmt.Printf("Time to compress: %s\n", time.Since(before))
					fmt.Printf("Building the index took %s\n", time.Since(init))

					if includeDeletes {
						// delete every 3rd ID from the index
						for i := uint64(0); i < uint64(vectors_size); i += 3 {
							index.Delete(i)
						}
					}

					var relevant uint64
					var retrieved int

					mutex := sync.Mutex{}
					compressionhelpers.Concurrently(logger, uint64(len(queries)), func(i uint64) {
						results, _, _ := index.flatSearch(ctx, queries[i], k, 100, allowList)
						mutex.Lock()
						retrieved += k
						relevant += testinghelpers.MatchesInLists(truths[i], results)
						mutex.Unlock()
					})

					recall := float32(relevant) / float32(retrieved)
					fmt.Println(recall)
					assert.True(t, recall > 0.9)
					assert.True(t, rescored)

					err := os.RemoveAll(path)
					if err != nil {
						fmt.Println(err)
					}
					wg.Done()
				})
				wg.Wait()
			})
		}
	}
}

func makeRange(min, max uint64) []uint64 {
	a := make([]uint64, max-min+1)
	for i := range a {
		a[i] = min + uint64(i)
	}
	return a
}
