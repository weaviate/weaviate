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

package flat

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
)

func distanceWrapper(provider distancer.Provider) func(x, y []float32) float32 {
	return func(x, y []float32) float32 {
		dist, _ := provider.SingleDist(x, y)
		return dist
	}
}

func run(ctx context.Context, dirName string, logger *logrus.Logger, compression string, vectorCache bool,
	vectors [][]float32, queries [][]float32, k int, truths [][]uint64,
	extraVectorsForDelete [][]float32, allowIds []uint64,
	distancer distancer.Provider, concurrentCacheReads int,
) (float32, float32, error) {
	vectors_size := len(vectors)
	queries_size := len(queries)
	runId := uuid.New().String()

	store, err := lsmkv.New(dirName, dirName, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		return 0, 0, err
	}

	defer store.Shutdown(context.Background())

	pq := flatent.CompressionUserConfig{
		Enabled: false,
	}
	bq := flatent.CompressionUserConfig{
		Enabled: false,
	}
	switch compression {
	case compressionPQ:
		pq.Enabled = true
		pq.RescoreLimit = 100 * k
		pq.Cache = vectorCache
	case compressionBQ:
		bq.Enabled = true
		bq.RescoreLimit = 100 * k
		bq.Cache = vectorCache
	}
	index, err := New(Config{
		ID:               runId,
		RootPath:         dirName,
		DistanceProvider: distancer,
	}, flatent.UserConfig{
		PQ: pq,
		BQ: bq,
	}, store)
	if err != nil {
		return 0, 0, err
	}
	defer index.Shutdown(context.Background())

	if concurrentCacheReads != 0 {
		index.concurrentCacheReads = concurrentCacheReads
	}

	compressionhelpers.ConcurrentlyWithError(logger, uint64(vectors_size), func(id uint64) error {
		return index.Add(ctx, id, vectors[id])
	})

	for i := range extraVectorsForDelete {
		index.Add(ctx, uint64(vectors_size+i), extraVectorsForDelete[i])
	}

	for i := range extraVectorsForDelete {
		Id := make([]byte, 16)
		binary.BigEndian.PutUint64(Id[8:], uint64(vectors_size+i))
		err := index.Delete(uint64(vectors_size + i))
		if err != nil {
			return 0, 0, err
		}
	}

	buckets := store.GetBucketsByName()
	for _, bucket := range buckets {
		bucket.FlushMemtable()
	}

	var relevant uint64
	var retrieved int
	var querying time.Duration = 0
	mutex := new(sync.Mutex)

	var allowList helpers.AllowList = nil
	if allowIds != nil {
		allowList = helpers.NewAllowList(allowIds...)
	}
	err = nil
	compressionhelpers.Concurrently(logger, uint64(len(queries)), func(i uint64) {
		before := time.Now()
		results, _, _ := index.SearchByVector(ctx, queries[i], k, allowList)

		since := time.Since(before)
		len := len(results)
		matches := testinghelpers.MatchesInLists(truths[i], results)

		if hasDuplicates(results) {
			err = errors.New("results have duplicates")
		}

		mutex.Lock()
		querying += since
		retrieved += len
		relevant += matches
		mutex.Unlock()
	})

	return float32(relevant) / float32(retrieved), float32(querying.Microseconds()) / float32(queries_size), err
}

func hasDuplicates(results []uint64) bool {
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[i] == results[j] {
				return true
			}
		}
	}
	return false
}

func Test_NoRaceFlatIndex(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	dimensions := 256
	vectors_size := 12000
	queries_size := 100
	k := 10
	vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, dimensions)
	testinghelpers.Normalize(vectors)
	testinghelpers.Normalize(queries)
	distancer := distancer.NewCosineDistanceProvider()

	truths := make([][]uint64, queries_size)
	for i := range queries {
		truths[i], _ = testinghelpers.BruteForce(logger, vectors, queries[i], k, distanceWrapper(distancer))
	}

	extraVectorsForDelete, _ := testinghelpers.RandomVecs(5_000, 0, dimensions)
	for _, compression := range []string{compressionNone, compressionBQ} {
		t.Run("compression: "+compression, func(t *testing.T) {
			for _, cache := range []bool{false, true} {
				t.Run("cache: "+strconv.FormatBool(cache), func(t *testing.T) {
					if compression == compressionNone && cache == true {
						return
					}
					targetRecall := float32(0.99)
					if compression == compressionBQ {
						targetRecall = 0.8
					}
					t.Run("recall", func(t *testing.T) {
						recall, latency, err := run(ctx, dirName, logger, compression, cache, vectors, queries, k, truths, nil, nil, distancer, 0)
						require.Nil(t, err)

						fmt.Println(recall, latency)
						assert.Greater(t, recall, targetRecall)
						assert.Less(t, latency, float32(1_000_000))
					})

					t.Run("recall with deletes", func(t *testing.T) {
						recall, latency, err := run(ctx, dirName, logger, compression, cache, vectors, queries, k, truths, extraVectorsForDelete, nil, distancer, 0)
						require.Nil(t, err)

						fmt.Println(recall, latency)
						assert.Greater(t, recall, targetRecall)
						assert.Less(t, latency, float32(1_000_000))
					})
				})
			}
		})
	}
	for _, compression := range []string{compressionNone, compressionBQ} {
		t.Run("compression: "+compression, func(t *testing.T) {
			for _, cache := range []bool{false, true} {
				t.Run("cache: "+strconv.FormatBool(cache), func(t *testing.T) {
					from := 0
					to := 3_000
					for i := range queries {
						truths[i], _ = testinghelpers.BruteForce(logger, vectors[from:to], queries[i], k, distanceWrapper(distancer))
					}

					allowIds := make([]uint64, 0, to-from)
					for i := uint64(from); i < uint64(to); i++ {
						allowIds = append(allowIds, i)
					}
					targetRecall := float32(0.99)
					if compression == compressionBQ {
						targetRecall = 0.8
					}

					t.Run("recall on filtered", func(t *testing.T) {
						recall, latency, err := run(ctx, dirName, logger, compression, cache, vectors, queries, k, truths, nil, allowIds, distancer, 0)
						require.Nil(t, err)

						fmt.Println(recall, latency)
						assert.Greater(t, recall, targetRecall)
						assert.Less(t, latency, float32(1_000_000))
					})

					t.Run("recall on filtered with deletes", func(t *testing.T) {
						recall, latency, err := run(ctx, dirName, logger, compression, cache, vectors, queries, k, truths, extraVectorsForDelete, allowIds, distancer, 0)
						require.Nil(t, err)

						fmt.Println(recall, latency)
						assert.Greater(t, recall, targetRecall)
						assert.Less(t, latency, float32(1_000_000))
					})
				})
			}
		})
	}

	err := os.RemoveAll(dirName)
	if err != nil {
		fmt.Println(err)
	}
}

func TestFlat_QueryVectorDistancer(t *testing.T) {
	logger, _ := test.NewNullLogger()

	cases := []struct {
		pq    bool
		cache bool
		bq    bool
	}{
		{pq: false, cache: false, bq: false},
		{pq: true, cache: false, bq: false},
		{pq: true, cache: true, bq: false},
		{pq: false, cache: false, bq: true},
		{pq: false, cache: true, bq: true},
	}
	for _, tt := range cases {
		t.Run("tt.name", func(t *testing.T) {
			dirName := t.TempDir()

			pq := flatent.CompressionUserConfig{
				Enabled: tt.pq, Cache: tt.cache,
			}
			bq := flatent.CompressionUserConfig{
				Enabled: tt.bq, Cache: tt.cache, RescoreLimit: 10,
			}
			store, err := lsmkv.New(dirName, dirName, logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
			require.Nil(t, err)

			distancr := distancer.NewCosineDistanceProvider()

			index, err := New(Config{
				ID:               "id",
				DistanceProvider: distancr,
			}, flatent.UserConfig{
				PQ: pq,
				BQ: bq,
			}, store)
			require.Nil(t, err)

			index.Add(context.TODO(), uint64(0), []float32{-2, 0})

			dist := index.QueryVectorDistancer([]float32{0, 0})
			require.NotNil(t, dist)
			distance, err := dist.DistanceToNode(0)
			require.Nil(t, err)
			require.Equal(t, distance, float32(1.))

			// get distance for non-existing node above default cache size
			_, err = dist.DistanceToNode(1001)
			require.NotNil(t, err)
		})
	}
}

func TestConcurrentReads(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	dimensions := 256
	vectors_size := 12000
	queries_size := 100
	k := 10
	vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, dimensions)
	testinghelpers.Normalize(vectors)
	testinghelpers.Normalize(queries)
	distancer := distancer.NewCosineDistanceProvider()

	truths := make([][]uint64, queries_size)
	for i := range queries {
		truths[i], _ = testinghelpers.BruteForce(logger, vectors, queries[i], k, distanceWrapper(distancer))
	}

	cores := runtime.GOMAXPROCS(0) * 2

	concurrentReads := []int{1, 2, 4, 8, 16, 32, 64, 128, 256, cores - 1, cores, cores + 1}
	for i := range concurrentReads {
		t.Run("concurrent reads: "+strconv.Itoa(concurrentReads[i]), func(t *testing.T) {
			targetRecall := float32(0.8)
			recall, latency, err := run(ctx, dirName, logger, compressionBQ, true, vectors, queries, k, truths, nil, nil, distancer, concurrentReads[i])
			require.Nil(t, err)

			fmt.Println(recall, latency)
			assert.Greater(t, recall, targetRecall)
			assert.Less(t, latency, float32(1_000_000))
		})
	}
}
