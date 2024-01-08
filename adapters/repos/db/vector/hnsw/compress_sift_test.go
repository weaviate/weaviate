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

//go:build benchmarkSiftRecall
// +build benchmarkSiftRecall

package hnsw_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func distanceWrapper(provider distancer.Provider) func(x, y []float32) float32 {
	return func(x, y []float32) float32 {
		dist, _, _ := provider.SingleDist(x, y)
		return dist
	}
}

const rootPath = "doesnt-matter-as-committlogger-is-mocked-out"

func TestRecall(t *testing.T) {
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(rootPath)
	fmt.Println("Sift1MPQKMeans 10K/1K")
	efConstruction := 64
	ef := 32
	maxNeighbors := 32
	dimensions := 128
	vectors_size := 200000
	queries_size := 100
	switch_at := vectors_size
	before := time.Now()
	vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, dimensions)
	k := 10
	distancer := distancer.NewL2SquaredProvider()
	fmt.Printf("generating data took %s\n", time.Since(before))

	uc := ent.UserConfig{}
	uc.MaxConnections = maxNeighbors
	uc.EFConstruction = efConstruction
	uc.EF = ef
	uc.VectorCacheMaxObjects = 10e12

	index, _ := hnsw.New(hnsw.Config{
		RootPath:              rootPath,
		ID:                    "recallbenchmark",
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		DistanceProvider:      distancer,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
	}, uc, newDummyStore(t))
	init := time.Now()
	compressionhelpers.Concurrently(uint64(switch_at), func(_, id uint64, _ *sync.Mutex) {
		index.Add(uint64(id), vectors[id])
		if id%1000 == 0 {
			fmt.Println(id, time.Since(before))
		}
	})
	before = time.Now()
	uc.PQ.Enabled = true
	index.UpdateUserConfig(uc, func() {}) /*should have configuration.pr.enabled = true*/
	fmt.Printf("Time to compress: %s", time.Since(before))
	fmt.Println()
	compressionhelpers.Concurrently(uint64(vectors_size-switch_at), func(_, id uint64, _ *sync.Mutex) {
		idx := switch_at + int(id)
		index.Add(uint64(idx), vectors[idx])
		if id%1000 == 0 {
			fmt.Println(idx, time.Since(before))
		}
	})
	fmt.Printf("Building the index took %s\n", time.Since(init))

	lastRecall := float32(0.0)
	for _, currentEF := range []int{32, 64, 128, 256, 512} {
		uc.EF = currentEF
		index.UpdateUserConfig(uc, func() {})
		fmt.Println(currentEF)
		var relevant uint64
		var retrieved int

		var querying time.Duration = 0
		for i := 0; i < len(queries); i++ {
			truth := testinghelpers.BruteForce(vectors, queries[i], k, distanceWrapper(distancer))
			before = time.Now()
			results, _, _ := index.SearchByVector(queries[i], k, nil)
			querying += time.Since(before)
			retrieved += k
			relevant += testinghelpers.MatchesInLists(truth, results)
		}

		recall := float32(relevant) / float32(retrieved)
		assert.True(t, recall > float32(lastRecall))
		lastRecall = recall
	}
	assert.True(t, lastRecall > 0.95)
}

func TestHnswPqGist(t *testing.T) {
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(rootPath)
	params := [][]int{
		//{64, 64, 32},
		{128, 128, 64},
		{256, 256, 128},
		{512, 512, 256},
	}
	dimensions := 960
	vectors_size := 1000000
	queries_size := 1000
	switch_at := 200000

	before := time.Now()
	vectors, queries := testinghelpers.ReadVecs(vectors_size, queries_size, dimensions, "gist", "../diskAnn/testdata")
	testinghelpers.Normalize(vectors)
	testinghelpers.Normalize(queries)
	for i, v := range vectors {
		for j, x := range v {
			if math.IsNaN(float64(x)) {
				fmt.Println(i, j, v, x)
			}
		}
	}
	k := 100
	distancer := distancer.NewCosineDistanceProvider()
	truths := testinghelpers.BuildTruths(queries_size, vectors_size, queries, vectors, k, distanceWrapper(distancer), "../diskAnn/testdata/gist/cosine")
	fmt.Printf("generating data took %s\n", time.Since(before))
	for segmentRate := 3; segmentRate < 4; segmentRate++ {
		fmt.Println(segmentRate)
		fmt.Println()
		for i := 0; i < len(params); i++ {
			efConstruction := params[i][0]
			ef := params[i][1]
			maxNeighbors := params[i][2]

			uc := ent.UserConfig{
				MaxConnections:        maxNeighbors,
				EFConstruction:        efConstruction,
				EF:                    ef,
				VectorCacheMaxObjects: 10e12,
				PQ: ent.PQConfig{
					Enabled:  false,
					Segments: dimensions / int(math.Pow(2, float64(segmentRate))),
					Encoder: ent.PQEncoder{
						Type:         ent.PQEncoderTypeKMeans,
						Distribution: ent.PQEncoderDistributionLogNormal,
					},
				},
			}
			index, _ := hnsw.New(hnsw.Config{
				RootPath:              rootPath,
				ID:                    "recallbenchmark",
				MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
				DistanceProvider:      distancer,
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					return vectors[int(id)], nil
				},
			}, uc, newDummyStore(t))
			init := time.Now()
			total := 200000
			compressionhelpers.Concurrently(uint64(switch_at), func(_, id uint64, _ *sync.Mutex) {
				total++
				if total%100000 == 0 {
					fmt.Println(total)
				}
				index.Add(uint64(id), vectors[id])
			})
			before = time.Now()
			uc.PQ.Enabled = true
			index.UpdateUserConfig(uc, func() {})
			fmt.Printf("Time to compress: %s", time.Since(before))
			fmt.Println()
			compressionhelpers.Concurrently(uint64(vectors_size-switch_at), func(_, id uint64, _ *sync.Mutex) {
				idx := switch_at + int(id)
				index.Add(uint64(idx), vectors[idx])
			})
			fmt.Printf("Building the index took %s\n", time.Since(init))
			var relevant uint64
			var retrieved int

			var querying time.Duration = 0
			compressionhelpers.Concurrently(uint64(len(queries)), func(_, i uint64, _ *sync.Mutex) {
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
			assert.True(t, latency < 100000)
		}
	}
}

/*
10K
128 segments, 16 centroids -> 5.280255291s 0.90662 387.505
64 segments, 256 centroids -> 6.585159916s 0.9326827 410.413

100K
128 segments, 16 centroids -> 1m17.634662125s 0.88258 692.081
64 segments, 256 centroids -> 1m29.259369458s 0.92157 575.844

100000
128
Building the index took 47.7846745s
0.92627 664.66

{64, 64, 32, 256, 0},

	{64, 64, 32, 1024, 1},
	{64, 64, 32, 4096, 1},
	{64, 64, 32, 16384, 1},
	{64, 64, 32, 65536, 1},

generating data took 2.072473792s
0
Time to compress: 5m39.750884042s
Building the index took 16m30.632114542s
0.91401 747.82
1
Time to compress: 13m12.011102334s
Building the index took 28m12.879802125s
0.89564 1041.358
2
Time to compress: 58m15.252058416s
Building the index took 1h37m10.217039334s
0.90836 2299.629
3
Time to compress: 3h59m39.032524584s
Building the index took 5h54m55.046038916s
0.91295 4786.8

generating data took 2.119674416s
0
Start compressing...
Time to compress: 1m3.037853584s
Building the index took 3m53.429316209s
0.40992 169.937
1
Start compressing...
Time to compress: 2m4.653952334s
Building the index took 5m44.454856667s
0.46251252 207.299
2
Start compressing...
Time to compress: 4m7.585857584s
Building the index took 8m36.2004675s
0.50494 293.362
3
Start compressing...
Time to compress: 8m16.4155035s
Building the index took 14m37.089003166s
0.54421 390.49
4
Start compressing...
Time to compress: 16m32.313318708s
Building the index took 26m46.66661125s
0.57827 442.589
*/
func TestHnswPqSift(t *testing.T) {
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(rootPath)
	params := [][]int{
		{64, 64, 32, 256, 3},
		{64, 64, 32, 512, 3},
		{64, 64, 32, 1024, 3},
		{64, 64, 32, 2048, 3},
		{64, 64, 32, 4096, 3},
		{64, 64, 32, 65536, 3},
	}
	dimensions := 128
	vectors_size := 1000000
	queries_size := 1000
	switch_at := 200000
	fmt.Println("Sift1M PQ")
	before := time.Now()
	vectors, queries := testinghelpers.ReadVecs(vectors_size, queries_size, dimensions, "sift", "../diskAnn/testdata")
	k := 100
	distancer := distancer.NewL2SquaredProvider()
	truths := testinghelpers.BuildTruths(queries_size, vectors_size, queries, vectors, k, distanceWrapper(distancer), "../diskAnn/testdata")
	fmt.Printf("generating data took %s\n", time.Since(before))
	for i := 0; i < len(params); i++ {
		fmt.Println(i)
		efConstruction := params[i][0]
		ef := params[i][1]
		maxNeighbors := params[i][2]
		centroids := params[i][3]
		segmentRate := params[i][4]
		if centroids > switch_at {
			fmt.Println("Increasing switch at...")
			switch_at = 650000
		}

		uc := ent.UserConfig{
			MaxConnections: maxNeighbors,
			EFConstruction: efConstruction,
			EF:             ef,
			PQ: ent.PQConfig{
				Enabled:  false,
				Segments: dimensions / int(math.Pow(2, float64(segmentRate))),
				Encoder: ent.PQEncoder{
					Type:         ent.PQEncoderTypeTile,
					Distribution: ent.PQEncoderDistributionLogNormal,
				},
			},
			VectorCacheMaxObjects: 10e12,
		}
		index, _ := hnsw.New(hnsw.Config{
			RootPath:              rootPath,
			ID:                    "recallbenchmark",
			MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
			DistanceProvider:      distancer,
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
		}, uc, newDummyStore(t))
		init := time.Now()
		compressionhelpers.Concurrently(uint64(switch_at), func(_, id uint64, _ *sync.Mutex) {
			index.Add(uint64(id), vectors[id])
		})
		before = time.Now()
		fmt.Println("Start compressing...")

		cfg := ent.PQConfig{
			Enabled:        true,
			Segments:       dimensions / int(math.Pow(2, float64(segmentRate))),
			Centroids:      centroids,
			BitCompression: false,
			Encoder: ent.PQEncoder{
				Type:         ent.PQEncoderTypeKMeans,
				Distribution: ent.PQEncoderDistributionLogNormal,
			},
		}

		index.Compress(cfg) /*should have configuration.compressed = true*/
		fmt.Printf("Time to compress: %s", time.Since(before))
		fmt.Println()
		compressionhelpers.Concurrently(uint64(vectors_size-switch_at), func(_, id uint64, _ *sync.Mutex) {
			idx := switch_at + int(id)

			index.Add(uint64(idx), vectors[idx])
		})
		fmt.Printf("Building the index took %s\n", time.Since(init))

		var relevant uint64
		var retrieved int

		var querying time.Duration = 0
		compressionhelpers.Concurrently(uint64(len(queries)), func(_, i uint64, _ *sync.Mutex) {
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
		assert.True(t, latency < 100000)
	}
}

func TestHnswPqSiftDeletes(t *testing.T) {
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(rootPath)
	params := [][]int{
		{64, 64, 32},
	}
	dimensions := 128
	vectors_size := 10000
	queries_size := 1000
	switch_at := 2000
	fmt.Println("Sift1M PQ Deletes")
	before := time.Now()
	vectors, queries := testinghelpers.ReadVecs(vectors_size, queries_size, dimensions, "sift", "../diskAnn/testdata")
	k := 100
	distancer := distancer.NewL2SquaredProvider()
	truths := testinghelpers.BuildTruths(queries_size, vectors_size, queries, vectors, k, distanceWrapper(distancer), "../diskAnn/testdata")
	fmt.Printf("generating data took %s\n", time.Since(before))
	for segmentRate := 0; segmentRate < 1; segmentRate++ {
		fmt.Println(segmentRate)
		fmt.Println()
		for i := 0; i < len(params); i++ {
			efConstruction := params[i][0]
			ef := params[i][1]
			maxNeighbors := params[i][2]

			uc := ent.UserConfig{
				MaxConnections: maxNeighbors,
				EFConstruction: efConstruction,
				EF:             ef,
				PQ: ent.PQConfig{
					Enabled:  false,
					Segments: dimensions / int(math.Pow(2, float64(segmentRate))),
					Encoder: ent.PQEncoder{
						Type:         "tile",
						Distribution: "log-normal",
					},
				},
				VectorCacheMaxObjects: 10e12,
			}
			index, _ := hnsw.New(hnsw.Config{
				RootPath:              rootPath,
				ID:                    "recallbenchmark",
				MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
				DistanceProvider:      distancer,
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					return vectors[int(id)], nil
				},
			}, uc, newDummyStore(t))
			init := time.Now()
			compressionhelpers.Concurrently(uint64(switch_at), func(_, id uint64, _ *sync.Mutex) {
				index.Add(uint64(id), vectors[id])
			})
			before = time.Now()
			uc.PQ.Enabled = true
			index.UpdateUserConfig(uc, func() {}) /*should have configuration.compressed = true*/
			fmt.Printf("Time to compress: %s", time.Since(before))
			fmt.Println()
			compressionhelpers.Concurrently(uint64(vectors_size-switch_at), func(_, id uint64, _ *sync.Mutex) {
				idx := switch_at + int(id)
				index.Add(uint64(idx), vectors[idx])
			})
			fmt.Printf("Building the index took %s\n", time.Since(init))
			var relevant uint64
			var retrieved int

			var querying time.Duration = 0
			compressionhelpers.Concurrently(uint64(len(queries)), func(_, i uint64, _ *sync.Mutex) {
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
			assert.True(t, latency < 100000)
		}
	}
}

func TestHnswPqDeepImage(t *testing.T) {
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(rootPath)
	vectors_size := 9990000
	queries_size := 1000
	vectors := parseFromTxt("../diskAnn/testdata/deep-image/train.txt", vectors_size)
	queries := parseFromTxt("../diskAnn/testdata/deep-image/test.txt", queries_size)
	dimensions := 96

	params := [][]int{
		{64, 64, 32},
		{128, 128, 64},
		{256, 256, 128},
		{512, 512, 256},
	}
	switch_at := 1000000

	fmt.Println("Sift1MPQKMeans 10K/10K")
	before := time.Now()
	k := 100
	distancer := distancer.NewL2SquaredProvider()
	truths := testinghelpers.BuildTruths(queries_size, vectors_size, queries, vectors, k, distanceWrapper(distancer), "../diskAnn/testdata/deep-image")
	fmt.Printf("generating data took %s\n", time.Since(before))
	for segmentRate := 1; segmentRate < 4; segmentRate++ {
		fmt.Println(segmentRate)
		fmt.Println()
		for i := 0; i < len(params); i++ {
			efConstruction := params[i][0]
			ef := params[i][1]
			maxNeighbors := params[i][2]

			uc := ent.UserConfig{
				MaxConnections: maxNeighbors,
				EFConstruction: efConstruction,
				EF:             ef,
				PQ: ent.PQConfig{
					Enabled:  false,
					Segments: dimensions / int(math.Pow(2, float64(segmentRate))),
					Encoder: ent.PQEncoder{
						Type:         ent.PQEncoderTypeKMeans,
						Distribution: ent.PQEncoderDistributionNormal,
					},
				},
				VectorCacheMaxObjects: 10e12,
			}
			index, _ := hnsw.New(hnsw.Config{
				RootPath:              rootPath,
				ID:                    "recallbenchmark",
				MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
				DistanceProvider:      distancer,
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					return vectors[int(id)], nil
				},
			}, uc, newDummyStore(t))
			init := time.Now()
			compressionhelpers.Concurrently(uint64(switch_at), func(_, id uint64, _ *sync.Mutex) {
				index.Add(uint64(id), vectors[id])
			})
			before = time.Now()
			uc.PQ.Enabled = true
			index.UpdateUserConfig(uc, func() {})
			fmt.Printf("Time to compress: %s", time.Since(before))
			fmt.Println()
			compressionhelpers.Concurrently(uint64(vectors_size-switch_at), func(_, id uint64, _ *sync.Mutex) {
				idx := switch_at + int(id)
				index.Add(uint64(idx), vectors[idx])
			})
			fmt.Printf("Building the index took %s\n", time.Since(init))
			var relevant uint64
			var retrieved int

			var querying time.Duration = 0
			compressionhelpers.Concurrently(uint64(len(queries)), func(_, i uint64, _ *sync.Mutex) {
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
			assert.True(t, latency < 100000)
		}
	}
}

func parseFromTxt(file string, size int) [][]float32 {
	content, _ := ioutil.ReadFile(file)
	strContent := string(content)
	testArray := strings.Split(strContent, "\n")
	test := make([][]float32, 0, len(testArray))
	for j := 0; j < size; j++ {
		elementArray := strings.Split(testArray[j], " ")
		test = append(test, make([]float32, len(elementArray)))
		for i := range elementArray {
			f, _ := strconv.ParseFloat(elementArray[i], 16)
			test[j][i] = float32(f)
		}
	}
	return test
}

func newDummyStore(t *testing.T) *lsmkv.Store {
	logger, _ := test.NewNullLogger()
	storeDir := t.TempDir()
	store, err := lsmkv.New(storeDir, storeDir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)
	return store
}
