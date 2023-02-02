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

//go:build benchmarkSiftRecall
// +build benchmarkSiftRecall

package hnsw_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func distanceWrapper(provider distancer.Provider) func(x, y []float32) float32 {
	return func(x, y []float32) float32 {
		dist, _, _ := provider.SingleDist(x, y)
		return dist
	}
}

func TestRecall(t *testing.T) {
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

	index, _ := hnsw.New(
		hnsw.Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "recallbenchmark",
			MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
			DistanceProvider:      distancer,
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
		}, uc,
	)
	init := time.Now()
	ssdhelpers.Concurrently(uint64(switch_at), func(_, id uint64, _ *sync.Mutex) {
		index.Add(uint64(id), vectors[id])
		if id%1000 == 0 {
			fmt.Println(id, time.Since(before))
		}
	})
	before = time.Now()
	uc.PQ.Enabled = true
	index.UpdateUserConfig(uc) /*should have configuration.pr.enabled = true*/
	fmt.Printf("Time to compress: %s", time.Since(before))
	fmt.Println()
	ssdhelpers.Concurrently(uint64(vectors_size-switch_at), func(_, id uint64, _ *sync.Mutex) {
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
		index.UpdateUserConfig(uc)
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
	params := [][]int{
		{64, 64, 32},
		{128, 128, 64},
		{256, 256, 128},
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

	for i := 0; i < len(params); i++ {
		efConstruction := params[i][0]
		ef := params[i][1]
		maxNeighbors := params[i][2]

		uc := ent.UserConfig{
			MaxConnections:        maxNeighbors,
			EFConstruction:        efConstruction,
			EF:                    ef,
			VectorCacheMaxObjects: 10e12,
			PQ: &ent.PQConfig{
				Enabled:  false,
				Segments: dimensions,
				Encoder: &ent.PQEncoder{
					Type:         int(ssdhelpers.UseTileEncoder),
					Distribution: int(ssdhelpers.LogNormalEncoderDistribution),
				},
			},
		}
		index, _ := hnsw.New(
			hnsw.Config{
				RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
				ID:                    "recallbenchmark",
				MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
				DistanceProvider:      distancer,
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					return vectors[int(id)], nil
				},
			}, uc,
		)
		init := time.Now()
		ssdhelpers.Concurrently(uint64(switch_at), func(_, id uint64, _ *sync.Mutex) {
			index.Add(uint64(id), vectors[id])
		})
		before = time.Now()
		uc.PQ.Enabled = true
		index.UpdateUserConfig(uc) /*should have configuration.compressed = true*/
		fmt.Printf("Time to compress: %s", time.Since(before))
		fmt.Println()
		ssdhelpers.Concurrently(uint64(vectors_size-switch_at), func(_, id uint64, _ *sync.Mutex) {
			idx := switch_at + int(id)
			index.Add(uint64(idx), vectors[idx])
		})
		fmt.Printf("Building the index took %s\n", time.Since(init))
		var relevant uint64
		var retrieved int

		var querying time.Duration = 0
		ssdhelpers.Concurrently(uint64(len(queries)), func(_, i uint64, _ *sync.Mutex) {
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

func TestHnswPqSift(t *testing.T) {
	params := [][]int{
		{64, 64, 32},
		{128, 128, 64},
		{256, 256, 128},
		{512, 512, 256},
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
	for segmentRate := 0; segmentRate < 4; segmentRate++ {
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
				PQ: &ent.PQConfig{
					Enabled:  false,
					Segments: dimensions / int(math.Pow(2, float64(segmentRate))),
					Encoder: &ent.PQEncoder{
						Type:         int(ssdhelpers.UseKMeansEncoder),
						Distribution: int(ssdhelpers.LogNormalEncoderDistribution),
					},
				},
				VectorCacheMaxObjects: 10e12,
			}
			index, _ := hnsw.New(
				hnsw.Config{
					RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
					ID:                    "recallbenchmark",
					MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
					DistanceProvider:      distancer,
					VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
						return vectors[int(id)], nil
					},
				}, uc,
			)
			init := time.Now()
			ssdhelpers.Concurrently(uint64(switch_at), func(_, id uint64, _ *sync.Mutex) {
				index.Add(uint64(id), vectors[id])
			})
			before = time.Now()
			uc.PQ.Enabled = true
			index.UpdateUserConfig(uc) /*should have configuration.compressed = true*/
			fmt.Printf("Time to compress: %s", time.Since(before))
			fmt.Println()
			ssdhelpers.Concurrently(uint64(vectors_size-switch_at), func(_, id uint64, _ *sync.Mutex) {
				idx := switch_at + int(id)
				index.Add(uint64(idx), vectors[idx])
			})
			fmt.Printf("Building the index took %s\n", time.Since(init))
			var relevant uint64
			var retrieved int

			var querying time.Duration = 0
			ssdhelpers.Concurrently(uint64(len(queries)), func(_, i uint64, _ *sync.Mutex) {
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
	vectors_size := 9990000
	queries_size := 1000
	vectors := parseFromTxt("../diskAnn/testdata/deep-image/train.txt", vectors_size)
	queries := parseFromTxt("../diskAnn/testdata/deep-image/test.txt", queries_size)

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
	for i := 0; i < len(params); i++ {
		efConstruction := params[i][0]
		ef := params[i][1]
		maxNeighbors := params[i][2]

		uc := ent.UserConfig{
			MaxConnections: maxNeighbors,
			EFConstruction: efConstruction,
			EF:             ef,
			PQ: &ent.PQConfig{
				Enabled:  false,
				Segments: 96,
				Encoder: &ent.PQEncoder{
					Type:         int(ssdhelpers.UseKMeansEncoder),
					Distribution: int(ssdhelpers.LogNormalEncoderDistribution),
				},
			},
			VectorCacheMaxObjects: 10e12,
		}
		index, _ := hnsw.New(
			hnsw.Config{
				RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
				ID:                    "recallbenchmark",
				MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
				DistanceProvider:      distancer,
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					return vectors[int(id)], nil
				},
			}, uc,
		)
		init := time.Now()
		ssdhelpers.Concurrently(uint64(switch_at), func(_, id uint64, _ *sync.Mutex) {
			index.Add(uint64(id), vectors[id])
		})
		before = time.Now()
		uc.PQ.Enabled = true
		index.UpdateUserConfig(uc)
		fmt.Printf("Time to compress: %s", time.Since(before))
		fmt.Println()
		ssdhelpers.Concurrently(uint64(vectors_size-switch_at), func(_, id uint64, _ *sync.Mutex) {
			idx := switch_at + int(id)
			index.Add(uint64(idx), vectors[idx])
		})
		fmt.Printf("Building the index took %s\n", time.Since(init))
		var relevant uint64
		var retrieved int

		var querying time.Duration = 0
		ssdhelpers.Concurrently(uint64(len(queries)), func(_, i uint64, _ *sync.Mutex) {
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
