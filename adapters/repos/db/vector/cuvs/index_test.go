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

package cuvs_index

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
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

func run(dirName string, logger *logrus.Logger, compression string, vectorCache bool,
	vectors [][]float32, queries [][]float32, k int, truths [][]uint64,
	extraVectorsForDelete [][]float32, allowIds []uint64,
	distancer distancer.Provider,
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

	index, err := New(Config{
		ID:               runId,
		DistanceProvider: distancer,
	}, flatent.UserConfig{
		PQ: pq,
		BQ: bq,
	}, store)
	if err != nil {
		return 0, 0, err
	}

	compressionhelpers.ConcurrentlyWithError(logger, uint64(vectors_size), func(id uint64) error {
		return index.Add(id, vectors[id])
	})

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
		results, _, _ := index.SearchByVector(queries[i], k, allowList)

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
