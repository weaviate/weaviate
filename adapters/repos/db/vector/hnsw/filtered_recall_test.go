//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build benchmarkRecall
// +build benchmarkRecall

package hnsw

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"runtime"
	"sync"
	"testing"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

//"github.com/weaviate/weaviate/adapters/repos/db/helpers"

func init() {
	go func() {
		runtime.SetBlockProfileRate(1)
		http.ListenAndServe("localhost:6060", nil)
	}()
}

type Vector struct {
	ID     int       `json:"id"`
	Vector []float32 `json:"vector"`
}

type Filters struct {
	ID        int         `json:"id"`
	FilterMap map[int]int `json:"filterMap"`
}

type vecWithFilters struct {
	ID        int         `json:"id"`
	Vector    []float32   `json:"vector"`
	FilterMap map[int]int `json:"filterMap"`
}

type GroundTruths struct {
	QueryID int      `json:"queryID"`
	Truths  []uint64 `json:"truths"`
}

func TestFilteredRecall(t *testing.T) {
	efConstruction := 256
	ef := 256
	maxNeighbors := 64

	var indexVectors []Vector
	var indexFilters []Filters
	var queryVectors []Vector
	var queryFilters []Filters
	var truths []GroundTruths
	var vectorIndex *hnsw

	t.Run("Loading vectors for testing...", func(t *testing.T) {
		// vectors.json
		/*
			[{"id": 0, "vector": [0,15,35,...]},{"id": 0, "vector": [119,15,4,...]},...]
		*/
		indexVectorsJSON, err := ioutil.ReadFile("indexVectors.json")
		require.Nil(t, err)
		err = json.Unmarshal(indexVectorsJSON, &indexVectors)
		require.Nil(t, err)
		// vectorFilters.json
		/*
			[{"id": 0, "filterMap": {0: 1, 1: 3, ...}}, {"id": 1, "filterMap": {0: 2, 1: 4}}, ...]

			For now, running one test at a time, future - loop through filter paths
		*/
		indexFiltersJSON, err := ioutil.ReadFile("indexFilters.json")
		require.Nil(t, err)
		err = json.Unmarshal(indexFiltersJSON, &indexFilters)
		require.Nil(t, err)

		indexVectorsWithFilters := mergeData(indexVectors, indexFilters) // returns []vecWithFilters

		/* =================================================
			SAME JOINING OF VECTORS AND FILTERS FOR QUERIES
		   =================================================
		*/

		queryVectorsJSON, err := ioutil.ReadFile("queryVectors.json")
		require.Nil(t, err)
		err = json.Unmarshal(queryVectorsJSON, &queryVectors)
		require.Nil(t, err)

		queryFiltersJSON, err := ioutil.ReadFile("queryFilters.json")
		require.Nil(t, err)
		err = json.Unmarshal(queryFiltersJSON, &queryFilters)

		queryVectorsWithFilters := mergeData(queryVectors, queryFilters)

		truthsJSON, err := ioutil.ReadFile("filtered_recall_truths.json")
		require.Nil(t, err)
		err = json.Unmarshal(truthsJSON, &truths)
		require.Nil(t, err)

		fmt.Printf("importing into hnsw\n")

		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "recallbenchmark",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewCosineDistanceProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return indexVectorsWithFilters[int(id)].Vector, nil
			},
		}, ent.UserConfig{
			MaxConnections: maxNeighbors,
			EFConstruction: efConstruction,
			EF:             ef,
		}, cyclemanager.NewNoop())

		filterToIDs := make(map[int]map[int][]uint64)

		require.Nil(t, err)
		vectorIndex = index

		workerCount := runtime.GOMAXPROCS(0)
		jobsForWorker := make([][]vecWithFilters, workerCount)

		before := time.Now()
		for i, vecWithFilters := range indexVectorsWithFilters {
			workerID := i % workerCount
			jobsForWorker[workerID] = append(jobsForWorker[workerID], vecWithFilters)
		}

		wg := &sync.WaitGroup{}
		mutex := &sync.Mutex{}
		for workerID, jobs := range jobsForWorker {
			wg.Add(1)
			go func(workerID int, myJobs []vecWithFilters) {
				defer wg.Done()
				for i, vec := range myJobs {
					originalIndex := (i * workerCount) + workerID
					nodeId := uint64(originalIndex)
					/* TEST FILTERED HNSW */
					err := vectorIndex.FilteredAdd(nodeId, vec.Vector, vec.FilterMap) // change signature to add vec.Label
					/* TEST HNSW */
					//err := vectorIndex.Add(nodeId, vec.Vector)
					require.Nil(t, err)
					mutex.Lock()
					// filterToIDs is now a map[int]map[int][]uint64
					for _, filter := range vec.FilterMap {
						if _, ok := filterToIDs[filter]; !ok {
							filterToIDs[filter] = make(map[int][]uint64)
							filterToIDs[filter][vec.FilterMap[filter]] = []uint64{nodeId}
						} else {
							if _, ok := filterToIDs[filter][vec.FilterMap[filter]]; !ok {
								filterToIDs[filter][vec.FilterMap[filter]] = []uint64{nodeId}
							} else {
								filterToIDs[filter][vec.FilterMap[filter]] = append(filterToIDs[filter][vec.FilterMap[filter]], nodeId)
							}
						}
					}
					mutex.Unlock()
					require.Nil(t, err)
				}
			}(workerID, jobs)
		}

		wg.Wait()
		fmt.Printf("importing took %s\n", time.Since(before))

		fmt.Printf("With k=20")

		fmt.Print(filterToIDs[0][3])

		k := 100

		var relevant_retrieved int
		var recall float32

		/* Will need to think of how to generalize this logging to multiple filters */
		totalRecallPerFilter := make(map[int]float32)
		totalCountPerFilter := make(map[int]float32)

		for i := 0; i < len(queryVectorsWithFilters); i++ {
			// change to queryFilters
			queryFilters := queryVectorsWithFilters[i].FilterMap
			allowListIDs := []uint64{}
			for _, filter := range queryFilters {
				allowListIDs = append(allowListIDs, filterToIDs[filter][queryFilters[filter]]...)
			}
			//construct an allowList from the []uint64 of ids that match the filter
			queryAllowList := helpers.NewAllowList(allowListIDs...)
			/* TEST FILTERED HNSW */
			results, _, err := vectorIndex.SearchByVectorWithFilters(queryVectorsWithFilters[i].Vector, k, queryFilters, queryAllowList)
			/* TEST HNSW */
			//results, _, err := vectorIndex.SearchByVector(queryVectorsWithFilters[i].Vector, k, queryAllowList)

			require.Nil(t, err)

			relevant_retrieved = matchesInLists(truths[i].Truths, results)
			local_recall := float32(relevant_retrieved) / 100 // might want to modify to len(Truths) for extreme filter cases
			recall += local_recall
			for _, filter := range queryFilters {
				if _, ok := totalRecallPerFilter[filter]; !ok {
					totalRecallPerFilter[filter] = local_recall
					totalCountPerFilter[filter] = 1.0
				} else {
					totalRecallPerFilter[filter] += local_recall
					totalCountPerFilter[filter] += 1.0
				}
			}
		}

		recall = float32(recall) / float32(len(queryVectorsWithFilters))
		fmt.Printf("Average Recall for all filters = %f\n", recall)

		fmt.Print("\n =========== \n")
		fmt.Print("\n DEBUGGING \n")
		fmt.Print(totalRecallPerFilter)
		fmt.Print("\n DEBUGGING \n")
		fmt.Print(totalCountPerFilter)
		fmt.Print("\n =========== \n")

		/* Loop through query filters, adding new recall score */
		for filterKey, totalRecallValue := range totalRecallPerFilter {
			RecallPerFilter := totalRecallValue / totalCountPerFilter[filterKey]
			fmt.Printf("Recall for filter %d = %f \n", filterKey, RecallPerFilter)
		}

		assert.True(t, recall >= 0.09)
	})
}

func mergeData(vectors []Vector, filters []Filters) []vecWithFilters {
	// Create a map for quick lookup of filters
	IDtoFilterMap := make(map[int]map[int]int)
	for _, filter := range filters {
		IDtoFilterMap[filter.ID] = filter.FilterMap
	}
	// Merge vectors and filters
	var results []vecWithFilters
	for _, vector := range vectors {
		result := vecWithFilters{
			ID:        vector.ID,
			Vector:    vector.Vector,
			FilterMap: IDtoFilterMap[vector.ID],
		}
		results = append(results, result)
	}

	return results
}

func matchesInLists(control []uint64, results []uint64) int {
	desired := map[uint64]struct{}{}
	for _, relevant := range control {
		desired[relevant] = struct{}{}
	}

	var matches int
	for _, candidate := range results {
		_, ok := desired[candidate]
		if ok {
			matches++
		}
	}

	return matches
}
