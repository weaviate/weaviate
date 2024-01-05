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

package classification

import (
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/search"
)

func testParallelBatchWrite(batchWriter Writer, items search.Results, resultChannel chan<- WriterResults) {
	batchWriter.Start()
	for _, item := range items {
		batchWriter.Store(item)
	}
	res := batchWriter.Stop()
	resultChannel <- res
}

func generateSearchResultsToSave(size int) search.Results {
	items := make(search.Results, 0)
	for i := 0; i < size; i++ {
		res := search.Result{
			ID:        strfmt.UUID(fmt.Sprintf("75ba35af-6a08-40ae-b442-3bec69b35%03d", i)),
			ClassName: "Article",
			Vector:    []float32{0.78, 0, 0},
			Schema: map[string]interface{}{
				"description": "Barack Obama is a former US president",
			},
		}
		items = append(items, res)
	}
	return items
}

func TestWriter_SimpleWrite(t *testing.T) {
	// given
	searchResultsToBeSaved := testDataToBeClassified()
	vectorRepo := newFakeVectorRepoKNN(searchResultsToBeSaved, testDataAlreadyClassified())
	batchWriter := newBatchWriter(vectorRepo)
	// when
	batchWriter.Start()
	for _, item := range searchResultsToBeSaved {
		batchWriter.Store(item)
	}
	res := batchWriter.Stop()
	// then
	assert.Equal(t, int64(len(searchResultsToBeSaved)), res.SuccessCount())
	assert.Equal(t, int64(0), res.ErrorCount())
	assert.Equal(t, nil, res.Err())
}

func TestWriter_LoadWrites(t *testing.T) {
	// given
	searchResultsCount := 640
	searchResultsToBeSaved := generateSearchResultsToSave(searchResultsCount)
	vectorRepo := newFakeVectorRepoKNN(searchResultsToBeSaved, testDataAlreadyClassified())
	batchWriter := newBatchWriter(vectorRepo)
	// when
	batchWriter.Start()
	for _, item := range searchResultsToBeSaved {
		batchWriter.Store(item)
	}
	res := batchWriter.Stop()
	// then
	assert.Equal(t, int64(searchResultsCount), res.SuccessCount())
	assert.Equal(t, int64(0), res.ErrorCount())
	assert.Equal(t, nil, res.Err())
}

func TestWriter_ParallelLoadWrites(t *testing.T) {
	// given
	searchResultsToBeSavedCount1 := 600
	searchResultsToBeSavedCount2 := 440
	searchResultsToBeSaved1 := generateSearchResultsToSave(searchResultsToBeSavedCount1)
	searchResultsToBeSaved2 := generateSearchResultsToSave(searchResultsToBeSavedCount2)
	vectorRepo1 := newFakeVectorRepoKNN(searchResultsToBeSaved1, testDataAlreadyClassified())
	batchWriter1 := newBatchWriter(vectorRepo1)
	resChannel1 := make(chan WriterResults)
	vectorRepo2 := newFakeVectorRepoKNN(searchResultsToBeSaved2, testDataAlreadyClassified())
	batchWriter2 := newBatchWriter(vectorRepo2)
	resChannel2 := make(chan WriterResults)
	// when
	go testParallelBatchWrite(batchWriter1, searchResultsToBeSaved1, resChannel1)
	go testParallelBatchWrite(batchWriter2, searchResultsToBeSaved2, resChannel2)
	res1 := <-resChannel1
	res2 := <-resChannel2
	// then
	assert.Equal(t, int64(searchResultsToBeSavedCount1), res1.SuccessCount())
	assert.Equal(t, int64(0), res1.ErrorCount())
	assert.Equal(t, nil, res1.Err())
	assert.Equal(t, int64(searchResultsToBeSavedCount2), res2.SuccessCount())
	assert.Equal(t, int64(0), res2.ErrorCount())
	assert.Equal(t, nil, res2.Err())
}
