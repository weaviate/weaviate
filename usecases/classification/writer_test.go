//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package classification

import (
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/stretchr/testify/assert"
)

func testParallelBatchWrite(batchWriter writer, items search.Results, resultChannel chan<- batchWriterResults) {
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
			Kind:      kind.Thing,
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
	assert.Equal(t, int64(len(searchResultsToBeSaved)), res.successCount)
	assert.Equal(t, int64(0), res.errorCount)
	assert.Equal(t, nil, res.err)
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
	assert.Equal(t, int64(searchResultsCount), res.successCount)
	assert.Equal(t, int64(0), res.errorCount)
	assert.Equal(t, nil, res.err)
}

func TestWriter_ParallelLoadWrites(t *testing.T) {
	// given
	searchResultsToBeSavedCount1 := 600
	searchResultsToBeSavedCount2 := 440
	searchResultsToBeSaved1 := generateSearchResultsToSave(searchResultsToBeSavedCount1)
	searchResultsToBeSaved2 := generateSearchResultsToSave(searchResultsToBeSavedCount2)
	vectorRepo1 := newFakeVectorRepoKNN(searchResultsToBeSaved1, testDataAlreadyClassified())
	batchWriter1 := newBatchWriter(vectorRepo1)
	resChannel1 := make(chan batchWriterResults)
	vectorRepo2 := newFakeVectorRepoKNN(searchResultsToBeSaved2, testDataAlreadyClassified())
	batchWriter2 := newBatchWriter(vectorRepo2)
	resChannel2 := make(chan batchWriterResults)
	// when
	go testParallelBatchWrite(batchWriter1, searchResultsToBeSaved1, resChannel1)
	go testParallelBatchWrite(batchWriter2, searchResultsToBeSaved2, resChannel2)
	res1 := <-resChannel1
	res2 := <-resChannel2
	// then
	assert.Equal(t, int64(searchResultsToBeSavedCount1), res1.successCount)
	assert.Equal(t, int64(0), res1.errorCount)
	assert.Equal(t, nil, res1.err)
	assert.Equal(t, int64(searchResultsToBeSavedCount2), res2.successCount)
	assert.Equal(t, int64(0), res2.errorCount)
	assert.Equal(t, nil, res2.err)
}
