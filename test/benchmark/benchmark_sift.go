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

package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sync"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
)

const (
	class           = "Benchmark"
	nrSearchResults = 79
)

func createSchemaSIFTRequest(url string) *http.Request {
	classObj := &models.Class{
		Class:       class,
		Description: "Dummy class for benchmarking purposes",
		Properties: []*models.Property{
			{
				DataType:    []string{"int"},
				Description: "The value of the counter in the dataset",
				Name:        "counter",
			},
		},
		VectorIndexConfig: map[string]interface{}{ // values are from benchmark script
			"distance":              "l2-squared",
			"ef":                    -1,
			"efConstruction":        64,
			"maxConnections":        64,
			"vectorCacheMaxObjects": 1000000000,
		},
		Vectorizer: "none",
	}
	request := createRequest(url+"schema", "POST", classObj)
	return request
}

func float32FromBytes(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)
	float := math.Float32frombits(bits)
	return float
}

func int32FromBytes(bytes []byte) int {
	return int(binary.LittleEndian.Uint32(bytes))
}

func readSiftFloat(file string, maxObjects int) []*models.Object {
	var objects []*models.Object

	f, err := os.Open("sift/" + file)
	if err != nil {
		panic(errors.Wrap(err, "Could not open SIFT file"))
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		panic(errors.Wrap(err, "Could not get SIFT file properties"))
	}
	fileSize := fi.Size()
	if fileSize < 1000000 {
		panic("The file is only " + fmt.Sprint(fileSize) + " bytes long. Did you forgot to install git lfs?")
	}

	// The sift data is a binary file containing floating point vectors
	// For each entry, the first 4 bytes is the length of the vector (in number of floats, not in bytes)
	// which is followed by the vector data with vector length * 4 bytes.
	// |-length-vec1 (4bytes)-|-Vec1-data-(4*length-vector-1 bytes)-|-length-vec2 (4bytes)-|-Vec2-data-(4*length-vector-2 bytes)-|
	// The vector length needs to be converted from bytes to int
	// The vector data needs to be converted from bytes to float
	// Note that the vector entries are of type float but are integer numbers eg 2.0
	bytesPerF := 4
	vectorLengthFloat := 128
	vectorBytes := make([]byte, bytesPerF+vectorLengthFloat*bytesPerF)
	for i := 0; i >= 0; i++ {
		_, err = f.Read(vectorBytes)
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		if int32FromBytes(vectorBytes[0:bytesPerF]) != vectorLengthFloat {
			panic("Each vector must have 128 entries.")
		}
		var vectorFloat []float32
		for j := 0; j < vectorLengthFloat; j++ {
			start := (j + 1) * bytesPerF // first bytesPerF are length of vector
			vectorFloat = append(vectorFloat, float32FromBytes(vectorBytes[start:start+bytesPerF]))
		}
		ObjectUuid := uuid.New()
		object := &models.Object{
			Class:  class,
			ID:     strfmt.UUID(ObjectUuid.String()),
			Vector: models.C11yVector(vectorFloat),
			Properties: map[string]interface{}{
				"counter": i,
			},
		}
		objects = append(objects, object)

		if i >= maxObjects {
			break
		}
	}
	if len(objects) < maxObjects {
		panic("Could not load all elements.")
	}

	return objects
}

func benchmarkSift(c *http.Client, url string, maxObjects, numBatches int) (map[string]int64, error) {
	clearExistingObjects(c, url)
	objects := readSiftFloat("sift_base.fvecs", maxObjects)
	queries := readSiftFloat("sift_query.fvecs", maxObjects/100)
	requestSchema := createSchemaSIFTRequest(url)

	passedTime := make(map[string]int64)

	// Add schema
	responseSchemaCode, _, timeSchema, err := performRequest(c, requestSchema)
	passedTime["AddSchema"] = timeSchema
	if err != nil {
		return nil, errors.Wrap(err, "Could not add schema, error: ")
	} else if responseSchemaCode != 200 {
		return nil, errors.Errorf("Could not add schma, http error code: %v", responseSchemaCode)
	}

	// Batch-add
	passedTime["BatchAdd"] = 0
	wg := sync.WaitGroup{}
	batchSize := len(objects) / numBatches
	errorChan := make(chan error, numBatches)
	timeChan := make(chan int64, numBatches)

	for i := 0; i < numBatches; i++ {
		wg.Add(1)
		go func(batchId int, errChan chan<- error) {
			batchObjects := objects[batchId*batchSize : (batchId+1)*batchSize]
			requestAdd := createRequest(url+"batch/objects", "POST", batch{batchObjects})
			responseAddCode, _, timeBatchAdd, err := performRequest(c, requestAdd)

			timeChan <- timeBatchAdd
			if err != nil {
				errChan <- errors.Wrap(err, "Could not add batch, error: ")
			} else if responseAddCode != 200 {
				errChan <- errors.Errorf("Could not add batch, http error code: %v", responseAddCode)
			}
			wg.Done()
		}(i, errorChan)

	}
	wg.Wait()
	close(errorChan)
	close(timeChan)
	for err := range errorChan {
		return nil, err
	}
	for timing := range timeChan {
		passedTime["BatchAdd"] += timing
	}

	// Read entries
	nrSearchResultsUse := nrSearchResults
	if maxObjects < nrSearchResultsUse {
		nrSearchResultsUse = maxObjects
	}
	requestRead := createRequest(url+"objects?limit="+fmt.Sprint(nrSearchResultsUse)+"&class="+class, "GET", nil)
	responseReadCode, body, timeGetObjects, err := performRequest(c, requestRead)
	passedTime["GetObjects"] = timeGetObjects
	if err != nil {
		return nil, errors.Wrap(err, "Could not read objects")
	} else if responseReadCode != 200 {
		return nil, errors.New("Could not read objects, http error code: " + fmt.Sprint(responseReadCode))
	}
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, errors.Wrap(err, "Could not unmarshal read response")
	}
	if int(result["totalResults"].(float64)) != nrSearchResultsUse {
		errString := "Found " + fmt.Sprint(int(result["totalResults"].(float64))) +
			" results. Expected " + fmt.Sprint(nrSearchResultsUse) + "."
		return nil, errors.New(errString)
	}

	// Use sample queries
	for _, query := range queries {
		queryString := "{Get{" + class + "(nearVector: {vector:" + fmt.Sprint(query.Vector) + " }){counter}}}"
		requestQuery := createRequest(url+"graphql", "POST", models.GraphQLQuery{
			Query: queryString,
		})
		responseQueryCode, body, timeQuery, err := performRequest(c, requestQuery)
		passedTime["Query"] += timeQuery
		if err != nil {
			return nil, errors.Wrap(err, "Could not query objects")
		} else if responseQueryCode != 200 {
			return nil, errors.Errorf("Could not query objects, http error code: %v", responseQueryCode)
		}
		var result map[string]interface{}
		if err := json.Unmarshal(body, &result); err != nil {
			return nil, errors.Wrap(err, "Could not unmarshal query response")
		}
		if result["data"] == nil || result["errors"] != nil {
			return nil, errors.New("GraphQL Error")
		}
	}

	// Delete class (with schema and all entries) to clear all entries so next round can start fresh
	requestDelete := createRequest(url+"schema/"+class, "DELETE", nil)
	responseDeleteCode, _, timeDelete, err := performRequest(c, requestDelete)
	passedTime["Delete"] += timeDelete
	if err != nil {
		return nil, errors.Wrap(err, "Could not delete class")
	} else if responseDeleteCode != 200 {
		return nil, errors.Errorf("Could not delete class, http error code: %v", responseDeleteCode)
	}

	return passedTime, nil
}
