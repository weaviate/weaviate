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

package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/semi-technologies/weaviate/entities/models"
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
	}

	jsonSchema, _ := json.Marshal(classObj)
	request, err := http.NewRequest("POST", url+"schema", bytes.NewReader(jsonSchema))
	if err != nil {
		panic("Could not create schema request, error: " + err.Error())
	}
	request.Header.Set("content-type", "application/json")
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
	objects := []*models.Object{}

	f, err := os.Open("sift/" + file)
	if err != nil {
		panic("Could not open SIFT file, error: " + err.Error())
	}
	defer f.Close()

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
		vectorFloat := []float32{}
		for j := 0; j < vectorLengthFloat; j++ {
			start := (j + 1) * bytesPerF // first bytesPerF are length of vector
			vectorFloat = append(vectorFloat, float32FromBytes(vectorBytes[start:start+bytesPerF]))
		}
		uuid := uuid.New()
		object := &models.Object{
			Class:  class,
			ID:     strfmt.UUID(uuid.String()),
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

func sendRequests(c *http.Client, request *http.Request) (*http.Response, int64, error) {
	timeStart := time.Now()
	response, err := c.Do(request)
	return response, time.Since(timeStart).Milliseconds(), err
}

// If there is already a schema present, clear it out
func clearExistingObjects(c *http.Client, url string) {
	checkSchemaRequest, _ := http.NewRequest("GET", url+"schema", bytes.NewReader(make([]byte, 0)))
	checkSchemaResponse, err := c.Do(checkSchemaRequest)
	if err == nil && checkSchemaResponse.StatusCode != 200 {
		return
	}
	schemaResponseBytes, _ := ioutil.ReadAll(checkSchemaResponse.Body)
	checkSchemaResponse.Body.Close()

	var dump models.Schema
	if err := json.Unmarshal(schemaResponseBytes, &dump); err != nil {
		panic("Could not unmarshal read response, error: " + err.Error())
	}
	for _, classObj := range dump.Classes {
		requestDelete, _ := http.NewRequest("DELETE", url+"schema/"+classObj.Class, nil)
		responseDelete, err := c.Do(requestDelete)
		if err != nil || responseDelete.StatusCode != 200 {
			panic("Could delete schema, error: " + err.Error())
		}
		responseDelete.Body.Close()
	}
}

func benchmarkSift(c *http.Client, url string, maxObjects int) map[string]int64 {
	clearExistingObjects(c, url)
	objects := readSiftFloat("sift_base.fvecs", maxObjects)
	objectsJSON, _ := json.Marshal(batch{objects})

	queries := readSiftFloat("sift_query.fvecs", maxObjects/100)
	requestSchema := createSchemaSIFTRequest(url)

	passedTime := make(map[string]int64)

	// Add schema
	responseSchema, timeSchema, err := sendRequests(c, requestSchema)
	passedTime["AddSchema"] = timeSchema
	if err != nil || responseSchema.StatusCode != 200 {
		panic("Could not add schema, error: " + err.Error())
	}
	responseSchema.Body.Close()

	// Batch-add
	requestAdd, err := http.NewRequest("POST", url+"batch/objects", bytes.NewReader(objectsJSON))
	if err != nil {
		panic("Could not create batch request, error: " + err.Error())
	}
	requestAdd.Header.Set("content-type", "application/json")
	responseAdd, timeBatchAdd, err := sendRequests(c, requestAdd)
	passedTime["BatchAdd"] = timeBatchAdd
	if err != nil || responseAdd.StatusCode != 200 {
		panic("Could not add batch, error: " + err.Error())
	}
	responseAdd.Body.Close()

	// Read entries
	nrSearchResultsUse := nrSearchResults
	if maxObjects < nrSearchResultsUse {
		nrSearchResultsUse = maxObjects
	}
	requestRead, _ := http.NewRequest("GET", url+"objects?limit="+fmt.Sprint(nrSearchResultsUse)+"&class="+class, bytes.NewReader(make([]byte, 0)))
	requestRead.Header.Set("content-type", "application/json")
	responseRead, timeGetObjects, err := sendRequests(c, requestRead)
	passedTime["GetObjects"] = timeGetObjects
	if err != nil || responseRead.StatusCode != 200 {
		panic("Could not add batch, error: " + err.Error())
	}
	entriesBytes, _ := ioutil.ReadAll(responseRead.Body)
	responseRead.Body.Close()
	var result map[string]interface{}
	if err := json.Unmarshal(entriesBytes, &result); err != nil {
		panic("Could not unmarshal read response, error: " + err.Error())
	}
	if int(result["totalResults"].(float64)) != nrSearchResultsUse {
		panic("Found " + fmt.Sprint(int(result["totalResults"].(float64))) + " results. Expected " + fmt.Sprint(nrSearchResultsUse) + ".")
	}

	// Use sample queries
	for _, query := range queries {
		queryString := "{Get{" + class + "(nearVector: {vector:" + fmt.Sprint(query.Vector) + " }){counter}}}"
		queryJSON, _ := json.Marshal(models.GraphQLQuery{
			Query: queryString,
		})

		requestQuery, _ := http.NewRequest("POST", url+"graphql", bytes.NewReader(queryJSON))
		requestQuery.Header.Set("content-type", "application/json")
		responseQuery, timeQuery, err := sendRequests(c, requestQuery)
		passedTime["Query"] += timeQuery

		if err != nil || responseQuery.StatusCode != 200 {
			panic("Query error, error: " + err.Error())
		}
		bytes, _ := ioutil.ReadAll(responseQuery.Body)
		responseQuery.Body.Close()
		var result map[string]interface{}
		if err := json.Unmarshal(bytes, &result); err != nil {
			panic("Could not unmarshal query response, error: " + err.Error())
		}
		if result["data"] == nil || result["errors"] != nil {
			panic("GraphQL Error")
		}
	}

	// Delete class (with schema and all entries) to clear all entries so next round can start fresh
	requestDelete, _ := http.NewRequest("DELETE", url+"schema/"+class, nil)
	responseDelete, timeDelete, err := sendRequests(c, requestDelete)
	passedTime["Delete"] += timeDelete
	if err != nil || responseDelete.StatusCode != 200 {
		panic("Could delete schema, error: " + err.Error())
	}
	responseDelete.Body.Close()

	return passedTime
}
