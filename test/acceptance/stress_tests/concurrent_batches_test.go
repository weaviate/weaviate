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

package stress_tests

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
)

type batch struct {
	Objects []*models.Object
}

const class = "TestClass"

func Test_AddConcurrentSchemas_sameObject(t *testing.T) {
	url := "http://localhost:8080/v1/"
	objects := createObject(class)
	requestAdd := createRequest(url+"batch/objects", "POST", batch{objects})
	// Add schema and object
	parallelReqs := 50
	wg := sync.WaitGroup{}
	wgStartReqests := sync.WaitGroup{}
	wgStartReqests.Add(parallelReqs)
	wg.Add(parallelReqs)

	for i := 0; i < parallelReqs; i++ {
		go func(j int) {
			c := createHttpClient()
			if j == 0 {
				clearExistingObjects(c, url)
				requestSchema := createSchemaRequest(url, class, false)
				performRequest(c, requestSchema)
			}
			wgStartReqests.Done()
			wgStartReqests.Wait()

			performRequest(c, requestAdd)

			wg.Done()
		}(i)
	}
	wg.Wait()

	c := createHttpClient()
	requestRead := createRequest(url+"objects?limit="+fmt.Sprint(10)+"&class="+class, "GET", nil)
	_, body, _ := performRequest(c, requestRead)
	var result map[string]interface{}
	json.Unmarshal(body, &result)
	assert.Equal(t, 1, int(result["totalResults"].(float64)))
	clearExistingObjects(c, url)
}

func Test_AddConcurrentBatches_differentObjects(t *testing.T) {
	url := "http://localhost:8080/v1/"

	requestAdd1 := createRequest(url+"batch/objects", "POST", batch{createObject(class)})
	requestAdd2 := createRequest(url+"batch/objects", "POST", batch{createObject(class)})
	// Add schema and object
	parallelReqs := 150
	wg := sync.WaitGroup{}
	wgStartReqests := sync.WaitGroup{}
	wgStartReqests.Add(parallelReqs)
	wg.Add(parallelReqs)

	for i := 0; i < parallelReqs; i++ {
		go func(j int) {
			c := createHttpClient()

			if j == 0 {
				clearExistingObjects(c, url)
				requestSchema := createSchemaRequest(url, class, false)
				performRequest(c, requestSchema)
			}

			wgStartReqests.Done()
			wgStartReqests.Wait()

			if j%2 == 0 {
				performRequest(c, requestAdd1)
			} else {
				performRequest(c, requestAdd2)
			}

			wg.Done()
		}(i)
	}
	wg.Wait()

	c := createHttpClient()
	requestRead := createRequest(url+"objects?limit="+fmt.Sprint(10)+"&class="+class, "GET", nil)
	_, body, _ := performRequest(c, requestRead)
	var result map[string]interface{}
	json.Unmarshal(body, &result)
	assert.Equal(t, 2, int(result["totalResults"].(float64)))
	clearExistingObjects(c, url)
}
