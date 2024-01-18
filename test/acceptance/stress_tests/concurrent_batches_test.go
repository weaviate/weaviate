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
	batch := batch{createObject(class)}
	parallelReqs := 10
	wg := sync.WaitGroup{}
	wg.Add(parallelReqs)

	// Add schema and object
	c := createHttpClient()
	clearExistingObjects(c, url)
	requestSchema := createSchemaRequest(url, class, false)
	performRequest(c, requestSchema)

	for i := 0; i < parallelReqs; i++ {
		go func(j int) {
			defer wg.Done()
			c := createHttpClient()
			performRequest(c, createRequest(url+"batch/objects", "POST", batch))
		}(i)
	}
	wg.Wait()

	requestRead := createRequest(url+"objects?limit="+fmt.Sprint(10)+"&class="+class, "GET", nil)
	_, body, _ := performRequest(c, requestRead)
	var result map[string]interface{}
	json.Unmarshal(body, &result)
	assert.Equal(t, 1, int(result["totalResults"].(float64)))
}

func Test_AddConcurrentBatches_differentObjects(t *testing.T) {
	url := "http://localhost:8080/v1/"

	parallelReqs := 150
	wg := sync.WaitGroup{}
	wg.Add(parallelReqs)

	// Add schema and object
	c := createHttpClient()
	clearExistingObjects(c, url)
	requestSchema := createSchemaRequest(url, class, false)
	performRequest(c, requestSchema)
	batch1 := batch{createObject(class)}
	batch2 := batch{createObject(class)}

	for i := 0; i < parallelReqs; i++ {
		go func(j int) {
			defer wg.Done()

			c := createHttpClient()
			if j%2 == 0 {
				performRequest(c, createRequest(url+"batch/objects", "POST", batch1))
			} else {
				performRequest(c, createRequest(url+"batch/objects", "POST", batch2))
			}
		}(i)
	}
	wg.Wait()

	requestRead := createRequest(url+"objects?limit="+fmt.Sprint(10)+"&class="+class, "GET", nil)
	_, body, _ := performRequest(c, requestRead)
	var result map[string]interface{}
	json.Unmarshal(body, &result)
	assert.Equal(t, 2, int(result["totalResults"].(float64)))
	clearExistingObjects(c, url)
}
