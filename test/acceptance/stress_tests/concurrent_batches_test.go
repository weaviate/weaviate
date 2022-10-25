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

package stress_tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
)

type batch struct {
	Objects []*models.Object
}

const class = "TestClass"

// createRequest creates requests
func createRequest(url string, method string, payload interface{}) *http.Request {
	var body io.Reader = nil
	if payload != nil {
		jsonBody, err := json.Marshal(payload)
		if err != nil {
			panic(errors.Wrap(err, "Could not marshal request"))
		}
		body = bytes.NewBuffer(jsonBody)
	}
	request, err := http.NewRequest(method, url, body)
	if err != nil {
		panic(errors.Wrap(err, "Could not create request"))
	}
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Accept", "application/json")

	return request
}

// performRequest runs requests
func performRequest(c *http.Client, request *http.Request) (int, []byte, error) {
	response, err := c.Do(request)
	if err != nil {
		return 0, nil, err
	}

	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		return 0, nil, err
	}

	return response.StatusCode, body, nil
}

func createSchemaRequest(url string) *http.Request {
	classObj := &models.Class{
		Class:       class,
		Description: "Dummy class for benchmarking purposes",
		Properties: []*models.Property{
			{
				DataType:    []string{"int"},
				Description: "The value of the counter in the dataset",
				Name:        "counter",
			},
			{
				DataType:    []string{"int"},
				Description: "The value of the counter in the dataset",
				Name:        "counter2",
			},
			{
				DataType:    []string{"string"},
				Description: "The value of the counter in the dataset",
				Name:        "something",
			},
		},
		VectorIndexConfig: map[string]interface{}{
			"distance":              "l2-squared",
			"ef":                    -1,
			"efConstruction":        64,
			"maxConnections":        64,
			"vectorCacheMaxObjects": 1000000000,
		},
	}
	request := createRequest(url+"schema", "POST", classObj)
	return request
}

func createObject() []*models.Object {
	objects := []*models.Object{
		{
			Class:  class,
			ID:     strfmt.UUID(uuid.New().String()),
			Vector: models.C11yVector([]float32{1.0, 2, 534, 324, 0.0001}),
			Properties: map[string]interface{}{
				"counter":   50,
				"counter2":  45,
				"something": "JustSlammedMyKeyboardahudghoig",
			},
		},
	}
	return objects
}

// If there is already a schema present, clear it out
func clearExistingObjects(c *http.Client, url string) {
	checkSchemaRequest := createRequest(url+"schema", "GET", nil)
	checkSchemaResponseCode, body, err := performRequest(c, checkSchemaRequest)
	if err != nil {
		panic(errors.Wrap(err, "perform request"))
	}
	if checkSchemaResponseCode != 200 {
		return
	}

	var dump models.Schema
	if err := json.Unmarshal(body, &dump); err != nil {
		panic(errors.Wrap(err, "Could not unmarshal read response"))
	}
	for _, classObj := range dump.Classes {
		requestDelete := createRequest(url+"schema/"+classObj.Class, "DELETE", nil)
		responseDeleteCode, _, err := performRequest(c, requestDelete)
		if err != nil {
			panic(errors.Wrap(err, "Could delete schema"))
		}
		if responseDeleteCode != 200 {
			panic(fmt.Sprintf("Could not delete schema, code: %v", responseDeleteCode))
		}
	}
}

func createHttpClient() *http.Client {
	httpT := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   500 * time.Second,
			KeepAlive: 120 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost:   100,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &http.Client{Transport: httpT}
}

func Test_AddConcurrentSchemas_sameObject(t *testing.T) {
	url := "http://localhost:8080/v1/"
	objects := createObject()
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
				requestSchema := createSchemaRequest(url)
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

	requestAdd1 := createRequest(url+"batch/objects", "POST", batch{createObject()})
	requestAdd2 := createRequest(url+"batch/objects", "POST", batch{createObject()})
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
				requestSchema := createSchemaRequest(url)
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
