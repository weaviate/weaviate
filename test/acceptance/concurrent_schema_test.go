package test

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

func Test_AddConcurrentSchemas(t *testing.T) {
	httpT := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 120 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost:   100,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	c := &http.Client{Transport: httpT}
	url := "http://localhost:8080/v1/"
	clearExistingObjects(c, url)
	requestSchema := createSchemaRequest(url)
	objects := createObject()

	requestAdd := createRequest(url+"batch/objects", "POST", batch{objects})
	// Add schema and object
	parallelReqs := 150
	wg := sync.WaitGroup{}
	wgStartReqests := sync.WaitGroup{}
	wgStartReqests.Add(parallelReqs)
	wg.Add(parallelReqs)
	for i := 0; i < parallelReqs; i++ {
		go func() {
			wgStartReqests.Done()
			wgStartReqests.Wait()

			performRequest(c, requestSchema)
			performRequest(c, requestAdd)

			wg.Done()
		}()
	}
	wg.Wait()

	requestRead := createRequest(url+"objects?limit="+fmt.Sprint(10)+"&class="+class, "GET", nil)
	_, body, _ := performRequest(c, requestRead)
	var result map[string]interface{}
	json.Unmarshal(body, &result)
	assert.Equal(t, 1, int(result["totalResults"].(float64)))
}
