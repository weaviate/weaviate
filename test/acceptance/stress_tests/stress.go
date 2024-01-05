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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

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
	for {
		response, err := c.Do(request)
		if err != nil {
			return 0, nil, err
		}

		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			return 0, nil, err
		}

		if response.StatusCode == 200 {
			return response.StatusCode, body, nil
		}
		time.Sleep(time.Millisecond * 10)
		var result map[string]interface{}
		json.Unmarshal(body, &result)
		message := result["error"].([]interface{})[0].(map[string]interface{})["message"].(string)

		if strings.Contains(message, "concurrent transaction") {
			time.Sleep(time.Millisecond * 10)
			continue
		}
		return response.StatusCode, body, nil
	}
}

func createSchemaRequest(url string, class string, multiTenantcy bool) *http.Request {
	classObj := &models.Class{
		Class:       class,
		Description: "Dummy class for benchmarking purposes",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: multiTenantcy,
		},
		Properties: []*models.Property{
			{
				DataType:    []string{"int"},
				Description: "The value of the counter in the dataset",
				Name:        "counter",
			},
			{
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				Description:  "The value of the counter in the dataset",
				Name:         "name",
			},
		},
	}
	request := createRequest(url+"schema", "POST", classObj)
	return request
}

func createObject(class string) []*models.Object {
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

func createBatch(class string, batchSize int, tenants []models.Tenant) []*models.Object {
	objects := make([]*models.Object, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		objects = append(objects, &models.Object{
			Class: class,
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"counter": i,
				"name":    tenants[i%len(tenants)].Name,
			},
			Tenant: tenants[i%len(tenants)].Name,
		})
	}
	return objects
}
