/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	// "github.com/weaviate/weaviate/connectors"
	// "github.com/weaviate/weaviate/connectors/datastore"
	"github.com/weaviate/weaviate/models"
)

/*
 * Request function
 */
func doRequest(endpoint string, method string, accept string, body io.Reader, apiKey string) *http.Response {
	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}
	client := &http.Client{Transport: tr}

	req, _ := http.NewRequest(method, "http://localhost:8080/weaviate/v1-alpha"+endpoint, body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", accept)

	if apiKey != "" {
		req.Header.Set("X-API-KEY", apiKey)
	}

	response, err := client.Do(req)

	if err != nil {
		panic(err)
	}

	return response

}

func getResponseBody(response *http.Response) []byte {
	defer response.Body.Close()
	body, _ := ioutil.ReadAll(response.Body)

	return body
}

var apiKeyCmdLine string
var locationId string

func init() {
	flag.StringVar(&apiKeyCmdLine, "api-key", "", "API-KEY as used as haeder in the tests.")
	flag.Parse()

	// testUserObject := &dbconnector.DatabaseUsersObjectsObject{
	// 	Delete:   true,
	// 	Email:    "test@ing.nl",
	// 	IpOrigin: "*",
	// 	Read:     true,
	// 	Write:    true,
	// }

	// jsonValue, _ := json.Marshal(testUserObject)

	// testUser := &dbconnector.DatabaseUsersObject{
	// 	Uuid:         "sdf",
	// 	KeyToken:     "TESTTEST",
	// 	KeyExpiresMs: 0,
	// 	Object:       string(jsonValue),
	// 	Parent:       "",
	// }

	// databaseConnector := &datastore.Datastore{}
	// databaseConnector.Connect()

	// UUID, _ := databaseConnector.AddUser(*testUser)

	// print(UUID)
}

/******************
 * START TESTS
 ******************/

// weaviate.location.insert
func Test__weaviate_location_insert_JSON(t *testing.T) {
	jsonStr := bytes.NewBuffer([]byte(`{"address_components":[{"long_name":"TEST","short_name":"string","types":["UNDEFINED"]}],"formatted_address":"string","geometry":{"location":{},"location_type":"string","viewport":{"northeast":{},"southwest":{}}},"place_id":"string","types":["UNDEFINED"]} `))
	response := doRequest("/locations", "POST", "application/json", jsonStr, apiKeyCmdLine)

	if response.StatusCode != http.StatusAccepted {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusAccepted, response.StatusCode)
	}

	body := getResponseBody(response)

	respObject := &models.Location{}
	json.Unmarshal(body, respObject)

	locationId = respObject.ID

	expLength := 36
	if len(locationId) != expLength {
		t.Errorf("Expected length of ID %d. Got %d\n", expLength, len(locationId))
	}
}

// weaviate.location.list
func Test__weaviate_location_list_JSON(t *testing.T) {
	response := doRequest("/locations", "GET", "application/json", nil, apiKeyCmdLine)
	if response.StatusCode != http.StatusOK {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusOK, response.StatusCode)
	}

	body := getResponseBody(response)

	respObject := &models.LocationsListResponse{}
	json.Unmarshal(body, respObject)

	if respObject.Locations[0].ID != locationId {
		t.Errorf("Expected ID %s. Got %s\n", locationId, respObject.Locations[0].ID)
	}
}

// weaviate.location.get
func Test__weaviate_location_get_JSON(t *testing.T) {
	response := doRequest("/locations/"+locationId, "GET", "application/json", nil, apiKeyCmdLine)
	if response.StatusCode != http.StatusOK {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusOK, response.StatusCode)
	}

	body := getResponseBody(response)

	respObject := &models.Location{}
	json.Unmarshal(body, respObject)

	if respObject.ID != locationId {
		t.Errorf("Expected ID %s. Got %s\n", locationId, respObject.ID)
	}

	responseNotFound := doRequest("/locations/11111111-1111-1111-1111-111111111111", "GET", "application/json", nil, apiKeyCmdLine)
	if responseNotFound.StatusCode != http.StatusNotFound {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotFound, responseNotFound.StatusCode)
	}
}

// weaviate.location.update
// weaviate.location.patch
// weaviate.location.delete

// weaviate.adapters.list
func Test__weaviate_adapters_list_JSON(t *testing.T) {
	result := doRequest("/adapters", "GET", "application/json", nil, apiKeyCmdLine)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

func Test__weaviate_adapters_list_XML(t *testing.T) {
	result := doRequest("/adapters", "GET", "application/xml", nil, apiKeyCmdLine)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

// weaviate.adapters.insert
func Test__weaviate_adapters_insert_JSON(t *testing.T) {
	jsonStr := bytes.NewBuffer([]byte(`{}`))
	result := doRequest("/adapters", "POST", "application/json", jsonStr, apiKeyCmdLine)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

func Test__weaviate_adapters_insert_XML(t *testing.T) {
	jsonStr := bytes.NewBuffer([]byte(`{}`))
	result := doRequest("/adapters", "POST", "application/xml", jsonStr, apiKeyCmdLine)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

// weaviate.adapters.delete
func Test__weaviate_adapters_delete_JSON(t *testing.T) {
	result := doRequest("/adapters/1", "DELETE", "application/json", nil, apiKeyCmdLine)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

func Test__weaviate_adapters_delete_XML(t *testing.T) {
	result := doRequest("/adapters/1", "DELETE", "application/xml", nil, apiKeyCmdLine)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

// weaviate.adapters.get
func Test__weaviate_adapters_get_JSON(t *testing.T) {
	result := doRequest("/adapters/1", "GET", "application/json", nil, apiKeyCmdLine)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

func Test__weaviate_adapters_get_XML(t *testing.T) {
	result := doRequest("/adapters/1", "GET", "application/xml", nil, apiKeyCmdLine)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

// weaviate.adapters.update
func Test__weaviate_adapters_update_JSON(t *testing.T) {
	jsonStr := bytes.NewBuffer([]byte(`{ "activateUrl": "string", "activated": true, "deactivateUrl": "string", "displayName": "string", "iconUrl": "string", "id": "string", "manageUrl": "string" }`))
	result := doRequest("/adapters/1", "PUT", "application/json", jsonStr, apiKeyCmdLine)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

func Test__weaviate_adapters_update_XML(t *testing.T) {
	jsonStr := bytes.NewBuffer([]byte(`{"activateUrl": "string"}`))
	result := doRequest("/adapters/1", "PUT", "application/xml", jsonStr, apiKeyCmdLine)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

// weaviate.adapters.patch
func Test__weaviate_adapters_patch_JSON(t *testing.T) {
	jsonStr := bytes.NewBuffer([]byte(`{}`))
	result := doRequest("/adapters/1", "PATCH", "application/json", jsonStr, apiKeyCmdLine)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

func Test__weaviate_adapters_patch_XML(t *testing.T) {
	jsonStr := bytes.NewBuffer([]byte(`{}`))
	result := doRequest("/adapters/1", "PATCH", "application/xml", jsonStr, apiKeyCmdLine)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}
