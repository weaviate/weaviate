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

	req, _ := http.NewRequest(method, "http://localhost:8080/weaviate/v1"+endpoint, body)
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
}

/******************
 * START TESTS
 ******************/

// weaviate.location.insert
func Test__weaviate_location_insert_JSON(t *testing.T) {
	// Create insert request
	jsonStr := bytes.NewBuffer([]byte(`{"address_components":[{"long_name":"TEST","short_name":"string","types":["UNDEFINED"]}],"formatted_address":"string","geometry":{"location":{},"location_type":"string","viewport":{"northeast":{},"southwest":{}}},"place_id":"string","types":["UNDEFINED"]} `))
	response := doRequest("/locations", "POST", "application/json", jsonStr, apiKeyCmdLine)

	// Check status code of insert
	if response.StatusCode != http.StatusAccepted {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusAccepted, response.StatusCode)
	}

	body := getResponseBody(response)

	respObject := &models.LocationGetResponse{}
	json.Unmarshal(body, respObject)

	// Check whether generated UUID is added
	locationId = string(respObject.ID)

	expLength := 36
	if len(locationId) != expLength {
		t.Errorf("Expected length of ID %d. Got %d\n", expLength, len(locationId))
	}

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)
}

// weaviate.location.list
func Test__weaviate_location_list_JSON(t *testing.T) {
	// Create list request
	response := doRequest("/locations", "GET", "application/json", nil, apiKeyCmdLine)

	// Check status code of list
	if response.StatusCode != http.StatusOK {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusOK, response.StatusCode)
	}

	body := getResponseBody(response)

	respObject := &models.LocationsListResponse{}
	json.Unmarshal(body, respObject)

	// Check most recent
	if string(respObject.Locations[0].ID) != locationId {
		t.Errorf("Expected ID %s. Got %s\n", locationId, respObject.Locations[0].ID)
	}
}

// weaviate.location.get
func Test__weaviate_location_get_JSON(t *testing.T) {
	// Create get request
	response := doRequest("/locations/"+locationId, "GET", "application/json", nil, apiKeyCmdLine)

	// Check status code get request
	if response.StatusCode != http.StatusOK {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusOK, response.StatusCode)
	}

	body := getResponseBody(response)

	respObject := &models.LocationGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID of object
	if string(respObject.ID) != locationId {
		t.Errorf("Expected ID %s. Got %s\n", locationId, respObject.ID)
	}

	// Create get request with non-existing location
	responseNotFound := doRequest("/locations/11111111-1111-1111-1111-111111111111", "GET", "application/json", nil, apiKeyCmdLine)

	// Check response of non-existing location
	if responseNotFound.StatusCode != http.StatusNotFound {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotFound, responseNotFound.StatusCode)
	}
}

// weaviate.location.update
func Test__weaviate_location_update_JSON(t *testing.T) {
	// Create update request
	newLongName := "updated_name"
	jsonStr := bytes.NewBuffer([]byte(`{"address_components":[{"long_name":"` + newLongName + `","short_name":"string","types":["UNDEFINED"]}],"formatted_address":"string","geometry":{"location":{},"location_type":"string","viewport":{"northeast":{},"southwest":{}}},"place_id":"","types":["UNDEFINED"]} `))
	response := doRequest("/locations/"+locationId, "PUT", "application/json", jsonStr, apiKeyCmdLine)

	body := getResponseBody(response)

	respObject := &models.LocationGetResponse{}
	json.Unmarshal(body, respObject)

	// Check location ID is same
	if string(respObject.ID) != locationId {
		t.Errorf("Expected ID %s. Got %s\n", locationId, respObject.ID)
	}

	// Check name after update
	if respObject.AddressComponents[0].LongName != newLongName {
		t.Errorf("Expected updated Long Name %s. Got %s\n", newLongName, respObject.AddressComponents[0].LongName)
	}

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Check if update is also applied on object when using a new GET request on same object
	responseGet := doRequest("/locations/"+locationId, "GET", "application/json", nil, apiKeyCmdLine)

	bodyGet := getResponseBody(responseGet)

	respObjectGet := &models.Location{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check name after update and get
	if respObjectGet.AddressComponents[0].LongName != newLongName {
		t.Errorf("Expected updated Long Name after GET %s. Got %s\n", newLongName, respObject.AddressComponents[0].LongName)
	}

	// Check put on non-existing ID
	emptyJSON := bytes.NewBuffer([]byte(`{}`))
	responseNotFound := doRequest("/locations/11111111-1111-1111-1111-111111111111", "PUT", "application/json", emptyJSON, apiKeyCmdLine)
	if responseNotFound.StatusCode != http.StatusNotFound {
		t.Errorf("Expected response code for not found %d. Got %d\n", http.StatusNotFound, responseNotFound.StatusCode)
	}
}

// weaviate.location.patch
func Test__weaviate_location_patch_JSON(t *testing.T) {
	// Create patch request
	newLongName := "patched_name"

	jsonStr := bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/address_components/0/long_name", "value": "` + newLongName + `"}]`))
	response := doRequest("/locations/"+locationId, "PATCH", "application/json", jsonStr, apiKeyCmdLine)

	body := getResponseBody(response)

	respObject := &models.LocationGetResponse{}
	json.Unmarshal(body, respObject)

	// Check name is the same
	if string(respObject.ID) != locationId {
		t.Errorf("Expected ID %s. Got %s\n", locationId, respObject.ID)
	}

	// Check ID after patch
	if respObject.AddressComponents[0].LongName != newLongName {
		t.Errorf("Expected patched Long Name %s. Got %s\n", newLongName, respObject.AddressComponents[0].LongName)
	}

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Check if patch is also applied on object when using a new GET request on same object
	responseGet := doRequest("/locations/"+locationId, "GET", "application/json", nil, apiKeyCmdLine)

	bodyGet := getResponseBody(responseGet)

	respObjectGet := &models.LocationGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check name after patch and get
	if respObjectGet.AddressComponents[0].LongName != newLongName {
		t.Errorf("Expected patched Long Name after GET %s. Got %s\n", newLongName, respObject.AddressComponents[0].LongName)
	}

	// Check patch with incorrect contents
	jsonStrError := bytes.NewBuffer([]byte(`{ "op": "replace", "path": "/address_components/long_name", "value": "` + newLongName + `"}`))
	responseError := doRequest("/locations/"+locationId, "PATCH", "application/json", jsonStrError, apiKeyCmdLine)

	if responseError.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected response code for wrong input %d. Got %d\n", http.StatusBadRequest, responseError.StatusCode)
	}

	// Check patch on non-existing ID
	emptyJSON := bytes.NewBuffer([]byte(`[{}]`))
	responseNotFound := doRequest("/locations/11111111-1111-1111-1111-111111111111", "PATCH", "application/json", emptyJSON, apiKeyCmdLine)

	if responseNotFound.StatusCode != http.StatusNotFound {
		t.Errorf("Expected response code for not found %d. Got %d\n", http.StatusNotFound, responseNotFound.StatusCode)
	}
}

// weaviate.location.delete
func Test__weaviate_location_delete_JSON(t *testing.T) {
	// Create delete request
	response := doRequest("/locations/"+locationId, "DELETE", "application/json", nil, apiKeyCmdLine)

	// Check status code get request
	if response.StatusCode != http.StatusNoContent {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNoContent, response.StatusCode)
	}

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Create delete request
	responseAlreadyDeleted := doRequest("/locations/"+locationId, "DELETE", "application/json", nil, apiKeyCmdLine)

	// Check status code get request
	if responseAlreadyDeleted.StatusCode != http.StatusNotFound {
		t.Errorf("Expected response code already deleted %d. Got %d\n", http.StatusNotFound, responseAlreadyDeleted.StatusCode)
	}

	// Create get request with non-existing location
	responseNotFound := doRequest("/locations/11111111-1111-1111-1111-111111111111", "DELETE", "application/json", nil, apiKeyCmdLine)

	// Check response of non-existing location
	if responseNotFound.StatusCode != http.StatusNotFound {
		t.Errorf("Expected response code not found %d. Got %d\n", http.StatusNotFound, responseNotFound.StatusCode)
	}
}
