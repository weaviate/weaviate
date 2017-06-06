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
	"github.com/go-openapi/strfmt"
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

// testNotExistsRequest with starting endpoint
func testNotExistsRequest(t *testing.T, endpointStartsWith string, method string, accept string, body io.Reader, apiKey string) {
	// Create get request with non-existing ID
	responseNotFound := doRequest(endpointStartsWith+"/11111111-1111-1111-1111-111111111111", "GET", "application/json", nil, apiKeyCmdLine)

	// Check response of non-existing ID
	testStatusCode(t, responseNotFound.StatusCode, http.StatusNotFound)
}

// testStatusCode standard with response
func testStatusCode(t *testing.T, responseStatusCode int, httpStatusCode int) {
	if responseStatusCode != httpStatusCode {
		t.Errorf("Expected response code %d. Got %d\n", httpStatusCode, responseStatusCode)
	}
}

// testKind standard with static
func testKind(t *testing.T, responseKind string, staticKind string) {
	if staticKind != responseKind {
		t.Errorf("Expected kind '%s'. Got '%s'.\n", staticKind, responseKind)
	}
}

// testID globally saved id with response
func testID(t *testing.T, responseID string, shouldBeID string) {
	testIDFormat(t, responseID)
	testIDFormat(t, shouldBeID)

	if string(responseID) != shouldBeID {
		t.Errorf("Expected ID %s. Got %s\n", shouldBeID, responseID)
	}
}

// testIDFormat tests whether an ID is of a valid UUID format
func testIDFormat(t *testing.T, responseID string) {
	if !strfmt.IsUUID(responseID) {
		t.Errorf("ID is not of expected UUID-format. Got %s.\n", responseID)
	}
}

// testValues tests whether two values are the same
func testValues(t *testing.T, expected string, got string) {
	if expected != got {
		t.Errorf("Expected value is %s. Got %s\n", expected, got)
	}
}

func getResponseBody(response *http.Response) []byte {
	defer response.Body.Close()
	body, _ := ioutil.ReadAll(response.Body)

	return body
}

var apiKeyCmdLine string
var commandID string
var locationID string
var thingTemplateID string

var emptyJSON = bytes.NewBuffer([]byte(`{}`))
var emptyPatchJSON = bytes.NewBuffer([]byte(`[{}]`))

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
	testStatusCode(t, response.StatusCode, http.StatusAccepted)

	body := getResponseBody(response)

	respObject := &models.LocationGetResponse{}
	json.Unmarshal(body, respObject)

	// Check whether generated UUID is added
	locationID = string(respObject.ID)
	testIDFormat(t, locationID)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#locationGetResponse")

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)
}

// weaviate.location.list
func Test__weaviate_location_list_JSON(t *testing.T) {
	// Create list request
	response := doRequest("/locations", "GET", "application/json", nil, apiKeyCmdLine)

	// Check status code of list
	testStatusCode(t, response.StatusCode, http.StatusOK)

	body := getResponseBody(response)

	respObject := &models.LocationsListResponse{}
	json.Unmarshal(body, respObject)

	// Check most recent
	testID(t, string(respObject.Locations[0].ID), locationID)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#locationsListResponse")
}

// weaviate.location.get
func Test__weaviate_location_get_JSON(t *testing.T) {
	// Create get request
	response := doRequest("/locations/"+locationID, "GET", "application/json", nil, apiKeyCmdLine)

	// Check status code get request
	testStatusCode(t, response.StatusCode, http.StatusOK)

	body := getResponseBody(response)

	respObject := &models.LocationGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID of object
	testID(t, string(respObject.ID), locationID)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#locationGetResponse")

	// Create get request with non-existing location
	testNotExistsRequest(t, "/locations", "GET", "application/json", nil, apiKeyCmdLine)
}

// weaviate.location.update
func Test__weaviate_location_update_JSON(t *testing.T) {
	// Create update request
	newLongName := "updated_name"
	jsonStr := bytes.NewBuffer([]byte(`{"address_components":[{"long_name":"` + newLongName + `","short_name":"string","types":["UNDEFINED"]}],"formatted_address":"string","geometry":{"location":{},"location_type":"string","viewport":{"northeast":{},"southwest":{}}},"place_id":"","types":["UNDEFINED"]} `))
	response := doRequest("/locations/"+locationID, "PUT", "application/json", jsonStr, apiKeyCmdLine)

	body := getResponseBody(response)

	respObject := &models.LocationGetResponse{}
	json.Unmarshal(body, respObject)

	// Check location ID is same
	testID(t, string(respObject.ID), locationID)

	// Check name after update
	testValues(t, newLongName, respObject.AddressComponents[0].LongName)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#locationGetResponse")

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Check if update is also applied on object when using a new GET request on same object
	responseGet := doRequest("/locations/"+locationID, "GET", "application/json", nil, apiKeyCmdLine)

	bodyGet := getResponseBody(responseGet)

	// Test response obj
	respObjectGet := &models.LocationGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check name after update and get
	testValues(t, newLongName, respObject.AddressComponents[0].LongName)

	// Check put on non-existing ID
	testNotExistsRequest(t, "/locations", "PUT", "application/json", emptyJSON, apiKeyCmdLine)
}

// weaviate.location.patch
func Test__weaviate_location_patch_JSON(t *testing.T) {
	// Create patch request
	newLongName := "patched_name"

	jsonStr := bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/address_components/0/long_name", "value": "` + newLongName + `"}]`))
	response := doRequest("/locations/"+locationID, "PATCH", "application/json", jsonStr, apiKeyCmdLine)

	body := getResponseBody(response)

	respObject := &models.LocationGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID is the same
	testID(t, string(respObject.ID), locationID)

	// Check name after patch
	testValues(t, newLongName, respObject.AddressComponents[0].LongName)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#locationGetResponse")

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Check if patch is also applied on object when using a new GET request on same object
	responseGet := doRequest("/locations/"+locationID, "GET", "application/json", nil, apiKeyCmdLine)

	bodyGet := getResponseBody(responseGet)

	// Test response obj
	respObjectGet := &models.LocationGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check name after patch and get
	testValues(t, newLongName, respObject.AddressComponents[0].LongName)

	// Check patch with incorrect contents
	jsonStrError := bytes.NewBuffer([]byte(`{ "op": "replace", "path": "/address_components/long_name", "value": "` + newLongName + `"}`))
	responseError := doRequest("/locations/"+locationID, "PATCH", "application/json", jsonStrError, apiKeyCmdLine)
	testStatusCode(t, responseError.StatusCode, http.StatusBadRequest)

	// Check patch on non-existing ID
	testNotExistsRequest(t, "/locations", "PATCH", "application/json", emptyPatchJSON, apiKeyCmdLine)
}

// weaviate.location.delete
func Test__weaviate_location_delete_JSON(t *testing.T) {
	// Create delete request
	response := doRequest("/locations/"+locationID, "DELETE", "application/json", nil, apiKeyCmdLine)

	// Check status code get request
	testStatusCode(t, response.StatusCode, http.StatusNoContent)

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Create delete request
	responseAlreadyDeleted := doRequest("/locations/"+locationID, "DELETE", "application/json", nil, apiKeyCmdLine)

	// Check status code get request
	testStatusCode(t, responseAlreadyDeleted.StatusCode, http.StatusNotFound)

	// Create get request with non-existing location
	testNotExistsRequest(t, "/locations", "DELETE", "application/json", nil, apiKeyCmdLine)
}

// weaviate.thing_template.create
func Test__weaviate_thing_template_create_JSON(t *testing.T) {
	// Create create request
	jsonStr := bytes.NewBuffer([]byte(`{
		"commandsId": "11111111-1111-1111-1111-111111111111",
		"thingModelTemplate": {
			"modelName": "TEST name",
			"oemNumber": "TEST number",
			"oemAdditions": {
				"testAdd": 1,
				"testAdd2": "two",
				"testAddObj": {
					"testobj": "value"
				}
			},
			"oemName": "TEST oemname",
			"oemIcon": "TEST oemicon",
			"oemContact": "TEST oemcontact"
		},
		"name": "TEST THING TEMPLATE"
		}`))
	response := doRequest("/thingTemplates", "POST", "application/json", jsonStr, apiKeyCmdLine)

	// Check status code of create
	testStatusCode(t, response.StatusCode, http.StatusAccepted)

	body := getResponseBody(response)

	respObject := &models.ThingGetResponse{}
	json.Unmarshal(body, respObject)

	// Check whether generated UUID is added
	thingTemplateID = string(respObject.ID)
	testIDFormat(t, thingTemplateID)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#thingTemplateGetResponse")

	//dTest is faster than adding to DB.
	time.Sleep(1 * time.Second)
}

// weaviate.thing_template.list
func Test__weaviate_thing_template_list_JSON(t *testing.T) {
	// Create list request
	response := doRequest("/thingTemplates", "GET", "application/json", nil, apiKeyCmdLine)

	// Check status code of list
	testStatusCode(t, response.StatusCode, http.StatusOK)

	body := getResponseBody(response)

	respObject := &models.ThingTemplatesListResponse{}
	json.Unmarshal(body, respObject)

	// Check most recent
	testID(t, string(respObject.ThingTemplates[0].ID), thingTemplateID)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#thingTemplatesListResponse")
}

// weaviate.thing_template.get
func Test__weaviate_thing_template_get_JSON(t *testing.T) {
	// Create get request
	response := doRequest("/thingTemplates/"+thingTemplateID, "GET", "application/json", nil, apiKeyCmdLine)

	// Check status code get request
	testStatusCode(t, response.StatusCode, http.StatusOK)

	body := getResponseBody(response)

	respObject := &models.ThingTemplateGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID of object
	testID(t, string(respObject.ID), thingTemplateID)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#thingTemplateGetResponse")

	//dCreate get request with non-existing thingTemplate
	testNotExistsRequest(t, "/thingTemplates", "GET", "application/json", nil, apiKeyCmdLine)
}

// weaviate.thing_template.update
func Test__weaviate_thing_template_update_JSON(t *testing.T) {
	// Create update request
	newValue := "updated_name"
	jsonStr := bytes.NewBuffer([]byte(`{
		"commandsId": "11111111-1111-1111-1111-111111111111",
		"thingModelTemplate": {
			"modelName": "` + newValue + `",
			"oemNumber": "TEST number",
			"oemAdditions": {
				"testAdd": 1,
				"testAdd2": "two",
				"testAddObj": {
					"testobj": "value"
				}
			},
			"oemName": "TEST oemname",
			"oemIcon": "TEST oemicon",
			"oemContact": "TEST oemcontact"
		},
		"name": "TEST THING TEMPLATE"
	}`))
	response := doRequest("/thingTemplates/"+thingTemplateID, "PUT", "application/json", jsonStr, apiKeyCmdLine)

	body := getResponseBody(response)

	respObject := &models.ThingTemplateGetResponse{}
	json.Unmarshal(body, respObject)

	// Check thingTemplate ID is same

	testID(t, string(respObject.ID), thingTemplateID)

	// Check name after update
	testValues(t, newValue, respObject.ThingModelTemplate.ModelName)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#thingTemplateGetResponse")

	//dTest is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Check if update is also applied on object when using a new GET request on same object
	responseGet := doRequest("/thingTemplates/"+thingTemplateID, "GET", "application/json", nil, apiKeyCmdLine)

	bodyGet := getResponseBody(responseGet)

	// Test response obj
	respObjectGet := &models.ThingTemplateGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check name after update and get
	testValues(t, newValue, respObject.ThingModelTemplate.ModelName)

	// Check put on non-existing ID
	testNotExistsRequest(t, "/thingTemplates", "PUT", "application/json", emptyJSON, apiKeyCmdLine)
}

// weaviate.thing_template.patch
func Test__weaviate_thing_template_patch_JSON(t *testing.T) {
	// Create patch request
	newValue := "patched_name"

	jsonStr := bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/thingModelTemplate/modelName", "value": "` + newValue + `"}]`))
	response := doRequest("/thingTemplates/"+thingTemplateID, "PATCH", "application/json", jsonStr, apiKeyCmdLine)

	body := getResponseBody(response)

	respObject := &models.ThingTemplateGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID is the same

	testID(t, string(respObject.ID), thingTemplateID)

	// Check name after patch
	testValues(t, newValue, respObject.ThingModelTemplate.ModelName)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#thingTemplateGetResponse")

	//dTest is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Check if patch is also applied on object when using a new GET request on same object
	responseGet := doRequest("/thingTemplates/"+thingTemplateID, "GET", "application/json", nil, apiKeyCmdLine)

	bodyGet := getResponseBody(responseGet)

	// Create response object
	respObjectGet := &models.ThingTemplateGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check name after patch and get
	testValues(t, newValue, respObject.ThingModelTemplate.ModelName)

	// Check patch with incorrect contents
	jsonStrError := bytes.NewBuffer([]byte(`{ "op": "replace", "path": "/address_components/long_name", "value": "` + newValue + `"}`))
	responseError := doRequest("/thingTemplates/"+thingTemplateID, "PATCH", "application/json", jsonStrError, apiKeyCmdLine)
	testStatusCode(t, responseError.StatusCode, http.StatusBadRequest)

	// Check patch on non-existing ID
	testNotExistsRequest(t, "/thingTemplates", "PATCH", "application/json", emptyPatchJSON, apiKeyCmdLine)
}

// weaviate.thing_template.delete
func Test__weaviate_thing_template_delete_JSON(t *testing.T) {
	// Create delete request
	response := doRequest("/thingTemplates/"+thingTemplateID, "DELETE", "application/json", nil, apiKeyCmdLine)

	// Check status code get request
	testStatusCode(t, response.StatusCode, http.StatusNoContent)

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Create delete request
	responseAlreadyDeleted := doRequest("/thingTemplates/"+thingTemplateID, "DELETE", "application/json", nil, apiKeyCmdLine)

	// Check status code get request
	testStatusCode(t, responseAlreadyDeleted.StatusCode, http.StatusNotFound)

	// Create get request with non-existing ID
	testNotExistsRequest(t, "/thingTemplates", "DELETE", "application/json", nil, apiKeyCmdLine)
}

// weaviate.command.insert
func Test__weaviate_command_insert_JSON(t *testing.T) {
	// Create insert request
	jsonStr := bytes.NewBuffer([]byte(`
		{
			"creationTimeMs": 123,
			"creatorKey": "string",
			"thingTemplateId": "` + thingTemplateID + `",
			"error": {
				"arguments": [
				"string"
				],
				"code": "string",
				"message": "string"
			},
			"expirationTimeMs": 123,
			"expirationTimeoutMs": 123,
			"lastUpdateTimeMs": 123,
			"name": "string",
			"parameters": {},
			"progress": "aborted",
			"results": {},
			"userAction": "string"
		}
	`))
	response := doRequest("/commands", "POST", "application/json", jsonStr, apiKeyCmdLine)

	// Check status code of insert
	testStatusCode(t, response.StatusCode, http.StatusAccepted)

	body := getResponseBody(response)

	respObject := &models.CommandGetResponse{}
	json.Unmarshal(body, respObject)

	// Check whether generated UUID is added
	commandID = string(respObject.ID)
	testIDFormat(t, commandID)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#commandGetResponse")

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)
}

// weaviate.command.list
func Test__weaviate_command_list_JSON(t *testing.T) {
	// Create list request
	response := doRequest("/commands", "GET", "application/json", nil, apiKeyCmdLine)

	// Check status code of list
	testStatusCode(t, response.StatusCode, http.StatusOK)

	body := getResponseBody(response)

	respObject := &models.CommandsListResponse{}
	json.Unmarshal(body, respObject)

	// Check most recent
	testID(t, string(respObject.Commands[0].ID), commandID)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#commandsListResponse")
}

// weaviate.command.get
func Test__weaviate_command_get_JSON(t *testing.T) {
	// Create get request
	response := doRequest("/commands/"+commandID, "GET", "application/json", nil, apiKeyCmdLine)

	// Check status code get request
	testStatusCode(t, response.StatusCode, http.StatusOK)

	body := getResponseBody(response)

	respObject := &models.CommandGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID of object
	testID(t, string(respObject.ID), commandID)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#commandGetResponse")

	// Create get request with non-existing ID
	testNotExistsRequest(t, "/commands", "GET", "application/json", nil, apiKeyCmdLine)
}

// // weaviate.command.update
// func Test__weaviate_command_update_JSON(t *testing.T) {
// 	// Create update request
// 	newLongName := "updated_name"
// 	jsonStr := bytes.NewBuffer([]byte(`{"address_components":[{"long_name":"` + newLongName + `","short_name":"string","types":["UNDEFINED"]}],"formatted_address":"string","geometry":{"location":{},"location_type":"string","viewport":{"northeast":{},"southwest":{}}},"place_id":"","types":["UNDEFINED"]} `))
// 	response := doRequest("/commands/"+commandID, "PUT", "application/json", jsonStr, apiKeyCmdLine)

// 	body := getResponseBody(response)

// 	respObject := &models.CommandGetResponse{}
// 	json.Unmarshal(body, respObject)

// 	// Check location ID is same
// 	if string(respObject.ID) != commandID {
// 		t.Errorf("Expected ID %s. Got %s\n", commandID, respObject.ID)
// 	}

// 	// Check name after update
// 	if respObject.AddressComponents[0].LongName != newLongName {
// 		t.Errorf("Expected updated Long Name %s. Got %s\n", newLongName, respObject.AddressComponents[0].LongName)
// 	}

// 	// Check kind
//
// 	testKind(t, string(*respObject.Kind), "weaviate#commandGetResponse")

// 	// Test is faster than adding to DB.
// 	time.Sleep(1 * time.Second)

// 	// Check if update is also applied on object when using a new GET request on same object
// 	responseGet := doRequest("/commands/"+commandID, "GET", "application/json", nil, apiKeyCmdLine)

// 	bodyGet := getResponseBody(responseGet)

// 	respObjectGet := &models.CommandGetResponse{}
// 	json.Unmarshal(bodyGet, respObjectGet)

// 	// Check name after update and get
// 	if respObjectGet.AddressComponents[0].LongName != newLongName {
// 		t.Errorf("Expected updated Long Name after GET %s. Got %s\n", newLongName, respObjectGet.AddressComponents[0].LongName)
// 	}

// 	// Check put on non-existing ID
// 	responseNotFound := doRequest("/commands/11111111-1111-1111-1111-111111111111", "PUT", "application/json", emptyJSON, apiKeyCmdLine)
// 	if responseNotFound.StatusCode != http.StatusNotFound {
// 		t.Errorf("Expected response code for not found %d. Got %d\n", http.StatusNotFound, responseNotFound.StatusCode)
// 	}
// }

// // weaviate.command.patch
// func Test__weaviate_command_patch_JSON(t *testing.T) {
// 	// Create patch request
// 	newLongName := "patched_name"

// 	jsonStr := bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/address_components/0/long_name", "value": "` + newLongName + `"}]`))
// 	response := doRequest("/commands/"+commandID, "PATCH", "application/json", jsonStr, apiKeyCmdLine)

// 	body := getResponseBody(response)

// 	respObject := &models.CommandGetResponse{}
// 	json.Unmarshal(body, respObject)

// 	// Check ID is the same
// 	if string(respObject.ID) != commandID {
// 		t.Errorf("Expected ID %s. Got %s\n", commandID, respObject.ID)
// 	}

// 	// Check name after patch
// 	if respObject.AddressComponents[0].LongName != newLongName {
// 		t.Errorf("Expected patched Long Name %s. Got %s\n", newLongName, respObject.AddressComponents[0].LongName)
// 	}

// 	// Check kind
//
//  testKind(t, string(*respObject.Kind), "weaviate#commandGetResponse")

// 	// Test is faster than adding to DB.
// 	time.Sleep(1 * time.Second)

// 	// Check if patch is also applied on object when using a new GET request on same object
// 	responseGet := doRequest("/commands/"+commandID, "GET", "application/json", nil, apiKeyCmdLine)

// 	bodyGet := getResponseBody(responseGet)

// 	respObjectGet := &models.CommandGetResponse{}
// 	json.Unmarshal(bodyGet, respObjectGet)

// 	// Check name after patch and get
// 	if respObjectGet.AddressComponents[0].LongName != newLongName {
// 		t.Errorf("Expected patched Long Name after GET %s. Got %s\n", newLongName, respObjectGet.AddressComponents[0].LongName)
// 	}

// 	// Check patch with incorrect contents
// 	jsonStrError := bytes.NewBuffer([]byte(`{ "op": "replace", "path": "/address_components/long_name", "value": "` + newLongName + `"}`))
// 	responseError := doRequest("/commands/"+commandID, "PATCH", "application/json", jsonStrError, apiKeyCmdLine)

// 	if responseError.StatusCode != http.StatusBadRequest {
// 		t.Errorf("Expected response code for wrong input %d. Got %d\n", http.StatusBadRequest, responseError.StatusCode)
// 	}

// 	// Check patch on non-existing ID
// 	responseNotFound := doRequest("/commands/11111111-1111-1111-1111-111111111111", "PATCH", "application/json", emptyPatchJSON, apiKeyCmdLine)

// 	if responseNotFound.StatusCode != http.StatusNotFound {
// 		t.Errorf("Expected response code for not found %d. Got %d\n", http.StatusNotFound, responseNotFound.StatusCode)
// 	}
// }

// // weaviate.command.delete
// func Test__weaviate_command_delete_JSON(t *testing.T) {
// 	// Create delete request
// 	response := doRequest("/commands/"+commandID, "DELETE", "application/json", nil, apiKeyCmdLine)

// 	// Check status code get request
// 	if response.StatusCode != http.StatusNoContent {
// 		t.Errorf("Expected response code %d. Got %d\n", http.StatusNoContent, response.StatusCode)
// 	}

// 	// Test is faster than adding to DB.
// 	time.Sleep(1 * time.Second)

// 	// Create delete request
// 	responseAlreadyDeleted := doRequest("/commands/"+commandID, "DELETE", "application/json", nil, apiKeyCmdLine)

// 	// Check status code get request
// 	if responseAlreadyDeleted.StatusCode != http.StatusNotFound {
// 		t.Errorf("Expected response code already deleted %d. Got %d\n", http.StatusNotFound, responseAlreadyDeleted.StatusCode)
// 	}

// 	// Create get request with non-existing location
// 	responseNotFound := doRequest("/commands/11111111-1111-1111-1111-111111111111", "DELETE", "application/json", nil, apiKeyCmdLine)

// 	// Check response of non-existing location
// 	if responseNotFound.StatusCode != http.StatusNotFound {
// 		t.Errorf("Expected response code not found %d. Got %d\n", http.StatusNotFound, responseNotFound.StatusCode)
// 	}
// }
