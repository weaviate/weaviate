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
	"runtime"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/models"
	"sort"
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

func decorate() (string, int) {
	_, file, line, ok := runtime.Caller(2) // decorate + log + public function.
	if ok {
		// Truncate file name at last file name separator.
		if index := strings.LastIndex(file, "/"); index >= 0 {
			file = file[index+1:]
		} else if index = strings.LastIndex(file, "\\"); index >= 0 {
			file = file[index+1:]
		}
	} else {
		file = "???"
		line = 1
	}

	return file, line
}

// testNotExistsRequest with starting endpoint
func testNotExistsRequest(t *testing.T, endpointStartsWith string, method string, accept string, body io.Reader, apiKey string) {
	// Create get request with non-existing ID
	responseNotFound := doRequest(endpointStartsWith+"/"+fakeID, method, accept, body, apiKey)

	// Check response of non-existing ID
	testStatusCode(t, responseNotFound.StatusCode, http.StatusNotFound)
}

// testStatusCode standard with response
func testStatusCode(t *testing.T, responseStatusCode int, httpStatusCode int) {
	file, line := decorate()
	if responseStatusCode != httpStatusCode {
		t.Errorf("%s:%d: Expected response code %d. Got %d\n", file, line, httpStatusCode, responseStatusCode)
	}
}

// testKind standard with static
func testKind(t *testing.T, responseKind string, staticKind string) {
	file, line := decorate()
	if staticKind != responseKind {
		t.Errorf("%s:%d: Expected kind '%s'. Got '%s'.\n", file, line, staticKind, responseKind)
	}
}

// testID globally saved id with response
func testID(t *testing.T, responseID string, shouldBeID string) {
	testIDFormat(t, responseID)
	testIDFormat(t, shouldBeID)

	file, line := decorate()
	if string(responseID) != shouldBeID {
		t.Errorf("%s:%d: Expected ID %s. Got %s\n", file, line, shouldBeID, responseID)
	}
}

// testIDFormat tests whether an ID is of a valid UUID format
func testIDFormat(t *testing.T, responseID string) {
	file, line := decorate()
	if !strfmt.IsUUID(responseID) {
		t.Errorf("%s:%d: ID is not of expected UUID-format. Got %s.\n", file, line, responseID)
	}
}

// testValues tests whether two values are the same
func testValues(t *testing.T, expected string, got string) {
	file, line := decorate()
	if expected != got {
		t.Errorf("%s:%d: Expected value is %s. Got %s\n", file, line, expected, got)
	}
}

// testIntegerValues tests whether two integers are the same
func testIntegerValues(t *testing.T, expected int, got int) {
	file, line := decorate()
	if expected != got {
		t.Errorf("%s:%d: Expected value is %d. Got %d\n", file, line, expected, got)
	}
}

// testBooleanValues tests wheter two booleans are the same
func testBooleanValues(t *testing.T, expected bool, got bool) {
	file, line := decorate()
	if expected != got {
		t.Errorf("%s:%d: Expected value is %t. Got %t\n", file, line, expected, got)
	}
}

// getResponseBody converts response body to bytes
func getResponseBody(response *http.Response) []byte {
	defer response.Body.Close()
	body, _ := ioutil.ReadAll(response.Body)

	return body
}

// getEmptyJSON returns a buffer with emtpy JSON
func getEmptyJSON() io.Reader {
	return bytes.NewBuffer([]byte(`{}`))
}

// getEmptyPatchJSON returns a buffer with emtpy Patch-JSON
func getEmptyPatchJSON() io.Reader {
	return bytes.NewBuffer([]byte(`[{}]`))
}

var apiKeyCmdLine string
var commandID string
var eventID string
var fakeID string
var groupID string
var headToken string
var headID string
var keyID string
var newAPIToken string
var newAPIKeyID string
var newSubAPIToken string
var newSubAPIKeyID string
var locationID string
var thingID string
var thingTemplateID string
var rootID string

func init() {
	flag.StringVar(&apiKeyCmdLine, "api-key", "", "API-KEY as used as haeder in the tests.")
	flag.Parse()

	fakeID = "11111111-1111-1111-1111-111111111111"
}

/******************
 * KEY TESTS
 ******************/

// weaviate.key.create
func Test__weaviate_key_create_JSON(t *testing.T) {
	// Create create request
	jsonStr := bytes.NewBuffer([]byte(`{
		"delete": true,
		"email": "string",
		"ipOrigin": ["127.0.0.*", "*"],
		"keyExpiresUnix": 1,
		"read": false,
		"write": false,
		"execute": true
	}`))
	response := doRequest("/keys", "POST", "application/json", jsonStr, apiKeyCmdLine)

	// Check status code of create
	testStatusCode(t, response.StatusCode, http.StatusAccepted)

	body := getResponseBody(response)

	respObject := &models.KeyTokenGetResponse{}
	json.Unmarshal(body, respObject)

	// Check whether generated UUID is added
	keyID = string(respObject.ID)
	testIDFormat(t, keyID)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#keyTokenGetResponse")

	// Test Rights
	testBooleanValues(t, true, respObject.Delete)
	testBooleanValues(t, true, respObject.Execute)
	testBooleanValues(t, false, respObject.Read)
	testBooleanValues(t, false, respObject.Write)

	// Test given Token
	newAPIToken = string(respObject.Key)
	testIDFormat(t, newAPIToken)

	// Test given ID
	newAPIKeyID = string(respObject.ID)
	testIDFormat(t, newAPIKeyID)

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Create create request
	jsonStrNewKey := bytes.NewBuffer([]byte(`{
		"delete": false,
		"email": "string",
		"ipOrigin": ["127.0.0.*", "*"],
		"keyExpiresUnix": 0,
		"read": true,
		"write": true,
		"execute": false
	}`))
	responseNewToken := doRequest("/keys", "POST", "application/json", jsonStrNewKey, newAPIToken)

	// Test second statuscode
	testStatusCode(t, responseNewToken.StatusCode, http.StatusAccepted)

	// Process response
	bodyNewToken := getResponseBody(responseNewToken)
	respObjectNewToken := &models.KeyTokenGetResponse{}
	json.Unmarshal(bodyNewToken, respObjectNewToken)

	// Test key ID parent is correct
	testID(t, respObjectNewToken.Parent, keyID)

	// Test given Token
	newSubAPIToken = string(respObjectNewToken.Key)
	testIDFormat(t, newAPIToken)

	// Test given ID
	newSubAPIKeyID = string(respObjectNewToken.ID)
	testIDFormat(t, newAPIKeyID)

	// Test expiration set
	testIntegerValues(t, 0, int(respObjectNewToken.KeyExpiresUnix))

	// Test Rights
	testBooleanValues(t, false, respObjectNewToken.Delete)
	testBooleanValues(t, false, respObjectNewToken.Execute)
	testBooleanValues(t, true, respObjectNewToken.Read)
	testBooleanValues(t, true, respObjectNewToken.Write)

	// TODO:
	// Test expiration

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)
}

// weaviate.key.me.get
func Test__weaviate_key_me_get_JSON(t *testing.T) {
	// Create get request
	response := doRequest("/keys/me", "GET", "application/json", nil, newAPIToken)

	// Check status code get request
	testStatusCode(t, response.StatusCode, http.StatusOK)

	body := getResponseBody(response)

	respObject := &models.KeyTokenGetResponse{}
	json.Unmarshal(body, respObject)

	// Add general User ID
	rootID = string(respObject.Parent)

	// Check ID of object
	testID(t, string(respObject.ID), keyID)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#keyTokenGetResponse")
}

// weaviate.key.get
func Test__weaviate_key_get_JSON(t *testing.T) {
	// Create get request
	response := doRequest("/keys/"+keyID, "GET", "application/json", nil, apiKeyCmdLine)

	// Check status code get request
	testStatusCode(t, response.StatusCode, http.StatusOK)

	body := getResponseBody(response)

	respObject := &models.KeyGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID of object
	testID(t, string(respObject.ID), keyID)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#keyGetResponse")

	// Create get request
	responseForbidden := doRequest("/keys/"+rootID, "GET", "application/json", nil, newAPIToken)

	// Check status code forbidden request
	testStatusCode(t, responseForbidden.StatusCode, http.StatusForbidden)

	// Create get request with non-existing ID
	testNotExistsRequest(t, "/keys", "GET", "application/json", nil, apiKeyCmdLine)
}

// weaviate.key.children.get
func Test__weaviate_key_children_get_JSON(t *testing.T) {
	// HEAD: Create create request tree-head and process request
	jsonStrKeyHead := bytes.NewBuffer([]byte(`{
		"delete": true,
		"email": "string",
		"ipOrigin": ["127.0.0.*", "*"],
		"keyExpiresUnix": 0,
		"read": true,
		"write": true,
		"execute": true
	}`))
	responseHead := doRequest("/keys", "POST", "application/json", jsonStrKeyHead, newAPIToken)
	testStatusCode(t, responseHead.StatusCode, http.StatusAccepted)
	bodyHead := getResponseBody(responseHead)
	respObjectHead := &models.KeyTokenGetResponse{}
	json.Unmarshal(bodyHead, respObjectHead)

	time.Sleep(1 * time.Second)

	// Set reusable keys
	headToken = respObjectHead.Key
	headID = string(respObjectHead.ID)

	// Create get request
	response := doRequest("/keys/"+newAPIKeyID+"/children", "GET", "application/json", nil, newAPIToken)

	// Check status code get request
	testStatusCode(t, response.StatusCode, http.StatusOK)

	body := getResponseBody(response)

	respObject := &models.KeyChildrenGetResponse{}
	json.Unmarshal(body, respObject)

	// Add general User ID
	if 2 != len(respObject.Children) {
		t.Errorf("Expected number of children '%d'. Got '%d'.\n", 2, len(respObject.Children))
	}

	// Check IDs of objects are correct by adding them to an array and sorting
	responseChildren := []string{
		string(respObject.Children[0]),
		string(respObject.Children[1]),
	}

	checkIDs := []string{
		headID,
		newSubAPIKeyID,
	}

	sort.Strings(responseChildren)
	sort.Strings(checkIDs)

	testID(t, responseChildren[0], checkIDs[0])
	testID(t, responseChildren[1], checkIDs[1])

	// Create get request
	responseForbidden := doRequest("/keys/"+rootID+"/children", "GET", "application/json", nil, newAPIToken)

	// Check status code forbidden request
	testStatusCode(t, responseForbidden.StatusCode, http.StatusForbidden)

	// Create get request with non-existing ID
	responseNotFound := doRequest("keys/"+fakeID+"/children", "GET", "application/json", nil, newAPIToken)

	// Check response of non-existing ID
	testStatusCode(t, responseNotFound.StatusCode, http.StatusNotFound)
}

// weaviate.key.me.children.get
func Test__weaviate_key_me_children_get_JSON(t *testing.T) {
	// Create get request
	response := doRequest("/keys/me/children", "GET", "application/json", nil, newAPIToken)

	// Check status code get request
	testStatusCode(t, response.StatusCode, http.StatusOK)

	body := getResponseBody(response)

	respObject := &models.KeyChildrenGetResponse{}
	json.Unmarshal(body, respObject)

	// Add general User ID
	if 2 != len(respObject.Children) {
		t.Errorf("Expected number of children '%d'. Got '%d'.\n", 2, len(respObject.Children))
	}

	// Check IDs of objects are correct by adding them to an array and sorting
	responseChildren := []string{
		string(respObject.Children[0]),
		string(respObject.Children[1]),
	}

	checkIDs := []string{
		headID,
		newSubAPIKeyID,
	}

	sort.Strings(responseChildren)
	sort.Strings(checkIDs)

	testID(t, responseChildren[0], checkIDs[0])
	testID(t, responseChildren[1], checkIDs[1])
}

// weaviate.key.delete
func Test__weaviate_key_delete_JSON(t *testing.T) {
	// Sleep, otherwise head-key is not added
	time.Sleep(1 * time.Second)

	// SUB1: Create create request and process request
	jsonStrKeySub1 := bytes.NewBuffer([]byte(`{
		"delete": true,
		"email": "string",
		"ipOrigin": ["127.0.0.*", "*"],
		"keyExpiresUnix": 0,
		"read": true,
		"write": true,
		"execute": true
	}`))
	responseSub1 := doRequest("/keys", "POST", "application/json", jsonStrKeySub1, headToken)
	testStatusCode(t, responseSub1.StatusCode, http.StatusAccepted)
	bodySub1 := getResponseBody(responseSub1)
	respObjectSub1 := &models.KeyTokenGetResponse{}
	json.Unmarshal(bodySub1, respObjectSub1)

	// Sleep, otherwise head-key is not added
	time.Sleep(1 * time.Second)

	// Set reusable keys
	// sub1Token := respObjectSub1.Key
	sub1ID := string(respObjectSub1.ID)

	// SUB2: Create create request and process request
	jsonStrKeySub2 := bytes.NewBuffer([]byte(`{
		"delete": true,
		"email": "string",
		"ipOrigin": ["127.0.0.*", "*"],
		"keyExpiresUnix": 0,
		"read": true,
		"write": true,
		"execute": true
	}`))
	responseSub2 := doRequest("/keys", "POST", "application/json", jsonStrKeySub2, headToken)
	testStatusCode(t, responseSub2.StatusCode, http.StatusAccepted)
	bodySub2 := getResponseBody(responseSub2)
	respObjectSub2 := &models.KeyTokenGetResponse{}
	json.Unmarshal(bodySub2, respObjectSub2)

	// Sleep, otherwise head-key is not added
	time.Sleep(1 * time.Second)

	// Set reusable keys
	sub2Token := respObjectSub2.Key
	sub2ID := string(respObjectSub2.ID)

	// Delete head with sub2, which is not allowed
	responseDelHeadWithSub := doRequest("/keys/"+headID, "DELETE", "application/json", nil, sub2Token)
	testStatusCode(t, responseDelHeadWithSub.StatusCode, http.StatusForbidden)
	time.Sleep(2 * time.Second)

	// Delete sub1, check status and delay for faster check then request
	responseDelSub1 := doRequest("/keys/"+sub1ID, "DELETE", "application/json", nil, apiKeyCmdLine)
	testStatusCode(t, responseDelSub1.StatusCode, http.StatusNoContent)
	time.Sleep(2 * time.Second)

	// Check sub1 removed and check its statuscode (404)
	responseSub1Deleted := doRequest("/keys/"+sub1ID, "DELETE", "application/json", nil, apiKeyCmdLine)
	testStatusCode(t, responseSub1Deleted.StatusCode, http.StatusNotFound)
	time.Sleep(2 * time.Second)

	// Check sub2 exists, check positive status code
	responseSub2Exists := doRequest("/keys/"+sub2ID, "GET", "application/json", nil, sub2Token)
	testStatusCode(t, responseSub2Exists.StatusCode, http.StatusOK)
	time.Sleep(2 * time.Second)

	// Delete head, check status and delay for faster check then request
	responseDelHead := doRequest("/keys/"+headID, "DELETE", "application/json", nil, headToken)
	testStatusCode(t, responseDelHead.StatusCode, http.StatusNoContent)
	time.Sleep(2 * time.Second)

	// Check sub2 removed and check its statuscode (404)
	// TODO: FAILS WITH MEMORY DB (https://github.com/weaviate/weaviate/issues/107)
	// responseSub2Deleted := doRequest("/keys/"+sub2ID, "DELETE", "application/json", nil, apiKeyCmdLine)
	// testStatusCode(t, responseSub2Deleted.StatusCode, http.StatusNotFound)
	// time.Sleep(2 * time.Second)

	// Check head removed and check its statuscode (404)
	responseHeadDeleted := doRequest("/keys/"+headID, "GET", "application/json", nil, apiKeyCmdLine)
	testStatusCode(t, responseHeadDeleted.StatusCode, http.StatusNotFound)
	time.Sleep(2 * time.Second)
}

// weaviate.key.me.delete
func Test__weaviate_key_me_delete_JSON(t *testing.T) {
	// Delete keyID from database
	responseKeyIDDeleted := doRequest("/keys/"+keyID, "DELETE", "application/json", nil, apiKeyCmdLine)
	testStatusCode(t, responseKeyIDDeleted.StatusCode, http.StatusNoContent)
}

/******************
 * LOCATION TESTS
 ******************/

// weaviate.location.create
func Test__weaviate_location_create_JSON(t *testing.T) {
	// Create create request
	jsonStr := bytes.NewBuffer([]byte(`{"address_components":[{"long_name":"TEST","short_name":"string","types":["UNDEFINED"]}],"formatted_address":"string","geometry":{"location":{},"location_type":"string","viewport":{"northeast":{},"southwest":{}}},"place_id":"string","types":["UNDEFINED"]} `))
	response := doRequest("/locations", "POST", "application/json", jsonStr, apiKeyCmdLine)

	// Check status code of create
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

	// Create get request with non-existing ID
	testNotExistsRequest(t, "/locations", "GET", "application/json", nil, apiKeyCmdLine)
}

// weaviate.location.update
func Test__weaviate_location_update_JSON(t *testing.T) {
	// Create update request
	newValue := "updated_name"
	jsonStr := bytes.NewBuffer([]byte(`{"address_components":[{"long_name":"` + newValue + `","short_name":"string","types":["UNDEFINED"]}],"formatted_address":"string","geometry":{"location":{},"location_type":"string","viewport":{"northeast":{},"southwest":{}}},"place_id":"","types":["UNDEFINED"]} `))
	response := doRequest("/locations/"+locationID, "PUT", "application/json", jsonStr, apiKeyCmdLine)

	body := getResponseBody(response)

	respObject := &models.LocationGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID is same
	testID(t, string(respObject.ID), locationID)

	// Check name after update
	testValues(t, newValue, respObject.AddressComponents[0].LongName)

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
	testValues(t, newValue, respObject.AddressComponents[0].LongName)

	// Check put on non-existing ID
	testNotExistsRequest(t, "/locations", "PUT", "application/json", getEmptyJSON(), apiKeyCmdLine)
}

// weaviate.location.patch
func Test__weaviate_location_patch_JSON(t *testing.T) {
	// Create patch request
	newValue := "patched_name"

	jsonStr := bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/address_components/0/long_name", "value": "` + newValue + `"}]`))
	response := doRequest("/locations/"+locationID, "PATCH", "application/json", jsonStr, apiKeyCmdLine)

	body := getResponseBody(response)

	respObject := &models.LocationGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID is the same
	testID(t, string(respObject.ID), locationID)

	// Check name after patch
	testValues(t, newValue, respObject.AddressComponents[0].LongName)

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
	testValues(t, newValue, respObject.AddressComponents[0].LongName)

	// Check patch with incorrect contents
	jsonStrError := bytes.NewBuffer([]byte(`{ "op": "replace", "path": "/address_components/long_name", "value": "` + newValue + `"}`))
	responseError := doRequest("/locations/"+locationID, "PATCH", "application/json", jsonStrError, apiKeyCmdLine)
	testStatusCode(t, responseError.StatusCode, http.StatusBadRequest)

	// Check patch on non-existing ID
	testNotExistsRequest(t, "/locations", "PATCH", "application/json", getEmptyPatchJSON(), apiKeyCmdLine)
}

// weaviate.location.list.offset
func Test__weaviate_location_list_offset_JSON(t *testing.T) {
	// Init variables
	var ids [10]string
	ids[9] = locationID

	// Fill database with locations and array with ID's
	for i := 1; i < 10; i++ {
		// Handle request
		jsonStr := bytes.NewBuffer([]byte(`{"address_components":[{"long_name":"TEST","short_name":"string","types":["UNDEFINED"]}],"formatted_address":"string","geometry":{"location":{},"location_type":"string","viewport":{"northeast":{},"southwest":{}}},"place_id":"string","types":["UNDEFINED"]} `))
		response := doRequest("/locations", "POST", "application/json", jsonStr, apiKeyCmdLine)
		body := getResponseBody(response)
		respObject := &models.LocationGetResponse{}
		json.Unmarshal(body, respObject)

		// Fill array and time out for unlucky sorting issues
		ids[9-i] = string(respObject.ID)
		time.Sleep(1 * time.Second)
	}

	// Query whole list just created
	listResponse := doRequest("/locations?maxResults=3", "GET", "application/json", nil, apiKeyCmdLine)
	listResponseObject := &models.LocationsListResponse{}
	json.Unmarshal(getResponseBody(listResponse), listResponseObject)

	// Test total results
	if 10 != listResponseObject.TotalResults {
		t.Errorf("Expected total results '%d'. Got '%d'.\n", 10, listResponseObject.TotalResults)
	}

	// Test amount in current response
	if 3 != len(listResponseObject.Locations) {
		t.Errorf("Expected page results '%d'. Got '%d'.\n", 3, len(listResponseObject.Locations))
	}

	// Test ID in the middle
	if ids[1] != string(listResponseObject.Locations[1].ID) {
		t.Errorf("Expected ID '%s'. Got '%s'.\n", ids[1], string(listResponseObject.Locations[1].ID))
	}

	// Query whole list just created
	listResponse2 := doRequest("/locations?maxResults=5&page=2", "GET", "application/json", nil, apiKeyCmdLine)
	listResponseObject2 := &models.LocationsListResponse{}
	json.Unmarshal(getResponseBody(listResponse2), listResponseObject2)

	// Test total results
	if 10 != listResponseObject2.TotalResults {
		t.Errorf("Expected total results '%d'. Got '%d'.\n", 10, listResponseObject2.TotalResults)
	}

	// Test amount in current response
	if 5 != len(listResponseObject2.Locations) {
		t.Errorf("Expected page results '%d'. Got '%d'.\n", 5, len(listResponseObject2.Locations))
	}

	// Test ID in the middle
	if ids[7] != string(listResponseObject2.Locations[2].ID) {
		t.Errorf("Expected ID '%s'. Got '%s'.\n", ids[7], string(listResponseObject2.Locations[2].ID))
	}
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
	testID(t, string(respObject.Locations[9].ID), locationID)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#locationsListResponse")
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

	// Check status already deleted
	testStatusCode(t, responseAlreadyDeleted.StatusCode, http.StatusNotFound)

	// Create get request with non-existing ID
	testNotExistsRequest(t, "/locations", "DELETE", "application/json", nil, apiKeyCmdLine)
}

// weaviate.location.list.delete
func Test__weaviate_location_list_delete_JSON(t *testing.T) {
	// Create list request
	response := doRequest("/locations", "GET", "application/json", nil, apiKeyCmdLine)
	body := getResponseBody(response)

	// Check most recent
	respObject := &models.LocationsListResponse{}
	json.Unmarshal(body, respObject)

	// Check most recent
	// Test total results
	if 9 != respObject.TotalResults {
		t.Errorf("Expected total results '%d'. Got '%d'.\n", 9, respObject.TotalResults)
	}
}

/******************
 * THING TEMPLATE TESTS
 ******************/

// weaviate.thing_template.create
func Test__weaviate_thing_template_create_JSON(t *testing.T) {
	// Create create request
	jsonStr := bytes.NewBuffer([]byte(`{
		"commandsId": "` + fakeID + `",
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
		"commandsId": "` + fakeID + `",
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

	// Test is faster than adding to DB.
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
	testNotExistsRequest(t, "/thingTemplates", "PUT", "application/json", getEmptyJSON(), apiKeyCmdLine)
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

	// Test response obj
	respObjectGet := &models.ThingTemplateGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check name after patch and get
	testValues(t, newValue, respObject.ThingModelTemplate.ModelName)

	// Check patch with incorrect contents
	jsonStrError := bytes.NewBuffer([]byte(`{ "op": "replace", "path": "/address_components/long_name", "value": "` + newValue + `"}`))
	responseError := doRequest("/thingTemplates/"+thingTemplateID, "PATCH", "application/json", jsonStrError, apiKeyCmdLine)
	testStatusCode(t, responseError.StatusCode, http.StatusBadRequest)

	// Check patch on non-existing ID
	testNotExistsRequest(t, "/thingTemplates", "PATCH", "application/json", getEmptyPatchJSON(), apiKeyCmdLine)
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

	// Check status code already deleted
	testStatusCode(t, responseAlreadyDeleted.StatusCode, http.StatusNotFound)

	// Create get request with non-existing ID
	testNotExistsRequest(t, "/thingTemplates", "DELETE", "application/json", nil, apiKeyCmdLine)
}

/******************
 * COMMAND TESTS
 ******************/

// weaviate.command.create
func Test__weaviate_command_create_JSON(t *testing.T) {
	// Create create request
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

	// Check status code of create
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

// weaviate.command.update
func Test__weaviate_command_update_JSON(t *testing.T) {
	// Create update request
	newValue := "updated_name"
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
			"name": "` + newValue + `",
			"parameters": {},
			"progress": "aborted",
			"results": {},
			"userAction": "string"
		}
	`))
	response := doRequest("/commands/"+commandID, "PUT", "application/json", jsonStr, apiKeyCmdLine)

	body := getResponseBody(response)

	respObject := &models.CommandGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID is same
	testID(t, string(respObject.ID), commandID)

	// Check name after update
	testValues(t, newValue, respObject.Name)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#commandGetResponse")

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Check if update is also applied on object when using a new GET request on same object
	responseGet := doRequest("/commands/"+commandID, "GET", "application/json", nil, apiKeyCmdLine)

	bodyGet := getResponseBody(responseGet)

	// Test response obj
	respObjectGet := &models.CommandGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check name after update and get
	testValues(t, newValue, respObject.Name)

	// Check put on non-existing ID
	testNotExistsRequest(t, "/commands", "PUT", "application/json", getEmptyJSON(), apiKeyCmdLine)
}

// weaviate.command.patch
func Test__weaviate_command_patch_JSON(t *testing.T) {
	// Create patch request
	newValue := "patched_name"

	jsonStr := bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/name", "value": "` + newValue + `"}]`))
	response := doRequest("/commands/"+commandID, "PATCH", "application/json", jsonStr, apiKeyCmdLine)

	body := getResponseBody(response)

	respObject := &models.CommandGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID is the same
	testID(t, string(respObject.ID), commandID)

	// Check name after patch
	testValues(t, newValue, respObject.Name)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#commandGetResponse")

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Check if patch is also applied on object when using a new GET request on same object
	responseGet := doRequest("/commands/"+commandID, "GET", "application/json", nil, apiKeyCmdLine)

	bodyGet := getResponseBody(responseGet)

	// Test response obj
	respObjectGet := &models.CommandGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check name after patch and get
	testValues(t, newValue, respObject.Name)

	// Check patch with incorrect contents
	jsonStrError := bytes.NewBuffer([]byte(`{ "op": "replace", "path": "/name", "value": "` + newValue + `"}`))
	responseError := doRequest("/commands/"+commandID, "PATCH", "application/json", jsonStrError, apiKeyCmdLine)
	testStatusCode(t, responseError.StatusCode, http.StatusBadRequest)

	// Check patch on non-existing ID
	testNotExistsRequest(t, "/commands", "PATCH", "application/json", getEmptyPatchJSON(), apiKeyCmdLine)
}

// weaviate.command.delete
func Test__weaviate_command_delete_JSON(t *testing.T) {
	// Create delete request
	response := doRequest("/commands/"+commandID, "DELETE", "application/json", nil, apiKeyCmdLine)

	// Check status code get request
	testStatusCode(t, response.StatusCode, http.StatusNoContent)

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Create delete request
	responseAlreadyDeleted := doRequest("/commands/"+commandID, "DELETE", "application/json", nil, apiKeyCmdLine)

	// Check status code already deleted
	testStatusCode(t, responseAlreadyDeleted.StatusCode, http.StatusNotFound)

	// Create get request with non-existing ID
	testNotExistsRequest(t, "/commands", "DELETE", "application/json", nil, apiKeyCmdLine)
}

/******************
 * GROUP TESTS
 ******************/

// weaviate.group.create
func Test__weaviate_group_create_JSON(t *testing.T) {
	// Create create request
	jsonStr := bytes.NewBuffer([]byte(`
		{
			"name": "Group 1"
		}
	`))
	response := doRequest("/groups", "POST", "application/json", jsonStr, apiKeyCmdLine)

	// Check status code of create
	testStatusCode(t, response.StatusCode, http.StatusAccepted)

	body := getResponseBody(response)

	respObject := &models.GroupGetResponse{}
	json.Unmarshal(body, respObject)

	// Check whether generated UUID is added
	groupID = string(respObject.ID)
	testIDFormat(t, groupID)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#groupGetResponse")

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)
}

// weaviate.group.list
func Test__weaviate_group_list_JSON(t *testing.T) {
	// Create list request
	response := doRequest("/groups", "GET", "application/json", nil, apiKeyCmdLine)

	// Check status code of list
	testStatusCode(t, response.StatusCode, http.StatusOK)

	body := getResponseBody(response)

	respObject := &models.GroupsListResponse{}
	json.Unmarshal(body, respObject)

	// Check most recent
	testID(t, string(respObject.Groups[0].ID), groupID)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#groupsListResponse")
}

// weaviate.group.get
func Test__weaviate_group_get_JSON(t *testing.T) {
	// Create get request
	response := doRequest("/groups/"+groupID, "GET", "application/json", nil, apiKeyCmdLine)

	// Check status code get request
	testStatusCode(t, response.StatusCode, http.StatusOK)

	body := getResponseBody(response)

	respObject := &models.GroupGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID of object
	testID(t, string(respObject.ID), groupID)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#groupGetResponse")

	// Create get request with non-existing ID
	testNotExistsRequest(t, "/groups", "GET", "application/json", nil, apiKeyCmdLine)
}

// weaviate.group.update
func Test__weaviate_group_update_JSON(t *testing.T) {
	// Create update request
	newValue := "updated_group_name"
	jsonStr := bytes.NewBuffer([]byte(`
		{
			"name": "` + newValue + `"
		}
	`))
	response := doRequest("/groups/"+groupID, "PUT", "application/json", jsonStr, apiKeyCmdLine)

	body := getResponseBody(response)

	respObject := &models.GroupGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID is same
	testID(t, string(respObject.ID), groupID)

	// Check name after update
	testValues(t, newValue, respObject.Name)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#groupGetResponse")

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Check if update is also applied on object when using a new GET request on same object
	responseGet := doRequest("/groups/"+groupID, "GET", "application/json", nil, apiKeyCmdLine)

	bodyGet := getResponseBody(responseGet)

	// Test response obj
	respObjectGet := &models.GroupGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check name after update and get
	testValues(t, newValue, respObject.Name)

	// Check put on non-existing ID
	testNotExistsRequest(t, "/groups", "PUT", "application/json", getEmptyJSON(), apiKeyCmdLine)
}

// weaviate.group.patch
func Test__weaviate_group_patch_JSON(t *testing.T) {
	// Create patch request
	newValue := "patched_group_name"

	jsonStr := bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/name", "value": "` + newValue + `"}]`))
	response := doRequest("/groups/"+groupID, "PATCH", "application/json", jsonStr, apiKeyCmdLine)

	body := getResponseBody(response)

	respObject := &models.GroupGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID is the same
	testID(t, string(respObject.ID), groupID)

	// Check name after patch
	testValues(t, newValue, respObject.Name)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#groupGetResponse")

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Check if patch is also applied on object when using a new GET request on same object
	responseGet := doRequest("/groups/"+groupID, "GET", "application/json", nil, apiKeyCmdLine)

	bodyGet := getResponseBody(responseGet)

	// Test response obj
	respObjectGet := &models.GroupGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check name after patch and get
	testValues(t, newValue, respObject.Name)

	// Check patch with incorrect contents
	jsonStrError := bytes.NewBuffer([]byte(`{ "op": "replace", "path": "/name", "value": "` + newValue + `"}`))
	responseError := doRequest("/groups/"+groupID, "PATCH", "application/json", jsonStrError, apiKeyCmdLine)
	testStatusCode(t, responseError.StatusCode, http.StatusBadRequest)

	// Check patch on non-existing ID
	testNotExistsRequest(t, "/groups", "PATCH", "application/json", getEmptyPatchJSON(), apiKeyCmdLine)
}

// weaviate.group.delete
func Test__weaviate_group_delete_JSON(t *testing.T) {
	// Create delete request
	response := doRequest("/groups/"+groupID, "DELETE", "application/json", nil, apiKeyCmdLine)

	// Check status code get request
	testStatusCode(t, response.StatusCode, http.StatusNoContent)

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Create delete request
	responseAlreadyDeleted := doRequest("/groups/"+groupID, "DELETE", "application/json", nil, apiKeyCmdLine)

	// Check status code already deleted
	testStatusCode(t, responseAlreadyDeleted.StatusCode, http.StatusNotFound)

	// Create get request with non-existing ID
	testNotExistsRequest(t, "/groups", "DELETE", "application/json", nil, apiKeyCmdLine)
}

/******************
 * THING TESTS
 ******************/

// weaviate.thing.create
func Test__weaviate_thing_create_JSON(t *testing.T) {
	// Create create request
	jsonStr := bytes.NewBuffer([]byte(`{
		"commandsId": "string",
		"description": "string",
		"groups": "string",
		"locationId": "` + locationID + `",
		"name": "string",
		"owner": "string",
		"serialNumber": "string",
		"tags": [
			null
		],
		"thingTemplateId": "` + thingTemplateID + `"
	}`))
	response := doRequest("/things", "POST", "application/json", jsonStr, apiKeyCmdLine)

	// Check status code of create
	if response.StatusCode != http.StatusAccepted {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusAccepted, response.StatusCode)
	}

	body := getResponseBody(response)

	respObject := &models.ThingGetResponse{}
	json.Unmarshal(body, respObject)

	// Check whether generated UUID is added
	thingID = string(respObject.ID)

	if !strfmt.IsUUID(thingID) {
		t.Errorf("ID is not what expected. Got %s.\n", thingID)
	}

	// Check kind
	kind := "weaviate#thingGetResponse"
	respKind := string(*respObject.Kind)
	if kind != respKind {
		t.Errorf("Expected kind '%s'. Got '%s'.\n", kind, respKind)
	}

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)
}

// weaviate.thing.list
func Test__weaviate_thing_list_JSON(t *testing.T) {
	// Create list request
	response := doRequest("/things", "GET", "application/json", nil, apiKeyCmdLine)

	// Check status code of list
	testStatusCode(t, response.StatusCode, http.StatusOK)

	body := getResponseBody(response)

	respObject := &models.ThingsListResponse{}
	json.Unmarshal(body, respObject)

	// Check most recent
	testID(t, string(respObject.Things[0].ID), thingID)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#thingsListResponse")
}

// weaviate.thing.get
func Test__weaviate_thing_get_JSON(t *testing.T) {
	// Create get request
	response := doRequest("/things/"+thingID, "GET", "application/json", nil, apiKeyCmdLine)

	// Check status code get request
	testStatusCode(t, response.StatusCode, http.StatusOK)

	body := getResponseBody(response)

	respObject := &models.ThingGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID of object
	testID(t, string(respObject.ID), thingID)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#thingGetResponse")

	//dCreate get request with non-existing thing
	testNotExistsRequest(t, "/things", "GET", "application/json", nil, apiKeyCmdLine)
}

// weaviate.thing.update
func Test__weaviate_thing_update_JSON(t *testing.T) {
	// Create update request
	newValue := "updated_description"
	jsonStr := bytes.NewBuffer([]byte(`{
			"commandsIds": [
				"string"
			],
			"thingModelTemplate": {
				"modelName": "string",
				"oemNumber": "string",
				"oemAdditions": {},
				"oemName": "string",
				"oemIcon": "string",
				"oemContact": "string"
			},
			"name": "string",
			"connectionStatus": "string",
			"creationTimeMs": 0,
			"description": "` + newValue + `",
			"groups": [
				"string"
			],
			"lastSeenTimeMs": 0,
			"lastUpdateTimeMs": 0,
			"lastUseTimeMs": 0,
			"locationId": "` + locationID + `",
			"thingTemplateId": "` + thingTemplateID + `",
			"owner": "string",
			"serialNumber": "string",
			"tags": [
				"string"
			]
		}`))
	response := doRequest("/things/"+thingID, "PUT", "application/json", jsonStr, apiKeyCmdLine)

	body := getResponseBody(response)

	respObject := &models.ThingGetResponse{}
	json.Unmarshal(body, respObject)

	// Check thing ID is same
	testID(t, string(respObject.ID), thingID)

	// Check name after update
	testValues(t, newValue, respObject.Description)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#thingGetResponse")

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Check if update is also applied on object when using a new GET request on same object
	responseGet := doRequest("/things/"+thingID, "GET", "application/json", nil, apiKeyCmdLine)

	bodyGet := getResponseBody(responseGet)

	// Test response obj
	respObjectGet := &models.ThingGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check name after update and get
	testValues(t, newValue, respObject.Description)

	// Check put on non-existing ID
	testNotExistsRequest(t, "/things", "PUT", "application/json", getEmptyJSON(), apiKeyCmdLine)
}

// weaviate.thing.patch
func Test__weaviate_thing_patch_JSON(t *testing.T) {
	// Create patch request
	newValue := "patched_description"

	jsonStr := bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/description", "value": "` + newValue + `"}]`))
	response := doRequest("/things/"+thingID, "PATCH", "application/json", jsonStr, apiKeyCmdLine)

	body := getResponseBody(response)

	respObject := &models.ThingGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID is the same
	testID(t, string(respObject.ID), thingID)

	// Check name after patch
	testValues(t, newValue, respObject.Description)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#thingGetResponse")

	//dTest is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Check if patch is also applied on object when using a new GET request on same object
	responseGet := doRequest("/things/"+thingID, "GET", "application/json", nil, apiKeyCmdLine)

	bodyGet := getResponseBody(responseGet)

	// Test response obj
	respObjectGet := &models.ThingGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check name after patch and get
	testValues(t, newValue, respObject.Description)

	// Check patch with incorrect contents
	jsonStrError := bytes.NewBuffer([]byte(`{ "op": "replace", "path": "/address_components/long_name", "value": "` + newValue + `"}`))
	responseError := doRequest("/things/"+thingID, "PATCH", "application/json", jsonStrError, apiKeyCmdLine)
	testStatusCode(t, responseError.StatusCode, http.StatusBadRequest)

	// Check patch on non-existing ID
	testNotExistsRequest(t, "/things", "PATCH", "application/json", getEmptyPatchJSON(), apiKeyCmdLine)
}

// weaviate.thing.delete
func Test__weaviate_thing_delete_JSON(t *testing.T) {
	// Create delete request
	response := doRequest("/things/"+thingID, "DELETE", "application/json", nil, apiKeyCmdLine)

	// Check status code get request
	testStatusCode(t, response.StatusCode, http.StatusNoContent)

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Create delete request
	responseAlreadyDeleted := doRequest("/things/"+thingID, "DELETE", "application/json", nil, apiKeyCmdLine)

	// Check status code already deleted
	testStatusCode(t, responseAlreadyDeleted.StatusCode, http.StatusNotFound)

	// Create get request with non-existing ID
	testNotExistsRequest(t, "/things", "DELETE", "application/json", nil, apiKeyCmdLine)
}

/******************
 * EVENTS TESTS
 ******************/

// weaviate.events.things.create
func Test__weaviate_events_things_create_JSON(t *testing.T) {
	// Create create request
	jsonStr := bytes.NewBuffer([]byte(`
	{
		"commandId": "` + commandID + `",
		"command": {
			"commandParameters": {}
		},
		"timeMs": 0,
		"userkey": "` + rootID + `"
	}
	`))
	response := doRequest("/things/"+thingID+"/events", "POST", "application/json", jsonStr, apiKeyCmdLine)

	// Check status code of create
	testStatusCode(t, response.StatusCode, http.StatusAccepted)

	body := getResponseBody(response)

	respObject := &models.EventGetResponse{}
	json.Unmarshal(body, respObject)

	// Check whether generated UUID is added
	eventID = string(respObject.ID)
	testIDFormat(t, eventID)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#eventGetResponse")

	// Add another event for another thing
	jsonStr2 := bytes.NewBuffer([]byte(`
	{
		"commandId": "` + commandID + `",
		"command": {
			"commandParameters": {}
		},
		"timeMs": 0,
		"userkey": "` + rootID + `"
	}
	`))
	responseSecond := doRequest("/things/"+fakeID+"/events", "POST", "application/json", jsonStr2, apiKeyCmdLine)
	testStatusCode(t, responseSecond.StatusCode, http.StatusAccepted)

	// Test is faster than adding to DB.
	time.Sleep(2 * time.Second)
}

// weaviate.event.things.list
func Test__weaviate_event_things_list_JSON(t *testing.T) {
	// Create list request
	response := doRequest("/things/"+thingID+"/events", "GET", "application/json", nil, apiKeyCmdLine)

	// Check status code of list
	testStatusCode(t, response.StatusCode, http.StatusOK)

	body := getResponseBody(response)

	respObject := &models.EventsListResponse{}
	json.Unmarshal(body, respObject)

	// Check most recent
	testID(t, string(respObject.Events[0].ID), eventID)

	// Check there is only one event
	testIntegerValues(t, 1, len(respObject.Events))

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#eventsListResponse")
}

// weaviate.event.get
func Test__weaviate_event_get_JSON(t *testing.T) {
	// Create get request
	response := doRequest("/events/"+eventID, "GET", "application/json", nil, apiKeyCmdLine)

	// Check status code get request
	testStatusCode(t, response.StatusCode, http.StatusOK)

	body := getResponseBody(response)

	respObject := &models.EventGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID of object
	testID(t, string(respObject.ID), eventID)

	// Check kind
	testKind(t, string(*respObject.Kind), "weaviate#eventGetResponse")

	// Create get request with non-existing ID
	testNotExistsRequest(t, "/events", "GET", "application/json", nil, apiKeyCmdLine)
}
