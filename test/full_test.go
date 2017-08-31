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
	"fmt"
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
	"github.com/weaviate/weaviate/connectors/utils"
	"github.com/weaviate/weaviate/models"
	// "sort"
	// "strconv"
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

	req, _ := http.NewRequest(method, "http://"+serverHost+":"+serverPort+"/weaviate/v1"+endpoint, body)
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
		t.Errorf("%s:%d: Expected value is '%s'. Got '%s'\n", file, line, expected, got)
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

// Set all re-used vars
var apiKeyCmdLine string
var serverPort string
var serverHost string

var actionID string
var expiredKey string
var expiredID string
var fakeID string
var headToken string
var headID string
var newAPIToken string
var newAPIKeyID string
var newSubAPIToken string
var newSubAPIKeyID string
var thingID string
var rootID string
var unixTimeExpire int64

func init() {
	flag.StringVar(&apiKeyCmdLine, "api-key", "", "API-KEY as used as haeder in the tests.")
	flag.StringVar(&serverPort, "server-port", "", "Port number on which the server is running.")
	flag.StringVar(&serverHost, "server-host", "", "Host-name on which the server is running.")
	flag.Parse()

	fakeID = "11111111-1111-1111-1111-111111111111"
}

// /******************
//  * KEY TESTS
//  ******************/

// // weaviate.key.create
// func Test__weaviate_key_create_JSON(t *testing.T) {
// 	// Create create request
// 	jsonStr := bytes.NewBuffer([]byte(`{
// 		"delete": true,
// 		"email": "string",
// 		"ipOrigin": ["127.0.0.*", "*"],
// 		"keyExpiresUnix": -1,
// 		"read": false,
// 		"write": false,
// 		"execute": true
// 	}`))
// 	response := doRequest("/keys", "POST", "application/json", jsonStr, apiKeyCmdLine)

// 	// Check status code of create
// 	testStatusCode(t, response.StatusCode, http.StatusAccepted)

// 	body := getResponseBody(response)

// 	respObject := &models.KeyTokenGetResponse{}
// 	json.Unmarshal(body, respObject)

// 	// Check kind
// 	testKind(t, string(*respObject.Kind), "weaviate#keyTokenGetResponse")

// 	// Test Rights
// 	testBooleanValues(t, true, respObject.Delete)
// 	testBooleanValues(t, true, respObject.Execute)
// 	testBooleanValues(t, false, respObject.Read)
// 	testBooleanValues(t, false, respObject.Write)

// 	// Test given Token
// 	newAPIToken = string(respObject.Key)
// 	testIDFormat(t, newAPIToken)

// 	// Check whether generated UUID is added
// 	newAPIKeyID = string(respObject.KeyID)
// 	testIDFormat(t, newAPIKeyID)

// 	// Test is faster than adding to DB.
// 	time.Sleep(1 * time.Second)

// 	// Create request
// 	jsonStrNewKey := bytes.NewBuffer([]byte(`{
// 		"delete": false,
// 		"email": "string",
// 		"ipOrigin": ["127.0.0.*", "*"],
// 		"keyExpiresUnix": -1,
// 		"read": true,
// 		"write": true,
// 		"execute": false
// 	}`))
// 	responseNewToken := doRequest("/keys", "POST", "application/json", jsonStrNewKey, newAPIToken)

// 	// Test second statuscode
// 	testStatusCode(t, responseNewToken.StatusCode, http.StatusAccepted)

// 	// Process response
// 	bodyNewToken := getResponseBody(responseNewToken)
// 	respObjectNewToken := &models.KeyTokenGetResponse{}
// 	json.Unmarshal(bodyNewToken, respObjectNewToken)

// 	// Test key ID parent is correct
// 	testID(t, respObjectNewToken.Parent, newAPIKeyID)

// 	// Test given Token
// 	newSubAPIToken = string(respObjectNewToken.Key)
// 	testIDFormat(t, newAPIToken)

// 	// Test given ID
// 	newSubAPIKeyID = string(respObjectNewToken.KeyID)
// 	testIDFormat(t, newAPIKeyID)

// 	// Test expiration set
// 	testIntegerValues(t, -1, int(respObjectNewToken.KeyExpiresUnix))

// 	// Test Rights
// 	testBooleanValues(t, false, respObjectNewToken.Delete)
// 	testBooleanValues(t, false, respObjectNewToken.Execute)
// 	testBooleanValues(t, true, respObjectNewToken.Read)
// 	testBooleanValues(t, true, respObjectNewToken.Write)

// 	// Test is faster than adding to DB.
// 	time.Sleep(1 * time.Second)

// 	// Create create request with a key that will expire soon
// 	unixTimeExpire = connector_utils.NowUnix()
// 	jsonStrNewKeySoonExpire := bytes.NewBuffer([]byte(`{
// 		"delete": false,
// 		"email": "expiredkey",
// 		"ipOrigin": ["127.0.0.*", "*"],
// 		"keyExpiresUnix": ` + strconv.FormatInt(unixTimeExpire+2000, 10) + `,
// 		"read": true,
// 		"write": true,
// 		"execute": false
// 	}`))
// 	responseNewTokenSoonExpire := doRequest("/keys", "POST", "application/json", jsonStrNewKeySoonExpire, apiKeyCmdLine)

// 	// Test second statuscode
// 	testStatusCode(t, responseNewTokenSoonExpire.StatusCode, http.StatusAccepted)

// 	bodyExpireSoon := getResponseBody(responseNewTokenSoonExpire)
// 	respObjectExpireSoon := &models.KeyTokenGetResponse{}
// 	json.Unmarshal(bodyExpireSoon, respObjectExpireSoon)
// 	expiredKey = respObjectExpireSoon.Key
// 	expiredID = string(respObjectExpireSoon.KeyID)

// 	time.Sleep(1 * time.Second)

// 	// Create request that is invalid because time is lower then parent time
// 	jsonStrNewKeyInvalid := bytes.NewBuffer([]byte(`{
// 		"delete": false,
// 		"email": "string",
// 		"ipOrigin": ["127.0.0.*", "*"],
// 		"keyExpiresUnix": ` + strconv.FormatInt(unixTimeExpire+3000, 10) + `,
// 		"read": true,
// 		"write": true,
// 		"execute": false
// 	}`))
// 	responseNewTokenInvalid := doRequest("/keys", "POST", "application/json", jsonStrNewKeyInvalid, expiredKey)

// 	testStatusCode(t, responseNewTokenInvalid.StatusCode, http.StatusUnprocessableEntity)
// }

// // weaviate.key.me.get
// func Test__weaviate_key_me_get_JSON(t *testing.T) {
// 	// Create get request
// 	response := doRequest("/keys/me", "GET", "application/json", nil, newAPIToken)

// 	// Check status code get request
// 	testStatusCode(t, response.StatusCode, http.StatusOK)

// 	body := getResponseBody(response)

// 	respObject := &models.KeyTokenGetResponse{}
// 	json.Unmarshal(body, respObject)

// 	// Add general User ID
// 	rootID = string(respObject.Parent)

// 	// Check ID of object
// 	testID(t, string(respObject.KeyID), newAPIKeyID)

// 	// Check kind
// 	testKind(t, string(*respObject.Kind), "weaviate#keyTokenGetResponse")

// 	// Wait until key is expired
// 	time.Sleep(3 * time.Second)

// 	// Create get request with key that is expired
// 	responseExpired := doRequest("/keys/me", "GET", "application/json", nil, expiredKey)

// 	// Check status code get request
// 	testStatusCode(t, responseExpired.StatusCode, http.StatusUnauthorized)

// }

// // weaviate.key.get
// func Test__weaviate_key_get_JSON(t *testing.T) {
// 	// Create get request
// 	response := doRequest("/keys/"+newAPIKeyID, "GET", "application/json", nil, apiKeyCmdLine)

// 	// Check status code get request
// 	testStatusCode(t, response.StatusCode, http.StatusOK)

// 	body := getResponseBody(response)

// 	respObject := &models.KeyGetResponse{}
// 	json.Unmarshal(body, respObject)

// 	// Check ID of object
// 	testID(t, string(respObject.KeyID), newAPIKeyID)

// 	// Check kind
// 	testKind(t, string(*respObject.Kind), "weaviate#keyGetResponse")

// 	// Create get request
// 	responseForbidden := doRequest("/keys/"+rootID, "GET", "application/json", nil, newAPIToken)

// 	// Check status code forbidden request
// 	testStatusCode(t, responseForbidden.StatusCode, http.StatusForbidden)

// 	// Create get request with non-existing ID
// 	testNotExistsRequest(t, "/keys", "GET", "application/json", nil, apiKeyCmdLine)
// }

// // weaviate.key.children.get
// func Test__weaviate_key_children_get_JSON(t *testing.T) {
// 	// HEAD: Create create request tree-head and process request
// 	jsonStrKeyHead := bytes.NewBuffer([]byte(`{
// 		"delete": true,
// 		"email": "string",
// 		"ipOrigin": ["127.0.0.*", "*"],
// 		"keyExpiresUnix": -1,
// 		"read": true,
// 		"write": true,
// 		"execute": true
// 	}`))
// 	responseHead := doRequest("/keys", "POST", "application/json", jsonStrKeyHead, newAPIToken)
// 	testStatusCode(t, responseHead.StatusCode, http.StatusAccepted)
// 	bodyHead := getResponseBody(responseHead)
// 	respObjectHead := &models.KeyTokenGetResponse{}
// 	json.Unmarshal(bodyHead, respObjectHead)

// 	time.Sleep(1 * time.Second)

// 	// Set reusable keys
// 	headToken = respObjectHead.Key
// 	headID = string(respObjectHead.KeyID)

// 	// Create get request
// 	response := doRequest("/keys/"+newAPIKeyID+"/children", "GET", "application/json", nil, apiKeyCmdLine)

// 	// Check status code get request
// 	testStatusCode(t, response.StatusCode, http.StatusOK)

// 	body := getResponseBody(response)

// 	respObject := &models.KeyChildrenGetResponse{}
// 	json.Unmarshal(body, respObject)

// 	// Check the number of children corresponds the added number
// 	if 2 != len(respObject.Children) {
// 		t.Errorf("Expected number of children '%d'. Got '%d'.\n", 2, len(respObject.Children))
// 	} else {
// 		// Check IDs of objects are correct by adding them to an array and sorting
// 		responseChildren := []string{
// 			string(respObject.Children[0]),
// 			string(respObject.Children[1]),
// 		}

// 		checkIDs := []string{
// 			headID,
// 			newSubAPIKeyID,
// 		}

// 		sort.Strings(responseChildren)
// 		sort.Strings(checkIDs)

// 		testID(t, responseChildren[0], checkIDs[0])
// 		testID(t, responseChildren[1], checkIDs[1])
// 	}

// 	// Create get request
// 	responseForbidden := doRequest("/keys/"+rootID+"/children", "GET", "application/json", nil, newAPIToken)

// 	// Check status code forbidden request
// 	testStatusCode(t, responseForbidden.StatusCode, http.StatusForbidden)

// 	// Create get request with non-existing ID
// 	responseNotFound := doRequest("keys/"+fakeID+"/children", "GET", "application/json", nil, newAPIToken)

// 	// Check response of non-existing ID
// 	testStatusCode(t, responseNotFound.StatusCode, http.StatusNotFound)
// }

// // weaviate.key.me.children.get
// func Test__weaviate_key_me_children_get_JSON(t *testing.T) {
// 	// Create get request
// 	response := doRequest("/keys/me/children", "GET", "application/json", nil, newAPIToken)

// 	// Check status code get request
// 	testStatusCode(t, response.StatusCode, http.StatusOK)

// 	body := getResponseBody(response)

// 	respObject := &models.KeyChildrenGetResponse{}
// 	json.Unmarshal(body, respObject)

// 	// Check the number of children corresponds the added number
// 	if 2 != len(respObject.Children) {
// 		t.Errorf("Expected number of children '%d'. Got '%d'.\n", 2, len(respObject.Children))
// 	} else {
// 		// Check IDs of objects are correct by adding them to an array and sorting
// 		responseChildren := []string{
// 			string(respObject.Children[0]),
// 			string(respObject.Children[1]),
// 		}

// 		checkIDs := []string{
// 			headID,
// 			newSubAPIKeyID,
// 		}

// 		sort.Strings(responseChildren)
// 		sort.Strings(checkIDs)

// 		testID(t, responseChildren[0], checkIDs[0])
// 		testID(t, responseChildren[1], checkIDs[1])
// 	}
// }

// // weaviate.key.delete
// func Test__weaviate_key_delete_JSON(t *testing.T) {
// 	// Sleep, otherwise head-key is not added
// 	time.Sleep(1 * time.Second)

// 	// SUB1: Create create request and process request
// 	jsonStrKeySub1 := bytes.NewBuffer([]byte(`{
// 		"delete": true,
// 		"email": "string",
// 		"ipOrigin": ["127.0.0.*", "*"],
// 		"keyExpiresUnix": -1,
// 		"read": true,
// 		"write": true,
// 		"execute": true
// 	}`))
// 	responseSub1 := doRequest("/keys", "POST", "application/json", jsonStrKeySub1, headToken)
// 	testStatusCode(t, responseSub1.StatusCode, http.StatusAccepted)
// 	bodySub1 := getResponseBody(responseSub1)
// 	respObjectSub1 := &models.KeyTokenGetResponse{}
// 	json.Unmarshal(bodySub1, respObjectSub1)

// 	// Sleep, otherwise head-key is not added
// 	time.Sleep(1 * time.Second)

// 	// Set reusable keys
// 	// sub1Token := respObjectSub1.Key
// 	sub1ID := string(respObjectSub1.KeyID)

// 	// SUB2: Create create request and process request
// 	jsonStrKeySub2 := bytes.NewBuffer([]byte(`{
// 		"delete": true,
// 		"email": "string",
// 		"ipOrigin": ["127.0.0.*", "*"],
// 		"keyExpiresUnix": -1,
// 		"read": true,
// 		"write": true,
// 		"execute": true
// 	}`))
// 	responseSub2 := doRequest("/keys", "POST", "application/json", jsonStrKeySub2, headToken)
// 	testStatusCode(t, responseSub2.StatusCode, http.StatusAccepted)
// 	bodySub2 := getResponseBody(responseSub2)
// 	respObjectSub2 := &models.KeyTokenGetResponse{}
// 	json.Unmarshal(bodySub2, respObjectSub2)

// 	// Sleep, otherwise head-key is not added
// 	time.Sleep(1 * time.Second)

// 	// Set reusable keys
// 	sub2Token := respObjectSub2.Key
// 	sub2ID := string(respObjectSub2.KeyID)

// 	// Delete head with sub2, which is not allowed
// 	responseDelHeadWithSub := doRequest("/keys/"+headID, "DELETE", "application/json", nil, sub2Token)
// 	testStatusCode(t, responseDelHeadWithSub.StatusCode, http.StatusForbidden)
// 	time.Sleep(2 * time.Second)

// 	// Delete sub1, check status and delay for faster check then request
// 	responseDelSub1 := doRequest("/keys/"+sub1ID, "DELETE", "application/json", nil, apiKeyCmdLine)
// 	testStatusCode(t, responseDelSub1.StatusCode, http.StatusNoContent)
// 	time.Sleep(2 * time.Second)

// 	// Check sub1 removed and check its statuscode (404)
// 	responseSub1Deleted := doRequest("/keys/"+sub1ID, "DELETE", "application/json", nil, apiKeyCmdLine)
// 	testStatusCode(t, responseSub1Deleted.StatusCode, http.StatusNotFound)
// 	time.Sleep(2 * time.Second)

// 	// Check sub2 exists, check positive status code
// 	responseSub2Exists := doRequest("/keys/"+sub2ID, "GET", "application/json", nil, sub2Token)
// 	testStatusCode(t, responseSub2Exists.StatusCode, http.StatusOK)
// 	time.Sleep(2 * time.Second)

// 	// Delete head, check status and delay for faster check then request
// 	responseDelHead := doRequest("/keys/"+headID, "DELETE", "application/json", nil, headToken)
// 	testStatusCode(t, responseDelHead.StatusCode, http.StatusNoContent)
// 	time.Sleep(2 * time.Second)

// 	// Check sub2 removed and check its statuscode (404)
// 	responseSub2Deleted := doRequest("/keys/"+sub2ID, "DELETE", "application/json", nil, apiKeyCmdLine)
// 	testStatusCode(t, responseSub2Deleted.StatusCode, http.StatusNotFound)
// 	time.Sleep(2 * time.Second)

// 	// Check head removed and check its statuscode (404)
// 	responseHeadDeleted := doRequest("/keys/"+headID, "GET", "application/json", nil, apiKeyCmdLine)
// 	testStatusCode(t, responseHeadDeleted.StatusCode, http.StatusNotFound)
// 	time.Sleep(2 * time.Second)

// 	// Delete key that is expired
// 	responseExpiredDeleted := doRequest("/keys/"+expiredID, "DELETE", "application/json", nil, apiKeyCmdLine)
// 	testStatusCode(t, responseExpiredDeleted.StatusCode, http.StatusNoContent)
// 	time.Sleep(2 * time.Second)
// }

/******************
 * THING TESTS
 ******************/

// weaviate.thing.create
func Test__weaviate_thing_create_JSON(t *testing.T) {
	// Create create request
	jsonStr := bytes.NewBuffer([]byte(`{
		"@context": "http://schema.org",
		"@class": "Person",
		"schema": {
			"givenName": "Bob",
			"faxNumber": 1337
		}
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
	thingID = string(respObject.ThingID)

	if !strfmt.IsUUID(thingID) {
		t.Errorf("ID is not what expected. Got %s.\n", thingID)
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
	testID(t, string(respObject.Things[0].ThingID), thingID)

	// TODO: Add maxResults and page tests.
	testIntegerValues(t, 1, len(respObject.Things))
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
	testID(t, string(respObject.ThingID), thingID)

	//dCreate get request with non-existing thing
	testNotExistsRequest(t, "/things", "GET", "application/json", nil, apiKeyCmdLine)
}

// weaviate.thing.update
func Test__weaviate_thing_update_JSON(t *testing.T) {
	// Create update request
	newValue := "New Name!"
	jsonStr := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://schema.org",
		"@class": "Person",
		"schema": {
			"givenName": "%s",
			"faxNumber": 1337
		}
	}`, newValue)))
	response := doRequest("/things/"+thingID, "PUT", "application/json", jsonStr, apiKeyCmdLine)

	body := getResponseBody(response)

	respObject := &models.ThingGetResponse{}
	json.Unmarshal(body, respObject)

	// Check status code
	testStatusCode(t, response.StatusCode, http.StatusOK)

	// Check thing ID is same
	testID(t, string(respObject.ThingID), thingID)

	// Check given update time is after now, but not in the future
	now := connector_utils.NowUnix()
	if respObject.LastUpdateTimeUnix > now {
		t.Errorf("LastUpdateTimeUnix is incorrect, it was set in the future.")
	}

	if respObject.LastUpdateTimeUnix < now-2000 {
		t.Errorf("LastUpdateTimeUnix is incorrect, it was set to far back.")
	}

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Check if update is also applied on object when using a new GET request on same object
	responseGet := doRequest("/things/"+thingID, "GET", "application/json", nil, apiKeyCmdLine)

	bodyGet := getResponseBody(responseGet)

	// Test response obj
	respObjectGet := &models.ThingGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)
	testValues(t, newValue, respObject.Schema.(map[string]interface{})["givenName"].(string))

	// Check put on non-existing ID
	testNotExistsRequest(t, "/things", "PUT", "application/json", getEmptyJSON(), apiKeyCmdLine)
}

// weaviate.thing.patch
func Test__weaviate_thing_patch_JSON(t *testing.T) {
	// Create patch request
	newValue := "New name patched!"

	jsonStr := bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/schema/givenName", "value": "` + newValue + `"}]`))
	response := doRequest("/things/"+thingID, "PATCH", "application/json", jsonStr, apiKeyCmdLine)

	body := getResponseBody(response)

	respObject := &models.ThingGetResponse{}
	json.Unmarshal(body, respObject)

	// Check status code
	testStatusCode(t, response.StatusCode, http.StatusOK)

	// Check ID is the same
	testID(t, string(respObject.ThingID), thingID)

	// Check given update time is after now, but not in the future
	now := connector_utils.NowUnix()
	if respObject.LastUpdateTimeUnix > now {
		t.Errorf("LastUpdateTimeUnix is incorrect, it was set in the future.")
	}

	if respObject.LastUpdateTimeUnix < now-2000 {
		t.Errorf("LastUpdateTimeUnix is incorrect, it was set to far back.")
	}

	//dTest is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Check if patch is also applied on object when using a new GET request on same object
	responseGet := doRequest("/things/"+thingID, "GET", "application/json", nil, apiKeyCmdLine)

	bodyGet := getResponseBody(responseGet)

	// Test response obj
	respObjectGet := &models.ThingGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)
	testValues(t, newValue, respObject.Schema.(map[string]interface{})["givenName"].(string))

	// Check patch with incorrect contents
	jsonStrError := bytes.NewBuffer([]byte(`{ "op": "replace", "path": "/address_components/long_name", "value": "` + newValue + `"}`))
	responseError := doRequest("/things/"+thingID, "PATCH", "application/json", jsonStrError, apiKeyCmdLine)
	testStatusCode(t, responseError.StatusCode, http.StatusBadRequest)

	// Check patch on non-existing ID
	testNotExistsRequest(t, "/things", "PATCH", "application/json", getEmptyPatchJSON(), apiKeyCmdLine)
}

// /******************
//  * ACTIONS TESTS
//  ******************/

// weaviate.actions.create
func Test__weaviate_actions_create_JSON(t *testing.T) {
	// Create create request
	// TODO: Add second thing
	jsonStr := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://schema.org",
		"@class": "OnOffAction",
		"schema": {
			"hue": 123,
			"saturation": 32121,
			"on": 3412
		},
		"things": {
			"object": {
				"$cref": "%s",
				"locationUrl": "http://localhost/",
				"type": "Thing"
			},
			"subject": {
				"$cref": "%s",
				"locationUrl": "http://localhost/",
				"type": "Thing"
			}
		}
	}`, thingID, thingID)))
	response := doRequest("/actions", "POST", "application/json", jsonStr, apiKeyCmdLine)

	// Check status code of create
	testStatusCode(t, response.StatusCode, http.StatusAccepted)

	body := getResponseBody(response)

	respObject := &models.ActionGetResponse{}
	json.Unmarshal(body, respObject)

	// Check whether generated UUID is added
	actionID = string(respObject.ActionID)
	testIDFormat(t, actionID)

	// Check thing is set to known ThingID
	testID(t, string(respObject.Things.Object.NrDollarCref), thingID)

	// Check set user key is rootID
	// testID(t, string(respObject.UserKey), rootID) TODO

	// Check given creation time is after now, but not in the future
	now := connector_utils.NowUnix()
	if respObject.CreationTimeUnix > now {
		t.Errorf("CreationTimeUnix is incorrect, it was set in the future.")
	}

	if respObject.CreationTimeUnix < now-2000 {
		t.Errorf("CreationTimeUnix is incorrect, it was set to far back.")
	}

	// Add another action for another thing
	jsonStr2 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://schema.org",
		"@class": "OnOffAction",
		"schema": {
			"hue": 123,
			"saturation": 32121,
			"on": 3412
		},
		"things": {
			"object": {
				"$cref": "%s",
				"locationUrl": "http://localhost/",
				"type": "Thing"
			},
			"subject": {
				"$cref": "%s",
				"locationUrl": "http://localhost/",
				"type": "Thing"
			}
		}
	}`, thingID, thingID))) // TODO add second thing
	responseSecond := doRequest("/actions", "POST", "application/json", jsonStr2, apiKeyCmdLine)
	testStatusCode(t, responseSecond.StatusCode, http.StatusAccepted)

	// Test is faster than adding to DB.
	time.Sleep(2 * time.Second)
}

// weaviate.things.actions.list
func Test__weaviate_things_actions_list_JSON(t *testing.T) {
	// Create list request
	response := doRequest("/things/"+thingID+"/actions", "GET", "application/json", nil, apiKeyCmdLine)

	// Check status code of list
	testStatusCode(t, response.StatusCode, http.StatusOK)

	body := getResponseBody(response)

	respObject := &models.ActionsListResponse{}
	json.Unmarshal(body, respObject)

	// // Check most recent
	// testID(t, string(respObject.Actions[0].ActionID), actionID)

	// // Check there is only one action
	// testIntegerValues(t, 1, len(respObject.Actions))
}

// weaviate.action.get
func Test__weaviate_action_get_JSON(t *testing.T) {
	// Create get request
	response := doRequest("/actions/"+actionID, "GET", "application/json", nil, apiKeyCmdLine)

	// Check status code get request
	testStatusCode(t, response.StatusCode, http.StatusOK)

	body := getResponseBody(response)

	respObject := &models.ActionGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID of object
	testID(t, string(respObject.ActionID), actionID)

	// // Check ID of object
	// testID(t, string(respObject.ThingID), thingID)
	// TODO

	// Create get request with non-existing ID
	testNotExistsRequest(t, "/actions", "GET", "application/json", nil, apiKeyCmdLine)
}

// weaviate.action.patch
func Test__weaviate_action_patch_JSON(t *testing.T) {
	// Create patch request
	newValue := "1337"

	jsonStr := bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/creationTimeUnix", "value": ` + newValue + `}]`))
	response := doRequest("/actions/"+actionID, "PATCH", "application/json", jsonStr, apiKeyCmdLine)

	body := getResponseBody(response)

	respObject := &models.ActionGetResponse{}
	json.Unmarshal(body, respObject)

	// Check status code
	testStatusCode(t, response.StatusCode, http.StatusOK)

	// Check ID is the same
	testID(t, string(respObject.ActionID), actionID)

	// Check given creation time is after now, but not in the future
	now := connector_utils.NowUnix()
	if respObject.LastUpdateTimeUnix > now {
		t.Errorf("LastUpdateTimeUnix is incorrect, it was set in the future.")
	}

	if respObject.LastUpdateTimeUnix < now-2000 {
		t.Errorf("LastUpdateTimeUnix is incorrect, it was set to far back.")
	}

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Check if patch is also applied on object when using a new GET request on same object
	responseGet := doRequest("/actions/"+actionID, "GET", "application/json", nil, apiKeyCmdLine)

	bodyGet := getResponseBody(responseGet)

	// Test response obj
	respObjectGet := &models.ActionGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check patch with incorrect contents
	jsonStrError := bytes.NewBuffer([]byte(`{ "op": "replace", "path": "/xxxx", "value": "` + newValue + `"}`))
	responseError := doRequest("/actions/"+actionID, "PATCH", "application/json", jsonStrError, apiKeyCmdLine)
	testStatusCode(t, responseError.StatusCode, http.StatusBadRequest)

	// Check patch on non-existing ID
	testNotExistsRequest(t, "/actions", "PATCH", "application/json", getEmptyPatchJSON(), apiKeyCmdLine)
}

// // weaviate.key.me.delete
// func Test__weaviate_key_me_delete_JSON(t *testing.T) {
// 	// Delete keyID from database
// 	responseKeyIDDeleted := doRequest("/keys/me", "DELETE", "application/json", nil, newAPIToken)
// 	testStatusCode(t, responseKeyIDDeleted.StatusCode, http.StatusNoContent)
// }

// /******************
//  * REMOVE THING TESTS
//  ******************/

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

// weaviate.action.delete
func Test__weaviate_action_delete_JSON(t *testing.T) {
	// Create delete request
	response := doRequest("/actions/"+actionID, "DELETE", "application/json", nil, apiKeyCmdLine)

	// Check status code get request
	testStatusCode(t, response.StatusCode, http.StatusNoContent)

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Create delete request
	responseAlreadyDeleted := doRequest("/actions/"+actionID, "DELETE", "application/json", nil, apiKeyCmdLine)

	// Check status code already deleted
	testStatusCode(t, responseAlreadyDeleted.StatusCode, http.StatusNotFound)

	// Create get request with non-existing ID
	testNotExistsRequest(t, "/actions", "DELETE", "application/json", nil, apiKeyCmdLine)
}
