/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @CreativeSofwFdn / yourfriends@weaviate.com
 */
package test

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"io"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/creativesoftwarefdn/weaviate/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/models"
)

/*
 * Request function
 */
func doRequest(endpoint string, method string, accept string, body io.Reader, apiKey string, apiToken string) *http.Response {
	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}
	client := &http.Client{Transport: tr}

	req, _ := http.NewRequest(method, getWeaviateURL()+"/weaviate/v1"+endpoint, body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", accept)

	if apiKey != "" {
		req.Header.Set("X-API-KEY", apiKey)
	}

	if apiToken != "" {
		req.Header.Set("X-API-TOKEN", apiToken)
	}

	response, err := client.Do(req)

	if err != nil {
		panic(err)
	}

	return response
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
	return bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/xxx", "value": "xxx"}]`))
}

func getWeaviateURL() string {
	return fmt.Sprintf("%s://%s:%s", serverScheme, serverHost, serverPort)
}

// Set all re-used vars
var apiKeyCmdLine string
var apiTokenCmdLine string
var serverPort string
var serverHost string
var serverScheme string

var actionID string
var actionIDs [10]string
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
var thingIDs [10]string
var thingIDsubject string
var rootID string
var unixTimeExpire int64

var thingTestString string
var thingTestInt int64
var thingTestBoolean bool
var thingTestNumber float64
var thingTestDate string

var actionTestString string
var actionTestInt int64
var actionTestBoolean bool
var actionTestNumber float64
var actionTestDate string

var messaging *messages.Messaging

func init() {
	flag.StringVar(&apiKeyCmdLine, "api-key", "", "API-KEY as used as haeder in the tests.")
	flag.StringVar(&apiTokenCmdLine, "api-token", "", "API-KEY as used as haeder in the tests.")
	flag.StringVar(&serverPort, "server-port", "", "Port number on which the server is running.")
	flag.StringVar(&serverHost, "server-host", "", "Host-name on which the server is running.")
	flag.StringVar(&serverScheme, "server-scheme", "", "Scheme on which the server is running.")
	flag.Parse()

	if serverScheme == "" {
		serverScheme = "http"
	}

	fakeID = "11111111-1111-1111-1111-111111111111"

	messaging = &messages.Messaging{
		Debug: true,
	}
}

/******************
 * BENCHMARKS
 ******************/

func Benchmark__weaviate_adding_things(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping test in short mode.")
	}

	for i := 0; i < b.N; i++ {
		// Create create request
		body := bytes.NewBuffer([]byte(`{
			"@context": "http://example.org",
			"@class": "TestThing",
			"schema": {
				"testString": "Benchmark Thing",
				"testInt": 1,
				"testBoolean": true,
				"testNumber": 1.337,
				"testDateTime": "2017-10-06T08:15:30+01:00"
			}
		}`))

		tr := &http.Transport{
			MaxIdleConns:       10000,
			IdleConnTimeout:    10 * time.Second,
			DisableCompression: true,
		}
		client := &http.Client{Transport: tr}

		req, _ := http.NewRequest("POST", getWeaviateURL()+"/weaviate/v1/things", body)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		req.Header.Set("X-API-KEY", apiKeyCmdLine)
		req.Header.Set("X-API-TOKEN", apiTokenCmdLine)

		resp, err := client.Do(req)
		if err != nil {
			messaging.DebugMessage(fmt.Sprintf("Error in response occured during benchmark: %s", err.Error()))
		}

		resp.Body.Close()
		// req.Body.Close()

	}
}

// /******************
//  * KEY TESTS
//  ******************/

// TODO: TEST WRONG TOKEN / WRONG KEY COMBINATION IN HEADER

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
// 	response := doRequest("/keys", "POST", "application/json", jsonStr, apiKeyCmdLine, apiTokenCmdLine)

// 	// Check status code of create
// 	require.Equal(t, http.StatusAccepted, response.StatusCode)

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
// 	unixTimeExpire = connutils.NowUnix()
// 	jsonStrNewKeySoonExpire := bytes.NewBuffer([]byte(`{
// 		"delete": false,
// 		"email": "expiredkey",
// 		"ipOrigin": ["127.0.0.*", "*"],
// 		"keyExpiresUnix": ` + strconv.FormatInt(unixTimeExpire+2000, 10) + `,
// 		"read": true,
// 		"write": true,
// 		"execute": false
// 	}`))
// 	responseNewTokenSoonExpire := doRequest("/keys", "POST", "application/json", jsonStrNewKeySoonExpire, apiKeyCmdLine, apiTokenCmdLine)

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

// 	// Check status code get requestsOK
// require.Equal(t, http.StatusOK, response.StatusCode)

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
// 	response := doRequest("/keys/"+newAPIKeyID, "GET", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)

// 	// Check status code get requestsOK)
// require.Equal(t, http.StatusOK, response.StatusCode)

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

// 	// Create get request with non-existing ID, check its responsecode
// 	testNotExistsRequest(t, "/keys", "GET", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)
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
// 	response := doRequest("/keys/"+newAPIKeyID+"/children", "GET", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)

// 	// Check status code get requestsOK)
// require.Equal(t, http.StatusOK, response.StatusCode)

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

// 	// Create get request with non-existing ID, check its responsecode
// 	responseNotFound := doRequest("keys/"+fakeID+"/children", "GET", "application/json", nil, newAPIToken)

// 	require.Equal(t, http.StatusOK, response.StatusCode)
// }

// // weaviate.key.me.children.get
// func Test__weaviate_key_me_children_get_JSON(t *testing.T) {
// 	// Create get request
// 	response := doRequest("/keys/me/children", "GET", "application/json", nil, newAPIToken)

// 	// Check status code get requestsOK)
// require.Equal(t, http.StatusOK, response.StatusCode)

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
// 	responseDelSub1 := doRequest("/keys/"+sub1ID, "DELETE", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)
// 	testStatusCode(t, responseDelSub1.StatusCode, http.StatusNoContent)
// 	time.Sleep(2 * time.Second)

// 	// Check sub1 removed and check its statuscode (404)
// 	responseSub1Deleted := doRequest("/keys/"+sub1ID, "DELETE", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)
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
// 	responseSub2Deleted := doRequest("/keys/"+sub2ID, "DELETE", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)
// 	testStatusCode(t, responseSub2Deleted.StatusCode, http.StatusNotFound)
// 	time.Sleep(2 * time.Second)

// 	// Check head removed and check its statuscode (404)
// 	responseHeadDeleted := doRequest("/keys/"+headID, "GET", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)
// 	testStatusCode(t, responseHeadDeleted.StatusCode, http.StatusNotFound)
// 	time.Sleep(2 * time.Second)

// 	// Delete key that is expired
// 	responseExpiredDeleted := doRequest("/keys/"+expiredID, "DELETE", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)
// 	testStatusCode(t, responseExpiredDeleted.StatusCode, http.StatusNoContent)
// 	time.Sleep(2 * time.Second)
// }

/******************
 * META TESTS
 ******************/

func Test__weaviate_meta_get_JSON(t *testing.T) {
	response := doRequest("/meta", "GET", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)

	// Check status code of create
	require.Equal(t, http.StatusOK, response.StatusCode)

	body := getResponseBody(response)

	respObject := &models.Meta{}
	json.Unmarshal(body, respObject)

	// Check whether the returned information is the same as the data added
	require.Equal(t, getWeaviateURL(), respObject.Hostname)

	// TODO: https://github.com/creativesoftwarefdn/weaviate/issues/210
}

/******************
 * THING TESTS
 ******************/

func performInvalidThingRequests(t *testing.T, uri string, method string) {
	// Create invalid requests
	// Test missing class
	jsonStrInvalid1 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"schema": {
			"testString": "%s"
		}
	}`, thingTestString)))
	responseInvalid1 := doRequest(uri, method, "application/json", jsonStrInvalid1, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid1.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid1)), "the given class is empty")

	// Test missing context
	jsonStrInvalid2 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@class": "TestThing",
		"schema": {
			"testString": "%s"
		}
	}`, thingTestString)))
	responseInvalid2 := doRequest(uri, method, "application/json", jsonStrInvalid2, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid2.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid2)), "the given context is empty")

	// Test non-existing class
	jsonStrInvalid3 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestThings",
		"schema": {
			"testString": "%s"
		}
	}`, thingTestString)))
	responseInvalid3 := doRequest(uri, method, "application/json", jsonStrInvalid3, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid3.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid3)), "no such class with name")

	// Test non-existing property
	jsonStrInvalid4 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestThing",
		"schema": {
			"testStrings": "%s"
		}
	}`, thingTestString)))
	responseInvalid4 := doRequest(uri, method, "application/json", jsonStrInvalid4, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid4.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid4)), "no such prop with name")

	// Test invalid property cref
	jsonStrInvalid5 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestThing",
		"schema": {
			"testCref": {
				"locationUrl": "%s",
				"type": "Thing"
			}
		}
	}`, getWeaviateURL())))
	responseInvalid5 := doRequest(uri, method, "application/json", jsonStrInvalid5, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid5.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid5)), "requires exactly 3 arguments: '$cref', 'locationUrl' and 'type'. Check your input schema")

	// Test invalid property cref2
	jsonStrInvalid6 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestThing",
		"schema": {
			"testCref": {
				"$cref": "%s",
				"locationUrls": "%s",
				"type": "Thing"
			}
		}
	}`, thingID, getWeaviateURL())))
	responseInvalid6 := doRequest(uri, method, "application/json", jsonStrInvalid6, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid6.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid6)), "requires exactly 3 arguments: '$cref', 'locationUrl' and 'type'. 'locationUrl' is missing, check your input schema")

	// Test invalid property cref3
	jsonStrInvalid7 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestThing",
		"schema": {
			"testCref": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Things"
			}
		}
	}`, thingID, getWeaviateURL())))
	responseInvalid7 := doRequest(uri, method, "application/json", jsonStrInvalid7, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid7.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid7)), "requires one of the following values in 'type':")

	// Test invalid property cref4
	jsonStrInvalid7b := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestThing",
		"schema": {
			"testCref": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			}
		}
	}`, fakeID, getWeaviateURL())))
	responseInvalid7b := doRequest(uri, method, "application/json", jsonStrInvalid7b, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid7b.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid7b)), "error finding the 'cref' to a Thing in the database:")

	// Test invalid property string
	jsonStrInvalid8 := bytes.NewBuffer([]byte(`{
		"@context": "http://example.org",
		"@class": "TestThing",
		"schema": {
			"testString": 2
		}
	}`))
	responseInvalid8 := doRequest(uri, method, "application/json", jsonStrInvalid8, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid8.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid8)), "requires a string. The given value is")

	// Test invalid property int
	jsonStrInvalid9 := bytes.NewBuffer([]byte(`{
		"@context": "http://example.org",
		"@class": "TestThing",
		"schema": {
			"testInt": 2.7
		}
	}`))
	responseInvalid9 := doRequest(uri, method, "application/json", jsonStrInvalid9, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid9.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid9)), "requires an integer")

	// Test invalid property float
	jsonStrInvalid10 := bytes.NewBuffer([]byte(`{
		"@context": "http://example.org",
		"@class": "TestThing",
		"schema": {
			"testNumber": "test"
		}
	}`))
	responseInvalid10 := doRequest(uri, method, "application/json", jsonStrInvalid10, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid10.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid10)), "requires a float. The given value is")

	// Test invalid property bool
	jsonStrInvalid11 := bytes.NewBuffer([]byte(`{
		"@context": "http://example.org",
		"@class": "TestThing",
		"schema": {
			"testBoolean": "test"
		}
	}`))
	responseInvalid11 := doRequest(uri, method, "application/json", jsonStrInvalid11, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid11.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid11)), "requires a bool. The given value is")

	// Test invalid property date
	jsonStrInvalid12 := bytes.NewBuffer([]byte(`{
		"@context": "http://example.org",
		"@class": "TestThing",
		"schema": {
			"testDateTime": "test"
		}
	}`))
	responseInvalid12 := doRequest(uri, method, "application/json", jsonStrInvalid12, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid12.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid12)), "requires a string with a RFC3339 formatted date. The given value is")
}

// weaviate.thing.create
func Test__weaviate_things_create_JSON(t *testing.T) {
	// Set all thing values to compare
	thingTestString = "Test string"
	thingTestInt = 1
	thingTestBoolean = true
	thingTestNumber = 1.337
	thingTestDate = "2017-10-06T08:15:30+01:00"

	// Create create request
	jsonStr := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestThing",
		"schema": {
			"testString": "%s",
			"testInt": %d,
			"testBoolean": %t,
			"testNumber": %f,
			"testDateTime": "%s"
		}
	}`, thingTestString, thingTestInt, thingTestBoolean, thingTestNumber, thingTestDate)))
	response := doRequest("/things", "POST", "application/json", jsonStr, apiKeyCmdLine, apiTokenCmdLine)

	// Check status code of create
	require.Equal(t, http.StatusAccepted, response.StatusCode)

	body := getResponseBody(response)

	respObject := &models.ThingGetResponse{}
	json.Unmarshal(body, respObject)

	// Check whether generated UUID is added
	require.Regexp(t, strfmt.UUIDPattern, respObject.ThingID)

	// Globally set thingID
	thingID = string(respObject.ThingID)
	messaging.InfoMessage(fmt.Sprintf("INFO: The 'thingID'-variable is set with UUID '%s'", thingID))

	// Check whether the returned information is the same as the data added
	require.Equal(t, thingTestString, respObject.Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, thingTestInt, int64(respObject.Schema.(map[string]interface{})["testInt"].(float64)))
	require.Equal(t, thingTestBoolean, respObject.Schema.(map[string]interface{})["testBoolean"].(bool))
	require.Equal(t, thingTestNumber, respObject.Schema.(map[string]interface{})["testNumber"].(float64))
	require.Equal(t, thingTestDate, respObject.Schema.(map[string]interface{})["testDateTime"].(string))

	// Add multiple things to the database to check List functions
	// Fill database with things and set the IDs to the global thingIDs-array
	thingIDs[9] = thingID

	for i := 8; i >= 0; i-- {
		// Handle request
		jsonStr := bytes.NewBuffer([]byte(fmt.Sprintf(`{
			"@context": "http://example.org",
			"@class": "TestThing2",
			"schema": {
				"testString": "%s",
				"testInt": %d,
				"testBoolean": %t,
				"testNumber": %f,
				"testDateTime": "%s",
				"testCref": {
					"$cref": "%s",
					"locationUrl": "%s",
					"type": "Thing"
				}
			}
		}`, thingTestString, thingTestInt, thingTestBoolean, thingTestNumber, thingTestDate, thingID, getWeaviateURL())))
		response := doRequest("/things", "POST", "application/json", jsonStr, apiKeyCmdLine, apiTokenCmdLine)
		body := getResponseBody(response)
		respObject := &models.ThingGetResponse{}
		json.Unmarshal(body, respObject)

		// Check adding succeeds
		require.Equal(t, http.StatusAccepted, response.StatusCode)

		// Set thingIDsubject
		if i == 0 {
			thingIDsubject = string(respObject.ThingID)
		}

		// Fill array and time out for unlucky sorting issues
		thingIDs[i] = string(respObject.ThingID)
		time.Sleep(1 * time.Second)
	}

	// Create invalid requests
	performInvalidThingRequests(t, "/things", "POST")

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

}

// weaviate.thing.list
func Test__weaviate_things_list_JSON(t *testing.T) {
	// Create list request
	response := doRequest("/things", "GET", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)

	// Check status code of list
	require.Equal(t, http.StatusOK, response.StatusCode)

	body := getResponseBody(response)

	respObject := &models.ThingsListResponse{}
	json.Unmarshal(body, respObject)

	// Check most recent
	require.Regexp(t, strfmt.UUIDPattern, respObject.Things[0].ThingID)
	require.Regexp(t, strfmt.UUIDPattern, thingIDs[0])
	require.Equal(t, thingIDs[0], string(respObject.Things[0].ThingID))

	// Query whole list just created
	listResponse := doRequest("/things?maxResults=3", "GET", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)
	listResponseObject := &models.ThingsListResponse{}
	json.Unmarshal(getResponseBody(listResponse), listResponseObject)

	// Test total results
	require.Conditionf(t, func() bool { return listResponseObject.TotalResults >= 10 }, "Total results have to be higher or equal to 10.")

	// Test amount in current response
	require.Len(t, listResponseObject.Things, 3)

	// Test ID in the middle of the 3 results
	require.Equal(t, thingIDs[1], string(listResponseObject.Things[1].ThingID))

	// Query whole list just created
	listResponse2 := doRequest("/things?maxResults=5&page=2", "GET", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)
	listResponseObject2 := &models.ThingsListResponse{}
	json.Unmarshal(getResponseBody(listResponse2), listResponseObject2)

	// Test total results
	require.Conditionf(t, func() bool { return listResponseObject2.TotalResults >= 10 }, "Total results have to be higher or equal to 10.")

	// Test amount in current response
	require.Len(t, listResponseObject2.Things, 5)

	// Test ID in the middle
	require.Equal(t, thingIDs[7], string(listResponseObject2.Things[2].ThingID))
}

// weaviate.thing.get
func Test__weaviate_things_get_JSON(t *testing.T) {
	// Create get request
	response := doRequest("/things/"+thingIDs[0], "GET", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)

	// Check status code get request
	require.Equal(t, http.StatusOK, response.StatusCode)

	body := getResponseBody(response)

	respObject := &models.ThingGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID of object
	require.Regexp(t, strfmt.UUIDPattern, respObject.ThingID)
	require.Regexp(t, strfmt.UUIDPattern, thingIDs[0])
	require.Equal(t, thingIDs[0], string(respObject.ThingID))

	// Check whether the returned information is the same as the data added
	require.Equal(t, thingTestString, respObject.Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, thingTestInt, int64(respObject.Schema.(map[string]interface{})["testInt"].(float64)))
	require.Equal(t, thingTestBoolean, respObject.Schema.(map[string]interface{})["testBoolean"].(bool))
	require.Equal(t, thingTestNumber, respObject.Schema.(map[string]interface{})["testNumber"].(float64))
	require.Equal(t, thingTestDate, respObject.Schema.(map[string]interface{})["testDateTime"].(string))
	require.Equal(t, thingID, string(respObject.Schema.(map[string]interface{})["testCref"].(map[string]interface{})["$cref"].(string)))

	// Create get request with non-existing ID, check its responsecode
	responseNotFound := doRequest("/things/"+fakeID, "GET", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, responseNotFound.StatusCode)
}

// weaviate.thing.update
func Test__weaviate_things_update_JSON(t *testing.T) {
	// Create update request
	newValue := "New string updated!"
	jsonStr := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestThing",
		"schema": {
			"testString": "%s",
			"testInt": %d,
			"testBoolean": %t,
			"testNumber": %f,
			"testDateTime": "%s"
		}
	}`, newValue, thingTestInt, thingTestBoolean, thingTestNumber, thingTestDate)))
	response := doRequest("/things/"+thingID, "PUT", "application/json", jsonStr, apiKeyCmdLine, apiTokenCmdLine)

	body := getResponseBody(response)

	respObject := &models.ThingGetResponse{}
	json.Unmarshal(body, respObject)

	// Check status code
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Check thing ID is same
	require.Regexp(t, strfmt.UUIDPattern, respObject.ThingID)
	require.Regexp(t, strfmt.UUIDPattern, thingID)
	require.Equal(t, thingID, string(respObject.ThingID))

	// Check whether the returned information is the same as the data updated
	require.Equal(t, newValue, respObject.Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, thingTestInt, int64(respObject.Schema.(map[string]interface{})["testInt"].(float64)))
	require.Equal(t, thingTestBoolean, respObject.Schema.(map[string]interface{})["testBoolean"].(bool))
	require.Equal(t, thingTestNumber, respObject.Schema.(map[string]interface{})["testNumber"].(float64))
	require.Equal(t, thingTestDate, respObject.Schema.(map[string]interface{})["testDateTime"].(string))

	// Check given update time is after now, but not in the future
	now := connutils.NowUnix()
	require.Conditionf(t, func() bool { return !(respObject.LastUpdateTimeUnix > now) }, "LastUpdateTimeUnix is incorrect, it was set in the future.")
	require.Conditionf(t, func() bool { return !(respObject.LastUpdateTimeUnix < now-2000) }, "LastUpdateTimeUnix is incorrect, it was set to far back.")

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Check if update is also applied on object when using a new GET request on same object
	responseGet := doRequest("/things/"+thingID, "GET", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)

	bodyGet := getResponseBody(responseGet)

	// Test response obj
	respObjectGet := &models.ThingGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check whether the returned information is the same as the data updated
	require.Equal(t, newValue, respObjectGet.Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, thingTestInt, int64(respObjectGet.Schema.(map[string]interface{})["testInt"].(float64)))
	require.Equal(t, thingTestBoolean, respObjectGet.Schema.(map[string]interface{})["testBoolean"].(bool))
	require.Equal(t, thingTestNumber, respObjectGet.Schema.(map[string]interface{})["testNumber"].(float64))
	require.Equal(t, thingTestDate, respObjectGet.Schema.(map[string]interface{})["testDateTime"].(string))

	// Create get request with non-existing ID, check its responsecode
	responseNotFound := doRequest("/things/"+fakeID, "PUT", "application/json", getEmptyJSON(), apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, responseNotFound.StatusCode)

	// Check validation with invalid requests
	performInvalidThingRequests(t, "/things/"+thingIDsubject, "PUT")
}

// weaviate.thing.patch
func Test__weaviate_things_patch_JSON(t *testing.T) {
	// Create patch request
	newValue := "New string patched!"

	jsonStr := bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/schema/testString", "value": "` + newValue + `"}]`))
	response := doRequest("/things/"+thingID, "PATCH", "application/json", jsonStr, apiKeyCmdLine, apiTokenCmdLine)

	body := getResponseBody(response)

	respObject := &models.ThingGetResponse{}
	json.Unmarshal(body, respObject)

	// Check status code
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Check ID is the same
	require.Regexp(t, strfmt.UUIDPattern, respObject.ThingID)
	require.Regexp(t, strfmt.UUIDPattern, thingID)
	require.Equal(t, thingID, string(respObject.ThingID))

	// Check whether the returned information is the same as the data updated
	require.Equal(t, newValue, respObject.Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, thingTestInt, int64(respObject.Schema.(map[string]interface{})["testInt"].(float64)))
	require.Equal(t, thingTestBoolean, respObject.Schema.(map[string]interface{})["testBoolean"].(bool))
	require.Equal(t, thingTestNumber, respObject.Schema.(map[string]interface{})["testNumber"].(float64))
	require.Equal(t, thingTestDate, respObject.Schema.(map[string]interface{})["testDateTime"].(string))

	// Check given update time is after now, but not in the future
	now := connutils.NowUnix()
	require.Conditionf(t, func() bool { return !(respObject.LastUpdateTimeUnix > now) }, "LastUpdateTimeUnix is incorrect, it was set in the future.")
	require.Conditionf(t, func() bool { return !(respObject.LastUpdateTimeUnix < now-2000) }, "LastUpdateTimeUnix is incorrect, it was set to far back.")

	//dTest is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Check if patch is also applied on object when using a new GET request on same object
	responseGet := doRequest("/things/"+thingID, "GET", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)

	bodyGet := getResponseBody(responseGet)

	// Test response obj
	respObjectGet := &models.ThingGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check whether the returned information is the same as the data updated
	require.Equal(t, newValue, respObjectGet.Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, thingTestInt, int64(respObjectGet.Schema.(map[string]interface{})["testInt"].(float64)))
	require.Equal(t, thingTestBoolean, respObjectGet.Schema.(map[string]interface{})["testBoolean"].(bool))
	require.Equal(t, thingTestNumber, respObjectGet.Schema.(map[string]interface{})["testNumber"].(float64))
	require.Equal(t, thingTestDate, respObjectGet.Schema.(map[string]interface{})["testDateTime"].(string))

	// Check patch with incorrect contents
	jsonStrError := bytes.NewBuffer([]byte(`{ "op": "replace", "path": "/address_components/long_name", "value": "` + newValue + `"}`))
	responseError := doRequest("/things/"+thingID, "PATCH", "application/json", jsonStrError, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusBadRequest, responseError.StatusCode)

	// Create get request with non-existing ID, check its responsecode
	responseNotFound := doRequest("/things/"+fakeID, "PATCH", "application/json", getEmptyPatchJSON(), apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, responseNotFound.StatusCode)

	// Test non-existing class
	jsonStrInvalid1 := bytes.NewBuffer([]byte(fmt.Sprintf(`[{ "op": "replace", "path": "/@class", "value": "%s"}]`, "TestThings")))
	responseInvalid1 := doRequest("/things/"+thingIDsubject, "PATCH", "application/json", jsonStrInvalid1, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid1.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid1)), "no such class with name")

	// Test non-existing property
	jsonStrInvalid2 := bytes.NewBuffer([]byte(fmt.Sprintf(`[{ "op": "add", "path": "/schema/testStrings", "value": "%s"}]`, "Test")))
	responseInvalid2 := doRequest("/things/"+thingIDsubject, "PATCH", "application/json", jsonStrInvalid2, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid2.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid2)), "no such prop with name")

	// Test invalid property cref
	jsonStrInvalid3 := bytes.NewBuffer([]byte(`[{ "op": "remove", "path": "/schema/testCref/locationUrl"}]`))
	responseInvalid3 := doRequest("/things/"+thingIDsubject, "PATCH", "application/json", jsonStrInvalid3, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid3.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid3)), "requires exactly 3 arguments: '$cref', 'locationUrl' and 'type'. Check your input schema")

	// Test invalid property cref2
	jsonStrInvalid4 := bytes.NewBuffer([]byte(fmt.Sprintf(`[{ "op": "remove", "path": "/schema/testCref/locationUrl"}, { "op": "add", "path": "/schema/testCref/locationUrls", "value": "%s"}]`, getWeaviateURL())))
	responseInvalid4 := doRequest("/things/"+thingIDsubject, "PATCH", "application/json", jsonStrInvalid4, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid4.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid4)), "requires exactly 3 arguments: '$cref', 'locationUrl' and 'type'. 'locationUrl' is missing, check your input schema")

	// Test invalid property cref3
	jsonStrInvalid5 := bytes.NewBuffer([]byte(fmt.Sprintf(`[{ "op": "replace", "path": "/schema/testCref/type", "value": "%s"}]`, "Test")))
	responseInvalid5 := doRequest("/things/"+thingIDsubject, "PATCH", "application/json", jsonStrInvalid5, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid5.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid5)), "requires one of the following values in 'type':")

	// Test invalid property string
	jsonStrInvalid6 := bytes.NewBuffer([]byte(fmt.Sprintf(`[{ "op": "replace", "path": "/schema/testString", "value": %d}]`, 2)))
	responseInvalid6 := doRequest("/things/"+thingIDsubject, "PATCH", "application/json", jsonStrInvalid6, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid6.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid6)), "requires a string. The given value is")

	// Test invalid property int
	jsonStrInvalid7 := bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/schema/testInt", "value": 2.8}]`))
	responseInvalid7 := doRequest("/things/"+thingIDsubject, "PATCH", "application/json", jsonStrInvalid7, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid7.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid7)), "requires an integer")

	// Test invalid property float
	jsonStrInvalid8 := bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/schema/testNumber", "value": "test"}]`))
	responseInvalid8 := doRequest("/things/"+thingIDsubject, "PATCH", "application/json", jsonStrInvalid8, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid8.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid8)), "requires a float. The given value is")

	// Test invalid property bool
	jsonStrInvalid9 := bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/schema/testBoolean", "value": "test"}]`))
	responseInvalid9 := doRequest("/things/"+thingIDsubject, "PATCH", "application/json", jsonStrInvalid9, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid9.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid9)), "requires a bool. The given value is")

	// Test invalid property date
	jsonStrInvalid10 := bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/schema/testDateTime", "value": "test"}]`))
	responseInvalid10 := doRequest("/things/"+thingIDsubject, "PATCH", "application/json", jsonStrInvalid10, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid10.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid10)), "requires a string with a RFC3339 formatted date. The given value is")
}

func Test__weaviate_things_validate_JSON(t *testing.T) {
	// Test invalid requests
	performInvalidThingRequests(t, "/things/validate", "POST")

	// Test valid request
	jsonStr := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestThing",
		"schema": {
			"testString": "%s",
			"testInt": %d,
			"testBoolean": %t,
			"testNumber": %f,
			"testDateTime": "%s",
			"testCref": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			}
		}
	}`, thingTestString, thingTestInt, thingTestBoolean, thingTestNumber, thingTestDate, thingID, getWeaviateURL())))

	response := doRequest("/things/validate", "POST", "application/json", jsonStr, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusOK, response.StatusCode)
}

// /******************
//  * ACTIONS TESTS
//  ******************/

func performInvalidActionRequests(t *testing.T, uri string, method string) {
	// Test missing things
	jsonStrInvalidThings1 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testString": "%s"
		}
	}`, thingTestString)))
	responseInvalidThings1 := doRequest(uri, method, "application/json", jsonStrInvalidThings1, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings1.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings1)), "no things, object and subject, are added. Add 'things' by using the 'things' key in the root of the JSON")

	// Test missing things-object
	jsonStrInvalidThings2 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testString": "%s"
		},
		"things": {
			"subject": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			}
		}
	}`, thingTestString, thingID, getWeaviateURL())))
	responseInvalidThings2 := doRequest(uri, method, "application/json", jsonStrInvalidThings2, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings2.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings2)), "no object-thing is added. Add the 'object' inside the 'things' part of the JSON")

	// Test missing things-subject
	jsonStrInvalidThings3 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testString": "%s"
		},
		"things": {
			"object": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			}
		}
	}`, thingTestString, thingID, getWeaviateURL())))
	responseInvalidThings3 := doRequest(uri, method, "application/json", jsonStrInvalidThings3, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings3.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings3)), "no subject-thing is added. Add the 'subject' inside the 'things' part of the JSON")

	// Test missing things-object locationUrl
	jsonStrInvalidThings4 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testString": "%s"
		},
		"things": {
			"object": {
				"$cref": "%s",
				"locationUrls": "%s",
				"type": "Thing"
			},
			"subject": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			}
		}
	}`, thingTestString, thingID, getWeaviateURL(), thingIDsubject, getWeaviateURL())))
	responseInvalidThings4 := doRequest(uri, method, "application/json", jsonStrInvalidThings4, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings4.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings4)), "no 'locationURL' is found in the object-thing. Add the 'locationURL' inside the 'object-thing' part of the JSON")

	// Test missing things-object type
	jsonStrInvalidThings5 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testString": "%s"
		},
		"things": {
			"object": {
				"$cref": "%s",
				"locationUrl": "%s",
				"types": "Thing"
			},
			"subject": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			}
		}
	}`, thingTestString, thingID, getWeaviateURL(), thingIDsubject, getWeaviateURL())))
	responseInvalidThings5 := doRequest(uri, method, "application/json", jsonStrInvalidThings5, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings5.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings5)), "no 'type' is found in the object-thing. Add the 'type' inside the 'object-thing' part of the JSON")

	// Test faulty things-object type
	jsonStrInvalidThings6 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testString": "%s"
		},
		"things": {
			"object": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Things"
			},
			"subject": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			}
		}
	}`, thingTestString, thingID, getWeaviateURL(), thingIDsubject, getWeaviateURL())))
	responseInvalidThings6 := doRequest(uri, method, "application/json", jsonStrInvalidThings6, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings6.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings6)), "type in body should be one of [Thing Action Key]")

	// Test non-existing object-cref
	jsonStrInvalidThings7 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testString": "%s"
		},
		"things": {
			"object": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			},
			"subject": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			}
		}
	}`, thingTestString, fakeID, getWeaviateURL(), thingIDsubject, getWeaviateURL())))
	responseInvalidThings7 := doRequest(uri, method, "application/json", jsonStrInvalidThings7, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings7.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings7)), "error finding the 'object'-thing in the database")

	// Test missing things-subject locationUrl
	jsonStrInvalidThings8 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testString": "%s"
		},
		"things": {
			"object": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			},
			"subject": {
				"$cref": "%s",
				"locationUrls": "%s",
				"type": "Thing"
			}
		}
	}`, thingTestString, thingID, getWeaviateURL(), thingIDsubject, getWeaviateURL())))
	responseInvalidThings8 := doRequest(uri, method, "application/json", jsonStrInvalidThings8, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings8.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings8)), "no 'locationURL' is found in the subject-thing. Add the 'locationURL' inside the 'subject-thing' part of the JSON")

	// Test missing things-object type
	jsonStrInvalidThings9 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testString": "%s"
		},
		"things": {
			"object": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			},
			"subject": {
				"$cref": "%s",
				"locationUrl": "%s",
				"types": "Thing"
			}
		}
	}`, thingTestString, thingID, getWeaviateURL(), thingIDsubject, getWeaviateURL())))
	responseInvalidThings9 := doRequest(uri, method, "application/json", jsonStrInvalidThings9, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings9.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings9)), "no 'type' is found in the subject-thing. Add the 'type' inside the 'subject-thing' part of the JSON")

	// Test faulty things-object type
	jsonStrInvalidThings10 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testString": "%s"
		},
		"things": {
			"object": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			},
			"subject": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Things"
			}
		}
	}`, thingTestString, thingID, getWeaviateURL(), thingIDsubject, getWeaviateURL())))
	responseInvalidThings10 := doRequest(uri, method, "application/json", jsonStrInvalidThings10, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings10.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings10)), "type in body should be one of [Thing Action Key]")

	// Test non-existing object-cref
	jsonStrInvalidThings11 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testString": "%s"
		},
		"things": {
			"object": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			},
			"subject": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			}
		}
	}`, thingTestString, thingID, getWeaviateURL(), fakeID, getWeaviateURL())))
	responseInvalidThings11 := doRequest(uri, method, "application/json", jsonStrInvalidThings11, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings11.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings11)), "error finding the 'subject'-thing in the database")

	// Correct connected things
	actionThingsJSON := fmt.Sprintf(`,
		"things": {
			"object": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			},
			"subject": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			}
		}`, thingID, getWeaviateURL(), thingIDsubject, getWeaviateURL())

	// Test missing class
	jsonStrInvalid1 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"schema": {
			"testString": "%s"
		}%s
	}`, thingTestString, actionThingsJSON)))
	responseInvalid1 := doRequest(uri, method, "application/json", jsonStrInvalid1, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid1.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid1)), "the given class is empty")

	// Test missing context
	jsonStrInvalid2 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@class": "TestAction",
		"schema": {
			"testString": "%s"
		}%s
	}`, thingTestString, actionThingsJSON)))
	responseInvalid2 := doRequest(uri, method, "application/json", jsonStrInvalid2, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid2.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid2)), "the given context is empty")

	// Test non-existing class
	jsonStrInvalid3 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestActions",
		"schema": {
			"testString": "%s"
		}%s
	}`, thingTestString, actionThingsJSON)))
	responseInvalid3 := doRequest(uri, method, "application/json", jsonStrInvalid3, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid3.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid3)), "no such class with name")

	// Test non-existing property
	jsonStrInvalid4 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testStrings": "%s"
		}%s
	}`, thingTestString, actionThingsJSON)))
	responseInvalid4 := doRequest(uri, method, "application/json", jsonStrInvalid4, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid4.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid4)), "no such prop with name")

	// Test invalid property cref
	jsonStrInvalid5 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testCref": {
				"locationUrl": "%s",
				"type": "Thing"
			}
		}%s
	}`, getWeaviateURL(), actionThingsJSON)))
	responseInvalid5 := doRequest(uri, method, "application/json", jsonStrInvalid5, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid5.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid5)), "requires exactly 3 arguments: '$cref', 'locationUrl' and 'type'. Check your input schema")

	// Test invalid property cref2
	jsonStrInvalid6 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testCref": {
				"$cref": "%s",
				"locationUrls": "%s",
				"type": "Thing"
			}
		}%s
	}`, thingID, getWeaviateURL(), actionThingsJSON)))
	responseInvalid6 := doRequest(uri, method, "application/json", jsonStrInvalid6, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid6.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid6)), "requires exactly 3 arguments: '$cref', 'locationUrl' and 'type'. 'locationUrl' is missing, check your input schema")

	// Test invalid property cref3
	jsonStrInvalid7 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testCref": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Things"
			}
		}%s
	}`, thingID, getWeaviateURL(), actionThingsJSON)))
	responseInvalid7 := doRequest(uri, method, "application/json", jsonStrInvalid7, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid7.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid7)), "requires one of the following values in 'type':")

	// Test invalid property cref3
	jsonStrInvalid7b := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testCref": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			}
		}%s
	}`, fakeID, getWeaviateURL(), actionThingsJSON)))
	responseInvalid7b := doRequest(uri, method, "application/json", jsonStrInvalid7b, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid7b.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid7b)), "error finding the 'cref' to a Thing in the database:")

	// Test invalid property string
	jsonStrInvalid8 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testString": 2
		}%s
	}`, actionThingsJSON)))
	responseInvalid8 := doRequest(uri, method, "application/json", jsonStrInvalid8, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid8.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid8)), "requires a string. The given value is")

	// Test invalid property int
	jsonStrInvalid9 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testInt": 2.7
		}%s
	}`, actionThingsJSON)))
	responseInvalid9 := doRequest(uri, method, "application/json", jsonStrInvalid9, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid9.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid9)), "requires an integer")

	// Test invalid property float
	jsonStrInvalid10 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testNumber": "test"
		}%s
	}`, actionThingsJSON)))
	responseInvalid10 := doRequest(uri, method, "application/json", jsonStrInvalid10, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid10.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid10)), "requires a float. The given value is")

	// Test invalid property bool
	jsonStrInvalid11 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testBoolean": "test"
		}%s
	}`, actionThingsJSON)))
	responseInvalid11 := doRequest(uri, method, "application/json", jsonStrInvalid11, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid11.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid11)), "requires a bool. The given value is")

	// Test invalid property date
	jsonStrInvalid12 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testDateTime": "test"
		}%s
	}`, actionThingsJSON)))
	responseInvalid12 := doRequest(uri, method, "application/json", jsonStrInvalid12, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid12.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid12)), "requires a string with a RFC3339 formatted date. The given value is")
}

// weaviate.actions.create
func Test__weaviate_actions_create_JSON(t *testing.T) {
	// Set all thing values to compare
	actionTestString = "Test string 2"
	actionTestInt = 2
	actionTestBoolean = false
	actionTestNumber = 2.337
	actionTestDate = "2017-10-09T08:15:30+01:00"

	// Create create request
	jsonStr := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://schema.org",
		"@class": "TestAction",
		"schema": {
			"testString": "%s",
			"testInt": %d,
			"testBoolean": %t,
			"testNumber": %f,
			"testDateTime": "%s",
			"testCref": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			}
		},
		"things": {
			"object": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			},
			"subject": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			}
		}
	}`, actionTestString, actionTestInt, actionTestBoolean, actionTestNumber, actionTestDate, thingID, getWeaviateURL(), thingID, getWeaviateURL(), thingIDsubject, getWeaviateURL())))
	response := doRequest("/actions", "POST", "application/json", jsonStr, apiKeyCmdLine, apiTokenCmdLine)

	// Check status code of create
	require.Equal(t, http.StatusAccepted, response.StatusCode)

	body := getResponseBody(response)

	respObject := &models.ActionGetResponse{}
	json.Unmarshal(body, respObject)

	// Check whether generated UUID is added
	require.Regexp(t, strfmt.UUIDPattern, respObject.ActionID)

	// Globally set actionID
	actionID = string(respObject.ActionID)
	messaging.InfoMessage(fmt.Sprintf("INFO: The 'actionID'-variable is set with UUID '%s'", actionID))

	// Check thing is set to known ThingID
	require.Regexp(t, strfmt.UUIDPattern, respObject.Things.Object.NrDollarCref)
	require.Regexp(t, strfmt.UUIDPattern, thingID)
	require.Equal(t, thingID, string(respObject.Things.Object.NrDollarCref))

	// Check thing is set to known ThingIDSubject
	require.Regexp(t, strfmt.UUIDPattern, respObject.Things.Subject.NrDollarCref)
	require.Regexp(t, strfmt.UUIDPattern, thingIDsubject)
	require.Equal(t, thingIDsubject, string(respObject.Things.Subject.NrDollarCref))

	// Check whether the returned information is the same as the data added
	require.Equal(t, actionTestString, respObject.Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, actionTestInt, int64(respObject.Schema.(map[string]interface{})["testInt"].(float64)))
	require.Equal(t, actionTestBoolean, respObject.Schema.(map[string]interface{})["testBoolean"].(bool))
	require.Equal(t, actionTestNumber, respObject.Schema.(map[string]interface{})["testNumber"].(float64))
	require.Equal(t, actionTestDate, respObject.Schema.(map[string]interface{})["testDateTime"].(string))

	// Check set user key is rootID
	// testID(t, string(respObject.UserKey), rootID) TODO

	// Check given creation time is after now, but not in the future
	now := connutils.NowUnix()
	require.Conditionf(t, func() bool { return !(respObject.CreationTimeUnix > now) }, "CreationTimeUnix is incorrect, it was set in the future.")
	require.Conditionf(t, func() bool { return !(respObject.CreationTimeUnix < now-2000) }, "CreationTimeUnix is incorrect, it was set to far back.")

	// Add multiple actions to the database to check List functions
	// Fill database with actions and set the IDs to the global actionIDs-array
	actionIDs[9] = actionID

	for i := 8; i >= 0; i-- {
		// Handle request
		jsonStr := bytes.NewBuffer([]byte(fmt.Sprintf(`{
			"@context": "http://schema.org",
			"@class": "TestAction2",
			"schema": {
				"testString": "%s",
				"testInt": %d,
				"testBoolean": %t,
				"testNumber": %f,
				"testDateTime": "%s",
				"testCref": {
					"$cref": "%s",
					"locationUrl": "%s",
					"type": "Thing"
				}
			},
			"things": {
				"object": {
					"$cref": "%s",
					"locationUrl": "%s",
					"type": "Thing"
				},
				"subject": {
					"$cref": "%s",
					"locationUrl": "%s",
					"type": "Thing"
				}
			}
		}`, actionTestString, actionTestInt, actionTestBoolean, actionTestNumber, actionTestDate, thingID, getWeaviateURL(), thingID, getWeaviateURL(), thingIDsubject, getWeaviateURL())))
		response := doRequest("/actions", "POST", "application/json", jsonStr, apiKeyCmdLine, apiTokenCmdLine)
		body := getResponseBody(response)
		respObject := &models.ActionGetResponse{}
		json.Unmarshal(body, respObject)

		// Check adding succeeds
		require.Equal(t, http.StatusAccepted, response.StatusCode)

		// Fill array and time out for unlucky sorting issues
		actionIDs[i] = string(respObject.ActionID)
		time.Sleep(1 * time.Second)
	}

	// Create invalid requests
	performInvalidActionRequests(t, "/actions", "POST")

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)
}

// weaviate.things.actions.list
func Test__weaviate_things_actions_list_JSON(t *testing.T) {
	// Create list request
	response := doRequest("/things/"+thingID+"/actions", "GET", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)

	// Check status code of list
	require.Equal(t, http.StatusOK, response.StatusCode)

	body := getResponseBody(response)

	respObject := &models.ActionsListResponse{}
	json.Unmarshal(body, respObject)

	// Check most recent
	require.Regexp(t, strfmt.UUIDPattern, respObject.Actions[0].ActionID)
	require.Regexp(t, strfmt.UUIDPattern, actionIDs[0])
	require.Equal(t, actionIDs[0], string(respObject.Actions[0].ActionID))

	// Check there are ten actions
	require.Len(t, respObject.Actions, 10)

	// Query whole list just created
	listResponse := doRequest("/things/"+thingID+"/actions?maxResults=3", "GET", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)
	listResponseObject := &models.ActionsListResponse{}
	json.Unmarshal(getResponseBody(listResponse), listResponseObject)

	// Test total results
	require.Conditionf(t, func() bool { return listResponseObject.TotalResults == 10 }, "Total results have to be equal to 10.")

	// Test amount in current response
	require.Len(t, listResponseObject.Actions, 3)

	// Test ID in the middle of the 3 results
	require.Equal(t, actionIDs[1], string(listResponseObject.Actions[1].ActionID))

	// Query whole list just created
	listResponse2 := doRequest("/things/"+thingID+"/actions?maxResults=5&page=2", "GET", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)
	listResponseObject2 := &models.ActionsListResponse{}
	json.Unmarshal(getResponseBody(listResponse2), listResponseObject2)

	// Test total results
	require.Conditionf(t, func() bool { return listResponseObject2.TotalResults == 10 }, "Total results have to be equal to 10.")

	// Test amount in current response
	require.Len(t, listResponseObject2.Actions, 5)

	// Test ID in the middle
	require.Equal(t, actionIDs[7], string(listResponseObject2.Actions[2].ActionID))
}

// weaviate.action.get
func Test__weaviate_actions_get_JSON(t *testing.T) {
	// Create get request
	response := doRequest("/actions/"+actionID, "GET", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)

	// Check status code get request
	require.Equal(t, http.StatusOK, response.StatusCode)

	body := getResponseBody(response)

	respObject := &models.ActionGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID of object
	require.Regexp(t, strfmt.UUIDPattern, respObject.ActionID)
	require.Regexp(t, strfmt.UUIDPattern, actionID)
	require.Equal(t, actionID, string(respObject.ActionID))

	// Check ID of thing-object
	require.Regexp(t, strfmt.UUIDPattern, respObject.Things.Object.NrDollarCref)
	require.Regexp(t, strfmt.UUIDPattern, thingID)
	require.Equal(t, thingID, string(respObject.Things.Object.NrDollarCref))

	// Check ID of thing-subject
	require.Regexp(t, strfmt.UUIDPattern, respObject.Things.Subject.NrDollarCref)
	require.Regexp(t, strfmt.UUIDPattern, thingIDsubject)
	require.Equal(t, thingIDsubject, string(respObject.Things.Subject.NrDollarCref))

	// Check whether the returned information is the same as the data added
	require.Equal(t, actionTestString, respObject.Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, actionTestInt, int64(respObject.Schema.(map[string]interface{})["testInt"].(float64)))
	require.Equal(t, actionTestBoolean, respObject.Schema.(map[string]interface{})["testBoolean"].(bool))
	require.Equal(t, actionTestNumber, respObject.Schema.(map[string]interface{})["testNumber"].(float64))
	require.Equal(t, actionTestDate, respObject.Schema.(map[string]interface{})["testDateTime"].(string))
	require.Equal(t, thingID, string(respObject.Schema.(map[string]interface{})["testCref"].(map[string]interface{})["$cref"].(string)))

	// Create get request with non-existing ID, check its responsecode
	responseNotFound := doRequest("/actions/"+fakeID, "GET", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, responseNotFound.StatusCode)
}

// weaviate.action.update
func Test__weaviate_actions_update_JSON(t *testing.T) {
	// Create update request
	newValue := "New string updated!"
	// Create create request
	jsonStr := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://schema.org",
		"@class": "TestAction",
		"schema": {
			"testString": "%s",
			"testInt": %d,
			"testBoolean": %t,
			"testNumber": %f,
			"testDateTime": "%s",
			"testCref": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			}
		},
		"things": {
			"object": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			},
			"subject": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			}
		}
	}`, newValue, actionTestInt, actionTestBoolean, actionTestNumber, actionTestDate, thingID, getWeaviateURL(), thingID, getWeaviateURL(), thingIDsubject, getWeaviateURL())))
	response := doRequest("/actions/"+actionID, "PUT", "application/json", jsonStr, apiKeyCmdLine, apiTokenCmdLine)

	body := getResponseBody(response)

	respObject := &models.ActionGetResponse{}
	json.Unmarshal(body, respObject)

	// Check status code
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Check action ID is same
	require.Regexp(t, strfmt.UUIDPattern, respObject.ActionID)
	require.Regexp(t, strfmt.UUIDPattern, actionID)
	require.Equal(t, actionID, string(respObject.ActionID))

	// Check thing is set to known ThingID
	require.Regexp(t, strfmt.UUIDPattern, respObject.Things.Object.NrDollarCref)
	require.Regexp(t, strfmt.UUIDPattern, thingID)
	require.Equal(t, thingID, string(respObject.Things.Object.NrDollarCref))

	// Check thing is set to known ThingIDSubject
	require.Regexp(t, strfmt.UUIDPattern, respObject.Things.Subject.NrDollarCref)
	require.Regexp(t, strfmt.UUIDPattern, thingIDsubject)
	require.Equal(t, thingIDsubject, string(respObject.Things.Subject.NrDollarCref))

	// Check whether the returned information is the same as the data updated
	require.Equal(t, newValue, respObject.Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, actionTestInt, int64(respObject.Schema.(map[string]interface{})["testInt"].(float64)))
	require.Equal(t, actionTestBoolean, respObject.Schema.(map[string]interface{})["testBoolean"].(bool))
	require.Equal(t, actionTestNumber, respObject.Schema.(map[string]interface{})["testNumber"].(float64))
	require.Equal(t, actionTestDate, respObject.Schema.(map[string]interface{})["testDateTime"].(string))

	// Check given update time is after now, but not in the future
	now := connutils.NowUnix()
	require.Conditionf(t, func() bool { return !(respObject.LastUpdateTimeUnix > now) }, "LastUpdateTimeUnix is incorrect, it was set in the future.")
	require.Conditionf(t, func() bool { return !(respObject.LastUpdateTimeUnix < now-2000) }, "LastUpdateTimeUnix is incorrect, it was set to far back.")

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Check if update is also applied on object when using a new GET request on same object
	responseGet := doRequest("/actions/"+actionID, "GET", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)

	bodyGet := getResponseBody(responseGet)

	// Test response obj
	respObjectGet := &models.ActionGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check whether the returned information is the same as the data updated
	require.Equal(t, newValue, respObjectGet.Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, actionTestInt, int64(respObjectGet.Schema.(map[string]interface{})["testInt"].(float64)))
	require.Equal(t, actionTestBoolean, respObjectGet.Schema.(map[string]interface{})["testBoolean"].(bool))
	require.Equal(t, actionTestNumber, respObjectGet.Schema.(map[string]interface{})["testNumber"].(float64))
	require.Equal(t, actionTestDate, respObjectGet.Schema.(map[string]interface{})["testDateTime"].(string))

	// Create get request with non-existing ID, check its responsecode
	responseNotFound := doRequest("/actions/"+fakeID, "PUT", "application/json", getEmptyJSON(), apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, responseNotFound.StatusCode)

	// Check validation with invalid requests
	performInvalidActionRequests(t, "/actions/"+actionID, "PUT")
}

// weaviate.action.patch
func Test__weaviate_actions_patch_JSON(t *testing.T) {
	// Create patch request
	newValue := int64(1337)
	newValueStr := "New string patched!"

	// Create JSON and do the request
	jsonStr := bytes.NewBuffer([]byte(fmt.Sprintf(`[{ "op": "replace", "path": "/schema/testInt", "value": %d}, { "op": "replace", "path": "/schema/testString", "value": "%s"}]`, newValue, newValueStr)))
	response := doRequest("/actions/"+actionID, "PATCH", "application/json", jsonStr, apiKeyCmdLine, apiTokenCmdLine)

	body := getResponseBody(response)

	respObject := &models.ActionGetResponse{}
	json.Unmarshal(body, respObject)

	// Check status code
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Check ID is the same
	require.Regexp(t, strfmt.UUIDPattern, respObject.ActionID)
	require.Regexp(t, strfmt.UUIDPattern, actionID)
	require.Equal(t, actionID, string(respObject.ActionID))

	// Check whether the returned information is the same as the data updated
	require.Equal(t, newValueStr, respObject.Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, newValue, int64(respObject.Schema.(map[string]interface{})["testInt"].(float64)))
	require.Equal(t, actionTestBoolean, respObject.Schema.(map[string]interface{})["testBoolean"].(bool))
	require.Equal(t, actionTestNumber, respObject.Schema.(map[string]interface{})["testNumber"].(float64))
	require.Equal(t, actionTestDate, respObject.Schema.(map[string]interface{})["testDateTime"].(string))
	require.Equal(t, thingID, string(respObject.Schema.(map[string]interface{})["testCref"].(map[string]interface{})["$cref"].(string)))

	// Check given creation time is after now, but not in the future
	now := connutils.NowUnix()
	require.Conditionf(t, func() bool { return !(respObject.LastUpdateTimeUnix > now) }, "LastUpdateTimeUnix is incorrect, it was set in the future.")
	require.Conditionf(t, func() bool { return !(respObject.LastUpdateTimeUnix < now-2000) }, "LastUpdateTimeUnix is incorrect, it was set to far back.")

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Check if patch is also applied on object when using a new GET request on same object
	responseGet := doRequest("/actions/"+actionID, "GET", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)

	bodyGet := getResponseBody(responseGet)

	// Test response obj
	respObjectGet := &models.ActionGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check whether the returned information is the same as the data updated
	require.Equal(t, newValueStr, respObjectGet.Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, newValue, int64(respObjectGet.Schema.(map[string]interface{})["testInt"].(float64)))
	require.Equal(t, actionTestBoolean, respObjectGet.Schema.(map[string]interface{})["testBoolean"].(bool))
	require.Equal(t, actionTestNumber, respObjectGet.Schema.(map[string]interface{})["testNumber"].(float64))
	require.Equal(t, actionTestDate, respObjectGet.Schema.(map[string]interface{})["testDateTime"].(string))
	require.Equal(t, thingID, string(respObjectGet.Schema.(map[string]interface{})["testCref"].(map[string]interface{})["$cref"].(string)))

	// Check patch with incorrect contents
	jsonStrError := bytes.NewBuffer([]byte(`{ "op": "replace", "path": "/xxxx", "value": "` + string(newValue) + `"}`))
	responseError := doRequest("/actions/"+actionID, "PATCH", "application/json", jsonStrError, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusBadRequest, responseError.StatusCode)

	// Create get request with non-existing ID, check its responsecode
	responseNotFound := doRequest("/actions/"+fakeID, "PATCH", "application/json", getEmptyPatchJSON(), apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, responseNotFound.StatusCode)
}

func Test__weaviate_actions_validate_JSON(t *testing.T) {
	// Test invalid requests
	performInvalidActionRequests(t, "/actions/validate", "POST")

	// Test valid request
	jsonStr := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://schema.org",
		"@class": "TestAction",
		"schema": {
			"testString": "%s",
			"testInt": %d,
			"testBoolean": %t,
			"testNumber": %f,
			"testDateTime": "%s",
			"testCref": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			}
		},
		"things": {
			"object": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			},
			"subject": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			}
		}
	}`, actionTestString, actionTestInt, actionTestBoolean, actionTestNumber, actionTestDate, thingID, getWeaviateURL(), thingID, getWeaviateURL(), thingIDsubject, getWeaviateURL())))
	response := doRequest("/actions/validate", "POST", "application/json", jsonStr, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusOK, response.StatusCode)
}

/******************
 * GRAPHQL TESTS
 ******************/
type graphQLQueryObject struct {
	Query string `json:"query,omitempty"`
}

func doGraphQLRequest(body graphQLQueryObject, apiKey string, apiToken string) (*http.Response, *models.GraphQLResponse) {
	// Marshal into json
	bodyJSON, _ := json.Marshal(body)

	// Make the IO input
	jsonStr := bytes.NewBuffer(bodyJSON)

	// Do the GraphQL request
	response := doRequest("/graphql", "POST", "application/json", jsonStr, apiKey, apiToken)

	// Turn the response into a response object
	respObject := &models.GraphQLResponse{}
	json.Unmarshal(getResponseBody(response), respObject)

	return response, respObject
}

func Test__weaviate_graphql_common_JSON(t *testing.T) {
	// Set the graphQL body
	bodyUnpr := `{ 
		"querys": "{ }" 
	}`

	// Make the IO input
	jsonStr := bytes.NewBuffer([]byte(bodyUnpr))

	// Do the GraphQL request
	responseUnpr := doRequest("/graphql", "POST", "application/json", jsonStr, apiKeyCmdLine, apiTokenCmdLine)

	// Check statuscode
	require.Equal(t, http.StatusUnprocessableEntity, responseUnpr.StatusCode)

	// Set the graphQL body
	bodyNonExistingProperty := graphQLQueryObject{
		Query: fmt.Sprintf(`{ action(uuid:"%s") { uuids atContext atClass creationTimeUnix things { object { uuid } subject { uuid } } key { uuid read } } }`, actionID),
	}

	// Do the GraphQL request
	responseNonExistingProperty, respObjectNonExistingProperty := doGraphQLRequest(bodyNonExistingProperty, apiKeyCmdLine, apiTokenCmdLine)

	// Check statuscode
	require.Equal(t, http.StatusOK, responseNonExistingProperty.StatusCode)

	// Test that the data in the response is nil
	require.Nil(t, respObjectNonExistingProperty.Data)

	// Test that the error in the response is not nil
	require.NotNil(t, respObjectNonExistingProperty.Errors)
}

func Test__weaviate_graphql_thing_JSON(t *testing.T) {
	// Set the graphQL body
	body := `{ thing(uuid:"%s") { uuid atContext atClass creationTimeUnix key { uuid read } } }`

	bodyObj := graphQLQueryObject{
		Query: fmt.Sprintf(body, thingID),
	}

	// Do the GraphQL request
	response, respObject := doGraphQLRequest(bodyObj, apiKeyCmdLine, apiTokenCmdLine)

	// Check statuscode
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Test that the error in the response is nil
	require.Nil(t, respObject.Errors)

	// Test the given UUID in the response
	respUUID := respObject.Data["thing"].(map[string]interface{})["uuid"]
	require.Regexp(t, strfmt.UUIDPattern, respUUID)
	require.Regexp(t, strfmt.UUIDPattern, thingID)
	require.Equal(t, thingID, respUUID)

	// Test the given creation time in the response
	respCreationTime := int64(respObject.Data["thing"].(map[string]interface{})["creationTimeUnix"].(float64))
	now := connutils.NowUnix()
	require.Conditionf(t, func() bool { return !(respCreationTime > now) }, "CreationTimeUnix is incorrect, it was set in the future.")
	require.Conditionf(t, func() bool { return !(respCreationTime < now-60000) }, "CreationTimeUnix is incorrect, it was set to far back.")

	// Test the given key-object in the response TODO when keys are implemented
	// respKeyUUID := respObject.Data["thing"].(map[string]interface{})["key"].(map[string]interface{})["uuid"]
	// require.Regexp(t, strfmt.UUIDPattern, respKeyUUID)
	// require.Regexp(t, strfmt.UUIDPattern, headID)
	// require.Equal(t, headID, respKeyUUID)

	// Test whether the key has read rights (must have)
	respKeyRead := respObject.Data["thing"].(map[string]interface{})["key"].(map[string]interface{})["read"].(bool)
	require.Equal(t, true, respKeyRead)

	// Test the related actions
	// Set the graphQL body
	body = `{ thing(uuid:"%s") { actions { actions { uuid atContext atClass creationTimeUnix } totalResults } } }`
	bodyObj = graphQLQueryObject{
		Query: fmt.Sprintf(body, thingID),
	}

	// Do the GraphQL request
	responseActions, respObjectActions := doGraphQLRequest(bodyObj, apiKeyCmdLine, apiTokenCmdLine)

	// Check statuscode
	require.Equal(t, http.StatusOK, responseActions.StatusCode)

	// Test that the error in the responseActions is nil
	require.Nil(t, respObjectActions.Errors)

	// Test total results
	totalResults := respObjectActions.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["totalResults"].(float64)
	require.Conditionf(t, func() bool { return totalResults >= 10 }, "Total results have to be higher or equal to 10.")

	// Check most recent
	respActionsUUID := respObjectActions.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["actions"].([]interface{})[0].(map[string]interface{})["uuid"].(string)
	require.Regexp(t, strfmt.UUIDPattern, respActionsUUID)
	require.Regexp(t, strfmt.UUIDPattern, actionIDs[0])
	require.Equal(t, actionIDs[0], string(respActionsUUID))

	// Set the graphQL body
	body = `{ thing(uuid:"%s") { actions(first:3) { actions { uuid atContext atClass creationTimeUnix } totalResults } } }`
	bodyObj = graphQLQueryObject{
		Query: fmt.Sprintf(body, thingID),
	}

	// Do the GraphQL request
	responseActionsLimit, respObjectActionsLimit := doGraphQLRequest(bodyObj, apiKeyCmdLine, apiTokenCmdLine)

	// Check statuscode
	require.Equal(t, http.StatusOK, responseActionsLimit.StatusCode)

	// Test that the error in the responseActions is nil
	require.Nil(t, respObjectActionsLimit.Errors)

	// Test total results
	totalResultsLimit := respObjectActionsLimit.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["totalResults"].(float64)
	require.Conditionf(t, func() bool { return totalResultsLimit >= 10 }, "Total results have to be higher or equal to 10.")

	// Test amount in current responseActions
	resultActionsLimit := respObjectActionsLimit.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["actions"].([]interface{})
	require.Len(t, resultActionsLimit, 3)

	// Test ID in the middle of the 3 results
	require.Equal(t, actionIDs[1], string(resultActionsLimit[1].(map[string]interface{})["uuid"].(string)))

	// Set the graphQL body
	body = `{ thing(uuid:"%s") { actions(first:5, offset:5) { actions { uuid atContext atClass creationTimeUnix } totalResults } } }`
	bodyObj = graphQLQueryObject{
		Query: fmt.Sprintf(body, thingID),
	}

	// Do the GraphQL request
	responseActionsLimitOffset, respObjectActionsLimitOffset := doGraphQLRequest(bodyObj, apiKeyCmdLine, apiTokenCmdLine)

	// Check statuscode
	require.Equal(t, http.StatusOK, responseActionsLimitOffset.StatusCode)

	// Test that the error in the responseActions is nil
	require.Nil(t, respObjectActionsLimitOffset.Errors)

	// Test total results
	totalResultsLimitOffset := respObjectActionsLimitOffset.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["totalResults"].(float64)
	require.Conditionf(t, func() bool { return totalResultsLimitOffset >= 10 }, "Total results have to be higher or equal to 10.")

	// Test amount in current responseActions
	resultActionsLimitOffset := respObjectActionsLimitOffset.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["actions"].([]interface{})
	require.Len(t, resultActionsLimitOffset, 5)

	// Test ID in the middle of the 3 results
	require.Equal(t, actionIDs[7], string(resultActionsLimitOffset[2].(map[string]interface{})["uuid"].(string)))

	// Search class 'TestAction2', most recent should be the set actionID[0]
	bodyObj = graphQLQueryObject{Query: fmt.Sprintf(`{ thing(uuid:"%s") { actions(class:"TestAction2", first:1) { actions { uuid atClass } totalResults } } }`, thingID)}
	_, respObjectValueSearch1 := doGraphQLRequest(bodyObj, apiKeyCmdLine, apiTokenCmdLine)
	resultActionsValueSearch1 := respObjectValueSearch1.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["actions"].([]interface{})
	require.Equal(t, actionIDs[0], string(resultActionsValueSearch1[0].(map[string]interface{})["uuid"].(string)))

	// // Search class NOT 'TestAction2', most recent should be the set actionID
	// bodyObj = graphQLQueryObject{Query: fmt.Sprintf(`{ thing(uuid:"%s") { actions(class:"!:TestAction2", first:1) { actions { uuid atClass } totalResults } } }`, thingID)}
	// _, respObjectValueSearch2 := doGraphQLRequest(bodyObj, apiKeyCmdLine, apiTokenCmdLine)
	// resultActionsValueSearch2 := respObjectValueSearch2.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["actions"].([]interface{})
	// require.Equal(t, actionID, string(resultActionsValueSearch2[0].(map[string]interface{})["uuid"].(string)))

	// // Search class '~TestAction', most recent 9 should be 'TestAction2' and the 10th is 'TestAction' (with uuid = actionID).
	// bodyObj = graphQLQueryObject{Query: fmt.Sprintf(`{ thing(uuid:"%s") { actions(class:"~TestAction", first:10) { actions { uuid atClass } totalResults } } }`, thingID)}
	// _, respObjectValueSearch3 := doGraphQLRequest(bodyObj, apiKeyCmdLine, apiTokenCmdLine)
	// resultActionsValueSearch3 := respObjectValueSearch3.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["actions"].([]interface{})
	// require.Equal(t, "TestAction2", string(resultActionsValueSearch3[3].(map[string]interface{})["atClass"].(string)))
	// require.Equal(t, "TestAction2", string(resultActionsValueSearch3[6].(map[string]interface{})["atClass"].(string)))
	// require.Equal(t, "TestAction", string(resultActionsValueSearch3[9].(map[string]interface{})["atClass"].(string)))
	// require.Equal(t, actionID, string(resultActionsValueSearch3[9].(map[string]interface{})["uuid"].(string)))

	// // Search class 'TestAction' AND 'schema:"testString:patch"', should find nothing.
	// bodyObj = graphQLQueryObject{Query: fmt.Sprintf(`{ thing(uuid:"%s") { actions(class:"TestAction", schema:"testString:patch", first:1) { actions { uuid atClass } totalResults } } }`, thingID)}
	// _, respObjectValueSearch4 := doGraphQLRequest(bodyObj, apiKeyCmdLine, apiTokenCmdLine)
	// totalResultsValueSearch4 := respObjectValueSearch4.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["totalResults"].(float64)
	// require.Equal(t, float64(0), totalResultsValueSearch4)

	// // Search class 'TestAction' AND 'schema:"testString:~patch"', should find ActionID as most recent.
	// bodyObj = graphQLQueryObject{Query: fmt.Sprintf(`{ thing(uuid:"%s") { actions(class:"TestAction", schema:"testString:~patch", first:1){ actions { uuid atClass } totalResults } } }`, thingID)}
	// _, respObjectValueSearch5 := doGraphQLRequest(bodyObj, apiKeyCmdLine, apiTokenCmdLine)
	// resultActionsValueSearch5 := respObjectValueSearch5.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["actions"].([]interface{})
	// require.Equal(t, actionID, string(resultActionsValueSearch5[0].(map[string]interface{})["uuid"].(string)))

	// // Search class '~TestAction' AND NOT 'schema:"testString:~patch"', should find thing with id == actionIDs[0] and have a length of 9
	// bodyObj = graphQLQueryObject{Query: fmt.Sprintf(`{ thing(uuid:"%s") { actions(class:"~TestAction", schema:"testString!:~patch", first:1){ actions { uuid atClass } totalResults } } }`, thingID)}
	// _, respObjectValueSearch6 := doGraphQLRequest(bodyObj, apiKeyCmdLine, apiTokenCmdLine)
	// resultActionsValueSearch6 := respObjectValueSearch6.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["actions"].([]interface{})
	// require.Equal(t, actionIDs[0], string(resultActionsValueSearch6[0].(map[string]interface{})["uuid"].(string)))
	// totalResultsValueSearch6 := respObjectValueSearch6.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["totalResults"].(float64)
	// require.Equal(t, float64(9), totalResultsValueSearch6)
}

func Test__weaviate_graphql_thing_list_JSON(t *testing.T) {
	// Set the graphQL body
	bodyObj := graphQLQueryObject{
		Query: `{ listThings { things { uuid atContext atClass creationTimeUnix } totalResults } }`,
	}

	// Do the GraphQL request
	response, respObject := doGraphQLRequest(bodyObj, apiKeyCmdLine, apiTokenCmdLine)

	// Check statuscode
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Test that the error in the response is nil
	require.Nil(t, respObject.Errors)

	// Test total results
	totalResults := respObject.Data["listThings"].(map[string]interface{})["totalResults"].(float64)
	require.Conditionf(t, func() bool { return totalResults >= 10 }, "Total results have to be higher or equal to 10.")

	// Check most recent
	respUUID := respObject.Data["listThings"].(map[string]interface{})["things"].([]interface{})[0].(map[string]interface{})["uuid"].(string)
	require.Regexp(t, strfmt.UUIDPattern, respUUID)
	require.Regexp(t, strfmt.UUIDPattern, thingIDs[0])
	require.Equal(t, thingIDs[0], string(respUUID))

	// Set the graphQL body
	bodyObj = graphQLQueryObject{
		Query: `{ listThings(first: 3) { things { uuid atContext atClass creationTimeUnix } totalResults } }`,
	}

	// Do the GraphQL request
	responseLimit, respObjectLimit := doGraphQLRequest(bodyObj, apiKeyCmdLine, apiTokenCmdLine)

	// Check statuscode
	require.Equal(t, http.StatusOK, responseLimit.StatusCode)

	// Test that the error in the response is nil
	require.Nil(t, respObjectLimit.Errors)

	// Test total results
	totalResultsLimit := respObjectLimit.Data["listThings"].(map[string]interface{})["totalResults"].(float64)
	require.Conditionf(t, func() bool { return totalResultsLimit >= 10 }, "Total results have to be higher or equal to 10.")

	// Test amount in current response
	resultThingsLimit := respObjectLimit.Data["listThings"].(map[string]interface{})["things"].([]interface{})
	require.Len(t, resultThingsLimit, 3)

	// Test ID in the middle of the 3 results
	require.Equal(t, thingIDs[1], string(resultThingsLimit[1].(map[string]interface{})["uuid"].(string)))

	// Set the graphQL body
	bodyObj = graphQLQueryObject{
		Query: `{ listThings(first: 5, offset: 5) { things { uuid atContext atClass creationTimeUnix } totalResults } }`,
	}

	// Do the GraphQL request
	responseLimitOffset, respObjectLimitOffset := doGraphQLRequest(bodyObj, apiKeyCmdLine, apiTokenCmdLine)

	// Check statuscode
	require.Equal(t, http.StatusOK, responseLimitOffset.StatusCode)

	// Test that the error in the response is nil
	require.Nil(t, respObjectLimitOffset.Errors)

	// Test total results
	totalResultsLimitOffset := respObjectLimitOffset.Data["listThings"].(map[string]interface{})["totalResults"].(float64)
	require.Conditionf(t, func() bool { return totalResultsLimitOffset >= 10 }, "Total results have to be higher or equal to 10.")

	// Test amount in current response
	resultThingsLimitOffset := respObjectLimitOffset.Data["listThings"].(map[string]interface{})["things"].([]interface{})
	require.Len(t, resultThingsLimitOffset, 5)

	// Test ID in the middle of the 3 results
	require.Equal(t, thingIDs[7], string(resultThingsLimitOffset[2].(map[string]interface{})["uuid"].(string)))

	// Search class 'TestThing2', most recent should be the set thingIDsubject
	bodyObj = graphQLQueryObject{Query: `{ listThings(class:"TestThing2", first:1) { things { uuid atClass } totalResults } }`}
	_, respObjectValueSearch1 := doGraphQLRequest(bodyObj, apiKeyCmdLine, apiTokenCmdLine)
	resultThingsValueSearch1 := respObjectValueSearch1.Data["listThings"].(map[string]interface{})["things"].([]interface{})
	require.Equal(t, thingIDsubject, string(resultThingsValueSearch1[0].(map[string]interface{})["uuid"].(string)))

	// // Search class NOT 'TestThing2', most recent should be the set thingID
	// bodyObj = graphQLQueryObject{Query: `{ listThings(class:"!:TestThing2", first:1) { things { uuid atClass } totalResults } }`}
	// _, respObjectValueSearch2 := doGraphQLRequest(bodyObj, apiKeyCmdLine, apiTokenCmdLine)
	// resultThingsValueSearch2 := respObjectValueSearch2.Data["listThings"].(map[string]interface{})["things"].([]interface{})
	// require.Equal(t, thingID, string(resultThingsValueSearch2[0].(map[string]interface{})["uuid"].(string)))

	// // Search class '~TestThing', most recent 9 should be 'TestThing2' and the 10th is 'TestThing' (with uuid = thingID).
	// // bodyObj = graphQLQueryObject{Query: `{ listThings(class:"~TestThing", first:10) { things { uuid atClass } totalResults } }`}
	// // _, respObjectValueSearch3 := doGraphQLRequest(bodyObj, apiKeyCmdLine, apiTokenCmdLine)
	// // resultThingsValueSearch3 := respObjectValueSearch3.Data["listThings"].(map[string]interface{})["things"].([]interface{})
	// // require.Equal(t, "TestThing2", string(resultThingsValueSearch3[3].(map[string]interface{})["atClass"].(string)))
	// // require.Equal(t, "TestThing2", string(resultThingsValueSearch3[6].(map[string]interface{})["atClass"].(string)))
	// // require.Equal(t, "TestThing", string(resultThingsValueSearch3[9].(map[string]interface{})["atClass"].(string)))
	// // require.Equal(t, thingID, string(resultThingsValueSearch3[9].(map[string]interface{})["uuid"].(string)))

	// // Search class 'TestThing' AND 'schema:"testString:patch"', should find nothing.
	// bodyObj = graphQLQueryObject{Query: `{ listThings(class:"TestThing", schema:"testString:patch", first:1) { things { uuid atClass } totalResults } }`}
	// _, respObjectValueSearch4 := doGraphQLRequest(bodyObj, apiKeyCmdLine, apiTokenCmdLine)
	// totalResultsValueSearch4 := respObjectValueSearch4.Data["listThings"].(map[string]interface{})["totalResults"].(float64)
	// require.Equal(t, float64(0), totalResultsValueSearch4)

	// // Search class 'TestThing' AND 'schema:"testString:~patch"', should find ThingID as most recent.
	// bodyObj = graphQLQueryObject{Query: `{ listThings(class:"TestThing", schema:"testString:~patch", first:1){ things { uuid atClass } totalResults } }`}
	// _, respObjectValueSearch5 := doGraphQLRequest(bodyObj, apiKeyCmdLine, apiTokenCmdLine)
	// resultThingsValueSearch5 := respObjectValueSearch5.Data["listThings"].(map[string]interface{})["things"].([]interface{})
	// require.Equal(t, thingID, string(resultThingsValueSearch5[0].(map[string]interface{})["uuid"].(string)))

	// // Search class '~TestThing' AND NOT 'schema:"testString:~patch"', should find thing with id == thingIDs[0]
	// // bodyObj = graphQLQueryObject{Query: `{ listThings(class:"~TestThing", schema:"testString!:~patch", first:1){ things { uuid atClass } totalResults } }`}
	// // _, respObjectValueSearch6 := doGraphQLRequest(bodyObj, apiKeyCmdLine, apiTokenCmdLine)
	// // resultThingsValueSearch6 := respObjectValueSearch6.Data["listThings"].(map[string]interface{})["things"].([]interface{})
	// // require.Equal(t, thingIDs[0], string(resultThingsValueSearch6[0].(map[string]interface{})["uuid"].(string)))
}

func Test__weaviate_graphql_action_JSON(t *testing.T) {
	// Set the graphQL body
	body := `{ action(uuid:"%s") { uuid atContext atClass creationTimeUnix things { object { uuid } subject { uuid } } key { uuid read } } }`
	bodyObj := graphQLQueryObject{
		Query: fmt.Sprintf(body, actionID),
	}

	// Do the GraphQL request
	response, respObject := doGraphQLRequest(bodyObj, apiKeyCmdLine, apiTokenCmdLine)

	// Check statuscode
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Test that the error in the response is nil
	require.Nil(t, respObject.Errors)

	// Test the given UUID in the response
	respUUID := respObject.Data["action"].(map[string]interface{})["uuid"]
	require.Regexp(t, strfmt.UUIDPattern, respUUID)
	require.Regexp(t, strfmt.UUIDPattern, actionID)
	require.Equal(t, actionID, respUUID)

	// Test the given thing-object in the response
	respObjectUUID := respObject.Data["action"].(map[string]interface{})["things"].(map[string]interface{})["object"].(map[string]interface{})["uuid"]
	require.Regexp(t, strfmt.UUIDPattern, respObjectUUID)
	require.Regexp(t, strfmt.UUIDPattern, thingID)
	require.Equal(t, thingID, respObjectUUID)

	// Test the given thing-object in the response
	respSubjectUUID := respObject.Data["action"].(map[string]interface{})["things"].(map[string]interface{})["subject"].(map[string]interface{})["uuid"]
	require.Regexp(t, strfmt.UUIDPattern, respSubjectUUID)
	require.Regexp(t, strfmt.UUIDPattern, thingIDsubject)
	require.Equal(t, thingIDsubject, respSubjectUUID)
}

func Test__weaviate_graphql_key_JSON(t *testing.T) {
	// // Set the graphQL body
	// body := `{ key(uuid:"%s") { uuid read write ipOrigin parent { uuid read } } }`
	// bodyObj := graphQLQueryObject{
	// 	Query: fmt.Sprintf(body, keyID),
	// }

	// // Make the IO input
	// jsonStr := bytes.NewBuffer([]byte(fmt.Sprintf(body, newAPIKeyID)))

	// // Do the GraphQL request
	// response := doGraphQLRequest(jsonStr, apiKeyCmdLine, apiTokenCmdLine)

	// // Check statuscode
	// require.Equal(t, http.StatusOK, response.StatusCode)

	// // Turn the response into a response object
	// respObject := &models.GraphQLResponse{}
	// json.Unmarshal(getResponseBody(response), respObject)

	// // Test that the error in the response is nil
	// require.Nil(t, respObject.Errors)

	// // Test the given UUID in the response
	// respUUID := respObject.Data["key"].(map[string]interface{})["uuid"]
	// require.Regexp(t, strfmt.UUIDPattern, respUUID)
	// require.Regexp(t, strfmt.UUIDPattern, newAPIKeyID)
	// require.Equal(t, newAPIKeyID, respUUID)

	// TODO: check parent when key tests are implemented further
	// TODO: test children
}

/******************
 * REMOVE TESTS
 ******************/

// // weaviate.key.me.delete
// func Test__weaviate_key_me_delete_JSON(t *testing.T) {
// 	// Delete keyID from database
// 	responseKeyIDDeleted := doRequest("/keys/me", "DELETE", "application/json", nil, newAPIToken)
// 	testStatusCode(t, responseKeyIDDeleted.StatusCode, http.StatusNoContent)
//  TODO: test token in response at right time
// }

// weaviate.action.delete
func Test__weaviate_actions_delete_JSON(t *testing.T) {
	// Create delete request
	response := doRequest("/actions/"+actionID, "DELETE", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)

	// Check status code get request
	require.Equal(t, http.StatusNoContent, response.StatusCode)

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Create delete request
	responseAlreadyDeleted := doRequest("/actions/"+actionID, "DELETE", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)

	// Check status code already deleted
	require.Equal(t, http.StatusNotFound, responseAlreadyDeleted.StatusCode)

	// Create get request with non-existing ID, check its responsecode
	responseNotFound := doRequest("/actions/"+fakeID, "DELETE", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, responseNotFound.StatusCode)
}

// weaviate.thing.delete
func Test__weaviate_things_delete_JSON(t *testing.T) {
	// Test whether all actions aren't deleted yet
	for _, deletedActionID := range actionIDs {
		// Skip the action ID that is deleted with the previous function
		if deletedActionID == actionID {
			continue
		}

		responseNotYetDeletedByObjectThing := doRequest("/actions/"+deletedActionID, "GET", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)
		require.Equal(t, http.StatusOK, responseNotYetDeletedByObjectThing.StatusCode)
	}

	// Create delete request
	response := doRequest("/things/"+thingID, "DELETE", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)

	// Check status code get request
	require.Equal(t, http.StatusNoContent, response.StatusCode)

	// Test is faster than adding to DB.
	time.Sleep(1 * time.Second)

	// Create delete request
	responseAlreadyDeleted := doRequest("/things/"+thingID, "DELETE", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)

	// Check status code already deleted
	require.Equal(t, http.StatusNotFound, responseAlreadyDeleted.StatusCode)

	// Create get request with non-existing ID, check its responsecode
	responseNotFound := doRequest("/things/"+fakeID, "DELETE", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, responseNotFound.StatusCode)

	// Test whether all actions are deleted
	for _, deletedActionID := range actionIDs {
		responseDeletedByObjectThing := doRequest("/actions/"+deletedActionID, "GET", "application/json", nil, apiKeyCmdLine, apiTokenCmdLine)
		require.Equal(t, http.StatusNotFound, responseDeletedByObjectThing.StatusCode)
	}
}
