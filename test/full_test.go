/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package test

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/creativesoftwarefdn/weaviate/messages"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/creativesoftwarefdn/weaviate/connectors/utils"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"
	"github.com/creativesoftwarefdn/weaviate/validation"
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

// getEmptyJSON returns a buffer with empty JSON
func getEmptyJSON() io.Reader {
	return bytes.NewBuffer([]byte(`{}`))
}

// getEmptyPatchJSON returns a buffer with empty Patch-JSON
func getEmptyPatchJSON() io.Reader {
	return bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/xxx", "value": "xxx"}]`))
}

func getWeaviateURL() string {
	return fmt.Sprintf("%s://%s:%s", serverScheme, serverHost, serverPort)
}

// Set all re-used vars
var serverPort string
var serverHost string
var serverScheme string

// Keys structure
//				 apiKeyIDCmdLine
//				/				\
// 			 newAPIKeyID		expiredKeyID
//			/			\
//		 headKeyID	   newSubAPIKeyID
//		 /		 \
// sub1KeyID	sub2KeyID

var apiKeyIDCmdLine string
var apiTokenCmdLine string

var expiredToken string
var expiredKeyID string

var headToken string
var headKeyID string

var newAPIToken string
var newAPIKeyID string

var newSubAPIToken string
var newSubAPIKeyID string

var sub1Token string
var sub1KeyID string

var sub2Token string
var sub2KeyID string

var actionID string
var actionIDs [10]string
var actionIDExternal string
var thingID string
var thingIDs [10]string
var thingIDExternal string
var thingIDsubject string
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

const (
	newStringValue  string = "new_value"
	newStringValue2 string = "new_value_2"
	newIntValue     int64  = 12345
	newIntValue2    int64  = 67890
	fakeID          string = "11111111-1111-1111-1111-111111111111"
)

var messaging *messages.Messaging

func init() {
	flag.StringVar(&apiKeyIDCmdLine, "api-key", "", "API-KEY as used as haeder in the tests.")
	flag.StringVar(&apiTokenCmdLine, "api-token", "", "API-KEY as used as haeder in the tests.")
	flag.StringVar(&serverPort, "server-port", "", "Port number on which the server is running.")
	flag.StringVar(&serverHost, "server-host", "", "Host-name on which the server is running.")
	flag.StringVar(&serverScheme, "server-scheme", "", "Scheme on which the server is running.")
	flag.Parse()

	if serverScheme == "" {
		serverScheme = "http"
	}

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
		req.Header.Set("X-API-KEY", apiKeyIDCmdLine)
		req.Header.Set("X-API-TOKEN", apiTokenCmdLine)

		resp, err := client.Do(req)
		if err != nil {
			messaging.DebugMessage(fmt.Sprintf("Error in response occurred during benchmark: %s", err.Error()))
		}

		resp.Body.Close()
		// req.Body.Close()

	}
}

/******************
 * META TESTS
 ******************/

func Test__weaviate_GET_meta_JSON_internal(t *testing.T) {
	response := doRequest("/meta", "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

	// Check status code of create
	require.Equal(t, http.StatusOK, response.StatusCode)

	body := getResponseBody(response)

	respObject := &models.Meta{}
	json.Unmarshal(body, respObject)

	// Check whether the returned information is the same as the data added
	require.Equal(t, getWeaviateURL(), respObject.Hostname)

	// Check both ontologies are added
	require.NotNil(t, respObject.ActionsSchema)
	require.IsType(t, &models.SemanticSchema{}, respObject.ActionsSchema)
	require.NotNil(t, respObject.ThingsSchema)
	require.IsType(t, &models.SemanticSchema{}, respObject.ThingsSchema)
}

func Test__weaviate_GET_meta_JSON_internal_missing_headers(t *testing.T) {
	// Missing token in request
	responseMissingToken := doRequest("/meta", "GET", "application/json", nil, apiKeyIDCmdLine, "")

	// Check status code and message of error
	require.Equal(t, http.StatusUnauthorized, responseMissingToken.StatusCode)
	require.Contains(t, string(getResponseBody(responseMissingToken)), connutils.StaticMissingHeader)

	// Missing key in request
	responseMissingKey := doRequest("/meta", "GET", "application/json", nil, "", apiTokenCmdLine)

	// Check status code and message of error
	require.Equal(t, http.StatusUnauthorized, responseMissingKey.StatusCode)
	require.Contains(t, string(getResponseBody(responseMissingKey)), connutils.StaticMissingHeader)
}
func Test__weaviate_GET_meta_JSON_internal_invalid_headers(t *testing.T) {
	// Invalid token in request
	responseInvalidToken := doRequest("/meta", "GET", "application/json", nil, apiKeyIDCmdLine, fakeID)

	// Check status code and message of error
	require.Equal(t, http.StatusUnauthorized, responseInvalidToken.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidToken)), connutils.StaticInvalidToken)

	// Invalid key in request
	responseInvalidKey := doRequest("/meta", "GET", "application/json", nil, fakeID, apiTokenCmdLine)

	// Check status code and message of error
	require.Equal(t, http.StatusUnauthorized, responseInvalidKey.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidKey)), connutils.StaticKeyNotFound)
}

/******************
 * KEY TESTS
 ******************/
func Test__weaviate_POST_keys_JSON_internal_root_child(t *testing.T) {
	// Create create request
	jsonStr := bytes.NewBuffer([]byte(`{
		"delete": true,
		"email": "string",
		"ipOrigin": ["127.0.0.*", "*"],
		"keyExpiresUnix": -1,
		"read": false,
		"write": false,
		"execute": true
	}`))
	response := doRequest("/keys", "POST", "application/json", jsonStr, apiKeyIDCmdLine, apiTokenCmdLine)

	// Check status code of create
	require.Equal(t, http.StatusOK, response.StatusCode)
	body := getResponseBody(response)
	respObject := &models.KeyTokenGetResponse{}
	json.Unmarshal(body, respObject)

	// Test Rights
	require.Equal(t, true, respObject.Delete)
	require.Equal(t, true, respObject.Execute)
	require.Equal(t, false, respObject.Read)
	require.Equal(t, false, respObject.Write)

	// Test given Token
	newAPIToken = string(respObject.Token)
	newAPIKeyID = string(respObject.KeyID)

	// Check UUID-format
	require.Regexp(t, strfmt.UUIDPattern, newAPIToken)
	require.Regexp(t, strfmt.UUIDPattern, newAPIKeyID)
}

func Test__weaviate_POST_keys_JSON_internal_root_child_child(t *testing.T) {
	// ADD NEW KEY
	// Create Sub API-Key
	jsonStrNewKey := bytes.NewBuffer([]byte(`{
		"delete": false,
		"email": "string",
		"ipOrigin": ["127.0.0.*", "*"],
		"keyExpiresUnix": -1,
		"read": true,
		"write": true,
		"execute": false
	}`))
	responseNewToken := doRequest("/keys", "POST", "application/json", jsonStrNewKey, newAPIKeyID, newAPIToken)

	// Test second statuscode
	require.Equal(t, http.StatusOK, responseNewToken.StatusCode)

	// Process response
	bodyNewToken := getResponseBody(responseNewToken)
	respObjectNewToken := &models.KeyTokenGetResponse{}
	json.Unmarshal(bodyNewToken, respObjectNewToken)

	// Test key ID parent is correct
	require.Equal(t, newAPIKeyID, string(respObjectNewToken.Parent.NrDollarCref))

	// Test given Token
	newSubAPIToken = string(respObjectNewToken.Token)
	newSubAPIKeyID = string(respObjectNewToken.KeyID)

	// Check UUID-format
	require.Regexp(t, strfmt.UUIDPattern, newSubAPIToken)
	require.Regexp(t, strfmt.UUIDPattern, newSubAPIKeyID)

	// Test expiration set
	require.Equal(t, int64(-1), respObjectNewToken.KeyExpiresUnix)

	// Test Rights
	require.Equal(t, false, respObjectNewToken.Delete)
	require.Equal(t, false, respObjectNewToken.Execute)
	require.Equal(t, true, respObjectNewToken.Read)
	require.Equal(t, true, respObjectNewToken.Write)
}

func Test__weaviate_POST_keys_JSON_internal_root_child_child_2(t *testing.T) {
	// HEAD: Create create request tree-head and process request, for further tests
	jsonStrKeyHead := bytes.NewBuffer([]byte(`{
		"delete": true,
		"email": "string",
		"ipOrigin": ["127.0.0.*", "*"],
		"keyExpiresUnix": -1,
		"read": true,
		"write": true,
		"execute": true
	}`))
	responseHead := doRequest("/keys", "POST", "application/json", jsonStrKeyHead, newAPIKeyID, newAPIToken)

	// Check request
	require.Equal(t, responseHead.StatusCode, http.StatusOK)

	// Translate body
	bodyHead := getResponseBody(responseHead)
	respObjectHead := &models.KeyTokenGetResponse{}
	json.Unmarshal(bodyHead, respObjectHead)

	// Set reusable keys
	headToken = string(respObjectHead.Token)
	headKeyID = string(respObjectHead.KeyID)
}

func Test__weaviate_POST_keys_JSON_internal_root_child_child_child_1(t *testing.T) {
	// SUB1: Create create request and process request
	jsonStrKeySub1 := bytes.NewBuffer([]byte(`{
		"delete": true,
		"email": "string",
		"ipOrigin": ["127.0.0.*", "*"],
		"keyExpiresUnix": -1,
		"read": true,
		"write": true,
		"execute": true
	}`))
	responseSub1 := doRequest("/keys", "POST", "application/json", jsonStrKeySub1, headKeyID, headToken)

	// Check status code
	require.Equal(t, http.StatusOK, responseSub1.StatusCode)

	// Translate body
	bodySub1 := getResponseBody(responseSub1)
	respObjectSub1 := &models.KeyTokenGetResponse{}
	json.Unmarshal(bodySub1, respObjectSub1)

	// Set reusable keys
	sub1Token = string(respObjectSub1.Token)
	sub1KeyID = string(respObjectSub1.KeyID)
}
func Test__weaviate_POST_keys_JSON_internal_root_child_child_child_2(t *testing.T) {
	// SUB2: Create create request and process request
	jsonStrKeySub2 := bytes.NewBuffer([]byte(`{
		"delete": true,
		"email": "string",
		"ipOrigin": ["127.0.0.*", "*"],
		"keyExpiresUnix": -1,
		"read": true,
		"write": true,
		"execute": true
	}`))
	responseSub2 := doRequest("/keys", "POST", "application/json", jsonStrKeySub2, headKeyID, headToken)

	// Check status code
	require.Equal(t, http.StatusOK, responseSub2.StatusCode)

	// Translate Body
	bodySub2 := getResponseBody(responseSub2)
	respObjectSub2 := &models.KeyTokenGetResponse{}
	json.Unmarshal(bodySub2, respObjectSub2)

	// Set reusable keys
	sub2Token = string(respObjectSub2.Token)
	sub2KeyID = string(respObjectSub2.KeyID)
}

func Test__weaviate_POST_keys_JSON_internal_expire_soon(t *testing.T) {
	// Create create request with a key that will expire soon
	unixTimeExpire = connutils.NowUnix()
	jsonStrNewKeySoonExpire := bytes.NewBuffer([]byte(`{
		"delete": false,
		"email": "expiredkey",
		"ipOrigin": ["127.0.0.*", "*"],
		"keyExpiresUnix": ` + strconv.FormatInt(unixTimeExpire+2000, 10) + `,
		"read": true,
		"write": true,
		"execute": false
	}`))
	responseNewTokenSoonExpire := doRequest("/keys", "POST", "application/json", jsonStrNewKeySoonExpire, apiKeyIDCmdLine, apiTokenCmdLine)

	// Test second statuscode
	require.Equal(t, http.StatusOK, responseNewTokenSoonExpire.StatusCode)

	bodyExpireSoon := getResponseBody(responseNewTokenSoonExpire)
	respObjectExpireSoon := &models.KeyTokenGetResponse{}
	json.Unmarshal(bodyExpireSoon, respObjectExpireSoon)

	// Set global keys
	expiredToken = string(respObjectExpireSoon.Token)
	expiredKeyID = string(respObjectExpireSoon.KeyID)
}

func Test__weaviate_POST_keys_JSON_internal_expires_later_than_parent(t *testing.T) {
	// Create request that is invalid because time is lower then parent time
	jsonStrNewKeyInvalid := bytes.NewBuffer([]byte(`{
		"delete": false,
		"email": "string",
		"ipOrigin": ["127.0.0.*", "*"],
		"keyExpiresUnix": ` + strconv.FormatInt(unixTimeExpire+3000, 10) + `,
		"read": true,
		"write": true,
		"execute": false
	}`))
	responseNewTokenInvalid := doRequest("/keys", "POST", "application/json", jsonStrNewKeyInvalid, expiredKeyID, expiredToken)

	require.Equal(t, http.StatusUnprocessableEntity, responseNewTokenInvalid.StatusCode)
	require.Contains(t, string(getResponseBody(responseNewTokenInvalid)), "Key expiry time is later than the expiry time of parent.")
}

func Test__weaviate_POST_keys_JSON_internal_expires_in_past(t *testing.T) {
	// Create request that is invalid because time is lower then parent time
	jsonStrNewKeyInvalid := bytes.NewBuffer([]byte(`{
		"delete": false,
		"email": "string",
		"ipOrigin": ["127.0.0.*", "*"],
		"keyExpiresUnix": ` + strconv.FormatInt(connutils.NowUnix()-1000, 10) + `,
		"read": true,
		"write": true,
		"execute": false
	}`))
	responseNewTokenInvalid := doRequest("/keys", "POST", "application/json", jsonStrNewKeyInvalid, expiredKeyID, expiredToken)

	require.Equal(t, http.StatusUnprocessableEntity, responseNewTokenInvalid.StatusCode)
	require.Contains(t, string(getResponseBody(responseNewTokenInvalid)), "Key expiry time is in the past.")
}

func Test__weaviate_GET_keys_me_JSON_internal(t *testing.T) {
	// Create get request
	response := doRequest("/keys/me", "GET", "application/json", nil, newAPIKeyID, newAPIToken)

	// Check status code get requestsOK
	require.Equal(t, http.StatusOK, response.StatusCode)

	body := getResponseBody(response)
	respObject := &models.KeyTokenGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID of object
	require.Equal(t, newAPIKeyID, string(respObject.KeyID))
}

func Test__weaviate_GET_keys_me_JSON_internal_expired(t *testing.T) {
	// Create get request with key that is not yet expired
	responseNotYetExpired := doRequest("/keys/me", "GET", "application/json", nil, expiredKeyID, expiredToken)
	require.Equal(t, http.StatusOK, responseNotYetExpired.StatusCode)

	// Wait until key is expired
	time.Sleep(3 * time.Second)

	// Create get request with key that is expired (same as above)
	responseExpired := doRequest("/keys/me", "GET", "application/json", nil, expiredKeyID, expiredToken)
	require.Equal(t, http.StatusUnauthorized, responseExpired.StatusCode)
	require.Contains(t, string(getResponseBody(responseExpired)), "expired")
}

func Test__weaviate_GET_keys_id_JSON_internal(t *testing.T) {
	// Create get request
	response := doRequest("/keys/"+newAPIKeyID, "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

	// Check status code get requestsOK)
	require.Equal(t, http.StatusOK, response.StatusCode)

	body := getResponseBody(response)

	respObject := &models.KeyGetResponse{}
	json.Unmarshal(body, respObject)

	// Check ID of object
	require.Equal(t, newAPIKeyID, string(respObject.KeyID))
}

func Test__weaviate_GET_keys_id_JSON_internal_forbidden(t *testing.T) {
	// Create get request
	responseForbidden := doRequest("/keys/"+apiKeyIDCmdLine, "GET", "application/json", nil, newAPIKeyID, newAPIToken)

	// Check status code forbidden request
	require.Equal(t, responseForbidden.StatusCode, http.StatusForbidden)
}

func Test__weaviate_GET_keys_id_JSON_internal_not_found(t *testing.T) {
	// Create get request with non-existing ID, check its responsecode
	responseNotFound := doRequest("/keys/"+fakeID, "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, responseNotFound.StatusCode)
}

func Test__weaviate_GET_keys_id_children_JSON_internal(t *testing.T) {

	// Create get request
	response := doRequest("/keys/"+newAPIKeyID+"/children", "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

	// Check status code get requestsOK
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Translate body
	body := getResponseBody(response)
	respObject := &models.KeyChildrenGetResponse{}
	json.Unmarshal(body, respObject)

	// Check the number of children corresponds the added number
	require.Len(t, respObject.Children, 2)

	// Check IDs of objects are correct by adding them to an array and sorting
	responseChildren := []string{
		string(respObject.Children[0].NrDollarCref),
		string(respObject.Children[1].NrDollarCref),
	}

	checkIDs := []string{
		headKeyID,
		newSubAPIKeyID,
	}

	// Sort keys
	sort.Strings(responseChildren)
	sort.Strings(checkIDs)

	// Test the keys
	require.Equal(t, checkIDs[0], responseChildren[0])
	require.Equal(t, checkIDs[1], responseChildren[1])
}

func Test__weaviate_GET_keys_id_children_JSON_internal_forbidden(t *testing.T) {
	// Create get forbidden request, check response code
	responseForbidden := doRequest("/keys/"+apiKeyIDCmdLine+"/children", "GET", "application/json", nil, newAPIKeyID, newAPIToken)
	require.Equal(t, http.StatusForbidden, responseForbidden.StatusCode)
}

func Test__weaviate_GET_keys_id_children_JSON_internal_not_found(t *testing.T) {
	// Create get request with non-existing ID, check its responsecode
	responseNotFound := doRequest("/keys/"+fakeID+"/children", "GET", "application/json", nil, newAPIKeyID, newAPIToken)
	require.Equal(t, http.StatusNotFound, responseNotFound.StatusCode)
}

func Test__weaviate_GET_keys_me_children_JSON_internal_(t *testing.T) {
	// Create get request
	response := doRequest("/keys/me/children", "GET", "application/json", nil, newAPIKeyID, newAPIToken)

	// Check status code get requestsOK
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Translate body
	body := getResponseBody(response)
	respObject := &models.KeyChildrenGetResponse{}
	json.Unmarshal(body, respObject)

	// Check the number of children corresponds the added number
	require.Len(t, respObject.Children, 2)

	// Check IDs of objects are correct by adding them to an array and sorting
	responseChildren := []string{
		string(respObject.Children[0].NrDollarCref),
		string(respObject.Children[1].NrDollarCref),
	}

	checkIDs := []string{
		headKeyID,
		newSubAPIKeyID,
	}

	// Sort keys
	sort.Strings(responseChildren)
	sort.Strings(checkIDs)

	// Test the keys
	require.Equal(t, checkIDs[0], responseChildren[0])
	require.Equal(t, checkIDs[1], responseChildren[1])
}

func Test__weaviate_PUT_keys_id_renew_token_JSON_internal(t *testing.T) {
	// Create renew request
	response := doRequest("/keys/"+headKeyID+"/renew-token", "PUT", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

	// Check status code get requestsOK
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Translate body
	body := getResponseBody(response)
	respObject := &models.KeyTokenGetResponse{}
	json.Unmarshal(body, respObject)

	// Update headtoken
	headToken = string(respObject.Token)

	// Create get request with this token and key of changed key
	responseAfterRenew := doRequest("/keys/me", "GET", "application/json", nil, headKeyID, headToken)

	// Check status code get requestsOK
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Translate body
	bodyAfterRenew := getResponseBody(responseAfterRenew)
	respObjectAfterRenew := &models.KeyGetResponse{}
	json.Unmarshal(bodyAfterRenew, respObjectAfterRenew)

	// Check ID of object
	require.Equal(t, headKeyID, string(respObjectAfterRenew.KeyID))
}

func Test__weaviate_PUT_keys_id_renew_token_JSON_internal_forbidden_parent(t *testing.T) {
	// Create renew request for higher in tree, this is forbidden
	responseForbidden := doRequest("/keys/"+newAPIKeyID+"/renew-token", "PUT", "application/json", nil, headKeyID, headToken)
	require.Equal(t, responseForbidden.StatusCode, http.StatusForbidden)
}

func Test__weaviate_PUT_keys_id_renew_token_JSON_internal_forbidden_own(t *testing.T) {
	// Create renew request for own key, this is forbidden
	responseForbidden2 := doRequest("/keys/"+headKeyID+"/renew-token", "PUT", "application/json", nil, headKeyID, headToken)
	require.Equal(t, responseForbidden2.StatusCode, http.StatusForbidden)
}

func Test__weaviate_PUT_keys_id_renew_token_JSON_internal_not_found(t *testing.T) {
	// Create get request with non-existing ID, check its responsecode
	responseNotFound := doRequest("/keys/"+fakeID+"/renew-token", "PUT", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, responseNotFound.StatusCode)
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
	responseInvalid1 := doRequest(uri, method, "application/json", jsonStrInvalid1, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid1.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid1)), validation.ErrorMissingClass)

	// Test missing context
	jsonStrInvalid2 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@class": "TestThing",
		"schema": {
			"testString": "%s"
		}
	}`, thingTestString)))
	responseInvalid2 := doRequest(uri, method, "application/json", jsonStrInvalid2, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid2.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid2)), validation.ErrorMissingContext)

	// Test non-existing class
	jsonStrInvalid3 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestThings",
		"schema": {
			"testString": "%s"
		}
	}`, thingTestString)))
	responseInvalid3 := doRequest(uri, method, "application/json", jsonStrInvalid3, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid3.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid3)), fmt.Sprintf(schema.ErrorNoSuchClass, "TestThings"))

	// Test non-existing property
	jsonStrInvalid4 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestThing",
		"schema": {
			"testStrings": "%s"
		}
	}`, thingTestString)))
	responseInvalid4 := doRequest(uri, method, "application/json", jsonStrInvalid4, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid4.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid4)), fmt.Sprintf(schema.ErrorNoSuchProperty, "testStrings", "TestThing"))

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
	responseInvalid5 := doRequest(uri, method, "application/json", jsonStrInvalid5, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid5.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid5)), fmt.Sprintf(validation.ErrorInvalidSingleRef, "TestThing", "testCref"))

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
	responseInvalid6 := doRequest(uri, method, "application/json", jsonStrInvalid6, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid6.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid6)), fmt.Sprintf(validation.ErrorMissingSingleRefLocationURL, "TestThing", "testCref"))

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
	responseInvalid7 := doRequest(uri, method, "application/json", jsonStrInvalid7, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid7.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid7)), fmt.Sprintf(validation.ErrorInvalidClassType, "TestThing", "testCref", connutils.RefTypeAction, connutils.RefTypeThing, connutils.RefTypeKey))

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
	responseInvalid7b := doRequest(uri, method, "application/json", jsonStrInvalid7b, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid7b.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid7b)), connutils.StaticThingNotFound)

	// Test invalid property cref5: invalid LocationURL
	jsonStrInvalid7c := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestThing",
		"schema": {
			"testCref": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			}
		}
	}`, thingID, "http://example.org/")))
	responseInvalid7c := doRequest(uri, method, "application/json", jsonStrInvalid7c, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid7c.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid7c)), fmt.Sprintf(validation.ErrorNoExternalCredentials, "http://example.org/", "'cref' Thing TestThing:testCref"))

	// Test invalid property cref6: valid locationURL, invalid ThingID
	jsonStrInvalid7d := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestThing",
		"schema": {
			"testCref": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			}
		}
	}`, fakeID, "http://localhost:"+serverPort)))
	responseInvalid7d := doRequest(uri, method, "application/json", jsonStrInvalid7d, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid7d.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid7d)), fmt.Sprintf(validation.ErrorExternalNotFound, "http://localhost:"+serverPort, 404, ""))

	// Test invalid property string
	jsonStrInvalid8 := bytes.NewBuffer([]byte(`{
		"@context": "http://example.org",
		"@class": "TestThing",
		"schema": {
			"testString": 2
		}
	}`))
	responseInvalid8 := doRequest(uri, method, "application/json", jsonStrInvalid8, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid8.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid8)), fmt.Sprintf(validation.ErrorInvalidString, "TestThing", "testString", 2))

	// Test invalid property int
	jsonStrInvalid9 := bytes.NewBuffer([]byte(`{
		"@context": "http://example.org",
		"@class": "TestThing",
		"schema": {
			"testInt": 2.7
		}
	}`))
	responseInvalid9 := doRequest(uri, method, "application/json", jsonStrInvalid9, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid9.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid9)), fmt.Sprintf(validation.ErrorInvalidInteger, "TestThing", "testInt", 2.7))

	// Test invalid property float
	jsonStrInvalid10 := bytes.NewBuffer([]byte(`{
		"@context": "http://example.org",
		"@class": "TestThing",
		"schema": {
			"testNumber": "test"
		}
	}`))
	responseInvalid10 := doRequest(uri, method, "application/json", jsonStrInvalid10, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid10.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid10)), fmt.Sprintf(validation.ErrorInvalidFloat, "TestThing", "testNumber", "test"))

	// Test invalid property bool
	jsonStrInvalid11 := bytes.NewBuffer([]byte(`{
		"@context": "http://example.org",
		"@class": "TestThing",
		"schema": {
			"testBoolean": "test"
		}
	}`))
	responseInvalid11 := doRequest(uri, method, "application/json", jsonStrInvalid11, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid11.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid11)), fmt.Sprintf(validation.ErrorInvalidBool, "TestThing", "testBoolean", "test"))

	// Test invalid property date
	jsonStrInvalid12 := bytes.NewBuffer([]byte(`{
		"@context": "http://example.org",
		"@class": "TestThing",
		"schema": {
			"testDateTime": "test"
		}
	}`))
	responseInvalid12 := doRequest(uri, method, "application/json", jsonStrInvalid12, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid12.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid12)), fmt.Sprintf(validation.ErrorInvalidDate, "TestThing", "testDateTime", "test"))
}

func Test__weaviate_POST_things_JSON_internal(t *testing.T) {
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
	response := doRequest("/things", "POST", "application/json", jsonStr, apiKeyIDCmdLine, apiTokenCmdLine)

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
}

func Test__weaviate_POST_things_JSON_external(t *testing.T) {
	// Create create request
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
	}`, thingTestString, thingTestInt, thingTestBoolean, thingTestNumber, thingTestDate, thingID, "http://localhost:"+serverPort)))
	response := doRequest("/things", "POST", "application/json", jsonStr, apiKeyIDCmdLine, apiTokenCmdLine)

	// Check status code of create
	require.Equal(t, http.StatusAccepted, response.StatusCode)

	body := getResponseBody(response)

	respObject := &models.ThingGetResponse{}
	json.Unmarshal(body, respObject)

	// Check whether generated UUID is added
	require.Regexp(t, strfmt.UUIDPattern, respObject.ThingID)

	// Globally set thingID
	thingIDExternal = string(respObject.ThingID)
}
func Test__weaviate_POST_things_JSON_internal_multiple(t *testing.T) {
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
		response := doRequest("/things", "POST", "application/json", jsonStr, apiKeyIDCmdLine, apiTokenCmdLine)
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
		time.Sleep(1000 * time.Millisecond)
	}

	// Test is faster than adding to DB.
	time.Sleep(1000 * time.Millisecond)
}

func Test__weaviate_POST_things_JSON_internal_invalid(t *testing.T) {
	// Create invalid requests
	performInvalidThingRequests(t, "/things", "POST")
}

func Test__weaviate_POST_things_JSON_internal_forbidden(t *testing.T) {
	// Create forbidden requests
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
	response := doRequest("/things", "POST", "application/json", jsonStr, newAPIKeyID, newAPIToken)

	// Check status code of create
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}

func Test__weaviate_GET_things_JSON_internal(t *testing.T) {
	// Create list request
	response := doRequest("/things", "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

	// Check status code of list
	require.Equal(t, http.StatusOK, response.StatusCode)

	body := getResponseBody(response)

	listResponseObject := &models.ThingsListResponse{}
	json.Unmarshal(body, listResponseObject)

	// Check most recent
	require.Regexp(t, strfmt.UUIDPattern, listResponseObject.Things[0].ThingID)
	require.Regexp(t, strfmt.UUIDPattern, thingIDs[0])
	require.Equal(t, thingIDs[0], string(listResponseObject.Things[0].ThingID))
}

func Test__weaviate_GET_things_JSON_internal_forbidden_read(t *testing.T) {
	// Create list request
	response := doRequest("/things", "GET", "application/json", nil, newAPIKeyID, newAPIToken)

	// Check status code of list
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}

func Test__weaviate_GET_things_JSON_internal_nothing(t *testing.T) {
	// Create list request
	response := doRequest("/things", "GET", "application/json", nil, headKeyID, headToken)

	// Check status code of list
	require.Equal(t, http.StatusOK, response.StatusCode)

	body := getResponseBody(response)

	listResponseObject := &models.ThingsListResponse{}
	json.Unmarshal(body, listResponseObject)

	// Check most recent
	require.Len(t, listResponseObject.Things, 0)
}

func Test__weaviate_GET_things_JSON_internal_limit(t *testing.T) {
	// Query whole list just created
	listResponse := doRequest("/things?maxResults=3", "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)
	listResponseObject := &models.ThingsListResponse{}
	json.Unmarshal(getResponseBody(listResponse), listResponseObject)

	// Test total results
	require.Conditionf(t, func() bool { return listResponseObject.TotalResults >= 10 }, "Total results have to be higher or equal to 10.")

	// Test amount in current response
	require.Len(t, listResponseObject.Things, 3)

	// Test ID in the middle of the 3 results
	require.Equal(t, thingIDs[1], string(listResponseObject.Things[1].ThingID))
}

func Test__weaviate_GET_things_JSON_internal_limit_offset(t *testing.T) {
	// Query whole list just created
	listResponse2 := doRequest("/things?maxResults=5&page=2", "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)
	listResponseObject2 := &models.ThingsListResponse{}
	json.Unmarshal(getResponseBody(listResponse2), listResponseObject2)

	// Test total results
	require.Conditionf(t, func() bool { return listResponseObject2.TotalResults >= 10 }, "Total results have to be higher or equal to 10.")

	// Test amount in current response
	require.Len(t, listResponseObject2.Things, 5)

	// Test ID in the middle
	require.Equal(t, thingIDs[7], string(listResponseObject2.Things[2].ThingID))
}

func Test__weaviate_GET_things_id_JSON_internal(t *testing.T) {
	// Create get request
	response := doRequest("/things/"+thingIDs[0], "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

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
}

func Test__weaviate_GET_things_id_JSON_internal_forbidden_read(t *testing.T) {
	// Create get request
	response := doRequest("/things/"+thingIDs[0], "GET", "application/json", nil, newAPIKeyID, newAPIToken)

	// Check status code get request
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}

func Test__weaviate_GET_things_id_JSON_internal_forbidden_not_owned(t *testing.T) {
	// Create get request
	response := doRequest("/things/"+thingIDs[0], "GET", "application/json", nil, headKeyID, headToken)

	// Check status code get request
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}

func Test__weaviate_GET_things_id_JSON_internal_not_found(t *testing.T) {
	// Create get request with non-existing ID, check its responsecode
	responseNotFound := doRequest("/things/"+fakeID, "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, responseNotFound.StatusCode)
}

func Test__weaviate_GET_things_id_history_JSON_internal_no_updates_yet(t *testing.T) {
	// Create patch request
	response := doRequest("/things/"+thingID+"/history", "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

	// Translate body
	body := getResponseBody(response)
	respObject := &models.ThingGetHistoryResponse{}
	json.Unmarshal(body, respObject)

	// Check status code
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Check length = 0
	require.Len(t, respObject.PropertyHistory, 0)

	// Check deleted is false
	require.Equal(t, false, respObject.Deleted)
}

func Test__weaviate_PUT_things_id_JSON_internal(t *testing.T) {
	// Create update request
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
	}`, newStringValue, thingTestInt, thingTestBoolean, thingTestNumber, thingTestDate)))
	response := doRequest("/things/"+thingID, "PUT", "application/json", jsonStr, apiKeyIDCmdLine, apiTokenCmdLine)

	body := getResponseBody(response)

	respObject := &models.ThingGetResponse{}
	json.Unmarshal(body, respObject)

	// Check status code
	require.Equal(t, http.StatusAccepted, response.StatusCode)

	// Check thing ID is same
	require.Regexp(t, strfmt.UUIDPattern, respObject.ThingID)
	require.Regexp(t, strfmt.UUIDPattern, thingID)
	require.Equal(t, thingID, string(respObject.ThingID))

	// Check whether the returned information is the same as the data updated
	require.Equal(t, newStringValue, respObject.Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, thingTestInt, int64(respObject.Schema.(map[string]interface{})["testInt"].(float64)))
	require.Equal(t, thingTestBoolean, respObject.Schema.(map[string]interface{})["testBoolean"].(bool))
	require.Equal(t, thingTestNumber, respObject.Schema.(map[string]interface{})["testNumber"].(float64))
	require.Equal(t, thingTestDate, respObject.Schema.(map[string]interface{})["testDateTime"].(string))

	// Check given update time is after now, but not in the future
	now := connutils.NowUnix()
	require.Conditionf(t, func() bool { return !(respObject.LastUpdateTimeUnix > now) }, "LastUpdateTimeUnix is incorrect, it was set in the future.")
	require.Conditionf(t, func() bool { return !(respObject.LastUpdateTimeUnix < now-2000) }, "LastUpdateTimeUnix is incorrect, it was set to far back.")

	// Test is faster than adding to DB.
	time.Sleep(1000 * time.Millisecond)

	// Check if update is also applied on object when using a new GET request on same object
	responseGet := doRequest("/things/"+thingID, "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

	bodyGet := getResponseBody(responseGet)

	// Test response obj
	respObjectGet := &models.ThingGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check whether the returned information is the same as the data updated
	require.Equal(t, newStringValue, respObjectGet.Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, thingTestInt, int64(respObjectGet.Schema.(map[string]interface{})["testInt"].(float64)))
	require.Equal(t, thingTestBoolean, respObjectGet.Schema.(map[string]interface{})["testBoolean"].(bool))
	require.Equal(t, thingTestNumber, respObjectGet.Schema.(map[string]interface{})["testNumber"].(float64))
	require.Equal(t, thingTestDate, respObjectGet.Schema.(map[string]interface{})["testDateTime"].(string))
}

func Test__weaviate_PUT_things_id_JSON_internal_invalid(t *testing.T) {
	// Check validation with invalid requests
	performInvalidThingRequests(t, "/things/"+thingIDsubject, "PUT")
}

func Test__weaviate_PUT_things_id_JSON_internal_not_found(t *testing.T) {
	// Create get request with non-existing ID, check its responsecode
	responseNotFound := doRequest("/things/"+fakeID, "PUT", "application/json", getEmptyJSON(), apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, responseNotFound.StatusCode)
}

func Test__weaviate_PUT_things_id_JSON_internal_forbidden_write(t *testing.T) {
	// Create get request
	response := doRequest("/things/"+thingIDs[0], "PUT", "application/json", getEmptyJSON(), newAPIKeyID, newAPIToken)

	// Check status code get request
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}

func Test__weaviate_PUT_things_id_JSON_internal_forbidden_not_owned(t *testing.T) {
	// Create get request
	response := doRequest("/things/"+thingIDs[0], "PUT", "application/json", getEmptyJSON(), headKeyID, headToken)

	// Check status code get request
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}

func Test__weaviate_PATCH_things_id_JSON_internal(t *testing.T) {
	// Create patch request
	jsonStr := bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/schema/testString", "value": "` + newStringValue2 + `"}]`))
	response := doRequest("/things/"+thingID, "PATCH", "application/json", jsonStr, apiKeyIDCmdLine, apiTokenCmdLine)

	body := getResponseBody(response)

	respObject := &models.ThingGetResponse{}
	json.Unmarshal(body, respObject)

	// Check status code
	require.Equal(t, http.StatusAccepted, response.StatusCode)

	// Check ID is the same
	require.Regexp(t, strfmt.UUIDPattern, respObject.ThingID)
	require.Regexp(t, strfmt.UUIDPattern, thingID)
	require.Equal(t, thingID, string(respObject.ThingID))

	// Check whether the returned information is the same as the data updated
	require.Equal(t, newStringValue2, respObject.Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, thingTestInt, int64(respObject.Schema.(map[string]interface{})["testInt"].(float64)))
	require.Equal(t, thingTestBoolean, respObject.Schema.(map[string]interface{})["testBoolean"].(bool))
	require.Equal(t, thingTestNumber, respObject.Schema.(map[string]interface{})["testNumber"].(float64))
	require.Equal(t, thingTestDate, respObject.Schema.(map[string]interface{})["testDateTime"].(string))

	// Check given update time is after now, but not in the future
	now := connutils.NowUnix()
	require.Conditionf(t, func() bool { return !(respObject.LastUpdateTimeUnix > now) }, "LastUpdateTimeUnix is incorrect, it was set in the future.")
	require.Conditionf(t, func() bool { return !(respObject.LastUpdateTimeUnix < now-2000) }, "LastUpdateTimeUnix is incorrect, it was set to far back.")

	//dTest is faster than adding to DB.
	time.Sleep(1000 * time.Millisecond)

	// Check if patch is also applied on object when using a new GET request on same object
	responseGet := doRequest("/things/"+thingID, "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

	bodyGet := getResponseBody(responseGet)

	// Test response obj
	respObjectGet := &models.ThingGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check whether the returned information is the same as the data updated
	require.Equal(t, newStringValue2, respObjectGet.Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, thingTestInt, int64(respObjectGet.Schema.(map[string]interface{})["testInt"].(float64)))
	require.Equal(t, thingTestBoolean, respObjectGet.Schema.(map[string]interface{})["testBoolean"].(bool))
	require.Equal(t, thingTestNumber, respObjectGet.Schema.(map[string]interface{})["testNumber"].(float64))
	require.Equal(t, thingTestDate, respObjectGet.Schema.(map[string]interface{})["testDateTime"].(string))
}

func Test__weaviate_PATCH_things_id_JSON_internal_invalid(t *testing.T) {
	// Check patch with incorrect contents
	jsonStrError := bytes.NewBuffer([]byte(`{ "op": "replace", "path": "/address_components/long_name", "value": "test"}`))
	responseError := doRequest("/things/"+thingID, "PATCH", "application/json", jsonStrError, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusBadRequest, responseError.StatusCode)

	// Test non-existing class
	jsonStrInvalid1 := bytes.NewBuffer([]byte(fmt.Sprintf(`[{ "op": "replace", "path": "/@class", "value": "%s"}]`, "TestThings")))
	responseInvalid1 := doRequest("/things/"+thingIDsubject, "PATCH", "application/json", jsonStrInvalid1, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid1.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid1)), fmt.Sprintf(schema.ErrorNoSuchClass, "TestThings"))

	// Test non-existing property
	jsonStrInvalid2 := bytes.NewBuffer([]byte(fmt.Sprintf(`[{ "op": "add", "path": "/schema/testStrings", "value": "%s"}]`, "Test")))
	responseInvalid2 := doRequest("/things/"+thingIDsubject, "PATCH", "application/json", jsonStrInvalid2, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid2.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid2)), fmt.Sprintf(schema.ErrorNoSuchProperty, "testStrings", "TestThing2"))

	// Test invalid property cref
	jsonStrInvalid3 := bytes.NewBuffer([]byte(`[{ "op": "remove", "path": "/schema/testCref/locationUrl"}]`))
	responseInvalid3 := doRequest("/things/"+thingIDsubject, "PATCH", "application/json", jsonStrInvalid3, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid3.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid3)), fmt.Sprintf(validation.ErrorInvalidSingleRef, "TestThing2", "testCref"))

	// Test invalid property cref2
	jsonStrInvalid4 := bytes.NewBuffer([]byte(fmt.Sprintf(`[{ "op": "remove", "path": "/schema/testCref/locationUrl"}, { "op": "add", "path": "/schema/testCref/locationUrls", "value": "%s"}]`, getWeaviateURL())))
	responseInvalid4 := doRequest("/things/"+thingIDsubject, "PATCH", "application/json", jsonStrInvalid4, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid4.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid4)), fmt.Sprintf(validation.ErrorMissingSingleRefLocationURL, "TestThing2", "testCref"))

	// Test invalid property cref3
	jsonStrInvalid5 := bytes.NewBuffer([]byte(fmt.Sprintf(`[{ "op": "replace", "path": "/schema/testCref/type", "value": "%s"}]`, "Test")))
	responseInvalid5 := doRequest("/things/"+thingIDsubject, "PATCH", "application/json", jsonStrInvalid5, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid5.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid5)), fmt.Sprintf(validation.ErrorInvalidClassType, "TestThing2", "testCref", connutils.RefTypeAction, connutils.RefTypeThing, connutils.RefTypeKey))

	// Test invalid property string
	jsonStrInvalid6 := bytes.NewBuffer([]byte(fmt.Sprintf(`[{ "op": "replace", "path": "/schema/testString", "value": %d}]`, 2)))
	responseInvalid6 := doRequest("/things/"+thingIDsubject, "PATCH", "application/json", jsonStrInvalid6, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid6.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid6)), fmt.Sprintf(validation.ErrorInvalidString, "TestThing2", "testString", 2))

	// Test invalid property int
	jsonStrInvalid7 := bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/schema/testInt", "value": 2.8}]`))
	responseInvalid7 := doRequest("/things/"+thingIDsubject, "PATCH", "application/json", jsonStrInvalid7, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid7.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid7)), fmt.Sprintf(validation.ErrorInvalidInteger, "TestThing2", "testInt", 2.8))

	// Test invalid property float
	jsonStrInvalid8 := bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/schema/testNumber", "value": "test"}]`))
	responseInvalid8 := doRequest("/things/"+thingIDsubject, "PATCH", "application/json", jsonStrInvalid8, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid8.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid8)), fmt.Sprintf(validation.ErrorInvalidFloat, "TestThing2", "testNumber", "test"))

	// Test invalid property bool
	jsonStrInvalid9 := bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/schema/testBoolean", "value": "test"}]`))
	responseInvalid9 := doRequest("/things/"+thingIDsubject, "PATCH", "application/json", jsonStrInvalid9, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid9.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid9)), fmt.Sprintf(validation.ErrorInvalidBool, "TestThing2", "testBoolean", "test"))

	// Test invalid property date
	jsonStrInvalid10 := bytes.NewBuffer([]byte(`[{ "op": "replace", "path": "/schema/testDateTime", "value": "test"}]`))
	responseInvalid10 := doRequest("/things/"+thingIDsubject, "PATCH", "application/json", jsonStrInvalid10, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid10.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid10)), fmt.Sprintf(validation.ErrorInvalidDate, "TestThing2", "testDateTime", "test"))
}

func Test__weaviate_PATCH_things_id_JSON_internal_not_found(t *testing.T) {
	// Create get request with non-existing ID, check its responsecode
	responseNotFound := doRequest("/things/"+fakeID, "PATCH", "application/json", getEmptyPatchJSON(), apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, responseNotFound.StatusCode)
}

func Test__weaviate_PATCH_things_id_JSON_internal_forbidden_write(t *testing.T) {
	// Create get request
	response := doRequest("/things/"+thingIDs[0], "PATCH", "application/json", getEmptyPatchJSON(), newAPIKeyID, newAPIToken)

	// Check status code get request
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}

func Test__weaviate_PATCH_things_id_JSON_internal_forbidden_not_owned(t *testing.T) {
	// Create get request
	response := doRequest("/things/"+thingIDs[0], "PATCH", "application/json", getEmptyPatchJSON(), headKeyID, headToken)

	// Check status code get request
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}

func Test__weaviate_GET_things_id_history_JSON_internal(t *testing.T) {
	// Create patch request
	response := doRequest("/things/"+thingID+"/history", "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

	// Translate body
	body := getResponseBody(response)
	respObject := &models.ThingGetHistoryResponse{}
	json.Unmarshal(body, respObject)

	// Check status code
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Check ID is the same
	require.Regexp(t, strfmt.UUIDPattern, respObject.ThingID)
	require.Regexp(t, strfmt.UUIDPattern, thingID)
	require.Equal(t, thingID, string(respObject.ThingID))

	// Check deleted
	require.Equal(t, false, respObject.Deleted)

	// Check key
	require.Equal(t, apiKeyIDCmdLine, string(respObject.Key.NrDollarCref))

	// Check whether the returned information is the same as the data updated
	require.Len(t, respObject.PropertyHistory, 2)
	require.Equal(t, "TestThing", respObject.PropertyHistory[0].AtClass)
	require.Equal(t, thingTestString, respObject.PropertyHistory[1].Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, newStringValue, respObject.PropertyHistory[0].Schema.(map[string]interface{})["testString"].(string))

	// Check creation time
	now := connutils.NowUnix()
	require.Conditionf(t, func() bool { return !(respObject.PropertyHistory[0].CreationTimeUnix > now) }, "CreationTimeUnix is incorrect, it was set in the future.")
	require.Conditionf(t, func() bool { return !(respObject.PropertyHistory[0].CreationTimeUnix < now-20000) }, "CreationTimeUnix is incorrect, it was set to far back.")
}

func Test__weaviate_GET_things_id_history_JSON_internal_forbidden_read(t *testing.T) {
	// Create get request
	response := doRequest("/things/"+thingIDs[0]+"/history", "GET", "application/json", nil, newAPIKeyID, newAPIToken)

	// Check status code get request
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}

func Test__weaviate_GET_things_id_history_JSON_internal_forbidden_not_owned(t *testing.T) {
	// Create get request
	response := doRequest("/things/"+thingIDs[0]+"/history", "GET", "application/json", nil, headKeyID, headToken)

	// Check status code get request
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}

func Test__weaviate_GET_things_id_history_JSON_internal_not_found(t *testing.T) {
	// Create patch request
	response := doRequest("/things/"+fakeID+"/history", "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, response.StatusCode)
}

func Test__weaviate_POST_things_validate_JSON_internal(t *testing.T) {
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

	response := doRequest("/things/validate", "POST", "application/json", jsonStr, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusOK, response.StatusCode)
}

func Test__weaviate_POST_things_validate_JSON_internal_invalid(t *testing.T) {
	// Test invalid requests
	performInvalidThingRequests(t, "/things/validate", "POST")
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
	responseInvalidThings1 := doRequest(uri, method, "application/json", jsonStrInvalidThings1, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings1.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings1)), validation.ErrorMissingActionThings)

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
	responseInvalidThings2 := doRequest(uri, method, "application/json", jsonStrInvalidThings2, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings2.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings2)), validation.ErrorMissingActionThingsObject)

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
	responseInvalidThings3 := doRequest(uri, method, "application/json", jsonStrInvalidThings3, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings3.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings3)), validation.ErrorMissingActionThingsSubject)

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
	responseInvalidThings4 := doRequest(uri, method, "application/json", jsonStrInvalidThings4, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings4.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings4)), validation.ErrorMissingActionThingsObjectLocation)

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
	responseInvalidThings5 := doRequest(uri, method, "application/json", jsonStrInvalidThings5, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings5.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings5)), validation.ErrorMissingActionThingsObjectType)

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
	responseInvalidThings6 := doRequest(uri, method, "application/json", jsonStrInvalidThings6, apiKeyIDCmdLine, apiTokenCmdLine)
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
	responseInvalidThings7 := doRequest(uri, method, "application/json", jsonStrInvalidThings7, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Contains(t, []int{http.StatusUnprocessableEntity, http.StatusAccepted}, responseInvalidThings7.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings7)), connutils.StaticThingNotFound)

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
	responseInvalidThings8 := doRequest(uri, method, "application/json", jsonStrInvalidThings8, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings8.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings8)), validation.ErrorMissingActionThingsSubjectLocation)

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
	responseInvalidThings9 := doRequest(uri, method, "application/json", jsonStrInvalidThings9, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings9.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings9)), validation.ErrorMissingActionThingsSubjectType)

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
	responseInvalidThings10 := doRequest(uri, method, "application/json", jsonStrInvalidThings10, apiKeyIDCmdLine, apiTokenCmdLine)
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
	responseInvalidThings11 := doRequest(uri, method, "application/json", jsonStrInvalidThings11, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings11.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings11)), connutils.StaticThingNotFound)

	// Test non-existing object-thing location
	jsonStrInvalidThings12 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
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
	}`, thingTestString, thingID, "http://example.org", thingIDsubject, getWeaviateURL())))
	responseInvalidThings12 := doRequest(uri, method, "application/json", jsonStrInvalidThings12, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings12.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings12)), fmt.Sprintf(validation.ErrorNoExternalCredentials, "http://example.org", "Object-Thing"))

	// Test non-existing object-thing in existing location
	jsonStrInvalidThings13 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
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
	}`, thingTestString, fakeID, "http://localhost:"+serverPort, thingIDsubject, getWeaviateURL())))
	responseInvalidThings13 := doRequest(uri, method, "application/json", jsonStrInvalidThings13, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings13.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings13)), fmt.Sprintf(validation.ErrorExternalNotFound, "http://localhost:"+serverPort, 404, ""))

	// Test non-existing object-thing location
	jsonStrInvalidThings14 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
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
	}`, thingTestString, thingID, getWeaviateURL(), thingIDsubject, "http://example.org")))
	responseInvalidThings14 := doRequest(uri, method, "application/json", jsonStrInvalidThings14, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings14.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings14)), fmt.Sprintf(validation.ErrorNoExternalCredentials, "http://example.org", "Subject-Thing"))

	// Test non-existing object-thing in existing location
	jsonStrInvalidThings15 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
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
	}`, thingTestString, thingID, getWeaviateURL(), fakeID, "http://localhost:"+serverPort)))
	responseInvalidThings15 := doRequest(uri, method, "application/json", jsonStrInvalidThings15, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalidThings15.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalidThings15)), fmt.Sprintf(validation.ErrorExternalNotFound, "http://localhost:"+serverPort, 404, ""))

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
	responseInvalid1 := doRequest(uri, method, "application/json", jsonStrInvalid1, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid1.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid1)), validation.ErrorMissingClass)

	// Test missing context
	jsonStrInvalid2 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@class": "TestAction",
		"schema": {
			"testString": "%s"
		}%s
	}`, thingTestString, actionThingsJSON)))
	responseInvalid2 := doRequest(uri, method, "application/json", jsonStrInvalid2, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid2.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid2)), validation.ErrorMissingContext)

	// Test non-existing class
	jsonStrInvalid3 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestActions",
		"schema": {
			"testString": "%s"
		}%s
	}`, thingTestString, actionThingsJSON)))
	responseInvalid3 := doRequest(uri, method, "application/json", jsonStrInvalid3, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid3.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid3)), fmt.Sprintf(schema.ErrorNoSuchClass, "TestActions"))

	// Test non-existing property
	jsonStrInvalid4 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testStrings": "%s"
		}%s
	}`, thingTestString, actionThingsJSON)))
	responseInvalid4 := doRequest(uri, method, "application/json", jsonStrInvalid4, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid4.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid4)), fmt.Sprintf(schema.ErrorNoSuchProperty, "testStrings", "TestAction"))

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
	responseInvalid5 := doRequest(uri, method, "application/json", jsonStrInvalid5, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid5.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid5)), fmt.Sprintf(validation.ErrorInvalidSingleRef, "TestAction", "testCref"))

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
	responseInvalid6 := doRequest(uri, method, "application/json", jsonStrInvalid6, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid6.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid6)), fmt.Sprintf(validation.ErrorMissingSingleRefLocationURL, "TestAction", "testCref"))

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
	responseInvalid7 := doRequest(uri, method, "application/json", jsonStrInvalid7, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid7.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid7)), fmt.Sprintf(validation.ErrorInvalidClassType, "TestAction", "testCref", connutils.RefTypeAction, connutils.RefTypeThing, connutils.RefTypeKey))

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
	responseInvalid7b := doRequest(uri, method, "application/json", jsonStrInvalid7b, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid7b.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid7b)), connutils.StaticThingNotFound)

	// Test invalid property cref5: invalid LocationURL
	jsonStrInvalid7c := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testCref": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			}
		}%s
	}`, thingID, "http://example.org/", actionThingsJSON)))
	responseInvalid7c := doRequest(uri, method, "application/json", jsonStrInvalid7c, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid7c.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid7c)), fmt.Sprintf(validation.ErrorNoExternalCredentials, "http://example.org/", "'cref' Thing TestAction:testCref"))

	// Test invalid property cref6: valid locationURL, invalid ThingID
	jsonStrInvalid7d := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testCref": {
				"$cref": "%s",
				"locationUrl": "%s",
				"type": "Thing"
			}
		}%s
	}`, fakeID, "http://localhost:"+serverPort, actionThingsJSON)))
	responseInvalid7d := doRequest(uri, method, "application/json", jsonStrInvalid7d, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid7d.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid7d)), fmt.Sprintf(validation.ErrorExternalNotFound, "http://localhost:"+serverPort, 404, ""))

	// Test invalid property string
	jsonStrInvalid8 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testString": 2
		}%s
	}`, actionThingsJSON)))
	responseInvalid8 := doRequest(uri, method, "application/json", jsonStrInvalid8, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid8.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid8)), fmt.Sprintf(validation.ErrorInvalidString, "TestAction", "testString", 2))

	// Test invalid property int
	jsonStrInvalid9 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testInt": 2.7
		}%s
	}`, actionThingsJSON)))
	responseInvalid9 := doRequest(uri, method, "application/json", jsonStrInvalid9, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid9.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid9)), fmt.Sprintf(validation.ErrorInvalidInteger, "TestAction", "testInt", 2.7))

	// Test invalid property float
	jsonStrInvalid10 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testNumber": "test"
		}%s
	}`, actionThingsJSON)))
	responseInvalid10 := doRequest(uri, method, "application/json", jsonStrInvalid10, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid10.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid10)), fmt.Sprintf(validation.ErrorInvalidFloat, "TestAction", "testNumber", "test"))

	// Test invalid property bool
	jsonStrInvalid11 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testBoolean": "test"
		}%s
	}`, actionThingsJSON)))
	responseInvalid11 := doRequest(uri, method, "application/json", jsonStrInvalid11, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid11.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid11)), fmt.Sprintf(validation.ErrorInvalidBool, "TestAction", "testBoolean", "test"))

	// Test invalid property date
	jsonStrInvalid12 := bytes.NewBuffer([]byte(fmt.Sprintf(`{
		"@context": "http://example.org",
		"@class": "TestAction",
		"schema": {
			"testDateTime": "test"
		}%s
	}`, actionThingsJSON)))
	responseInvalid12 := doRequest(uri, method, "application/json", jsonStrInvalid12, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusUnprocessableEntity, responseInvalid12.StatusCode)
	require.Contains(t, string(getResponseBody(responseInvalid12)), fmt.Sprintf(validation.ErrorInvalidDate, "TestAction", "testDateTime", "test"))
}

func Test__weaviate_POST_actions_JSON_internal(t *testing.T) {
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
	response := doRequest("/actions", "POST", "application/json", jsonStr, apiKeyIDCmdLine, apiTokenCmdLine)

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
	require.Equal(t, apiKeyIDCmdLine, string(respObject.Key.NrDollarCref))

	// Check given creation time is after now, but not in the future
	now := connutils.NowUnix()
	require.Conditionf(t, func() bool { return !(respObject.CreationTimeUnix > now) }, "CreationTimeUnix is incorrect, it was set in the future.")
	require.Conditionf(t, func() bool { return !(respObject.CreationTimeUnix < now-2000) }, "CreationTimeUnix is incorrect, it was set to far back.")
}

func Test__weaviate_POST_actions_JSON_external(t *testing.T) {
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
	}`, actionTestString, actionTestInt, actionTestBoolean, actionTestNumber, actionTestDate, thingID, "http://localhost:"+serverPort, thingID, "http://localhost:"+serverPort, thingIDsubject, "http://localhost:"+serverPort)))
	response := doRequest("/actions", "POST", "application/json", jsonStr, apiKeyIDCmdLine, apiTokenCmdLine)

	// Check status code of create
	require.Equal(t, http.StatusAccepted, response.StatusCode)

	body := getResponseBody(response)

	respObject := &models.ActionGetResponse{}
	json.Unmarshal(body, respObject)

	// Check whether generated UUID is added
	require.Regexp(t, strfmt.UUIDPattern, respObject.ActionID)

	// Globally set actionID
	actionIDExternal = string(respObject.ActionID)
}

func Test__weaviate_POST_actions_JSON_internal_multiple(t *testing.T) {
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
		response := doRequest("/actions", "POST", "application/json", jsonStr, apiKeyIDCmdLine, apiTokenCmdLine)
		body := getResponseBody(response)
		respObject := &models.ActionGetResponse{}
		json.Unmarshal(body, respObject)

		// Check adding succeeds
		require.Equal(t, http.StatusAccepted, response.StatusCode)

		// Fill array and time out for unlucky sorting issues
		actionIDs[i] = string(respObject.ActionID)
		time.Sleep(1000 * time.Millisecond)
	}
}
func Test__weaviate_POST_actions_JSON_internal_invalid(t *testing.T) {
	// Create invalid requests
	performInvalidActionRequests(t, "/actions", "POST")

	// Test is faster than adding to DB.
	time.Sleep(1000 * time.Millisecond)
}

func Test__weaviate_POST_actions_JSON_internal_forbidden_write(t *testing.T) {
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
	}`, actionTestString, actionTestInt, actionTestBoolean, actionTestNumber, actionTestDate, thingID, "http://localhost:"+serverPort, thingID, "http://localhost:"+serverPort, thingIDsubject, "http://localhost:"+serverPort)))
	response := doRequest("/actions", "POST", "application/json", jsonStr, newAPIKeyID, newAPIToken)

	// Check status code get request
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}

func Test__weaviate_GET_things_id_actions_JSON_internal(t *testing.T) {
	// Create list request
	response := doRequest("/things/"+thingID+"/actions", "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

	// Check status code of list
	require.Equal(t, http.StatusOK, response.StatusCode)

	body := getResponseBody(response)

	respObject := &models.ActionsListResponse{}
	json.Unmarshal(body, respObject)

	// Check most recent
	require.Regexp(t, strfmt.UUIDPattern, respObject.Actions[0].ActionID)
	require.Regexp(t, strfmt.UUIDPattern, actionIDs[0])
	require.Equal(t, actionIDs[0], string(respObject.Actions[0].ActionID))

	// Check there are 11 actions
	require.Len(t, respObject.Actions, 11)
}

func Test__weaviate_GET_things_id_actions_JSON_internal_forbidden_read(t *testing.T) {
	// Create get request
	response := doRequest("/things/"+thingID+"/actions", "GET", "application/json", nil, newAPIKeyID, newAPIToken)

	// Check status code get request
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}

func Test__weaviate_GET_things_id_actions_JSON_internal_nothing(t *testing.T) {
	// Create list request
	response := doRequest("/things/"+thingIDs[3]+"/actions", "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

	// Check status code of list
	require.Equal(t, http.StatusOK, response.StatusCode)

	body := getResponseBody(response)

	respObject := &models.ActionsListResponse{}
	json.Unmarshal(body, respObject)

	// Check there are ten actions
	require.Len(t, respObject.Actions, 0)
}

func Test__weaviate_GET_things_id_actions_JSON_internal_limit(t *testing.T) {
	// Query whole list just created
	listResponse := doRequest("/things/"+thingID+"/actions?maxResults=3", "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)
	listResponseObject := &models.ActionsListResponse{}
	json.Unmarshal(getResponseBody(listResponse), listResponseObject)

	// Test total results
	require.Conditionf(t, func() bool { return listResponseObject.TotalResults == 11 }, "Total results have to be equal to 11.")

	// Test amount in current response
	require.Len(t, listResponseObject.Actions, 3)

	// Test ID in the middle of the 3 results
	require.Equal(t, actionIDs[1], string(listResponseObject.Actions[1].ActionID))
}

func Test__weaviate_GET_things_id_actions_JSON_internal_limit_offset(t *testing.T) {
	// Query whole list just created
	listResponse2 := doRequest("/things/"+thingID+"/actions?maxResults=5&page=2", "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)
	listResponseObject2 := &models.ActionsListResponse{}
	json.Unmarshal(getResponseBody(listResponse2), listResponseObject2)

	// Test total results
	require.Conditionf(t, func() bool { return listResponseObject2.TotalResults == 11 }, "Total results have to be equal to 11.")

	// Test amount in current response
	require.Len(t, listResponseObject2.Actions, 5)

	// Test ID in the middle
	require.Equal(t, actionIDs[7], string(listResponseObject2.Actions[2].ActionID))
}

func Test__weaviate_GET_actions_id_JSON_internal(t *testing.T) {
	// Create get request
	response := doRequest("/actions/"+actionID, "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

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
}

func Test__weaviate_GET_actions_id_JSON_internal_forbidden_read(t *testing.T) {
	// Create get request
	response := doRequest("/actions/"+actionIDs[0], "GET", "application/json", nil, newAPIKeyID, newAPIToken)

	// Check status code get request
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}

func Test__weaviate_GET_actions_id_JSON_internal_forbidden_not_owned(t *testing.T) {
	// Create get request
	response := doRequest("/actions/"+actionIDs[0], "GET", "application/json", nil, headKeyID, headToken)

	// Check status code get request
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}
func Test__weaviate_GET_actions_id_JSON_internal_not_found(t *testing.T) {
	// Create get request with non-existing ID, check its responsecode
	responseNotFound := doRequest("/actions/"+fakeID, "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, responseNotFound.StatusCode)
}

func Test__weaviate_GET_actions_id_history_JSON_internal_no_updates_yet(t *testing.T) {
	// Create patch request
	response := doRequest("/actions/"+actionID+"/history", "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

	// Translate body
	body := getResponseBody(response)
	respObject := &models.ActionGetHistoryResponse{}
	json.Unmarshal(body, respObject)

	// Check status code
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Check length = 0
	require.Len(t, respObject.PropertyHistory, 0)

	// Check deleted is false
	require.Equal(t, false, respObject.Deleted)
}

func Test__weaviate_PUT_actions_id_JSON_internal(t *testing.T) {
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
	}`, newStringValue, actionTestInt, actionTestBoolean, actionTestNumber, actionTestDate, thingID, getWeaviateURL(), thingID, getWeaviateURL(), thingIDsubject, getWeaviateURL())))
	response := doRequest("/actions/"+actionID, "PUT", "application/json", jsonStr, apiKeyIDCmdLine, apiTokenCmdLine)

	body := getResponseBody(response)

	respObject := &models.ActionGetResponse{}
	json.Unmarshal(body, respObject)

	// Check status code
	require.Equal(t, http.StatusAccepted, response.StatusCode)

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
	require.Equal(t, newStringValue, respObject.Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, actionTestInt, int64(respObject.Schema.(map[string]interface{})["testInt"].(float64)))
	require.Equal(t, actionTestBoolean, respObject.Schema.(map[string]interface{})["testBoolean"].(bool))
	require.Equal(t, actionTestNumber, respObject.Schema.(map[string]interface{})["testNumber"].(float64))
	require.Equal(t, actionTestDate, respObject.Schema.(map[string]interface{})["testDateTime"].(string))

	// Check given update time is after now, but not in the future
	now := connutils.NowUnix()
	require.Conditionf(t, func() bool { return !(respObject.LastUpdateTimeUnix > now) }, "LastUpdateTimeUnix is incorrect, it was set in the future.")
	require.Conditionf(t, func() bool { return !(respObject.LastUpdateTimeUnix < now-2000) }, "LastUpdateTimeUnix is incorrect, it was set to far back.")

	// Test is faster than adding to DB.
	time.Sleep(1000 * time.Millisecond)

	// Check if update is also applied on object when using a new GET request on same object
	responseGet := doRequest("/actions/"+actionID, "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

	bodyGet := getResponseBody(responseGet)

	// Test response obj
	respObjectGet := &models.ActionGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check whether the returned information is the same as the data updated
	require.Equal(t, newStringValue, respObjectGet.Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, actionTestInt, int64(respObjectGet.Schema.(map[string]interface{})["testInt"].(float64)))
	require.Equal(t, actionTestBoolean, respObjectGet.Schema.(map[string]interface{})["testBoolean"].(bool))
	require.Equal(t, actionTestNumber, respObjectGet.Schema.(map[string]interface{})["testNumber"].(float64))
	require.Equal(t, actionTestDate, respObjectGet.Schema.(map[string]interface{})["testDateTime"].(string))

	// Check thing is set to known ThingID
	require.Regexp(t, strfmt.UUIDPattern, respObject.Things.Object.NrDollarCref)
	require.Regexp(t, strfmt.UUIDPattern, thingID)
	require.Equal(t, thingID, string(respObject.Things.Object.NrDollarCref))

	// Check thing is set to known ThingIDSubject
	require.Regexp(t, strfmt.UUIDPattern, respObject.Things.Subject.NrDollarCref)
	require.Regexp(t, strfmt.UUIDPattern, thingIDsubject)
	require.Equal(t, thingIDsubject, string(respObject.Things.Subject.NrDollarCref))
}
func Test__weaviate_PUT_actions_id_JSON_internal_invalid(t *testing.T) {
	// Check validation with invalid requests
	performInvalidActionRequests(t, "/actions/"+actionID, "PUT")
}
func Test__weaviate_PUT_actions_id_JSON_internal_forbidden_write(t *testing.T) {
	// Create get request
	response := doRequest("/actions/"+actionID, "PUT", "application/json", getEmptyJSON(), newAPIKeyID, newAPIToken)

	// Check status code get request
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}

func Test__weaviate_PUT_actions_id_JSON_internal_forbidden_not_owned(t *testing.T) {
	// Create get request
	response := doRequest("/actions/"+actionID, "PUT", "application/json", getEmptyJSON(), headKeyID, headToken)

	// Check status code get request
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}

func Test__weaviate_PUT_actions_id_JSON_internal_not_found(t *testing.T) {
	// Create get request with non-existing ID, check its responsecode
	responseNotFound := doRequest("/actions/"+fakeID, "PUT", "application/json", getEmptyJSON(), apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, responseNotFound.StatusCode)
}

func Test__weaviate_PATCH_actions_id_JSON_internal(t *testing.T) {
	// Create JSON and do the request
	jsonStr := bytes.NewBuffer([]byte(fmt.Sprintf(`[{ "op": "replace", "path": "/schema/testInt", "value": %d}, { "op": "replace", "path": "/schema/testString", "value": "%s"}]`, newIntValue, newStringValue2)))
	response := doRequest("/actions/"+actionID, "PATCH", "application/json", jsonStr, apiKeyIDCmdLine, apiTokenCmdLine)

	body := getResponseBody(response)

	respObject := &models.ActionGetResponse{}
	json.Unmarshal(body, respObject)

	// Check status code
	require.Equal(t, http.StatusAccepted, response.StatusCode)

	// Check ID is the same
	require.Regexp(t, strfmt.UUIDPattern, respObject.ActionID)
	require.Regexp(t, strfmt.UUIDPattern, actionID)
	require.Equal(t, actionID, string(respObject.ActionID))

	// Check whether the returned information is the same as the data updated
	require.Equal(t, newStringValue2, respObject.Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, newIntValue, int64(respObject.Schema.(map[string]interface{})["testInt"].(float64)))
	require.Equal(t, actionTestBoolean, respObject.Schema.(map[string]interface{})["testBoolean"].(bool))
	require.Equal(t, actionTestNumber, respObject.Schema.(map[string]interface{})["testNumber"].(float64))
	require.Equal(t, actionTestDate, respObject.Schema.(map[string]interface{})["testDateTime"].(string))
	require.Equal(t, thingID, string(respObject.Schema.(map[string]interface{})["testCref"].(map[string]interface{})["$cref"].(string)))

	// Check given creation time is after now, but not in the future
	now := connutils.NowUnix()
	require.Conditionf(t, func() bool { return !(respObject.LastUpdateTimeUnix > now) }, "LastUpdateTimeUnix is incorrect, it was set in the future.")
	require.Conditionf(t, func() bool { return !(respObject.LastUpdateTimeUnix < now-2000) }, "LastUpdateTimeUnix is incorrect, it was set to far back.")

	// Test is faster than adding to DB.
	time.Sleep(1000 * time.Millisecond)

	// Check if patch is also applied on object when using a new GET request on same object
	responseGet := doRequest("/actions/"+actionID, "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

	bodyGet := getResponseBody(responseGet)

	// Test response obj
	respObjectGet := &models.ActionGetResponse{}
	json.Unmarshal(bodyGet, respObjectGet)

	// Check whether the returned information is the same as the data updated
	require.Equal(t, newStringValue2, respObjectGet.Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, newIntValue, int64(respObjectGet.Schema.(map[string]interface{})["testInt"].(float64)))
	require.Equal(t, actionTestBoolean, respObjectGet.Schema.(map[string]interface{})["testBoolean"].(bool))
	require.Equal(t, actionTestNumber, respObjectGet.Schema.(map[string]interface{})["testNumber"].(float64))
	require.Equal(t, actionTestDate, respObjectGet.Schema.(map[string]interface{})["testDateTime"].(string))
	require.Equal(t, thingID, string(respObjectGet.Schema.(map[string]interface{})["testCref"].(map[string]interface{})["$cref"].(string)))
}

func Test__weaviate_PATCH_actions_id_JSON_internal_invalid(t *testing.T) {
	// Check patch with incorrect contents
	jsonStrError := bytes.NewBuffer([]byte(`{ "op": "replace", "path": "/xxxx", "value": "test"}`))
	responseError := doRequest("/actions/"+actionID, "PATCH", "application/json", jsonStrError, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusBadRequest, responseError.StatusCode)
}

func Test__weaviate_PATCH_actions_id_JSON_internal_forbidden_write(t *testing.T) {
	// Create get request
	response := doRequest("/actions/"+actionID, "PATCH", "application/json", getEmptyPatchJSON(), newAPIKeyID, newAPIToken)

	// Check status code get request
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}

func Test__weaviate_PATCH_actions_id_JSON_internal_forbidden_not_owned(t *testing.T) {
	// Create get request
	response := doRequest("/actions/"+actionID, "PATCH", "application/json", getEmptyPatchJSON(), headKeyID, headToken)

	// Check status code get request
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}

func Test__weaviate_PATCH_actions_id_JSON_internal_not_found(t *testing.T) {
	// Create get request with non-existing ID, check its responsecode
	responseNotFound := doRequest("/actions/"+fakeID, "PATCH", "application/json", getEmptyPatchJSON(), apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, responseNotFound.StatusCode)
}

func Test__weaviate_GET_actions_id_history_JSON_internal(t *testing.T) {
	// Create patch request
	response := doRequest("/actions/"+actionID+"/history", "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

	// Translate body
	body := getResponseBody(response)
	respObject := &models.ActionGetHistoryResponse{}
	json.Unmarshal(body, respObject)

	// Check status code
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Check ID is the same
	require.Regexp(t, strfmt.UUIDPattern, respObject.ActionID)
	require.Regexp(t, strfmt.UUIDPattern, actionID)
	require.Equal(t, actionID, string(respObject.ActionID))

	// Check deleted
	require.Equal(t, false, respObject.Deleted)

	// Check key
	require.Equal(t, apiKeyIDCmdLine, string(respObject.Key.NrDollarCref))

	// Check whether the returned information is the same as the data updated
	require.Len(t, respObject.PropertyHistory, 2)
	require.Equal(t, "TestAction", respObject.PropertyHistory[0].AtClass)
	require.Equal(t, actionTestString, respObject.PropertyHistory[1].Schema.(map[string]interface{})["testString"].(string))
	require.Equal(t, newStringValue, respObject.PropertyHistory[0].Schema.(map[string]interface{})["testString"].(string))

	// Check thing is set to known ThingID
	require.Regexp(t, strfmt.UUIDPattern, respObject.PropertyHistory[0].Things.Object.NrDollarCref)
	require.Regexp(t, strfmt.UUIDPattern, thingID)
	require.Equal(t, thingID, string(respObject.PropertyHistory[0].Things.Object.NrDollarCref))

	// Check thing is set to known ThingIDSubject
	require.Regexp(t, strfmt.UUIDPattern, respObject.PropertyHistory[0].Things.Subject.NrDollarCref)
	require.Regexp(t, strfmt.UUIDPattern, thingIDsubject)
	require.Equal(t, thingIDsubject, string(respObject.PropertyHistory[0].Things.Subject.NrDollarCref))

	// Check thing is set to known ThingID
	require.Regexp(t, strfmt.UUIDPattern, respObject.PropertyHistory[1].Things.Object.NrDollarCref)
	require.Regexp(t, strfmt.UUIDPattern, thingID)
	require.Equal(t, thingID, string(respObject.PropertyHistory[1].Things.Object.NrDollarCref))

	// Check thing is set to known ThingIDSubject
	require.Regexp(t, strfmt.UUIDPattern, respObject.PropertyHistory[1].Things.Subject.NrDollarCref)
	require.Regexp(t, strfmt.UUIDPattern, thingIDsubject)
	require.Equal(t, thingIDsubject, string(respObject.PropertyHistory[1].Things.Subject.NrDollarCref))

	// Check creation time
	now := connutils.NowUnix()
	require.Conditionf(t, func() bool { return !(respObject.PropertyHistory[0].CreationTimeUnix > now) }, "CreationTimeUnix is incorrect, it was set in the future.")
	require.Conditionf(t, func() bool { return !(respObject.PropertyHistory[0].CreationTimeUnix < now-20000) }, "CreationTimeUnix is incorrect, it was set to far back.")
}

func Test__weaviate_GET_actions_id_history_JSON_internal_forbidden_read(t *testing.T) {
	// Create get request
	response := doRequest("/actions/"+actionID+"/history", "GET", "application/json", nil, newAPIKeyID, newAPIToken)

	// Check status code get request
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}

func Test__weaviate_GET_actions_id_history_JSON_internal_forbidden_not_owned(t *testing.T) {
	// Create get request
	response := doRequest("/actions/"+actionID+"/history", "GET", "application/json", nil, headKeyID, headToken)

	// Check status code get request
	require.Equal(t, http.StatusForbidden, response.StatusCode)
}

func Test__weaviate_GET_actions_id_history_JSON_internal_not_found(t *testing.T) {
	// Create patch request
	response := doRequest("/actions/"+fakeID+"/history", "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, response.StatusCode)
}

func Test__weaviate_POST_actions_validate_JSON_internal(t *testing.T) {
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
	response := doRequest("/actions/validate", "POST", "application/json", jsonStr, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusOK, response.StatusCode)
}

func Test__weaviate_POST_actions_validate_JSON_internal_invalid(t *testing.T) {
	// Test invalid requests
	performInvalidActionRequests(t, "/actions/validate", "POST")
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

func Test__weaviate_POST_graphql_JSON_internal_no_query(t *testing.T) {
	// Set the graphQL body
	bodyUnpr := `{
		"querys": "{ }"
	}`

	// Make the IO input
	jsonStr := bytes.NewBuffer([]byte(bodyUnpr))

	// Do the GraphQL request
	responseUnpr := doRequest("/graphql", "POST", "application/json", jsonStr, apiKeyIDCmdLine, apiTokenCmdLine)

	// Check statuscode
	require.Equal(t, http.StatusUnprocessableEntity, responseUnpr.StatusCode)
}

func Test__weaviate_POST_graphql_JSON_internal_non_existing_property(t *testing.T) {
	// Set the graphQL body
	bodyNonExistingProperty := graphQLQueryObject{
		Query: fmt.Sprintf(`{ action(uuid:"%s") { uuids atContext atClass creationTimeUnix things { object { uuid } subject { uuid } } key { uuid read } } }`, actionID),
	}

	// Do the GraphQL request
	responseNonExistingProperty, respObjectNonExistingProperty := doGraphQLRequest(bodyNonExistingProperty, apiKeyIDCmdLine, apiTokenCmdLine)

	// Check statuscode
	require.Equal(t, http.StatusOK, responseNonExistingProperty.StatusCode)

	// Test that the data in the response is nil
	require.Nil(t, respObjectNonExistingProperty.Data)

	// Test that the error in the response is not nil
	require.NotNil(t, respObjectNonExistingProperty.Errors)
}

func Test__weaviate_POST_graphql_JSON_internal_thing(t *testing.T) {
	// Set the graphQL body
	body := `{ thing(uuid:"%s") { uuid atContext atClass creationTimeUnix key { uuid read } actions { actions { uuid atContext atClass creationTimeUnix } totalResults } } }`

	bodyObj := graphQLQueryObject{
		Query: fmt.Sprintf(body, thingID),
	}

	// Do the GraphQL request
	response, respObject := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)

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

	// Test the given key-object in the response
	respKeyUUID := respObject.Data["thing"].(map[string]interface{})["key"].(map[string]interface{})["uuid"]
	require.Regexp(t, strfmt.UUIDPattern, respKeyUUID)
	require.Regexp(t, strfmt.UUIDPattern, apiKeyIDCmdLine)
	require.Equal(t, apiKeyIDCmdLine, respKeyUUID)

	// Test whether the key has read rights (must have)
	respKeyRead := respObject.Data["thing"].(map[string]interface{})["key"].(map[string]interface{})["read"].(bool)
	require.Equal(t, true, respKeyRead)

	// Test total results
	totalResults := respObject.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["totalResults"].(float64)
	require.Conditionf(t, func() bool { return totalResults >= 10 }, "Total results have to be higher or equal to 10.")

	// Check most recent
	respActionsUUID := respObject.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["actions"].([]interface{})[0].(map[string]interface{})["uuid"].(string)
	require.Regexp(t, strfmt.UUIDPattern, respActionsUUID)
	require.Regexp(t, strfmt.UUIDPattern, actionIDs[0])
	require.Equal(t, actionIDs[0], string(respActionsUUID))
}

func Test__weaviate_POST_graphql_JSON_internal_thing_forbidden_read(t *testing.T) {
	// Set the graphQL body
	body := `{ thing(uuid:"%s") { uuid atContext atClass creationTimeUnix key { uuid read } actions { actions { uuid atContext atClass creationTimeUnix } totalResults } } }`

	bodyObj := graphQLQueryObject{
		Query: fmt.Sprintf(body, thingID),
	}

	// Do the GraphQL request
	response, respObject := doGraphQLRequest(bodyObj, newAPIKeyID, newAPIToken)

	// Check statuscode
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Test that the error in the response is nil
	require.NotNil(t, respObject.Errors)
}

func Test__weaviate_POST_graphql_JSON_internal_thing_forbidden_parent(t *testing.T) {
	// Set the graphQL body
	body := `{ thing(uuid:"%s") { uuid atContext atClass creationTimeUnix key { uuid read } actions { actions { uuid atContext atClass creationTimeUnix } totalResults } } }`

	bodyObj := graphQLQueryObject{
		Query: fmt.Sprintf(body, thingID),
	}

	// Do the GraphQL request
	response, respObject := doGraphQLRequest(bodyObj, headKeyID, headToken)

	// Check statuscode
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Test that the error in the response is nil
	require.NotNil(t, respObject.Errors)
}

func Test__weaviate_POST_graphql_JSON_thing_external(t *testing.T) {
	// Set the graphQL body
	body := `{ thing(uuid:"%s") { uuid schema { testCref { uuid } } } }`

	bodyObj := graphQLQueryObject{
		Query: fmt.Sprintf(body, thingIDExternal),
	}

	// Do the GraphQL request
	response, respObject := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)

	// Check statuscode
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Test that the error in the response is nil
	require.Nil(t, respObject.Errors)

	// Test the given UUID in the response
	respUUID := respObject.Data["thing"].(map[string]interface{})["uuid"]
	require.Regexp(t, strfmt.UUIDPattern, respUUID)
	require.Regexp(t, strfmt.UUIDPattern, thingIDExternal)
	require.Equal(t, thingIDExternal, respUUID)

	// Test external uuid
	externalUUID := respObject.Data["thing"].(map[string]interface{})["schema"].(map[string]interface{})["testCref"].(map[string]interface{})["uuid"].(string)
	require.Regexp(t, strfmt.UUIDPattern, externalUUID)
	require.Regexp(t, strfmt.UUIDPattern, thingID)
	require.Equal(t, thingID, externalUUID)
}

func Test__weaviate_POST_graphql_JSON_internal_thing_actions_limit(t *testing.T) {
	// Set the graphQL body
	body := `{ thing(uuid:"%s") { actions(first:3) { actions { uuid atContext atClass creationTimeUnix } totalResults } } }`
	bodyObj := graphQLQueryObject{
		Query: fmt.Sprintf(body, thingID),
	}

	// Do the GraphQL request
	responseActionsLimit, respObjectActionsLimit := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)

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
}

func Test__weaviate_POST_graphql_JSON_internal_thing_actions_limit_ofset(t *testing.T) {
	// Set the graphQL body
	body := `{ thing(uuid:"%s") { actions(first:5, offset:5) { actions { uuid atContext atClass creationTimeUnix } totalResults } } }`
	bodyObj := graphQLQueryObject{
		Query: fmt.Sprintf(body, thingID),
	}

	// Do the GraphQL request
	responseActionsLimitOffset, respObjectActionsLimitOffset := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)

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

}

func Test__weaviate_POST_graphql_JSON_internal_thing_actions_filters(t *testing.T) {
	// Search class 'TestAction2', most recent should be the set actionID[0]
	bodyObj := graphQLQueryObject{Query: fmt.Sprintf(`{ thing(uuid:"%s") { actions(class:"TestAction2", first:1) { actions { uuid atClass } totalResults } } }`, thingID)}
	_, respObjectValueSearch1 := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)
	resultActionsValueSearch1 := respObjectValueSearch1.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["actions"].([]interface{})
	require.Equal(t, actionIDs[0], string(resultActionsValueSearch1[0].(map[string]interface{})["uuid"].(string)))

	// // Search class NOT 'TestAction2', most recent should be the set actionID
	// bodyObj = graphQLQueryObject{Query: fmt.Sprintf(`{ thing(uuid:"%s") { actions(class:"!:TestAction2", first:1) { actions { uuid atClass } totalResults } } }`, thingID)}
	// _, respObjectValueSearch2 := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)
	// resultActionsValueSearch2 := respObjectValueSearch2.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["actions"].([]interface{})
	// require.Equal(t, actionID, string(resultActionsValueSearch2[0].(map[string]interface{})["uuid"].(string)))

	// // Search class '~TestAction', most recent 9 should be 'TestAction2' and the 10th is 'TestAction' (with uuid = actionID).
	// bodyObj = graphQLQueryObject{Query: fmt.Sprintf(`{ thing(uuid:"%s") { actions(class:"~TestAction", first:10) { actions { uuid atClass } totalResults } } }`, thingID)}
	// _, respObjectValueSearch3 := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)
	// resultActionsValueSearch3 := respObjectValueSearch3.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["actions"].([]interface{})
	// require.Equal(t, "TestAction2", string(resultActionsValueSearch3[3].(map[string]interface{})["atClass"].(string)))
	// require.Equal(t, "TestAction2", string(resultActionsValueSearch3[6].(map[string]interface{})["atClass"].(string)))
	// require.Equal(t, "TestAction", string(resultActionsValueSearch3[9].(map[string]interface{})["atClass"].(string)))
	// require.Equal(t, actionID, string(resultActionsValueSearch3[9].(map[string]interface{})["uuid"].(string)))

	// // Search class 'TestAction' AND 'schema:"testString:patch"', should find nothing.
	// bodyObj = graphQLQueryObject{Query: fmt.Sprintf(`{ thing(uuid:"%s") { actions(class:"TestAction", schema:"testString:patch", first:1) { actions { uuid atClass } totalResults } } }`, thingID)}
	// _, respObjectValueSearch4 := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)
	// totalResultsValueSearch4 := respObjectValueSearch4.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["totalResults"].(float64)
	// require.Equal(t, float64(0), totalResultsValueSearch4)

	// // Search class 'TestAction' AND 'schema:"testString:~patch"', should find ActionID as most recent.
	// bodyObj = graphQLQueryObject{Query: fmt.Sprintf(`{ thing(uuid:"%s") { actions(class:"TestAction", schema:"testString:~patch", first:1){ actions { uuid atClass } totalResults } } }`, thingID)}
	// _, respObjectValueSearch5 := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)
	// resultActionsValueSearch5 := respObjectValueSearch5.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["actions"].([]interface{})
	// require.Equal(t, actionID, string(resultActionsValueSearch5[0].(map[string]interface{})["uuid"].(string)))

	// // Search class '~TestAction' AND NOT 'schema:"testString:~patch"', should find thing with id == actionIDs[0] and have a length of 9
	// bodyObj = graphQLQueryObject{Query: fmt.Sprintf(`{ thing(uuid:"%s") { actions(class:"~TestAction", schema:"testString!:~patch", first:1){ actions { uuid atClass } totalResults } } }`, thingID)}
	// _, respObjectValueSearch6 := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)
	// resultActionsValueSearch6 := respObjectValueSearch6.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["actions"].([]interface{})
	// require.Equal(t, actionIDs[0], string(resultActionsValueSearch6[0].(map[string]interface{})["uuid"].(string)))
	// totalResultsValueSearch6 := respObjectValueSearch6.Data["thing"].(map[string]interface{})["actions"].(map[string]interface{})["totalResults"].(float64)
	// require.Equal(t, float64(9), totalResultsValueSearch6)
}

func Test__weaviate_POST_graphql_JSON_internal_listThings(t *testing.T) {
	// Set the graphQL body
	bodyObj := graphQLQueryObject{
		Query: `{ listThings { things { uuid atContext atClass creationTimeUnix } totalResults } }`,
	}

	// Do the GraphQL request
	response, respObject := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)

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
}

func Test__weaviate_POST_graphql_JSON_internal_listThings_forbidden(t *testing.T) {
	// Set the graphQL body
	bodyObj := graphQLQueryObject{
		Query: `{ listThings { things { uuid atContext atClass creationTimeUnix } totalResults } }`,
	}

	// Do the GraphQL request
	response, respObject := doGraphQLRequest(bodyObj, newAPIKeyID, newAPIToken)

	// Check statuscode
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Test that the error in the response is not nil
	require.NotNil(t, respObject.Errors)
}

func Test__weaviate_POST_graphql_JSON_internal_listThings_limit(t *testing.T) {
	// Set the graphQL body
	bodyObj := graphQLQueryObject{
		Query: `{ listThings(first: 3) { things { uuid atContext atClass creationTimeUnix } totalResults } }`,
	}

	// Do the GraphQL request
	responseLimit, respObjectLimit := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)

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
}

func Test__weaviate_POST_graphql_JSON_internal_listThings_limit_offset(t *testing.T) {
	// Set the graphQL body
	bodyObj := graphQLQueryObject{
		Query: `{ listThings(first: 5, offset: 5) { things { uuid atContext atClass creationTimeUnix } totalResults } }`,
	}

	// Do the GraphQL request
	responseLimitOffset, respObjectLimitOffset := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)

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
}

func Test__weaviate_POST_graphql_JSON_internal_listThings_filters(t *testing.T) {
	// Search class 'TestThing2', most recent should be the set thingIDsubject
	bodyObj := graphQLQueryObject{Query: `{ listThings(class:"TestThing2", first:1) { things { uuid atClass } totalResults } }`}
	_, respObjectValueSearch1 := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)
	resultThingsValueSearch1 := respObjectValueSearch1.Data["listThings"].(map[string]interface{})["things"].([]interface{})
	require.Equal(t, thingIDsubject, string(resultThingsValueSearch1[0].(map[string]interface{})["uuid"].(string)))

	// // Search class NOT 'TestThing2', most recent should be the set thingID
	// bodyObj = graphQLQueryObject{Query: `{ listThings(class:"!:TestThing2", first:1) { things { uuid atClass } totalResults } }`}
	// _, respObjectValueSearch2 := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)
	// resultThingsValueSearch2 := respObjectValueSearch2.Data["listThings"].(map[string]interface{})["things"].([]interface{})
	// require.Equal(t, thingID, string(resultThingsValueSearch2[0].(map[string]interface{})["uuid"].(string)))

	// // Search class '~TestThing', most recent 9 should be 'TestThing2' and the 10th is 'TestThing' (with uuid = thingID).
	// // bodyObj = graphQLQueryObject{Query: `{ listThings(class:"~TestThing", first:10) { things { uuid atClass } totalResults } }`}
	// // _, respObjectValueSearch3 := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)
	// // resultThingsValueSearch3 := respObjectValueSearch3.Data["listThings"].(map[string]interface{})["things"].([]interface{})
	// // require.Equal(t, "TestThing2", string(resultThingsValueSearch3[3].(map[string]interface{})["atClass"].(string)))
	// // require.Equal(t, "TestThing2", string(resultThingsValueSearch3[6].(map[string]interface{})["atClass"].(string)))
	// // require.Equal(t, "TestThing", string(resultThingsValueSearch3[9].(map[string]interface{})["atClass"].(string)))
	// // require.Equal(t, thingID, string(resultThingsValueSearch3[9].(map[string]interface{})["uuid"].(string)))

	// // Search class 'TestThing' AND 'schema:"testString:patch"', should find nothing.
	// bodyObj = graphQLQueryObject{Query: `{ listThings(class:"TestThing", schema:"testString:patch", first:1) { things { uuid atClass } totalResults } }`}
	// _, respObjectValueSearch4 := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)
	// totalResultsValueSearch4 := respObjectValueSearch4.Data["listThings"].(map[string]interface{})["totalResults"].(float64)
	// require.Equal(t, float64(0), totalResultsValueSearch4)

	// // Search class 'TestThing' AND 'schema:"testString:~patch"', should find ThingID as most recent.
	// bodyObj = graphQLQueryObject{Query: `{ listThings(class:"TestThing", schema:"testString:~patch", first:1){ things { uuid atClass } totalResults } }`}
	// _, respObjectValueSearch5 := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)
	// resultThingsValueSearch5 := respObjectValueSearch5.Data["listThings"].(map[string]interface{})["things"].([]interface{})
	// require.Equal(t, thingID, string(resultThingsValueSearch5[0].(map[string]interface{})["uuid"].(string)))

	// // Search class '~TestThing' AND NOT 'schema:"testString:~patch"', should find thing with id == thingIDs[0]
	// // bodyObj = graphQLQueryObject{Query: `{ listThings(class:"~TestThing", schema:"testString!:~patch", first:1){ things { uuid atClass } totalResults } }`}
	// // _, respObjectValueSearch6 := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)
	// // resultThingsValueSearch6 := respObjectValueSearch6.Data["listThings"].(map[string]interface{})["things"].([]interface{})
	// // require.Equal(t, thingIDs[0], string(resultThingsValueSearch6[0].(map[string]interface{})["uuid"].(string)))
}

func Test__weaviate_POST_graphql_JSON_internal_action(t *testing.T) {
	// Set the graphQL body
	body := `{ action(uuid:"%s") { uuid atContext atClass creationTimeUnix things { object { uuid } subject { uuid } } key { uuid read } } }`
	bodyObj := graphQLQueryObject{
		Query: fmt.Sprintf(body, actionID),
	}

	// Do the GraphQL request
	response, respObject := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)

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

func Test__weaviate_POST_graphql_JSON_internal_action_forbidden_read(t *testing.T) {
	// Set the graphQL body
	body := `{ action(uuid:"%s") { uuid atContext atClass creationTimeUnix things { object { uuid } subject { uuid } } key { uuid read } } }`
	bodyObj := graphQLQueryObject{
		Query: fmt.Sprintf(body, actionID),
	}

	// Do the GraphQL request
	response, respObject := doGraphQLRequest(bodyObj, newAPIKeyID, newAPIToken)

	// Check statuscode
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Test that the error in the response is nil
	require.NotNil(t, respObject.Errors)
}

func Test__weaviate_POST_graphql_JSON_internal_action_forbidden_parent(t *testing.T) {
	// Set the graphQL body
	body := `{ action(uuid:"%s") { uuid atContext atClass creationTimeUnix things { object { uuid } subject { uuid } } key { uuid read } } }`
	bodyObj := graphQLQueryObject{
		Query: fmt.Sprintf(body, actionID),
	}

	// Do the GraphQL request
	response, respObject := doGraphQLRequest(bodyObj, headKeyID, headToken)

	// Check statuscode
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Test that the error in the response is nil
	require.NotNil(t, respObject.Errors)
}
func Test__weaviate_POST_graphql_JSON_action_external(t *testing.T) {
	// Set the graphQL body
	body := `{ action(uuid:"%s") { uuid things { object { uuid } subject { uuid } } schema { testCref { uuid } } } }`
	bodyObj := graphQLQueryObject{
		Query: fmt.Sprintf(body, actionIDExternal),
	}

	// Do the GraphQL request
	response, respObject := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)

	// Check statuscode
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Test that the error in the response is nil
	require.Nil(t, respObject.Errors)

	// Test the given UUID in the response
	respUUID := respObject.Data["action"].(map[string]interface{})["uuid"]
	require.Regexp(t, strfmt.UUIDPattern, respUUID)
	require.Regexp(t, strfmt.UUIDPattern, actionIDExternal)
	require.Equal(t, actionIDExternal, respUUID)

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

	// Test external uuid
	externalUUID := respObject.Data["action"].(map[string]interface{})["schema"].(map[string]interface{})["testCref"].(map[string]interface{})["uuid"].(string)
	require.Regexp(t, strfmt.UUIDPattern, externalUUID)
	require.Regexp(t, strfmt.UUIDPattern, thingID)
	require.Equal(t, thingID, externalUUID)
}

func Test__weaviate_POST_graphql_JSON_internal_key(t *testing.T) {
	// Set the graphQL body
	body := `{ key(uuid:"%s") { uuid read write ipOrigin children { uuid read } } }`
	bodyObj := graphQLQueryObject{
		Query: fmt.Sprintf(body, newAPIKeyID),
	}

	// Do the GraphQL request
	response, respObject := doGraphQLRequest(bodyObj, apiKeyIDCmdLine, apiTokenCmdLine)

	// Check statuscode
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Test that the error in the response is nil
	require.Nil(t, respObject.Errors)

	// Test the given UUID in the response
	respUUID := respObject.Data["key"].(map[string]interface{})["uuid"]
	require.Regexp(t, strfmt.UUIDPattern, respUUID)
	require.Regexp(t, strfmt.UUIDPattern, newAPIKeyID)
	require.Equal(t, newAPIKeyID, respUUID)

	// Test children
	child1UUID := respObject.Data["key"].(map[string]interface{})["children"].([]interface{})[0].(map[string]interface{})["uuid"].(string)
	child2UUID := respObject.Data["key"].(map[string]interface{})["children"].([]interface{})[1].(map[string]interface{})["uuid"].(string)

	responseIDs := []string{
		child1UUID,
		child2UUID,
	}

	checkIDs := []string{
		headKeyID,
		newSubAPIKeyID,
	}

	// Sort keys
	sort.Strings(responseIDs)
	sort.Strings(checkIDs)

	// Check equality
	require.Equal(t, checkIDs[0], responseIDs[0])
	require.Equal(t, checkIDs[1], responseIDs[1])
}

func Test__weaviate_POST_graphql_JSON_internal_myKey(t *testing.T) {
	// Set the graphQL body
	body := `{ myKey { uuid read write ipOrigin children { uuid read } } }`
	bodyObj := graphQLQueryObject{
		Query: body,
	}

	// Do the GraphQL request
	response, respObject := doGraphQLRequest(bodyObj, newAPIKeyID, newAPIToken)

	// Check statuscode
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Test that the error in the response is nil
	require.Nil(t, respObject.Errors)

	// Test the given UUID in the response
	respUUID := respObject.Data["myKey"].(map[string]interface{})["uuid"]
	require.Regexp(t, strfmt.UUIDPattern, respUUID)
	require.Regexp(t, strfmt.UUIDPattern, newAPIKeyID)
	require.Equal(t, newAPIKeyID, respUUID)

	// Test children
	child1UUID := respObject.Data["myKey"].(map[string]interface{})["children"].([]interface{})[0].(map[string]interface{})["uuid"].(string)
	child2UUID := respObject.Data["myKey"].(map[string]interface{})["children"].([]interface{})[1].(map[string]interface{})["uuid"].(string)

	responseIDs := []string{
		child1UUID,
		child2UUID,
	}

	checkIDs := []string{
		headKeyID,
		newSubAPIKeyID,
	}

	// Sort keys
	sort.Strings(responseIDs)
	sort.Strings(checkIDs)

	// Check equality
	require.Equal(t, checkIDs[0], responseIDs[0])
	require.Equal(t, checkIDs[1], responseIDs[1])
}

func Test__weaviate_POST_graphql_JSON_internal_key_forbidden_parent(t *testing.T) {
	// Set the graphQL body
	body := `{ key(uuid:"%s") { uuid read write ipOrigin children { uuid read } } }`
	bodyObj := graphQLQueryObject{
		Query: fmt.Sprintf(body, newAPIKeyID),
	}

	// Do the GraphQL request
	response, respObject := doGraphQLRequest(bodyObj, headKeyID, headToken)

	// Check statuscode
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Test that the error in the response is nil
	require.NotNil(t, respObject.Errors)
}

/******************
 * REMOVE TESTS
 ******************/

func Test__weaviate_DELETE_keys_id_JSON_internal(t *testing.T) {
	// Delete sub1, check status
	responseDelSub1 := doRequest("/keys/"+sub1KeyID, "DELETE", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNoContent, responseDelSub1.StatusCode)

	// Check sub1 removed and check its statuscode (404)
	responseSub1Deleted := doRequest("/keys/"+sub1KeyID, "DELETE", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, responseSub1Deleted.StatusCode)
}

func Test__weaviate_DELETE_keys_id_JSON_internal_forbidden_parent(t *testing.T) {
	// Delete head with sub2, which is not allowed
	responseDelHeadWithSub := doRequest("/keys/"+headKeyID, "DELETE", "application/json", nil, sub2KeyID, sub2Token)
	require.Equal(t, http.StatusForbidden, responseDelHeadWithSub.StatusCode)

	// Check sub2 exists, check positive status code
	responseSub2Exists := doRequest("/keys/"+sub2KeyID, "GET", "application/json", nil, sub2KeyID, sub2Token)
	require.Equal(t, http.StatusOK, responseSub2Exists.StatusCode)
}

func Test__weaviate_DELETE_keys_id_JSON_internal_forbidden_own(t *testing.T) {
	// Delete head with own key, not allowed
	responseDelHeadForbidden := doRequest("/keys/"+headKeyID, "DELETE", "application/json", nil, headKeyID, headToken)
	require.Equal(t, http.StatusForbidden, responseDelHeadForbidden.StatusCode)
}

func Test__weaviate_DELETE_keys_id_JSON_internal_with_child(t *testing.T) {
	// Delete head with its parent key, allowed
	responseDelHead := doRequest("/keys/"+headKeyID, "DELETE", "application/json", nil, newAPIKeyID, newAPIToken)
	require.Equal(t, http.StatusNoContent, responseDelHead.StatusCode)

	// Check sub2 removed by deleting its parent and check its statuscode (404)
	responseSub2Deleted := doRequest("/keys/"+sub2KeyID, "DELETE", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, responseSub2Deleted.StatusCode)

	// Check head removed and check its statuscode (404)
	responseHeadDeleted := doRequest("/keys/"+headKeyID, "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, responseHeadDeleted.StatusCode)
}

func Test__weaviate_DELETE_keys_id_JSON_internal_root_child_clean_up(t *testing.T) {
	// Delete key that is expired
	responseExpiredDeleted := doRequest("/keys/"+expiredKeyID, "DELETE", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNoContent, responseExpiredDeleted.StatusCode)

	// Delete keyID from database
	responseKeyIDDeleted := doRequest("/keys/"+newAPIKeyID, "DELETE", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNoContent, responseKeyIDDeleted.StatusCode)
}

func Test__weaviate_DELETE_actions_id_JSON_internal(t *testing.T) {
	// Create delete request
	response := doRequest("/actions/"+actionID, "DELETE", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

	// Check status code get request
	require.Equal(t, http.StatusNoContent, response.StatusCode)

	// Test is faster than adding to DB.
	time.Sleep(1000 * time.Millisecond)
}

func Test__weaviate_DELETE_actions_id_JSON_internal_not_found_already_deleted(t *testing.T) {
	// Create delete request
	responseAlreadyDeleted := doRequest("/actions/"+actionID, "DELETE", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

	// Check status code already deleted
	require.Equal(t, http.StatusNotFound, responseAlreadyDeleted.StatusCode)
}

func Test__weaviate_DELETE_actions_id_JSON_internal_not_found(t *testing.T) {
	// Create get request with non-existing ID, check its responsecode
	responseNotFound := doRequest("/actions/"+fakeID, "DELETE", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, responseNotFound.StatusCode)
}

func Test__weaviate_GET_actions_id_history_JSON_internal_after_delete(t *testing.T) {
	// Create patch request
	response := doRequest("/actions/"+actionID+"/history", "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

	// Translate body
	body := getResponseBody(response)
	respObject := &models.ActionGetHistoryResponse{}
	json.Unmarshal(body, respObject)

	// Check status code
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Check length = 3 (put, patch, delete)
	require.Len(t, respObject.PropertyHistory, 3)

	// Check deleted is true
	require.Equal(t, true, respObject.Deleted)
}

func Test__weaviate_DELETE_things_id_JSON_internal(t *testing.T) {
	// Test whether all actions aren't deleted yet
	for _, deletedActionID := range actionIDs {
		// Skip the action ID that is deleted with the previous function
		if deletedActionID == actionID {
			continue
		}

		responseNotYetDeletedByObjectThing := doRequest("/actions/"+deletedActionID, "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)
		require.Equal(t, http.StatusOK, responseNotYetDeletedByObjectThing.StatusCode)
	}

	// Create delete request
	response := doRequest("/things/"+thingID, "DELETE", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

	// Check status code get request
	require.Equal(t, http.StatusNoContent, response.StatusCode)

	// Test is faster than adding to DB.
	time.Sleep(1000 * time.Millisecond)
}

func Test__weaviate_DELETE_things_id_JSON_internal_not_found_already_deleted(t *testing.T) {
	// Create delete request
	responseAlreadyDeleted := doRequest("/things/"+thingID, "DELETE", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

	// Check status code already deleted
	require.Equal(t, http.StatusNotFound, responseAlreadyDeleted.StatusCode)
}

func Test__weaviate_DELETE_things_id_JSON_internal_not_found(t *testing.T) {
	// Create get request with non-existing ID, check its responsecode
	responseNotFound := doRequest("/things/"+fakeID, "DELETE", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)
	require.Equal(t, http.StatusNotFound, responseNotFound.StatusCode)
}

func Test__weaviate_DELETE_things_id_JSON_internal_actions_deleted(t *testing.T) {
	// Test whether all actions are deleted
	for _, deletedActionID := range actionIDs {
		responseDeletedByObjectThing := doRequest("/actions/"+deletedActionID, "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)
		require.Equal(t, http.StatusNotFound, responseDeletedByObjectThing.StatusCode)
	}
}

func Test__weaviate_GET_things_id_history_JSON_internal_after_delete(t *testing.T) {
	// Create patch request
	response := doRequest("/things/"+thingID+"/history", "GET", "application/json", nil, apiKeyIDCmdLine, apiTokenCmdLine)

	// Translate body
	body := getResponseBody(response)
	respObject := &models.ThingGetHistoryResponse{}
	json.Unmarshal(body, respObject)

	// Check status code
	require.Equal(t, http.StatusOK, response.StatusCode)

	// Check length = 3 (put, patch, delete)
	require.Len(t, respObject.PropertyHistory, 3)

	// Check deleted is true
	require.Equal(t, true, respObject.Deleted)
}
