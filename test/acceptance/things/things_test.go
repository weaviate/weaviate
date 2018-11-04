package test

// Acceptance tests for things.

import (
	"fmt"
	"testing"

	//	"sort"
	//	"time"

	//	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/stretchr/testify/assert"

	"github.com/creativesoftwarefdn/weaviate/client/things"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"github.com/creativesoftwarefdn/weaviate/validation"

	connutils "github.com/creativesoftwarefdn/weaviate/database/connectors/utils"
)

const fakeThingId strfmt.UUID = "11111111-1111-1111-1111-111111111111"

// Check if we can create a Thing, and that it's properties are stored correctly.
func TestCreateThingWorks(t *testing.T) {
	t.Parallel()
	// Set all thing values to compare
	thingTestString := "Test string"
	thingTestInt := 1
	thingTestBoolean := true
	thingTestNumber := 1.337
	thingTestDate := "2017-10-06T08:15:30+01:00"

	params := things.NewWeaviateThingsCreateParams().WithBody(things.WeaviateThingsCreateBody{
		Thing: &models.ThingCreate{
			AtContext: "http://example.org",
			AtClass:   "TestThing",
			Schema: map[string]interface{}{
				"testString":   thingTestString,
				"testInt":      thingTestInt,
				"testBoolean":  thingTestBoolean,
				"testNumber":   thingTestNumber,
				"testDateTime": thingTestDate,
			},
		},
	})

	resp, _, err := helper.Client(t).Things.WeaviateThingsCreate(params, helper.RootAuth)

	// Ensure that the response is OK
	helper.AssertRequestOk(t, resp, err, func() {
		thing := resp.Payload
		assert.Regexp(t, strfmt.UUIDPattern, thing.ThingID)

		schema, ok := thing.Schema.(map[string]interface{})
		if !ok {
			t.Fatal("The returned schema is not an JSON object")
		}

		// Check whether the returned information is the same as the data added
		assert.Equal(t, thingTestString, schema["testString"])
		assert.Equal(t, thingTestInt, int(schema["testInt"].(float64)))
		assert.Equal(t, thingTestBoolean, schema["testBoolean"])
		assert.Equal(t, thingTestNumber, schema["testNumber"])
		assert.Equal(t, thingTestDate, schema["testDateTime"])
	})
}

// TODO: add test for async creation

// Check that none of the examples of invalid things can be created.
func TestCannotCreateInvalidThings(t *testing.T) {
	t.Parallel()

	// invalidThingTestCases defined below this test.
	for _, example_ := range invalidThingTestCases {
		t.Run(example_.mistake, func(t *testing.T) {
			example := example_ // Needed; example is updated to point to a new test case.
			t.Parallel()

			params := things.NewWeaviateThingsCreateParams().WithBody(things.WeaviateThingsCreateBody{Thing: example.thing()})
			resp, _, err := helper.Client(t).Things.WeaviateThingsCreate(params, helper.RootAuth)
			helper.AssertRequestFail(t, resp, err, func() {
				errResponse, ok := err.(*things.WeaviateThingsCreateUnprocessableEntity)
				if !ok {
					t.Fatalf("Did not get not found response, but %#v", err)
				}
				example.errorCheck(t, errResponse.Payload)
			})
		})
	}
}

// Examples of how a Thing can be invalid.
var invalidThingTestCases = []struct {
	// What is wrong in this example
	mistake string

	// the example thing, with a mistake.
	// this is a function, so that we can use utility functions like
	// helper.GetWeaviateURL(), which might not be initialized yet
	// during the static construction of the examples.
	thing func() *models.ThingCreate

	// Enable the option to perform some extra assertions on the error response
	errorCheck func(t *testing.T, err *models.ErrorResponse)
}{
	{
		mistake: "missing the class",
		thing: func() *models.ThingCreate {
			return &models.ThingCreate{
				AtContext: "http://example.org",
				Schema: map[string]interface{}{
					"testString": "test",
				},
			}
		},
		errorCheck: func(t *testing.T, err *models.ErrorResponse) {
			assert.Equal(t, validation.ErrorMissingClass, err.Error.Message)
		},
	},
	{
		mistake: "missing the context",
		thing: func() *models.ThingCreate {
			return &models.ThingCreate{
				AtClass: "TestThing",
				Schema: map[string]interface{}{
					"testString": "test",
				},
			}
		},
		errorCheck: func(t *testing.T, err *models.ErrorResponse) {
			assert.Equal(t, validation.ErrorMissingContext, err.Error.Message)
		},
	},
	{
		mistake: "non existing class",
		thing: func() *models.ThingCreate {
			return &models.ThingCreate{
				AtClass:   "NonExistingClass",
				AtContext: "http://example.org",
				Schema: map[string]interface{}{
					"testString": "test",
				},
			}
		},
		errorCheck: func(t *testing.T, err *models.ErrorResponse) {
			assert.Equal(t, fmt.Sprintf(schema.ErrorNoSuchClass, "NonExistingClass"), err.Error.Message)
		},
	},
	{
		mistake: "non existing property",
		thing: func() *models.ThingCreate {
			return &models.ThingCreate{
				AtClass:   "TestThing",
				AtContext: "http://example.org",
				Schema: map[string]interface{}{
					"nonExistingProperty": "test",
				},
			}
		},
		errorCheck: func(t *testing.T, err *models.ErrorResponse) {
			assert.Equal(t, fmt.Sprintf(schema.ErrorNoSuchProperty, "nonExistingProperty", "TestThing"), err.Error.Message)
		},
	},
	{
		mistake: "invalid cref, property missing cref",
		thing: func() *models.ThingCreate {
			return &models.ThingCreate{
				AtClass:   "TestThing",
				AtContext: "http://example.org",
				Schema: map[string]interface{}{
					"testCref": map[string]interface{}{
						"locationUrl": helper.GetWeaviateURL(),
						"type":        "Thing",
					},
				},
			}
		},
		errorCheck: func(t *testing.T, err *models.ErrorResponse) {
			assert.Equal(t, fmt.Sprintf(validation.ErrorInvalidSingleRef, "TestThing", "testCref"), err.Error.Message)
		},
	},
	{
		/* TODO: don't count nr of elements in validation. Just validate keys, and _also_ generate an error on superfluous keys.
		   E.g.
		   var cref *string
		   var type_ *string
		   var locationUrl *string

		   for key, val := range(propertyValue) {
		     switch key {
		       case "$cref": cref = val
		       case "type": type_ = val
		       case "locationUrl": locationUrl = val
		       default:
		         return fmt.Errof("Unexpected key %s", key)
		     }
		   }
		   if cref == nil { return fmt.Errorf("$cref missing") }
		   if type_ == nil { return fmt.Errorf("type missing") }
		   if locationUrl == nil { return fmt.Errorf("locationUrl missing") }

		   // now everything has a valid state.
		*/
		mistake: "invalid cref, property missing locationUrl",
		thing: func() *models.ThingCreate {
			return &models.ThingCreate{
				AtClass:   "TestThing",
				AtContext: "http://example.org",
				Schema: map[string]interface{}{
					"testCref": map[string]interface{}{
						"$cref": fakeThingId,
						"x":     nil,
						"type":  "Thing",
					},
				},
			}
		},
		errorCheck: func(t *testing.T, err *models.ErrorResponse) {
			assert.Equal(t, fmt.Sprintf(validation.ErrorMissingSingleRefLocationURL, "TestThing", "testCref"), err.Error.Message)
		},
	},
	{
		mistake: "invalid cref, wrong type",
		thing: func() *models.ThingCreate {
			return &models.ThingCreate{
				AtClass:   "TestThing",
				AtContext: "http://example.org",
				Schema: map[string]interface{}{
					"testCref": map[string]interface{}{
						"$cref":       fakeThingId,
						"locationUrl": helper.GetWeaviateURL(),
						"type":        "invalid type",
					},
				},
			}
		},
		errorCheck: func(t *testing.T, err *models.ErrorResponse) {
			assert.Equal(t, fmt.Sprintf(validation.ErrorInvalidClassType, "TestThing", "testCref", connutils.RefTypeAction, connutils.RefTypeThing, connutils.RefTypeKey), err.Error.Message)
		},
	},
	{
		mistake: "invalid property; assign int to string",
		thing: func() *models.ThingCreate {
			return &models.ThingCreate{
				AtClass:   "TestThing",
				AtContext: "http://example.org",
				Schema: map[string]interface{}{
					"testString": 2,
				},
			}
		},
		errorCheck: func(t *testing.T, err *models.ErrorResponse) {
			assert.Contains(t, fmt.Sprintf(validation.ErrorInvalidString, "TestThing", "testString", 2), err.Error.Message)
		},
	},
}

/*
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
*/

/*
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
			"testDateTime": "%s",
			"testRandomType": "%s"
		}
	}`, thingTestString, thingTestInt, thingTestBoolean, thingTestNumber, thingTestDate, thingTestString)))
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
	require.Conditionf(t, func() bool { return !(respObject.LastUpdateTimeUnix < now-4000) }, "LastUpdateTimeUnix is incorrect, it was set too far back.")

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

// skip this?
func Test__weaviate_POST_things_JSON_external(t *testing.T) {
  // Don't know.
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

	debug(body)

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
				"testRandomType": %d,
				"testCref": {
					"$cref": "%s",
					"locationUrl": "%s",
					"type": "Thing"
				}
			}
		}`, thingTestString, thingTestInt, thingTestBoolean, thingTestNumber, thingTestDate, thingTestInt, thingID, getWeaviateURL())))
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

*/

func cleanupThing(uuid strfmt.UUID) {
	params := things.NewWeaviateThingsDeleteParams().WithThingID(uuid)
	resp, err := helper.Client(nil).Things.WeaviateThingsDelete(params, helper.RootAuth)
	if err != nil {
		panic(fmt.Sprintf("Could not clean up thing '%s', because %v. Response: %#v", string(uuid), err, resp))
	}
}
