/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package test

// Acceptance tests for things.

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"

	"github.com/stretchr/testify/assert"

	"github.com/creativesoftwarefdn/weaviate/client/things"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
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

	params := things.NewWeaviateThingsCreateParams().WithBody(
		&models.Thing{
			Class: "TestThing",
			Schema: map[string]interface{}{
				"testString":   thingTestString,
				"testInt":      thingTestInt,
				"testBoolean":  thingTestBoolean,
				"testNumber":   thingTestNumber,
				"testDateTime": thingTestDate,
			},
		})

	resp, err := helper.Client(t).Things.WeaviateThingsCreate(params, nil)

	// Ensure that the response is OK
	helper.AssertRequestOk(t, resp, err, func() {
		thing := resp.Payload
		assert.Regexp(t, strfmt.UUIDPattern, thing.ID)

		schema, ok := thing.Schema.(map[string]interface{})
		if !ok {
			t.Fatal("The returned schema is not an JSON object")
		}

		testInt, _ := schema["testInt"].(json.Number).Int64()
		testNumber, _ := schema["testNumber"].(json.Number).Float64()

		// Check whether the returned information is the same as the data added
		assert.Equal(t, thingTestString, schema["testString"])
		assert.Equal(t, thingTestInt, int(testInt))
		assert.Equal(t, thingTestBoolean, schema["testBoolean"])
		assert.Equal(t, thingTestNumber, testNumber)
		assert.Equal(t, thingTestDate, schema["testDateTime"])
	})
}

// Check that none of the examples of invalid things can be created.
func TestCannotCreateInvalidThings(t *testing.T) {
	t.Parallel()

	// invalidThingTestCases defined below this test.
	for _, example_ := range invalidThingTestCases {
		t.Run(example_.mistake, func(t *testing.T) {
			example := example_ // Needed; example is updated to point to a new test case.
			t.Parallel()

			params := things.NewWeaviateThingsCreateParams().WithBody(example.thing())
			resp, err := helper.Client(t).Things.WeaviateThingsCreate(params, nil)
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
	thing func() *models.Thing

	// Enable the option to perform some extra assertions on the error response
	errorCheck func(t *testing.T, err *models.ErrorResponse)
}{
	{
		mistake: "missing the class",
		thing: func() *models.Thing {
			return &models.Thing{
				Schema: map[string]interface{}{
					"testString": "test",
				},
			}
		},
		errorCheck: func(t *testing.T, err *models.ErrorResponse) {
			assert.Equal(t, "invalid thing: the given class is empty", err.Error[0].Message)
		},
	},
	{
		mistake: "non existing class",
		thing: func() *models.Thing {
			return &models.Thing{
				Class: "NonExistingClass",
				Schema: map[string]interface{}{
					"testString": "test",
				},
			}
		},
		errorCheck: func(t *testing.T, err *models.ErrorResponse) {
			assert.Equal(t, fmt.Sprintf("invalid thing: "+schema.ErrorNoSuchClass, "NonExistingClass"), err.Error[0].Message)
		},
	},
	{
		mistake: "non existing property",
		thing: func() *models.Thing {
			return &models.Thing{
				Class: "TestThing",
				Schema: map[string]interface{}{
					"nonExistingProperty": "test",
				},
			}
		},
		errorCheck: func(t *testing.T, err *models.ErrorResponse) {
			assert.Equal(t, fmt.Sprintf("invalid thing: "+schema.ErrorNoSuchProperty, "nonExistingProperty", "TestThing"), err.Error[0].Message)
		},
	},
	{
		/* TODO gh-616: don't count nr of elements in validation. Just validate keys, and _also_ generate an error on superfluous keys.
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
		thing: func() *models.Thing {
			return &models.Thing{
				Class: "TestThing",
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
			assert.NotNil(t, err)
		},
	},
	{
		mistake: "invalid property; assign int to string",
		thing: func() *models.Thing {
			return &models.Thing{
				Class: "TestThing",
				Schema: map[string]interface{}{
					"testString": 2,
				},
			}
		},
		errorCheck: func(t *testing.T, err *models.ErrorResponse) {
			assert.Contains(t,
				fmt.Sprintf("invalid thing: invalid string property 'testString' on class 'TestThing': not a string, but json.Number"),
				err.Error[0].Message)
		},
	},
}

func cleanupThing(uuid strfmt.UUID) {
	params := things.NewWeaviateThingsDeleteParams().WithID(uuid)
	resp, err := helper.Client(nil).Things.WeaviateThingsDelete(params, nil)
	if err != nil {
		panic(fmt.Sprintf("Could not clean up thing '%s', because %v. Response: %#v", string(uuid), err, resp))
	}
}
