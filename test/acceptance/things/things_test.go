//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

// Acceptance tests for things.

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"

	"github.com/stretchr/testify/assert"

	"github.com/semi-technologies/weaviate/client/things"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	testhelper "github.com/semi-technologies/weaviate/test/helper"
)

// run from setup_test.go
func creatingThings(t *testing.T) {
	const fakeThingId strfmt.UUID = "11111111-1111-1111-1111-111111111111"

	t.Run("create thing with user specified id", func(t *testing.T) {
		id := strfmt.UUID("d47ea61b-0ed7-4e5f-9c05-6d2c0786660f")
		// clean up to make sure we can run this test multiple times in a row
		defer func() {
			params := things.NewThingsDeleteParams().WithID(id)
			helper.Client(t).Things.ThingsDelete(params, nil)
		}()

		// Set all thing values to compare
		thingTestString := "Test string"

		params := things.NewThingsCreateParams().WithBody(
			&models.Thing{
				ID:    id,
				Class: "TestThing",
				Schema: map[string]interface{}{
					"testString": thingTestString,
				},
			})

		resp, err := helper.Client(t).Things.ThingsCreate(params, nil)

		// Ensure that the response is OK
		helper.AssertRequestOk(t, resp, err, func() {
			thing := resp.Payload
			assert.Regexp(t, strfmt.UUIDPattern, thing.ID)

			schema, ok := thing.Schema.(map[string]interface{})
			if !ok {
				t.Fatal("The returned schema is not an JSON object")
			}

			// Check whether the returned information is the same as the data added
			assert.Equal(t, thingTestString, schema["testString"])
		})

		// wait for the thing to be created
		testhelper.AssertEventuallyEqual(t, id, func() interface{} {
			thing, err := helper.Client(t).Things.ThingsGet(things.NewThingsGetParams().WithID(id), nil)
			if err != nil {
				return nil
			}

			return thing.Payload.ID
		})

		// Try to create the same thing again and make sure it fails
		params = things.NewThingsCreateParams().WithBody(
			&models.Thing{
				ID:    id,
				Class: "TestThing",
				Schema: map[string]interface{}{
					"testString": thingTestString,
				},
			})

		resp, err = helper.Client(t).Things.ThingsCreate(params, nil)
		helper.AssertRequestFail(t, resp, err, func() {
			errResponse, ok := err.(*things.ThingsCreateUnprocessableEntity)
			if !ok {
				t.Fatalf("Did not get not found response, but %#v", err)
			}

			assert.Equal(t, fmt.Sprintf("id '%s' already exists", id), errResponse.Payload.Error[0].Message)
		})
	})

	// Check if we can create a Thing, and that it's properties are stored correctly.
	t.Run("creating a thing", func(t *testing.T) {
		t.Parallel()
		// Set all thing values to compare
		thingTestString := "Test string"
		thingTestInt := 1
		thingTestBoolean := true
		thingTestNumber := 1.337
		thingTestDate := "2017-10-06T08:15:30+01:00"
		thingTestPhoneNumber := map[string]interface{}{
			"input":          "0171 11122233",
			"defaultCountry": "DE",
		}

		params := things.NewThingsCreateParams().WithBody(
			&models.Thing{
				Class: "TestThing",
				Schema: map[string]interface{}{
					"testString":      thingTestString,
					"testWholeNumber": thingTestInt,
					"testTrueFalse":   thingTestBoolean,
					"testNumber":      thingTestNumber,
					"testDateTime":    thingTestDate,
					"testPhoneNumber": thingTestPhoneNumber,
				},
			})

		resp, err := helper.Client(t).Things.ThingsCreate(params, nil)

		// Ensure that the response is OK
		helper.AssertRequestOk(t, resp, err, func() {
			thing := resp.Payload
			assert.Regexp(t, strfmt.UUIDPattern, thing.ID)

			schema, ok := thing.Schema.(map[string]interface{})
			if !ok {
				t.Fatal("The returned schema is not an JSON object")
			}

			testWholeNumber, _ := schema["testWholeNumber"].(json.Number).Int64()
			testNumber, _ := schema["testNumber"].(json.Number).Float64()

			expectedParsedPhoneNumber := map[string]interface{}{
				"input":                  "0171 11122233",
				"defaultCountry":         "DE",
				"countryCode":            json.Number("49"),
				"internationalFormatted": "+49 171 11122233",
				"national":               json.Number("17111122233"),
				"nationalFormatted":      "0171 11122233",
				"valid":                  true,
			}

			// Check whether the returned information is the same as the data added
			assert.Equal(t, thingTestString, schema["testString"])
			assert.Equal(t, thingTestInt, int(testWholeNumber))
			assert.Equal(t, thingTestBoolean, schema["testTrueFalse"])
			assert.Equal(t, thingTestNumber, testNumber)
			assert.Equal(t, thingTestDate, schema["testDateTime"])
			assert.Equal(t, expectedParsedPhoneNumber, schema["testPhoneNumber"])
		})
	})

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
				assert.Equal(t, fmt.Sprintf("invalid thing: class '%s' not present in schema", "NonExistingClass"), err.Error[0].Message)
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
			       case "beacon": cref = val
			       case "type": type_ = val
			       case "locationUrl": locationUrl = val
			       default:
			         return fmt.Errof("Unexpected key %s", key)
			     }
			   }
			   if cref == nil { return fmt.Errorf("beacon missing") }
			   if type_ == nil { return fmt.Errorf("type missing") }
			   if locationUrl == nil { return fmt.Errorf("locationUrl missing") }

			   // now everything has a valid state.
			*/
			mistake: "invalid cref, property missing locationUrl",
			thing: func() *models.Thing {
				return &models.Thing{
					Class: "TestThing",
					Schema: map[string]interface{}{
						"testReference": map[string]interface{}{
							"beacon": fakeThingId,
							"x":      nil,
							"type":   "Thing",
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

	// Check that none of the examples of invalid things can be created.
	t.Run("cannot create invalid things", func(t *testing.T) {
		// invalidThingTestCases defined below this test.
		for _, example_ := range invalidThingTestCases {
			t.Run(example_.mistake, func(t *testing.T) {
				example := example_ // Needed; example is updated to point to a new test case.
				t.Parallel()

				params := things.NewThingsCreateParams().WithBody(example.thing())
				resp, err := helper.Client(t).Things.ThingsCreate(params, nil)
				helper.AssertRequestFail(t, resp, err, func() {
					errResponse, ok := err.(*things.ThingsCreateUnprocessableEntity)
					if !ok {
						t.Fatalf("Did not get not found response, but %#v", err)
					}
					example.errorCheck(t, errResponse.Payload)
				})
			})
		}
	})
}
