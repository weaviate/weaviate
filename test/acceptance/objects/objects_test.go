//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

// Acceptance tests for objects.

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	testhelper "github.com/weaviate/weaviate/test/helper"
)

// run from setup_test.go
func creatingObjects(t *testing.T) {
	const fakeObjectId strfmt.UUID = "11111111-1111-1111-1111-111111111111"

	t.Run("create object with user specified id", func(t *testing.T) {
		var (
			id        = strfmt.UUID("d47ea61b-0ed7-4e5f-9c05-6d2c0786660f")
			className = "TestObject"
			// Set all object values to compare
			objectTestString = "Test string"
		)
		// clean up to make sure we can run this test multiple times in a row
		defer func() {
			params := objects.NewObjectsDeleteParams().WithID(id)
			helper.Client(t).Objects.ObjectsDelete(params, nil)
			{
				params := objects.NewObjectsClassGetParams()
				params.WithClassName(className).WithID(id)
				_, err := helper.Client(t).Objects.ObjectsClassGet(params, nil)
				if err == nil {
					t.Errorf("Object %v cannot exist after deletion", id)
				}
				werr := new(objects.ObjectsClassGetNotFound)
				if ok := errors.As(err, &werr); !ok {
					t.Errorf("get deleted object err got: %v want: %v", err, werr)
				}
			}
		}()

		params := objects.NewObjectsCreateParams().WithBody(
			&models.Object{
				ID:    id,
				Class: className,
				Properties: map[string]interface{}{
					"testString": objectTestString,
				},
			})

		resp, err := helper.Client(t).Objects.ObjectsCreate(params, nil)

		// Ensure that the response is OK
		helper.AssertRequestOk(t, resp, err, func() {
			object := resp.Payload
			assert.Regexp(t, strfmt.UUIDPattern, object.ID)

			schema, ok := object.Properties.(map[string]interface{})
			if !ok {
				t.Fatal("The returned schema is not an JSON object")
			}

			// Check whether the returned information is the same as the data added
			assert.Equal(t, objectTestString, schema["testString"])
		})

		// wait for the object to be created
		testhelper.AssertEventuallyEqual(t, id, func() interface{} {
			params := objects.NewObjectsClassGetParams()
			params.WithClassName(className).WithID(id)
			object, err := helper.Client(t).Objects.ObjectsClassGet(params, nil)
			if err != nil {
				return nil
			}

			return object.Payload.ID
		})
		// deprecated: is here because of backward compatibility reasons
		testhelper.AssertEventuallyEqual(t, id, func() interface{} {
			params := objects.NewObjectsGetParams().WithID(id)
			object, err := helper.Client(t).Objects.ObjectsGet(params, nil)
			if err != nil {
				return nil
			}

			return object.Payload.ID
		})

		// Try to create the same object again and make sure it fails
		params = objects.NewObjectsCreateParams().WithBody(
			&models.Object{
				ID:    id,
				Class: "TestObject",
				Properties: map[string]interface{}{
					"testString": objectTestString,
				},
			})

		resp, err = helper.Client(t).Objects.ObjectsCreate(params, nil)
		helper.AssertRequestFail(t, resp, err, func() {
			errResponse, ok := err.(*objects.ObjectsCreateUnprocessableEntity)
			if !ok {
				t.Fatalf("Did not get not found response, but %#v", err)
			}

			assert.Equal(t, fmt.Sprintf("id '%s' already exists", id), errResponse.Payload.Error[0].Message)
		})
	})

	// Check if we can create a Object, and that it's properties are stored correctly.
	t.Run("creating a object", func(t *testing.T) {
		t.Parallel()
		// Set all object values to compare
		objectTestString := "Test string"
		objectTestInt := 1
		objectTestBoolean := true
		objectTestNumber := 1.337
		objectTestDate := "2017-10-06T08:15:30+01:00"
		objectTestPhoneNumber := map[string]interface{}{
			"input":          "0171 11122233",
			"defaultCountry": "DE",
		}

		params := objects.NewObjectsCreateParams().WithBody(
			&models.Object{
				Class: "TestObject",
				Properties: map[string]interface{}{
					"testString":      objectTestString,
					"testWholeNumber": objectTestInt,
					"testTrueFalse":   objectTestBoolean,
					"testNumber":      objectTestNumber,
					"testDateTime":    objectTestDate,
					"testPhoneNumber": objectTestPhoneNumber,
				},
			})

		resp, err := helper.Client(t).Objects.ObjectsCreate(params, nil)

		// Ensure that the response is OK
		helper.AssertRequestOk(t, resp, err, func() {
			object := resp.Payload
			assert.Regexp(t, strfmt.UUIDPattern, object.ID)

			schema, ok := object.Properties.(map[string]interface{})
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
			assert.Equal(t, objectTestString, schema["testString"])
			assert.Equal(t, objectTestInt, int(testWholeNumber))
			assert.Equal(t, objectTestBoolean, schema["testTrueFalse"])
			assert.Equal(t, objectTestNumber, testNumber)
			assert.Equal(t, objectTestDate, schema["testDateTime"])
			assert.Equal(t, expectedParsedPhoneNumber, schema["testPhoneNumber"])
		})
	})

	// Examples of how a Object can be invalid.
	invalidObjectTestCases := []struct {
		// What is wrong in this example
		mistake string

		// the example object, with a mistake.
		// this is a function, so that we can use utility functions like
		// helper.GetWeaviateURL(), which might not be initialized yet
		// during the static construction of the examples.
		object func() *models.Object

		// Enable the option to perform some extra assertions on the error response
		errorCheck func(t *testing.T, err *models.ErrorResponse)
	}{
		{
			mistake: "missing the class",
			object: func() *models.Object {
				return &models.Object{
					Properties: map[string]interface{}{
						"testString": "test",
					},
				}
			},
			errorCheck: func(t *testing.T, err *models.ErrorResponse) {
				assert.Equal(t, "invalid object: the given class is empty", err.Error[0].Message)
			},
		},
		// AUTO_SCHEMA creates classes automatically
		// {
		// 	mistake: "non existing class",
		// 	object: func() *models.Object {
		// 		return &models.Object{
		// 			Class: "NonExistingClass",
		// 			Properties: map[string]interface{}{
		// 				"testString": "test",
		// 			},
		// 		}
		// 	},
		// 	errorCheck: func(t *testing.T, err *models.ErrorResponse) {
		// 		assert.Equal(t, fmt.Sprintf("invalid object: class '%s' not present in schema", "NonExistingClass"), err.Error[0].Message)
		// 	},
		// },
		// AUTO_SCHEMA creates missing properties automatically
		// {
		// 	mistake: "non existing property",
		// 	object: func() *models.Object {
		// 		return &models.Object{
		// 			Class: "TestObject",
		// 			Properties: map[string]interface{}{
		// 				"nonExistingProperty": "test",
		// 			},
		// 		}
		// 	},
		// 	errorCheck: func(t *testing.T, err *models.ErrorResponse) {
		// 		assert.Equal(t, fmt.Sprintf("invalid object: "+schema.ErrorNoSuchProperty, "nonExistingProperty", "TestObject"), err.Error[0].Message)
		// 	},
		// },
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
			object: func() *models.Object {
				return &models.Object{
					Class: "TestObject",
					Properties: map[string]interface{}{
						"testReference": map[string]interface{}{
							"beacon": fakeObjectId,
							"x":      nil,
							"type":   "Object",
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
			object: func() *models.Object {
				return &models.Object{
					Class: "TestObject",
					Properties: map[string]interface{}{
						"testString": 2,
					},
				}
			},
			errorCheck: func(t *testing.T, err *models.ErrorResponse) {
				assert.Contains(t,
					"invalid object: invalid text property 'testString' on class 'TestObject': not a string, but json.Number",
					err.Error[0].Message)
			},
		},
	}

	// Check that none of the examples of invalid objects can be created.
	t.Run("cannot create invalid objects", func(t *testing.T) {
		// invalidObjectTestCases defined below this test.
		for _, example_ := range invalidObjectTestCases {
			t.Run(example_.mistake, func(t *testing.T) {
				example := example_ // Needed; example is updated to point to a new test case.
				t.Parallel()

				params := objects.NewObjectsCreateParams().WithBody(example.object())
				resp, err := helper.Client(t).Objects.ObjectsCreate(params, nil)
				helper.AssertRequestFail(t, resp, err, func() {
					errResponse, ok := err.(*objects.ObjectsCreateUnprocessableEntity)
					if !ok {
						t.Fatalf("Did not get not found response, but %#v", err)
					}
					example.errorCheck(t, errResponse.Payload)
				})
			})
		}
	})
}
