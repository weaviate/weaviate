//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/client/objects"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
)

// executed in setup_test.go
func addingObjects(t *testing.T) {
	class := "TestObject"
	t.Run("can create object", func(t *testing.T) {
		// Set all object values to compare
		objectTestString := "Test string"
		objectTestInt := 1
		objectTestBoolean := true
		objectTestNumber := 1.337
		objectTestDate := "2017-10-06T08:15:30+01:00"

		params := objects.NewObjectsCreateParams().WithBody(
			&models.Object{
				Class: class,
				Properties: map[string]interface{}{
					"testString":      objectTestString,
					"testWholeNumber": objectTestInt,
					"testTrueFalse":   objectTestBoolean,
					"testNumber":      objectTestNumber,
					"testDateTime":    objectTestDate,
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

			// Check whether the returned information is the same as the data added
			assert.Equal(t, objectTestString, schema["testString"])
			assert.Equal(t, objectTestInt, int(testWholeNumber))
			assert.Equal(t, objectTestBoolean, schema["testTrueFalse"])
			assert.Equal(t, objectTestNumber, testNumber)
			assert.Equal(t, objectTestDate, schema["testDateTime"])
		})
	})

	t.Run("can create and get object", func(t *testing.T) {
		objectTestString := "Test string"
		objectTestInt := 1
		objectTestBoolean := true
		objectTestNumber := 1.337
		objectTestDate := "2017-10-06T08:15:30+01:00"

		objectID := assertCreateObject(t, class, map[string]interface{}{
			"testString":      objectTestString,
			"testWholeNumber": objectTestInt,
			"testTrueFalse":   objectTestBoolean,
			"testNumber":      objectTestNumber,
			"testDateTime":    objectTestDate,
		})
		assertGetObjectEventually(t, class, objectID)

		// Now fetch the object
		getResp, err := helper.Client(t).Objects.ObjectsGet(objects.NewObjectsGetParams().WithID(objectID), nil)

		helper.AssertRequestOk(t, getResp, err, func() {
			object := getResp.Payload

			schema, ok := object.Properties.(map[string]interface{})
			if !ok {
				t.Fatal("The returned schema is not an JSON object")
			}

			testWholeNumber, _ := schema["testWholeNumber"].(json.Number).Int64()
			testNumber, _ := schema["testNumber"].(json.Number).Float64()

			// Check whether the returned information is the same as the data added
			assert.Equal(t, objectTestString, schema["testString"])
			assert.Equal(t, objectTestInt, int(testWholeNumber))
			assert.Equal(t, objectTestBoolean, schema["testTrueFalse"])
			assert.Equal(t, objectTestNumber, testNumber)
			assert.Equal(t, objectTestDate, schema["testDateTime"])
		})
	})

	t.Run("can add single ref", func(t *testing.T) {
		fmt.Println("before first")
		firstID := assertCreateObject(t, class, map[string]interface{}{})
		assertGetObjectEventually(t, class, firstID)

		secondID := assertCreateObject(t, "TestObjectTwo", map[string]interface{}{
			"testString": "stringy",
			"testReference": []interface{}{
				map[string]interface{}{
					"beacon": fmt.Sprintf("weaviate://localhost/%s", firstID),
				},
			},
		})

		secondObject := assertGetObjectEventually(t, "TestObjectTwo", secondID)

		singleRef := secondObject.Properties.(map[string]interface{})["testReference"].([]interface{})[0].(map[string]interface{})
		assert.Equal(t, singleRef["beacon"].(string), fmt.Sprintf("weaviate://localhost/%s", firstID))
	})
}
