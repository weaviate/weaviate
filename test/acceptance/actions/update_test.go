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

// Acceptance tests for objects

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// run from setup_test.go
func updateObjectsDeprecated(t *testing.T) {
	t.Run("update and set number", func(t *testing.T) {
		uuid := helper.AssertCreateObject(t, "TestObject", map[string]interface{}{})
		helper.AssertGetObjectEventually(t, "TestObject", uuid)

		schema := models.PropertySchema(map[string]interface{}{
			"testNumber": 41.0,
		})

		update := models.Object{}
		update.Properties = schema
		update.Class = "TestObject"
		update.ID = uuid

		params := objects.NewObjectsUpdateParams().WithID(uuid).WithBody(&update)
		updateResp, err := helper.Client(t).Objects.ObjectsUpdate(params, nil)
		helper.AssertRequestOk(t, updateResp, err, nil)

		actualThunk := func() interface{} {
			updatedObject := helper.AssertGetObject(t, update.Class, uuid)
			updatedSchema := updatedObject.Properties.(map[string]interface{})
			if updatedSchema["testNumber"] == nil {
				return nil
			}
			num, _ := updatedSchema["testNumber"].(json.Number).Float64()
			return num
		}
		helper.AssertEventuallyEqual(t, 41.0, actualThunk)
	})

	t.Run("update and set string", func(t *testing.T) {
		uuid := helper.AssertCreateObject(t, "TestObject", map[string]interface{}{})
		helper.AssertGetObjectEventually(t, "TestObject", uuid)

		schema := models.PropertySchema(map[string]interface{}{
			"testString": "wibbly wobbly",
		})

		update := models.Object{}
		update.Properties = schema
		update.Class = "TestObject"
		update.ID = uuid

		params := objects.NewObjectsUpdateParams().WithID(uuid).WithBody(&update)
		updateResp, err := helper.Client(t).Objects.ObjectsUpdate(params, nil)
		helper.AssertRequestOk(t, updateResp, err, nil)

		actualThunk := func() interface{} {
			updatedObject := helper.AssertGetObject(t, update.Class, uuid)
			updatedSchema := updatedObject.Properties.(map[string]interface{})
			return updatedSchema["testString"]
		}
		helper.AssertEventuallyEqual(t, "wibbly wobbly", actualThunk)
	})

	t.Run("update and set bool", func(t *testing.T) {
		t.Parallel()
		uuid := helper.AssertCreateObject(t, "TestObject", map[string]interface{}{})
		helper.AssertGetObjectEventually(t, "TestObject", uuid)

		schema := models.PropertySchema(map[string]interface{}{
			"testTrueFalse": true,
		})

		update := models.Object{}
		update.Properties = schema
		update.Class = "TestObject"
		update.ID = uuid

		params := objects.NewObjectsUpdateParams().WithID(uuid).WithBody(&update)
		updateResp, err := helper.Client(t).Objects.ObjectsUpdate(params, nil)

		helper.AssertRequestOk(t, updateResp, err, nil)

		actualThunk := func() interface{} {
			updatedObject := helper.AssertGetObject(t, update.Class, uuid)
			updatedSchema := updatedObject.Properties.(map[string]interface{})
			return updatedSchema["testTrueFalse"]
		}
		helper.AssertEventuallyEqual(t, true, actualThunk)
	})

	t.Run("can patch object with cref", func(t *testing.T) {
		thingToRefID := helper.AssertCreateObject(t, "ObjectTestThing", nil)
		helper.AssertGetObjectEventually(t, "ObjectTestThing", thingToRefID)
		objectID := helper.AssertCreateObject(t, "TestObject", nil)
		helper.AssertGetObjectEventually(t, "TestObject", objectID)

		merge := &models.Object{
			Class: "TestObject",
			Properties: map[string]interface{}{
				"testReference": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/%s", thingToRefID),
					},
				},
			},
		}

		// Now to try to link
		params := objects.NewObjectsPatchParams().
			WithBody(merge).
			WithID(objectID)
		patchResp, err := helper.Client(t).Objects.ObjectsPatch(params, nil)
		spew.Dump(err)
		helper.AssertRequestOk(t, patchResp, err, nil)

		actualThunk := func() interface{} {
			patchedObject := helper.AssertGetObject(t, merge.Class, objectID)

			rawRef, ok := patchedObject.Properties.(map[string]interface{})["testReference"]
			if !ok {
				return nil
			}

			refsSlice, ok := rawRef.([]interface{})
			if !ok {
				t.Logf("found the ref prop, but it was not a slice, but %T", refsSlice)
				t.Fail()
			}

			if len(refsSlice) != 1 {
				t.Logf("expected ref slice to have one element, but got: %d", len(refsSlice))
				t.Fail()
			}

			refMap, ok := refsSlice[0].(map[string]interface{})
			if !ok {
				t.Logf("found the ref element, but it was not a map, but %T", refsSlice[0])
				t.Fail()
			}

			return refMap["beacon"]
		}

		helper.AssertEventuallyEqual(t, fmt.Sprintf("weaviate://localhost/ObjectTestThing/%s", thingToRefID), actualThunk)
	})
}
