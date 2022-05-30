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

// Acceptance tests for objects

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/semi-technologies/weaviate/client/objects"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	testhelper "github.com/semi-technologies/weaviate/test/helper"
)

// run from setup_test.go
func updateObjectsDeprecated(t *testing.T) {
	t.Run("update and set number", func(t *testing.T) {
		uuid := assertCreateObject(t, "TestObject", map[string]interface{}{})
		assertGetObjectEventually(t, uuid)

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
			updatedObject := assertGetObject(t, uuid)
			updatedSchema := updatedObject.Properties.(map[string]interface{})
			if updatedSchema["testNumber"] == nil {
				return nil
			}
			num, _ := updatedSchema["testNumber"].(json.Number).Float64()
			return num
		}
		testhelper.AssertEventuallyEqual(t, 41.0, actualThunk)
	})

	t.Run("update and set string", func(t *testing.T) {
		uuid := assertCreateObject(t, "TestObject", map[string]interface{}{})
		assertGetObjectEventually(t, uuid)

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
			updatedObject := assertGetObject(t, uuid)
			updatedSchema := updatedObject.Properties.(map[string]interface{})
			return updatedSchema["testString"]
		}
		testhelper.AssertEventuallyEqual(t, "wibbly wobbly", actualThunk)
	})

	t.Run("update and set bool", func(t *testing.T) {
		t.Parallel()
		uuid := assertCreateObject(t, "TestObject", map[string]interface{}{})
		assertGetObjectEventually(t, uuid)

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
			updatedObject := assertGetObject(t, uuid)
			updatedSchema := updatedObject.Properties.(map[string]interface{})
			return updatedSchema["testTrueFalse"]
		}
		testhelper.AssertEventuallyEqual(t, true, actualThunk)
	})

	t.Run("can patch object with cref", func(t *testing.T) {
		thingToRefID := assertCreateObject(t, "ObjectTestThing", nil)
		assertGetObjectEventually(t, thingToRefID)
		objectID := assertCreateObject(t, "TestObject", nil)
		assertGetObjectEventually(t, objectID)

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
			patchedObject := assertGetObject(t, objectID)

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

		testhelper.AssertEventuallyEqual(t, fmt.Sprintf("weaviate://localhost/%s", thingToRefID), actualThunk)
	})
}

func updateObjects(t *testing.T) {
	cls := "TestObjectsUpdate"
	// test setup
	//deleteClassObject(t, cls)
	createObjectClass(t, &models.Class{
		Class: cls,
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		Properties: []*models.Property{
			{
				Name:     "testString",
				DataType: []string{"string"},
			},
			{
				Name:     "testWholeNumber",
				DataType: []string{"int"},
			},
			{
				Name:     "testNumber",
				DataType: []string{"number"},
			},
			{
				Name:     "testDateTime",
				DataType: []string{"date"},
			},
			{
				Name:     "testTrueFalse",
				DataType: []string{"boolean"},
			},
		},
	})
	// tear down
	defer deleteClassObject(t, cls)

	uuid := assertCreateObject(t, cls, map[string]interface{}{
		"testWholeNumber": 2.0,
		"testDateTime":    time.Now(),
		"testString":      "wibbly",
	})
	assertGetObjectEventually(t, uuid)
	expected := map[string]interface{}{
		"testNumber":    2.0,
		"testTrueFalse": true,
		"testString":    "wibbly wobbly",
	}
	update := models.Object{
		Class:      cls,
		Properties: models.PropertySchema(expected),
		ID:         uuid,
	}
	params := objects.NewObjectsClassPutParams().WithID(uuid).WithBody(&update)
	updateResp, err := helper.Client(t).Objects.ObjectsClassPut(params, nil)
	helper.AssertRequestOk(t, updateResp, err, nil)
	actual := func() interface{} {
		obj := assertGetObject(t, uuid)
		props := obj.Properties.(map[string]interface{})
		if props["testNumber"] != nil {
			props["testNumber"], _ = props["testNumber"].(json.Number).Float64()
		}
		return props
	}
	testhelper.AssertEventuallyEqual(t, expected, actual)
}
