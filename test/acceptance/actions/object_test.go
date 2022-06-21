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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/client/batch"
	"github.com/semi-technologies/weaviate/client/objects"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	testhelper "github.com/semi-technologies/weaviate/test/helper"
	"github.com/stretchr/testify/assert"
)

func Test_ObjectHTTP(t *testing.T) {
	t.Run("HEAD", headObject)
	t.Run("PUT", updateObjects)
	t.Run("PATCH", patchObjects)
	t.Run("DELETE", deleteObject)
	t.Run("AddReference", addObjectReference)
}

func headObject(t *testing.T) {
	t.Parallel()
	cls := "TestObjectHTTPHead"
	// test setup
	assertCreateObjectClass(t, &models.Class{
		Class:      cls,
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: []string{"string"},
			},
		},
	})
	// tear down
	defer deleteClassObject(t, cls)

	uuid := assertCreateObject(t, cls, map[string]interface{}{
		"name": "John",
	})

	r := objects.NewObjectsClassHeadParams().WithID(uuid).WithClassName(cls)
	resp, err := helper.Client(t).Objects.ObjectsClassHead(r, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	// check for an object which doesn't exist
	unknown_uuid := strfmt.UUID("11110000-0000-0000-0000-000011110000")
	r = objects.NewObjectsClassHeadParams().WithID(unknown_uuid).WithClassName(cls)
	resp, err = helper.Client(t).Objects.ObjectsClassHead(r, nil)
	helper.AssertRequestFail(t, resp, err, nil)
}

func updateObjects(t *testing.T) {
	t.Parallel()
	cls := "TestObjectHTTPUpdate"
	// test setup
	assertCreateObjectClass(t, &models.Class{
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
	assertGetObjectEventually(t, cls, uuid)
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
		obj := assertGetObject(t, cls, uuid)
		props := obj.Properties.(map[string]interface{})
		if props["testNumber"] != nil {
			props["testNumber"], _ = props["testNumber"].(json.Number).Float64()
		}
		return props
	}
	testhelper.AssertEventuallyEqual(t, expected, actual)
}

func patchObjects(t *testing.T) {
	t.Parallel()
	var (
		cls        = "TestObjectHTTPPatch"
		friend_cls = "TestObjectHTTPPatchFriend"
		mconfig    = map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		}
	)
	// test setup
	assertCreateObjectClass(t, &models.Class{
		Class:        friend_cls,
		ModuleConfig: mconfig,
		Properties:   []*models.Property{},
	})
	assertCreateObjectClass(t, &models.Class{
		Class:        cls,
		ModuleConfig: mconfig,
		Properties: []*models.Property{
			{
				Name:     "string1",
				DataType: []string{"string"},
			},
			{
				Name:     "integer1",
				DataType: []string{"int"},
			},
			{
				Name:     "number1",
				DataType: []string{"number"},
			},
			{
				Name:     "friend",
				DataType: []string{friend_cls},
			},
			{
				Name:     "boolean1",
				DataType: []string{"boolean"},
			},
		},
	})

	// tear down
	defer deleteClassObject(t, cls)
	defer deleteClassObject(t, friend_cls)

	uuid := assertCreateObject(t, cls, map[string]interface{}{
		"integer1": 2.0,
		"string1":  "wibbly",
	})
	friendID := assertCreateObject(t, friend_cls, nil)
	expected := map[string]interface{}{
		"integer1": json.Number("2"),
		"number1":  json.Number("3"),
		"boolean1": true,
		"string1":  "wibbly wobbly",
		"friend": []interface{}{
			map[string]interface{}{
				"beacon": fmt.Sprintf("weaviate://localhost/%s", friendID),
				"href":   fmt.Sprintf("/v1/objects/%s", friendID),
			},
		},
	}
	update := map[string]interface{}{
		"number1":  3.0,
		"boolean1": true,
		"string1":  "wibbly wobbly",
		"friend": []interface{}{
			map[string]interface{}{
				"beacon": fmt.Sprintf("weaviate://localhost/%s", friendID),
			},
		},
	}
	updateObj := models.Object{
		Properties: models.PropertySchema(update),
	}
	params := objects.NewObjectsClassPatchParams().WithClassName(cls)
	params.WithID(uuid).WithBody(&updateObj)
	updateResp, err := helper.Client(t).Objects.ObjectsClassPatch(params, nil)
	helper.AssertRequestOk(t, updateResp, err, nil)
	actual := func() interface{} {
		obj := assertGetObject(t, cls, uuid)
		props := obj.Properties.(map[string]interface{})
		return props
	}
	testhelper.AssertEventuallyEqual(t, expected, actual)

	params.WithID(strfmt.UUID("e5be1f32-0001-0000-0000-ebb25dfc811f"))
	_, err = helper.Client(t).Objects.ObjectsClassPatch(params, nil)
	if err == nil {
		t.Errorf("must return an error for non existing object")
	}
}

func deleteObject(t *testing.T) {
	t.Parallel()
	var (
		id     = strfmt.UUID("21111111-1111-1111-1111-111111111111")
		classA = "TestObjectHTTPDeleteA"
		classB = "TestObjectHTTPDeleteB"
		props  = []*models.Property{
			{
				Name:     "text",
				DataType: []string{"text"},
			},
		}
	)
	// test setup
	assertCreateObjectClass(t, &models.Class{
		Class:      classA,
		Vectorizer: "none",
		Properties: props,
	})
	assertCreateObjectClass(t, &models.Class{
		Class:      classB,
		Vectorizer: "none",
		Properties: props,
	})

	defer deleteClassObject(t, classB)
	defer deleteClassObject(t, classA)

	object1 := &models.Object{
		Class: classA,
		ID:    id,
		Properties: map[string]interface{}{
			"text": "string 1",
		},
	}
	object2 := &models.Object{
		Class: classB,
		ID:    id,
		Properties: map[string]interface{}{
			"text": "string 2",
		},
	}

	// create objects
	returnedFields := "ALL"
	params := batch.NewBatchObjectsCreateParams().WithBody(
		batch.BatchObjectsCreateBody{
			Objects: []*models.Object{object1, object2},
			Fields:  []*string{&returnedFields},
		})

	resp, err := helper.BatchClient(t).BatchObjectsCreate(params, nil)

	// ensure that the response is OK
	helper.AssertRequestOk(t, resp, err, func() {
		objectsCreateResponse := resp.Payload

		// check if the batch response contains two batched responses
		assert.Equal(t, 2, len(objectsCreateResponse))

		for _, elem := range resp.Payload {
			assert.Nil(t, elem.Result.Errors)
		}
	})

	{ // "delete object from first class
		params := objects.NewObjectsClassDeleteParams().WithClassName(classA).WithID(id)
		resp, err := helper.Client(t).Objects.ObjectsClassDelete(params, nil)
		if err != nil {
			t.Errorf("cannot delete existing object err: %v", err)
		}
		assert.Equal(t, &objects.ObjectsClassDeleteNoContent{}, resp)
	}
	{ // check if object still exit
		params := objects.NewObjectsClassGetParams().WithClassName(classA).WithID(id)
		_, err := helper.Client(t).Objects.ObjectsClassGet(params, nil)
		werr := &objects.ObjectsClassGetNotFound{}
		if !errors.As(err, &werr) {
			t.Errorf("Get deleted object error got: %v want %v", err, werr)
		}
	}
	{ // object with a different class must exist
		params := objects.NewObjectsClassGetParams().WithClassName(classB).WithID(id)
		resp, err := helper.Client(t).Objects.ObjectsClassGet(params, nil)
		if err != nil {
			t.Errorf("object must exist err: %v", err)
		}
		if resp.Payload == nil {
			t.Errorf("payload of an existing object cannot be empty")
		}
	}

	{ // "delete object again from first class
		params := objects.NewObjectsClassDeleteParams().WithClassName(classA).WithID(id)
		_, err := helper.Client(t).Objects.ObjectsClassDelete(params, nil)
		werr := &objects.ObjectsClassDeleteNotFound{}
		if !errors.As(err, &werr) {
			t.Errorf("Get deleted object error got: %v want %v", err, werr)
		}
	}
}

func addObjectReference(t *testing.T) {
	t.Parallel()
	var (
		cls        = "TestObjectHTTPAddReference"
		friend_cls = "TestObjectHTTPAddReferenceFriend"
		mconfig    = map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		}
	)
	// test setup
	assertCreateObjectClass(t, &models.Class{
		Class:        friend_cls,
		ModuleConfig: mconfig,
		Properties:   []*models.Property{},
	})
	assertCreateObjectClass(t, &models.Class{
		Class:        cls,
		ModuleConfig: mconfig,
		Properties: []*models.Property{
			{
				Name:     "number",
				DataType: []string{"number"},
			},
			{
				Name:     "friend",
				DataType: []string{friend_cls},
			},
		},
	})

	// tear down
	defer deleteClassObject(t, cls)
	defer deleteClassObject(t, friend_cls)

	uuid := assertCreateObject(t, cls, map[string]interface{}{
		"number": 2.0,
	})
	friendID := assertCreateObject(t, friend_cls, nil)
	expected := map[string]interface{}{
		"number": json.Number("2"),
		"friend": []interface{}{
			map[string]interface{}{
				"beacon": fmt.Sprintf("weaviate://localhost/%s", friendID),
				"href":   fmt.Sprintf("/v1/objects/%s", friendID),
			},
		},
	}
	updateObj := crossref.New("localhost", friendID).SingleRef()
	params := objects.NewObjectsClassReferencesCreateParams().WithClassName(cls)
	params.WithID(uuid).WithBody(updateObj).WithPropertyName("friend")
	resp, err := helper.Client(t).Objects.ObjectsClassReferencesCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
	obj := assertGetObject(t, cls, uuid)
	actual := obj.Properties.(map[string]interface{})
	assert.Equal(t, expected, actual)

	params.WithPropertyName("unknown")
	_, err = helper.Client(t).Objects.ObjectsClassReferencesCreate(params, nil)
	if _, ok := err.(*objects.ObjectsClassReferencesCreateUnprocessableEntity); !ok {
		t.Errorf("error type expected: %T, got %T", objects.ObjectsClassReferencesCreateUnprocessableEntity{}, err)
	}

	params.WithPropertyName("friend")
	params.WithID("e7cd261a-0000-0000-0000-d7b8e7b5c9ea")
	fmt.Printf("%+v\n", *params)
	_, err = helper.Client(t).Objects.ObjectsClassReferencesCreate(params, nil)
	if _, ok := err.(*objects.ObjectsClassReferencesCreateNotFound); !ok {
		t.Errorf("error type expected: %T, got %T", objects.ObjectsClassReferencesCreateNotFound{}, err)
	}
}
