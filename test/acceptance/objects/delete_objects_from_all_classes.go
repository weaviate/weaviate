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

// Acceptance tests for objects.

import (
	"errors"
	"testing"

	"github.com/go-openapi/strfmt"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/semi-technologies/weaviate/client/batch"
	"github.com/semi-technologies/weaviate/client/objects"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
)

// run from setup_test.go
func deleteAllObjectsFromAllClasses(t *testing.T) {
	// We can have a situation that objects in different classes
	// have the same ID. This test is to ensure that the delete request
	// deletes all of the objects with a given ID in all classes
	// This test is connected with this issue:
	// https://github.com/semi-technologies/weaviate/issues/1836
	const fakeObjectId strfmt.UUID = "11111111-1111-1111-1111-111111111111"

	t.Run("create objects with a specified id", func(t *testing.T) {
		object1 := &models.Object{
			Class: "TestDeleteClassOne",
			ID:    fakeObjectId,
			Properties: map[string]interface{}{
				"text": "Test string 1",
			},
		}
		object2 := &models.Object{
			Class: "TestDeleteClassTwo",
			ID:    fakeObjectId,
			Properties: map[string]interface{}{
				"text": "Test string 2",
			},
		}

		testFields := "ALL"
		// generate request body
		params := batch.NewBatchObjectsCreateParams().WithBody(batch.BatchObjectsCreateBody{
			Objects: []*models.Object{object1, object2},
			Fields:  []*string{&testFields},
		})

		// perform the request
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
	})

	t.Run("check that object exists", func(t *testing.T) {
		// there are actually 2 objects in 2 classes with this ID
		params := objects.NewObjectsGetParams().WithID(fakeObjectId)
		resp, err := helper.Client(t).Objects.ObjectsGet(params, nil)
		require.Nil(t, err, "get should succeed")
		assert.NotNil(t, resp.Payload)
	})

	t.Run("delete objects with a given ID from all classes", func(t *testing.T) {
		params := objects.NewObjectsDeleteParams().WithID(fakeObjectId)
		resp, err := helper.Client(t).Objects.ObjectsDelete(params, nil)
		require.Nil(t, err, "delete should succeed")
		assert.Equal(t, &objects.ObjectsDeleteNoContent{}, resp)
	})

	t.Run("check that object with given ID is removed from all classes", func(t *testing.T) {
		params := objects.NewObjectsGetParams().WithID(fakeObjectId)
		resp, err := helper.Client(t).Objects.ObjectsGet(params, nil)
		require.Equal(t, &objects.ObjectsGetNotFound{}, err)
		assert.Nil(t, resp)
	})
}

func deleteClassObject(t *testing.T) {
	var (
		id          strfmt.UUID = "21111111-1111-1111-1111-111111111111"
		firstClass              = "TestDeleteClassOne"
		secondClass             = "TestDeleteClassTwo"
	)

	// test setup
	object1 := &models.Object{
		Class: firstClass,
		ID:    id,
		Properties: map[string]interface{}{
			"text": "Test string 1",
		},
	}
	object2 := &models.Object{
		Class: secondClass,
		ID:    id,
		Properties: map[string]interface{}{
			"text": "Test string 2",
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

	{ //"delete object from first class
		params := objects.NewObjectsClassDeleteParams().WithClassName(firstClass).WithID(id)
		resp, err := helper.Client(t).Objects.ObjectsClassDelete(params, nil)
		if err != nil {
			t.Errorf("cannot delete existing object err: %v", err)
		}
		assert.Equal(t, &objects.ObjectsClassDeleteNoContent{}, resp)
	}
	{ // check if object still exit
		params := objects.NewObjectsClassGetParams().WithClassName(firstClass).WithID(id)
		_, err := helper.Client(t).Objects.ObjectsClassGet(params, nil)
		werr := &objects.ObjectsClassGetNotFound{}
		if !errors.As(err, &werr) {
			t.Errorf("Get deleted object error got: %v want %v", err, werr)
		}
	}
	{ // object with a different class must exist
		params := objects.NewObjectsClassGetParams().WithClassName(secondClass).WithID(id)
		resp, err := helper.Client(t).Objects.ObjectsClassGet(params, nil)
		if err != nil {
			t.Errorf("object must exist err: %v", err)
		}
		if resp.Payload == nil {
			t.Errorf("payload of an existing object cannot be empty")
		}
	}

	{ //"delete object again from first class
		params := objects.NewObjectsClassDeleteParams().WithClassName(firstClass).WithID(id)
		resp, err := helper.Client(t).Objects.ObjectsClassDelete(params, nil)
		if err != nil {
			t.Errorf("cannot delete existing object again err: %v", err)
		}
		assert.Equal(t, &objects.ObjectsClassDeleteNoContent{}, resp)
	}
}
