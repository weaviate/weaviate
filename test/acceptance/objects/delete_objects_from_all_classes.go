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
	"testing"

	"github.com/go-openapi/strfmt"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// run from setup_test.go
func deleteAllObjectsFromAllClasses(t *testing.T) {
	// We can have a situation that objects in different classes
	// have the same ID. This test is to ensure that the delete request
	// deletes all of the objects with a given ID in all classes
	// This test is connected with this issue:
	// https://github.com/weaviate/weaviate/issues/1836
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
