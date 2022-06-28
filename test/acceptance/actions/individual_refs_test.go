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
	"testing"

	"github.com/semi-technologies/weaviate/client/objects"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	testhelper "github.com/semi-technologies/weaviate/test/helper"
	"github.com/stretchr/testify/assert"
)

// run from setup_test.go
func objectReferences(t *testing.T) {
	var (
		class1 = "TestObject"
		class2 = "TestObjectTwo"
	)
	t.Run("can add reference individually", func(t *testing.T) {
		t.Parallel()

		toPointToUuid := assertCreateObject(t, "TestObject", map[string]interface{}{})
		assertGetObjectEventually(t, class1, toPointToUuid)

		uuid := assertCreateObject(t, class2, map[string]interface{}{})

		// Verify that testReferences is empty
		updatedObject := assertGetObjectEventually(t, class2, uuid)
		updatedSchema := updatedObject.Properties.(map[string]interface{})
		assert.Nil(t, updatedSchema["testReferences"])

		// Append a property reference
		params := objects.NewObjectsReferencesCreateParams().
			WithID(uuid).
			WithPropertyName("testReferences").
			WithBody(crossref.NewLocalhost(class1, toPointToUuid).SingleRef())

		updateResp, err := helper.Client(t).Objects.ObjectsReferencesCreate(params, nil)
		helper.AssertRequestOk(t, updateResp, err, nil)

		checkThunk := func() interface{} {
			resp, err := helper.Client(t).Objects.ObjectsGet(objects.NewObjectsGetParams().WithID(uuid), nil)
			if err != nil {
				t.Log(err)
				return false
			}

			updatedSchema = resp.Payload.Properties.(map[string]interface{})
			return updatedSchema["testReferences"] != nil
		}

		testhelper.AssertEventuallyEqual(t, true, checkThunk)
	})

	t.Run("can replace all properties", func(t *testing.T) {
		t.Parallel()

		toPointToUuidFirst := assertCreateObject(t, "TestObject", map[string]interface{}{})
		toPointToUuidLater := assertCreateObject(t, "TestObject", map[string]interface{}{})
		assertGetObjectEventually(t, "TestObject", toPointToUuidFirst)
		assertGetObjectEventually(t, "TestObject", toPointToUuidLater)

		uuid := assertCreateObject(t, "TestObjectTwo", map[string]interface{}{
			"testReferences": models.MultipleRef{
				crossref.NewLocalhost("TestObject", toPointToUuidFirst).SingleRef(),
			},
		})

		// Verify that testReferences is empty
		updatedObject := assertGetObjectEventually(t, "TestObjectTwo", uuid)
		updatedSchema := updatedObject.Properties.(map[string]interface{})
		assert.NotNil(t, updatedSchema["testReferences"])

		// Replace
		params := objects.NewObjectsReferencesUpdateParams().
			WithID(uuid).
			WithPropertyName("testReferences").
			WithBody(models.MultipleRef{
				crossref.NewLocalhost("TestObject", toPointToUuidLater).SingleRef(),
			})

		updateResp, err := helper.Client(t).Objects.ObjectsReferencesUpdate(params, nil)
		helper.AssertRequestOk(t, updateResp, err, nil)

		checkThunk := func() interface{} {
			resp, err := helper.Client(t).Objects.ObjectsGet(objects.NewObjectsGetParams().WithID(uuid), nil)
			if err != nil {
				t.Log(err)
				return false
			}

			updatedSchema = resp.Payload.Properties.(map[string]interface{})
			return updatedSchema["testReferences"] != nil
		}

		testhelper.AssertEventuallyEqual(t, true, checkThunk)
	})

	t.Run("remove property individually", func(t *testing.T) {
		t.Parallel()

		toPointToUuid := assertCreateObject(t, "TestObject", map[string]interface{}{})
		assertGetObjectEventually(t, "TestObject", toPointToUuid)

		uuid := assertCreateObject(t, "TestObjectTwo", map[string]interface{}{
			"testReferences": models.MultipleRef{
				crossref.NewLocalhost("TestObject", toPointToUuid).SingleRef(),
			},
		})

		// Verify that testReferences is not empty
		updatedObject := assertGetObjectEventually(t, "TestObjectTwo", uuid)
		updatedSchema := updatedObject.Properties.(map[string]interface{})
		assert.NotNil(t, updatedSchema["testReferences"])

		// Delete a property reference
		params := objects.NewObjectsReferencesDeleteParams().
			WithID(uuid).
			WithPropertyName("testReferences").
			WithBody(
				crossref.NewLocalhost("TestObject", toPointToUuid).SingleRef(),
			)

		updateResp, err := helper.Client(t).Objects.ObjectsReferencesDelete(params, nil)
		helper.AssertRequestOk(t, updateResp, err, nil)

		checkThunk := func() interface{} {
			resp, err := helper.Client(t).Objects.ObjectsGet(objects.NewObjectsGetParams().WithID(uuid), nil)
			if err != nil {
				t.Log(err)
				return false
			}

			refs := resp.Payload.Properties.(map[string]interface{})["testReferences"]

			if refs == nil {
				return true
			}

			refsSlice, ok := refs.([]interface{})
			if ok {
				return len(refsSlice) == 0
			}

			// neither nil, nor a list
			t.Logf("prop %s was neither nil nor a list after deleting, instead we got %#v", "testReferences", refs)
			t.Fail()

			return false
		}

		testhelper.AssertEventuallyEqual(t, true, checkThunk)
	})
}
