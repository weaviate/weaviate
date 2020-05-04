//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package test

// Acceptance tests for actions

import (
	"testing"

	"github.com/semi-technologies/weaviate/client/actions"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	testhelper "github.com/semi-technologies/weaviate/test/helper"
	"github.com/stretchr/testify/assert"
)

// run from setup_test.go
func actionReferences(t *testing.T) {
	t.Run("can add reference individually", func(t *testing.T) {
		t.Parallel()

		toPointToUuid := assertCreateAction(t, "TestAction", map[string]interface{}{})
		assertGetActionEventually(t, toPointToUuid)

		uuid := assertCreateAction(t, "TestActionTwo", map[string]interface{}{})

		// Verify that testReferences is empty
		updatedAction := assertGetActionEventually(t, uuid)
		updatedSchema := updatedAction.Schema.(map[string]interface{})
		assert.Nil(t, updatedSchema["testReferences"])

		// Append a property reference
		params := actions.NewActionsReferencesCreateParams().
			WithID(uuid).
			WithPropertyName("testReferences").
			WithBody(crossref.New("localhost", toPointToUuid, kind.Action).SingleRef())

		updateResp, err := helper.Client(t).Actions.ActionsReferencesCreate(params, nil)
		helper.AssertRequestOk(t, updateResp, err, nil)

		checkThunk := func() interface{} {
			resp, err := helper.Client(t).Actions.ActionsGet(actions.NewActionsGetParams().WithID(uuid), nil)
			if err != nil {
				t.Log(err)
				return false
			}

			updatedSchema = resp.Payload.Schema.(map[string]interface{})
			return updatedSchema["testReferences"] != nil
		}

		testhelper.AssertEventuallyEqual(t, true, checkThunk)
	})

	t.Run("can replace all properties", func(t *testing.T) {
		t.Parallel()

		toPointToUuidFirst := assertCreateAction(t, "TestAction", map[string]interface{}{})
		toPointToUuidLater := assertCreateAction(t, "TestAction", map[string]interface{}{})
		assertGetActionEventually(t, toPointToUuidFirst)
		assertGetActionEventually(t, toPointToUuidLater)

		uuid := assertCreateAction(t, "TestActionTwo", map[string]interface{}{
			"testReferences": models.MultipleRef{
				crossref.New("localhost", toPointToUuidFirst, kind.Action).SingleRef(),
			},
		})

		// Verify that testReferences is empty
		updatedAction := assertGetActionEventually(t, uuid)
		updatedSchema := updatedAction.Schema.(map[string]interface{})
		assert.NotNil(t, updatedSchema["testReferences"])

		// Replace
		params := actions.NewActionsReferencesUpdateParams().
			WithID(uuid).
			WithPropertyName("testReferences").
			WithBody(models.MultipleRef{
				crossref.New("localhost", toPointToUuidLater, kind.Action).SingleRef(),
			})

		updateResp, err := helper.Client(t).Actions.ActionsReferencesUpdate(params, nil)
		helper.AssertRequestOk(t, updateResp, err, nil)

		checkThunk := func() interface{} {
			resp, err := helper.Client(t).Actions.ActionsGet(actions.NewActionsGetParams().WithID(uuid), nil)
			if err != nil {
				t.Log(err)
				return false
			}

			updatedSchema = resp.Payload.Schema.(map[string]interface{})
			return updatedSchema["testReferences"] != nil
		}

		testhelper.AssertEventuallyEqual(t, true, checkThunk)
	})

	t.Run("remove property individually", func(t *testing.T) {
		t.Parallel()

		toPointToUuid := assertCreateAction(t, "TestAction", map[string]interface{}{})
		assertGetActionEventually(t, toPointToUuid)

		uuid := assertCreateAction(t, "TestActionTwo", map[string]interface{}{
			"testReferences": models.MultipleRef{
				crossref.New("localhost", toPointToUuid, kind.Action).SingleRef(),
			},
		})

		// Verify that testReferences is not empty
		updatedAction := assertGetActionEventually(t, uuid)
		updatedSchema := updatedAction.Schema.(map[string]interface{})
		assert.NotNil(t, updatedSchema["testReferences"])

		// Delete a property reference
		params := actions.NewActionsReferencesDeleteParams().
			WithID(uuid).
			WithPropertyName("testReferences").
			WithBody(
				crossref.New("localhost", toPointToUuid, kind.Action).SingleRef(),
			)

		updateResp, err := helper.Client(t).Actions.ActionsReferencesDelete(params, nil)
		helper.AssertRequestOk(t, updateResp, err, nil)

		checkThunk := func() interface{} {
			resp, err := helper.Client(t).Actions.ActionsGet(actions.NewActionsGetParams().WithID(uuid), nil)
			if err != nil {
				t.Log(err)
				return false
			}

			refs := resp.Payload.Schema.(map[string]interface{})["testReferences"]

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
