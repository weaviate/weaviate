/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package test

// Acceptance tests for actions

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/client/actions"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/crossref"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
)

func TestCanAddAPropertyIndividually(t *testing.T) {
	t.Parallel()

	toPointToUuid := assertCreateAction(t, "TestAction", map[string]interface{}{})
	assertGetActionEventually(t, toPointToUuid)

	uuid := assertCreateAction(t, "TestActionTwo", map[string]interface{}{})

	// Verify that testCrefs is empty
	updatedAction := assertGetActionEventually(t, uuid)
	updatedSchema := updatedAction.Schema.(map[string]interface{})
	assert.Nil(t, updatedSchema["testCrefs"])

	// Append a property reference
	params := actions.NewWeaviateActionsReferencesCreateParams().
		WithID(uuid).
		WithPropertyName("testCrefs").
		WithBody(crossref.New("localhost", toPointToUuid, kind.ACTION_KIND).SingleRef())

	updateResp, err := helper.Client(t).Actions.WeaviateActionsReferencesCreate(params, nil)
	helper.AssertRequestOk(t, updateResp, err, nil)

	// Get the property again.
	updatedAction = assertGetAction(t, uuid)
	updatedSchema = updatedAction.Schema.(map[string]interface{})
	assert.NotNil(t, updatedSchema["testCrefs"])
}

func TestCanReplaceAllProperties(t *testing.T) {
	t.Parallel()

	toPointToUuidFirst := assertCreateAction(t, "TestAction", map[string]interface{}{})
	toPointToUuidLater := assertCreateAction(t, "TestAction", map[string]interface{}{})
	assertGetActionEventually(t, toPointToUuidFirst)
	assertGetActionEventually(t, toPointToUuidLater)

	uuid := assertCreateAction(t, "TestActionTwo", map[string]interface{}{
		"testCrefs": &models.MultipleRef{
			crossref.New("localhost", toPointToUuidFirst, kind.ACTION_KIND).SingleRef(),
		},
	})

	// Verify that testCrefs is empty
	updatedAction := assertGetActionEventually(t, uuid)
	updatedSchema := updatedAction.Schema.(map[string]interface{})
	assert.NotNil(t, updatedSchema["testCrefs"])

	// Replace
	params := actions.NewWeaviateActionsReferencesUpdateParams().
		WithID(uuid).
		WithPropertyName("testCrefs").
		WithBody(models.MultipleRef{
			crossref.New("localhost", toPointToUuidLater, kind.ACTION_KIND).SingleRef(),
		})

	updateResp, err := helper.Client(t).Actions.WeaviateActionsReferencesUpdate(params, nil)
	helper.AssertRequestOk(t, updateResp, err, nil)

	// Get the property again.
	updatedAction = assertGetAction(t, uuid)
	updatedSchema = updatedAction.Schema.(map[string]interface{})
	assert.NotNil(t, updatedSchema["testCrefs"])
}

func TestRemovePropertyIndividually(t *testing.T) {
	t.Parallel()

	toPointToUuid := assertCreateAction(t, "TestAction", map[string]interface{}{})
	assertGetActionEventually(t, toPointToUuid)

	uuid := assertCreateAction(t, "TestActionTwo", map[string]interface{}{
		"testCrefs": &models.MultipleRef{
			crossref.New("localhost", toPointToUuid, kind.ACTION_KIND).SingleRef(),
		},
	})

	// Verify that testCrefs is not empty
	updatedAction := assertGetActionEventually(t, uuid)
	updatedSchema := updatedAction.Schema.(map[string]interface{})
	assert.NotNil(t, updatedSchema["testCrefs"])

	// Delete a property reference
	params := actions.NewWeaviateActionsReferencesDeleteParams().
		WithID(uuid).
		WithPropertyName("testCrefs").
		WithBody(
			crossref.New("localhost", toPointToUuid, kind.ACTION_KIND).SingleRef(),
		)

	updateResp, err := helper.Client(t).Actions.WeaviateActionsReferencesDelete(params, nil)
	helper.AssertRequestOk(t, updateResp, err, nil)

	// Get the property again.
	updatedAction = assertGetAction(t, uuid)
	updatedSchema = updatedAction.Schema.(map[string]interface{})
	assert.Nil(t, updatedSchema["testCrefs"])
}
