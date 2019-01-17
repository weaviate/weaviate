/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package test

// Acceptance tests for actions

import (
	"fmt"
	"testing"

	"github.com/creativesoftwarefdn/weaviate/client/actions"
	"github.com/creativesoftwarefdn/weaviate/database/schema/crossref"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
)

func TestCanAddAPropertyIndividually(t *testing.T) {
	t.Parallel()

	toPointToUuid := assertCreateAction(t, "TestAction", map[string]interface{}{})

	uuid := assertCreateAction(t, "TestActionTwo", map[string]interface{}{})

	// Verify that testCrefs is empty
	updatedAction := assertGetAction(t, uuid)
	updatedSchema := updatedAction.Schema.(map[string]interface{})
	assert.Nil(t, updatedSchema["testCrefs"])

	// Append a property reference
	params := actions.NewWeaviateActionsPropertiesCreateParams().
		WithActionID(uuid).
		WithPropertyName("testCrefs").
		WithBody(crossref.New("localhost", toPointToUuid, kind.ACTION_KIND).SingleRef())

	updateResp, err := helper.Client(t).Actions.WeaviateActionsPropertiesCreate(params, helper.RootAuth)
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

	uuid := assertCreateAction(t, "TestActionTwo", map[string]interface{}{
		"testCrefs": &models.MultipleRef{
			crossref.New("localhost", toPointToUuidFirst, kind.ACTION_KIND).SingleRef(),
		},
	})

	fmt.Printf("creation was fine")

	// Verify that testCrefs is empty
	updatedAction := assertGetAction(t, uuid)
	fmt.Printf("get was fine")
	updatedSchema := updatedAction.Schema.(map[string]interface{})
	assert.NotNil(t, updatedSchema["testCrefs"])

	// Replace
	params := actions.NewWeaviateActionsPropertiesUpdateParams().
		WithActionID(uuid).
		WithPropertyName("testCrefs").
		WithBody(models.MultipleRef{
			crossref.New("localhost", toPointToUuidLater, kind.ACTION_KIND).SingleRef(),
		})

	updateResp, err := helper.Client(t).Actions.WeaviateActionsPropertiesUpdate(params, helper.RootAuth)
	helper.AssertRequestOk(t, updateResp, err, nil)
	fmt.Printf("update was fine")

	// Get the property again.
	updatedAction = assertGetAction(t, uuid)
	fmt.Printf("another get was fine")
	updatedSchema = updatedAction.Schema.(map[string]interface{})
	assert.NotNil(t, updatedSchema["testCrefs"])
}

func TestRemovePropertyIndividually(t *testing.T) {
	t.Parallel()

	toPointToUuid := assertCreateAction(t, "TestAction", map[string]interface{}{})

	uuid := assertCreateAction(t, "TestActionTwo", map[string]interface{}{
		"testCrefs": &models.MultipleRef{
			crossref.New("localhost", toPointToUuid, kind.ACTION_KIND).SingleRef(),
		},
	})

	// Verify that testCrefs is not empty
	updatedAction := assertGetAction(t, uuid)
	updatedSchema := updatedAction.Schema.(map[string]interface{})
	assert.NotNil(t, updatedSchema["testCrefs"])

	// Append a property reference
	params := actions.NewWeaviateActionsPropertiesDeleteParams().
		WithActionID(uuid).
		WithPropertyName("testCrefs").
		WithBody(
			crossref.New("localhost", toPointToUuid, kind.ACTION_KIND).SingleRef(),
		)

	updateResp, err := helper.Client(t).Actions.WeaviateActionsPropertiesDelete(params, helper.RootAuth)
	helper.AssertRequestOk(t, updateResp, err, nil)

	// Get the property again.
	updatedAction = assertGetAction(t, uuid)
	updatedSchema = updatedAction.Schema.(map[string]interface{})
	assert.Nil(t, updatedSchema["testCrefs"])
}
