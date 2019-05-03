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

import (
	"fmt"
	"testing"

	"github.com/creativesoftwarefdn/weaviate/client/actions"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanAddSingleNetworkRef(t *testing.T) {
	networkRefID := "711da979-4b0b-41e2-bcb8-fcc03554c7c8"
	actionID := assertCreateAction(t, "TestAction", map[string]interface{}{
		"testReference": map[string]interface{}{
			"$cref": strfmt.UUID(fmt.Sprintf("weaviate://RemoteWeaviateForAcceptanceTest/things/%s", networkRefID)),
		},
	})
	assertGetActionEventually(t, actionID)

	t.Run("it can query the resource again to verify the cross ref was added", func(t *testing.T) {
		action := assertGetAction(t, actionID)
		rawCref := action.Schema.(map[string]interface{})["testReference"]
		require.NotNil(t, rawCref, "cross-ref is present")
		cref := rawCref.(map[string]interface{})
		assert.Equal(t,
			fmt.Sprintf("weaviate://RemoteWeaviateForAcceptanceTest/things/%s", networkRefID), cref["$cref"])
	})

	t.Run("an implicit schema update has happened, we now include the network ref's class", func(t *testing.T) {
		schema := assertGetSchema(t)
		require.NotNil(t, schema.Actions)
		class := assertClassInSchema(t, schema.Actions, "TestAction")
		prop := assertPropertyInClass(t, class, "testReference")
		expectedDataType := []string{"TestThing", "RemoteWeaviateForAcceptanceTest/Instruments"}
		assert.Equal(t, expectedDataType, prop.DataType, "prop should have old and newly added dataTypes")
	})
}

func TestCanPatchSingleNetworkRef(t *testing.T) {
	t.Parallel()

	actionID := assertCreateAction(t, "TestAction", nil)
	assertGetActionEventually(t, actionID)
	networkRefID := "711da979-4b0b-41e2-bcb8-fcc03554c7c8"

	op := "add"
	path := "/schema/testReference"

	patch := &models.PatchDocument{
		Op:   &op,
		Path: &path,
		Value: map[string]interface{}{
			"$cref": strfmt.UUID(fmt.Sprintf("weaviate://RemoteWeaviateForAcceptanceTest/things/%s", networkRefID)),
		},
	}

	t.Run("it can apply the patch", func(t *testing.T) {
		params := actions.NewWeaviateActionsPatchParams().
			WithBody([]*models.PatchDocument{patch}).
			WithID(actionID)
		patchResp, err := helper.Client(t).Actions.WeaviateActionsPatch(params, nil)
		helper.AssertRequestOk(t, patchResp, err, nil)
	})

	t.Run("it can query the resource again to verify the cross ref was added", func(t *testing.T) {
		patchedAction := assertGetAction(t, actionID)
		rawCref := patchedAction.Schema.(map[string]interface{})["testReference"]
		require.NotNil(t, rawCref, "cross-ref is present")
		cref := rawCref.(map[string]interface{})
		assert.Equal(t, fmt.Sprintf("weaviate://RemoteWeaviateForAcceptanceTest/things/%s", networkRefID), cref["$cref"])
	})

	t.Run("an implicit schema update has happened, we now include the network ref's class", func(t *testing.T) {
		schema := assertGetSchema(t)
		require.NotNil(t, schema.Actions)
		class := assertClassInSchema(t, schema.Actions, "TestAction")
		prop := assertPropertyInClass(t, class, "testReference")
		expectedDataType := []string{"TestThing", "RemoteWeaviateForAcceptanceTest/Instruments"}
		assert.Equal(t, expectedDataType, prop.DataType, "prop should have old and newly added dataTypes")
	})
}
