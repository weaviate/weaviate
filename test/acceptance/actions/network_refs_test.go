package test

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/client/actions"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanPatchNetworkRef(t *testing.T) {
	t.Parallel()

	actionID := assertCreateAction(t, "TestAction", nil)
	networkRefID := "711da979-4b0b-41e2-bcb8-fcc03554c7c8"

	op := "add"
	path := "/schema/testCref"

	patch := &models.PatchDocument{
		Op:   &op,
		Path: &path,
		Value: map[string]interface{}{
			"$cref":       networkRefID,
			"locationUrl": "http://RemoteWeaviateForAcceptanceTest",
			"type":        "NetworkThing",
		},
	}

	t.Run("it can apply the patch", func(t *testing.T) {
		params := actions.NewWeaviateActionsPatchParams().
			WithBody([]*models.PatchDocument{patch}).
			WithActionID(actionID)
		patchResp, _, err := helper.Client(t).Actions.WeaviateActionsPatch(params, helper.RootAuth)
		helper.AssertRequestOk(t, patchResp, err, nil)
	})

	t.Run("it can query the resource again to verify the cross ref was added", func(t *testing.T) {
		patchedAction := assertGetAction(t, actionID)
		rawCref := patchedAction.Schema.(map[string]interface{})["testCref"]
		require.NotNil(t, rawCref, "cross-ref is present")
		cref := rawCref.(map[string]interface{})
		assert.Equal(t, cref["type"], "NetworkThing")
		assert.Equal(t, cref["$cref"], networkRefID)
	})

	t.Run("an implicit schema update has happened, we now include the network ref's class", func(t *testing.T) {
		schema := assertGetSchema(t)
		require.NotNil(t, schema.Actions)
		class := assertClassInSchema(t, schema.Actions, "TestAction")
		prop := assertPropertyInClass(t, class, "testCref")
		expectedDataType := []string{"TestThing", "RemoteWeaviateForAcceptanceTest/Instruments"}
		assert.Equal(t, expectedDataType, prop.AtDataType, "prop should have old and newly added dataTypes")
	})
}
