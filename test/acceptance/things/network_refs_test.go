package test

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/client/things"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanAddSingleNetworkRef(t *testing.T) {
	networkRefID := "711da979-4b0b-41e2-bcb8-fcc03554c7c8"
	thingID := assertCreateThing(t, "TestThing", map[string]interface{}{
		"testCref": map[string]interface{}{
			"locationUrl": "http://RemoteWeaviateForAcceptanceTest",
			"type":        "NetworkThing",
			"$cref":       networkRefID,
		},
	})

	t.Run("it can query the resource again to verify the cross ref was added", func(t *testing.T) {
		thing := assertGetThing(t, thingID)
		rawCref := thing.Schema.(map[string]interface{})["testCref"]
		require.NotNil(t, rawCref, "cross-ref is present")
		cref := rawCref.(map[string]interface{})
		assert.Equal(t, cref["type"], "NetworkThing")
		assert.Equal(t, cref["$cref"], networkRefID)
	})

	t.Run("an implicit schema update has happened, we now include the network ref's class", func(t *testing.T) {
		schema := assertGetSchema(t)
		require.NotNil(t, schema.Things)
		class := assertClassInSchema(t, schema.Things, "TestThing")
		prop := assertPropertyInClass(t, class, "testCref")
		expectedDataType := []string{"TestThingTwo", "RemoteWeaviateForAcceptanceTest/Instruments"}
		assert.Equal(t, expectedDataType, prop.AtDataType, "prop should have old and newly added dataTypes")
	})
}

func TestCanPatchNetworkRef(t *testing.T) {
	t.Parallel()

	thingID := assertCreateThing(t, "TestThing", nil)
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
		params := things.NewWeaviateThingsPatchParams().
			WithBody([]*models.PatchDocument{patch}).
			WithThingID(thingID)
		patchResp, _, err := helper.Client(t).Things.WeaviateThingsPatch(params, helper.RootAuth)
		helper.AssertRequestOk(t, patchResp, err, nil)
	})

	t.Run("it can query the resource again to verify the cross ref was added", func(t *testing.T) {
		patchedThing := assertGetThing(t, thingID)
		rawCref := patchedThing.Schema.(map[string]interface{})["testCref"]
		require.NotNil(t, rawCref, "cross-ref is present")
		cref := rawCref.(map[string]interface{})
		assert.Equal(t, cref["type"], "NetworkThing")
		assert.Equal(t, cref["$cref"], networkRefID)
	})

	t.Run("an implicit schema update has happened, we now include the network ref's class", func(t *testing.T) {
		schema := assertGetSchema(t)
		require.NotNil(t, schema.Things)
		class := assertClassInSchema(t, schema.Things, "TestThing")
		prop := assertPropertyInClass(t, class, "testCref")
		expectedDataType := []string{"TestThingTwo", "RemoteWeaviateForAcceptanceTest/Instruments"}
		assert.Equal(t, expectedDataType, prop.AtDataType, "prop should have old and newly added dataTypes")
	})
}
