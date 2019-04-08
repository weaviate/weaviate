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
	"encoding/json"
	"testing"

	"github.com/creativesoftwarefdn/weaviate/client/things"
	"github.com/creativesoftwarefdn/weaviate/models"
	graphql "github.com/creativesoftwarefdn/weaviate/test/acceptance/graphql_resolvers"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanAddSingleNetworkRef(t *testing.T) {
	networkRefID := "711da979-4b0b-41e2-bcb8-fcc03554c7c8"
	thingID := assertCreateThing(t, "TestThing", map[string]interface{}{
		"testCref": map[string]interface{}{
			"$cref": "weaviate://RemoteWeaviateForAcceptanceTest/things/" + networkRefID,
		},
	})

	t.Run("it can query the resource again to verify the cross ref was added", func(t *testing.T) {
		thing := assertGetThingEventually(t, thingID)
		rawCref := thing.Schema.(map[string]interface{})["testCref"]
		require.NotNil(t, rawCref, "cross-ref is present")
		cref := rawCref.(map[string]interface{})
		assert.Equal(t, cref["$cref"], "weaviate://RemoteWeaviateForAcceptanceTest/things/"+networkRefID)
	})

	t.Run("an implicit schema update has happened, we now include the network ref's class", func(t *testing.T) {
		schema := assertGetSchema(t)
		require.NotNil(t, schema.Things)
		class := assertClassInSchema(t, schema.Things, "TestThing")
		prop := assertPropertyInClass(t, class, "testCref")
		expectedDataType := []string{"TestThingTwo", "RemoteWeaviateForAcceptanceTest/Instruments"}
		assert.Equal(t, expectedDataType, prop.AtDataType, "prop should have old and newly added dataTypes")
	})

	t.Run("it can query the reference through the graphql api", func(t *testing.T) {
		result := graphql.AssertGraphQL(t, helper.RootAuth,
			"{ Local { Get { Things { TestThing { TestCref { ... on RemoteWeaviateForAcceptanceTest__Instruments { name } } } } } } }")
		things := result.Get("Local", "Get", "Things", "TestThing").AsSlice()
		assert.Contains(t, things, parseJSONObj(`{"TestCref":[{"name": "Talkbox"}]}`))
	})
}

func TestCanPatchNetworkRef(t *testing.T) {
	t.Parallel()

	thingID := assertCreateThing(t, "TestThing", nil)
	assertGetThingEventually(t, thingID)
	networkRefID := "711da979-4b0b-41e2-bcb8-fcc03554c7c8"

	op := "add"
	path := "/schema/testCref"

	patch := &models.PatchDocument{
		Op:   &op,
		Path: &path,
		Value: map[string]interface{}{
			"$cref": "weaviate://RemoteWeaviateForAcceptanceTest/things/" + networkRefID,
		},
	}

	t.Run("it can apply the patch", func(t *testing.T) {
		params := things.NewWeaviateThingsPatchParams().
			WithBody([]*models.PatchDocument{patch}).
			WithThingID(thingID)
		patchResp, err := helper.Client(t).Things.WeaviateThingsPatch(params, nil)
		helper.AssertRequestOk(t, patchResp, err, nil)
	})

	t.Run("it can query the resource again to verify the cross ref was added", func(t *testing.T) {
		patchedThing := assertGetThing(t, thingID)
		rawCref := patchedThing.Schema.(map[string]interface{})["testCref"]
		require.NotNil(t, rawCref, "cross-ref is present")
		cref := rawCref.(map[string]interface{})
		assert.Equal(t, cref["$cref"], "weaviate://RemoteWeaviateForAcceptanceTest/things/"+networkRefID)
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

func parseJSONObj(text string) interface{} {
	var result interface{}
	err := json.Unmarshal([]byte(text), &result)

	if err != nil {
		panic(err)
	}

	return result
}
