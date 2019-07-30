//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package test

import (
	"encoding/json"
	"testing"

	"github.com/semi-technologies/weaviate/client/things"
	"github.com/semi-technologies/weaviate/entities/models"
	graphql "github.com/semi-technologies/weaviate/test/acceptance/graphql_resolvers"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanAddSingleNetworkRef(t *testing.T) {
	networkRefID := "711da979-4b0b-41e2-bcb8-fcc03554c7c8"
	thingID := assertCreateThing(t, "TestThing", map[string]interface{}{
		"testReference": map[string]interface{}{
			"$cref": "weaviate://RemoteWeaviateForAcceptanceTest/things/" + networkRefID,
		},
	})

	t.Run("it can query the resource again to verify the cross ref was added", func(t *testing.T) {
		thing := assertGetThingEventually(t, thingID)
		rawCref := thing.Schema.(map[string]interface{})["testReference"]
		require.NotNil(t, rawCref, "cross-ref is present")
		cref := rawCref.(map[string]interface{})
		assert.Equal(t, cref["$cref"], "weaviate://RemoteWeaviateForAcceptanceTest/things/"+networkRefID)
	})

	t.Run("an implicit schema update has happened, we now include the network ref's class", func(t *testing.T) {
		schema := assertGetSchema(t)
		require.NotNil(t, schema.Things)
		class := assertClassInSchema(t, schema.Things, "TestThing")
		prop := assertPropertyInClass(t, class, "testReference")
		expectedDataType := []string{"TestThingTwo", "RemoteWeaviateForAcceptanceTest/Instruments"}
		assert.Equal(t, expectedDataType, prop.DataType, "prop should have old and newly added dataTypes")
	})

	t.Run("it can query the reference through the graphql api", func(t *testing.T) {
		result := graphql.AssertGraphQL(t, helper.RootAuth,
			"{  Get { Things { TestThing { TestReference { ... on RemoteWeaviateForAcceptanceTest__Instruments { name } } } } } }")
		things := result.Get("Get", "Things", "TestThing").AsSlice()
		assert.Contains(t, things, parseJSONObj(`{"TestReference":[{"name": "Talkbox"}]}`))
	})
}

func TestCanPatchNetworkRef(t *testing.T) {
	t.Parallel()

	thingID := assertCreateThing(t, "TestThing", nil)
	assertGetThingEventually(t, thingID)
	networkRefID := "711da979-4b0b-41e2-bcb8-fcc03554c7c8"

	op := "add"
	path := "/schema/testReference"

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
			WithID(thingID)
		patchResp, err := helper.Client(t).Things.WeaviateThingsPatch(params, nil)
		helper.AssertRequestOk(t, patchResp, err, nil)
	})

	t.Run("it can query the resource again to verify the cross ref was added", func(t *testing.T) {
		patchedThing := assertGetThing(t, thingID)
		rawCref := patchedThing.Schema.(map[string]interface{})["testReference"]
		require.NotNil(t, rawCref, "cross-ref is present")
		cref := rawCref.(map[string]interface{})
		assert.Equal(t, cref["$cref"], "weaviate://RemoteWeaviateForAcceptanceTest/things/"+networkRefID)
	})

	t.Run("an implicit schema update has happened, we now include the network ref's class", func(t *testing.T) {
		schema := assertGetSchema(t)
		require.NotNil(t, schema.Things)
		class := assertClassInSchema(t, schema.Things, "TestThing")
		prop := assertPropertyInClass(t, class, "testReference")
		expectedDataType := []string{"TestThingTwo", "RemoteWeaviateForAcceptanceTest/Instruments"}
		assert.Equal(t, expectedDataType, prop.DataType, "prop should have old and newly added dataTypes")
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
