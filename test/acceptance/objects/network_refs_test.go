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

// func TestCanAddSingleNetworkRef(t *testing.T) {
// 	networkRefID := "711da979-4b0b-41e2-bcb8-fcc03554c7c8"
// 	thingID := assertCreateThing(t, "TestThing", map[string]interface{}{
// 		"testReference": []interface{}{
// 			map[string]interface{}{
// 				"beacon": "weaviate://RemoteWeaviateForAcceptanceTest/things/" + networkRefID,
// 			},
// 		},
// 	})

// 	t.Run("it can query the resource again to verify the cross ref was added", func(t *testing.T) {
// 		thing := assertGetThingEventually(t, thingID)
// 		list := thing.Schema.(map[string]interface{})["testReference"]
// 		require.NotNil(t, list, "cross-ref is present")
// 		cref := list.([]interface{})[0].(map[string]interface{})
// 		assert.Equal(t, cref["beacon"], "weaviate://RemoteWeaviateForAcceptanceTest/things/"+networkRefID)
// 	})

// 	t.Run("an implicit schema update has happened, we now include the network ref's class", func(t *testing.T) {
// 		schema := assertGetSchema(t)
// 		require.NotNil(t, schema.Things)
// 		class := assertClassInSchema(t, schema.Things, "TestThing")
// 		prop := assertPropertyInClass(t, class, "testReference")
// 		expectedDataType := []string{"TestThingTwo", "RemoteWeaviateForAcceptanceTest/Instruments"}
// 		assert.Equal(t, expectedDataType, prop.DataType, "prop should have old and newly added dataTypes")
// 	})

// 	t.Run("it can query the reference through the graphql api", func(t *testing.T) {
// 		result := graphql.AssertGraphQL(t, helper.RootAuth,
// 			"{  Get { Things { TestThing { TestReference { ... on RemoteWeaviateForAcceptanceTest__Instruments { name } } } } } }")
// 		things := result.Get("Get", "Things", "TestThing").AsSlice()
// 		assert.Contains(t, things, parseJSONObj(`{"TestReference":[{"name": "Talkbox"}]}`))
// 	})
// }

// func TestCanPatchNetworkRef(t *testing.T) {
// 	t.Parallel()

// 	thingID := assertCreateThing(t, "TestThing", nil)
// 	assertGetThingEventually(t, thingID)
// 	networkRefID := "711da979-4b0b-41e2-bcb8-fcc03554c7c8"

// 	op := "add"
// 	path := "/schema/testReference"

// 	patch := &models.PatchDocument{
// 		Op:   &op,
// 		Path: &path,
// 		Value: []interface{}{
// 			map[string]interface{}{
// 				"beacon": "weaviate://RemoteWeaviateForAcceptanceTest/things/" + networkRefID,
// 			},
// 		},
// 	}

// 	t.Run("it can apply the patch", func(t *testing.T) {
// 		params := things.NewThingsPatchParams().
// 			WithBody([]*models.PatchDocument{patch}).
// 			WithID(thingID)
// 		patchResp, err := helper.Client(t).Things.ThingsPatch(params, nil)
// 		helper.AssertRequestOk(t, patchResp, err, nil)
// 	})

// 	t.Run("it can query the resource again to verify the cross ref was added", func(t *testing.T) {
// 		patchedThing := assertGetThing(t, thingID)
// 		list := patchedThing.Schema.(map[string]interface{})["testReference"]
// 		require.NotNil(t, list, "cross-ref is present")
// 		cref := list.([]interface{})[0].(map[string]interface{})
// 		assert.Equal(t, cref["beacon"], "weaviate://RemoteWeaviateForAcceptanceTest/things/"+networkRefID)
// 	})

// 	t.Run("an implicit schema update has happened, we now include the network ref's class", func(t *testing.T) {
// 		schema := assertGetSchema(t)
// 		require.NotNil(t, schema.Things)
// 		class := assertClassInSchema(t, schema.Things, "TestThing")
// 		prop := assertPropertyInClass(t, class, "testReference")
// 		expectedDataType := []string{"TestThingTwo", "RemoteWeaviateForAcceptanceTest/Instruments"}
// 		assert.Equal(t, expectedDataType, prop.DataType, "prop should have old and newly added dataTypes")
// 	})
// }

// func parseJSONObj(text string) interface{} {
// 	var result interface{}
// 	err := json.Unmarshal([]byte(text), &result)

// 	if err != nil {
// 		panic(err)
// 	}

// 	return result
// }
