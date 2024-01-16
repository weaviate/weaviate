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

package object_property_tests

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"testing"

	"acceptance_tests_with_client/fixtures"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
)

func TestObjectProperty_Data(t *testing.T) {
	ctx := context.Background()
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	className := fixtures.AllPropertiesClassName
	id1 := fixtures.AllPropertiesID1

	properties := fixtures.AllPropertiesProperties

	// clean up DB
	err = client.Schema().AllDeleter().Do(context.Background())
	require.Nil(t, err)

	t.Run("create schema and import object", func(t *testing.T) {
		class := fixtures.AllPropertiesClass
		err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
		require.NoError(t, err)
		// add object
		_, err = client.Data().Creator().
			WithClassName(className).
			WithID(id1).
			WithProperties(properties).
			Do(context.TODO())
		require.Nil(t, err)
		// check if object exists
		res, err := client.Data().ObjectsGetter().WithID(id1).Do(ctx)
		require.Nil(t, err)
		require.Len(t, res, 1)
		props, ok := res[0].Properties.(map[string]interface{})
		require.True(t, ok)
		assert.NotNil(t, props)
		assert.Equal(t, 17, len(props))
		nestedProps, ok := props["nested_objects"].(map[string]interface{})
		require.True(t, ok)
		assert.NotNil(t, nestedProps)
		assert.Equal(t, 2, len(nestedProps))
	})

	t.Run("create complicated object using auto schema", func(t *testing.T) {
		_, err = client.Data().Creator().
			WithClassName("NestedObject").
			WithID(id1).
			WithProperties(map[string]interface{}{
				"nestedProp": map[string]interface{}{
					"a": int64(1),
					"nested": map[string]interface{}{
						"b": int64(2),
						"nested": map[string]interface{}{
							"c": int64(3),
							"nested": map[string]interface{}{
								"d": int64(4),
								"nested": map[string]interface{}{
									"e": int64(5),
									"nested": map[string]interface{}{
										"f": int64(6),
										"nested": map[string]interface{}{
											"g": int64(7),
											"nested": map[string]interface{}{
												"h": int64(7),
												"nested": map[string]interface{}{
													"i": int64(7),
													"nested": map[string]interface{}{
														"j": int64(8),
														"nested": map[string]interface{}{
															"k": int64(9),
															"nested": map[string]interface{}{
																"l": int64(10),
																"nested": map[string]interface{}{
																	"m": int64(10),
																	"nested": map[string]interface{}{
																		"n": int64(11),
																	},
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			}).
			Do(context.TODO())
		require.Nil(t, err)
		// check if object exists
		res, err := client.Data().ObjectsGetter().WithID(id1).Do(ctx)
		require.Nil(t, err)
		require.Len(t, res, 1)
	})

	t.Run("load complicated json using auto schema", func(t *testing.T) {
		jsonFile, err := os.Open("./example.json")
		require.NoError(t, err)
		require.NotNil(t, jsonFile)
		byteValue, err := io.ReadAll(jsonFile)
		require.NoError(t, err)
		require.NotNil(t, byteValue)
		var result map[string]interface{}
		json.Unmarshal([]byte(byteValue), &result)
		_, err = client.Data().Creator().
			WithClassName("ComplicatedJson").
			WithID(id1).
			WithProperties(map[string]interface{}{
				"complicated": result,
			}).
			Do(context.TODO())
		require.Nil(t, err)
		res, err := client.Data().ObjectsGetter().WithClassName("ComplicatedJson").WithID(id1).Do(ctx)
		require.Nil(t, err)
		require.Len(t, res, 1)
		props, ok := res[0].Properties.(map[string]interface{})
		require.True(t, ok)
		assert.NotNil(t, props)
		assert.Equal(t, 1, len(props))
		complicated, ok := props["complicated"].(map[string]interface{})
		require.True(t, ok)
		assert.NotNil(t, complicated)
		assert.Equal(t, 2, len(complicated))
		objects, ok := complicated["objects"].([]interface{})
		require.True(t, ok)
		assert.NotNil(t, objects)
		assert.Equal(t, 2, len(objects))
		data, ok := objects[0].(map[string]interface{})
		require.True(t, ok)
		assert.NotNil(t, data)
		assert.Equal(t, 6, len(data))
		properties, ok := data["properties"].(map[string]interface{})
		require.True(t, ok)
		assert.NotNil(t, properties)
	})
}
