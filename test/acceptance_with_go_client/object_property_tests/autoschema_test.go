//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package object_property_tests

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestObjectProperty_AutoSchema(t *testing.T) {
	ctx := context.Background()
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	id1 := strfmt.UUID("00000000-0000-0000-0000-000000000001")

	// clean up DB
	err = client.Schema().AllDeleter().Do(context.Background())
	require.Nil(t, err)

	checkData := func(t *testing.T, className, id string) {
		res, err := client.Data().ObjectsGetter().WithClassName(className).WithID(id).Do(ctx)
		require.Nil(t, err)
		require.Len(t, res, 1)
		props, ok := res[0].Properties.(map[string]interface{})
		require.True(t, ok)
		assert.NotNil(t, props)
		assert.Equal(t, 1, len(props))
		jsonProp, ok := props["json"].(map[string]interface{})
		require.True(t, ok)
		assert.NotNil(t, jsonProp)
		assert.Equal(t, 3, len(jsonProp))
		firstName, ok := jsonProp["firstName"]
		require.True(t, ok)
		assert.Equal(t, "John", firstName)
		lastName, ok := jsonProp["lastName"]
		require.True(t, ok)
		assert.Equal(t, "Doe", lastName)
		phones, ok := jsonProp["phones"].([]interface{})
		require.True(t, ok)
		assert.NotNil(t, phones)
		assert.Equal(t, 2, len(phones))
		for _, phone := range phones {
			phoneNo, ok := phone.(map[string]interface{})
			require.True(t, ok)
			assert.NotNil(t, phoneNo)
		}
	}

	t.Run("without auto schema", func(t *testing.T) {
		className := "WithoutAutoSchema"
		err := client.Schema().ClassCreator().WithClass(&models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name:     "json",
					DataType: schema.DataTypeObject.PropString(),
					NestedProperties: []*models.NestedProperty{
						{
							Name:     "firstName",
							DataType: schema.DataTypeText.PropString(),
						},
						{
							Name:     "lastName",
							DataType: schema.DataTypeText.PropString(),
						},
						{
							Name:     "phones",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{
									Name:     "phoneNo",
									DataType: schema.DataTypeInt.PropString(),
								},
							},
						},
					},
				},
			},
		}).Do(ctx)
		require.NoError(t, err)
		_, err = client.Data().Creator().
			WithClassName(className).
			WithID(id1.String()).
			WithProperties(map[string]interface{}{
				"json": map[string]interface{}{
					"firstName": "John",
					"lastName":  "Doe",
					"phones": []interface{}{
						map[string]interface{}{
							"phoneNo": 1,
						},
						map[string]interface{}{
							"phoneNo": 2,
						},
					},
				},
			}).Do(ctx)
		require.Nil(t, err)
		checkData(t, className, id1.String())
	})

	t.Run("with auto schema", func(t *testing.T) {
		className := "WithAutoSchema"
		_, err = client.Data().Creator().
			WithClassName(className).
			WithID(id1.String()).
			WithProperties(map[string]interface{}{
				"json": map[string]interface{}{
					"firstName": "John",
					"lastName":  "Doe",
					"phones": []interface{}{
						map[string]interface{}{
							"phoneNo": 1,
						},
						map[string]interface{}{
							"phoneNo": 2,
						},
					},
				},
			}).Do(ctx)
		require.Nil(t, err)
		checkData(t, className, id1.String())
	})

	t.Run("partially with auto schema", func(t *testing.T) {
		className := "PartiallyAutoSchema"
		err := client.Schema().ClassCreator().WithClass(&models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name:     "json",
					DataType: schema.DataTypeObject.PropString(),
					NestedProperties: []*models.NestedProperty{
						{
							Name:     "firstName",
							DataType: schema.DataTypeText.PropString(),
						},
					},
				},
			},
		}).Do(ctx)
		require.NoError(t, err)
		_, err = client.Data().Creator().
			WithClassName(className).
			WithID(id1.String()).
			WithProperties(map[string]interface{}{
				"json": map[string]interface{}{
					"firstName": "John",
					"lastName":  "Doe",
					"phones": []interface{}{
						map[string]interface{}{
							"phoneNo": 1,
						},
						map[string]interface{}{
							"phoneNo": 2,
						},
					},
				},
			}).Do(ctx)
		require.Nil(t, err)
		checkData(t, className, id1.String())
	})
}
