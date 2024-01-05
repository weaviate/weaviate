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

	assertDataCreated := func(t *testing.T, className, id string) {
		res, err := client.Data().ObjectsGetter().WithClassName(className).WithID(id).Do(ctx)
		require.NoError(t, err)
		require.Len(t, res, 1)

		props, ok := res[0].Properties.(map[string]interface{})
		require.True(t, ok)
		require.Len(t, props, 2)

		require.Contains(t, props, "company")
		assert.Equal(t, "BestOne", props["company"])

		require.Contains(t, props, "json")
		jsonProp, ok := props["json"].(map[string]interface{})
		require.True(t, ok)
		require.Len(t, jsonProp, 3)

		require.Contains(t, jsonProp, "firstName")
		assert.Equal(t, "John", jsonProp["firstName"])

		require.Contains(t, jsonProp, "lastName")
		assert.Equal(t, "Doe", jsonProp["lastName"])

		require.Contains(t, jsonProp, "phones")
		phones, ok := jsonProp["phones"].([]interface{})
		require.True(t, ok)
		require.Len(t, phones, 2)
		for i, p := range phones {
			phone, ok := p.(map[string]interface{})
			require.True(t, ok)
			require.Contains(t, phone, "phoneNo")
			assert.Equal(t, float64(i+1), phone["phoneNo"])
		}
	}

	assertDataUpdated := func(t *testing.T, className, id string) {
		res, err := client.Data().ObjectsGetter().WithClassName(className).WithID(id).Do(ctx)
		require.NoError(t, err)
		require.Len(t, res, 1)

		props, ok := res[0].Properties.(map[string]interface{})
		require.True(t, ok)
		require.Len(t, props, 3)

		require.Contains(t, props, "company")
		assert.Equal(t, "BestTwo", props["company"])

		require.Contains(t, props, "founded")
		assert.Equal(t, float64(1950), props["founded"])

		require.Contains(t, props, "json")
		jsonProp, ok := props["json"].(map[string]interface{})
		require.True(t, ok)
		require.Len(t, jsonProp, 3)

		require.Contains(t, jsonProp, "firstName")
		assert.Equal(t, "Jane", jsonProp["firstName"])

		require.Contains(t, jsonProp, "age")
		assert.Equal(t, float64(32), jsonProp["age"])

		require.Contains(t, jsonProp, "phones")
		phones, ok := jsonProp["phones"].([]interface{})
		require.True(t, ok)
		require.Len(t, phones, 3)
		for i, p := range phones {
			phone, ok := p.(map[string]interface{})
			require.True(t, ok)
			require.Contains(t, phone, "phoneNo")
			assert.Equal(t, float64(i+1), phone["phoneNo"])
		}
	}

	assertDataMerged := func(t *testing.T, className, id string) {
		res, err := client.Data().ObjectsGetter().WithClassName(className).WithID(id).Do(ctx)
		require.NoError(t, err)
		require.Len(t, res, 1)

		props, ok := res[0].Properties.(map[string]interface{})
		require.True(t, ok)
		require.Len(t, props, 3)

		require.Contains(t, props, "company")
		assert.Equal(t, "BestTwo", props["company"])

		require.Contains(t, props, "founded")
		assert.Equal(t, float64(1960), props["founded"])

		require.Contains(t, props, "json")
		jsonProp, ok := props["json"].(map[string]interface{})
		require.True(t, ok)
		require.Len(t, jsonProp, 2)

		require.Contains(t, jsonProp, "lastName")
		assert.Equal(t, "Smith", jsonProp["lastName"])

		require.Contains(t, jsonProp, "phones")
		phones, ok := jsonProp["phones"].([]interface{})
		require.True(t, ok)
		require.Len(t, phones, 0)
	}

	type testCase struct {
		name      string
		className string
		before    func(t *testing.T, className string)
	}

	testCases := []testCase{
		{
			name:      "without auto schema",
			className: "WithoutAutoSchema",
			before: func(t *testing.T, className string) {
				err := client.Schema().ClassCreator().WithClass(&models.Class{
					Class: className,
					Properties: []*models.Property{
						{
							Name:     "company",
							DataType: schema.DataTypeText.PropString(),
						},
						{
							Name:     "founded",
							DataType: schema.DataTypeInt.PropString(),
						},
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
									Name:     "age",
									DataType: schema.DataTypeInt.PropString(),
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
			},
		},
		{
			name:      "with auto schema",
			className: "WithAutoSchema",
			before:    func(t *testing.T, className string) {},
		},
		{
			name:      "partially with auto schema",
			className: "PartiallyAutoSchema",
			before: func(t *testing.T, className string) {
				err := client.Schema().ClassCreator().WithClass(&models.Class{
					Class: className,
					Properties: []*models.Property{
						{
							Name:     "company",
							DataType: schema.DataTypeText.PropString(),
						},
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
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t, tc.className)

			t.Run("create", func(t *testing.T) {
				_, err := client.Data().Creator().
					WithClassName(tc.className).
					WithID(id1.String()).
					WithProperties(map[string]interface{}{
						"company": "BestOne",
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
				require.NoError(t, err)
				assertDataCreated(t, tc.className, id1.String())
			})

			t.Run("update", func(t *testing.T) {
				err := client.Data().Updater().
					WithClassName(tc.className).
					WithID(id1.String()).
					WithProperties(map[string]interface{}{
						"company": "BestTwo",
						"founded": 1950,
						"json": map[string]interface{}{
							"firstName": "Jane",
							"age":       32,
							"phones": []interface{}{
								map[string]interface{}{
									"phoneNo": 1,
								},
								map[string]interface{}{
									"phoneNo": 2,
								},
								map[string]interface{}{
									"phoneNo": 3,
								},
							},
						},
					}).Do(ctx)
				require.NoError(t, err)
				assertDataUpdated(t, tc.className, id1.String())
			})

			t.Run("merge", func(t *testing.T) {
				err := client.Data().Updater().
					WithMerge().
					WithClassName(tc.className).
					WithID(id1.String()).
					WithProperties(map[string]interface{}{
						"founded": 1960,
						"json": map[string]interface{}{
							"lastName": "Smith",
							"phones":   []interface{}{},
						},
					}).Do(ctx)
				require.NoError(t, err)
				assertDataMerged(t, tc.className, id1.String())
			})
		})
	}
}
