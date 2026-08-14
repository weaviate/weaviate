//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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
	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/schema"

	acceptance_with_go_client "acceptance_tests_with_client"
	"acceptance_tests_with_client/internal/wvhost"
)

func TestObjectProperty_AutoSchema(t *testing.T) {
	ctx := context.Background()
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: wvhost.REST()})
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

func TestAutoSchema_DefaultNamedVector(t *testing.T) {
	ctx := context.Background()
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: wvhost.REST()})
	require.Nil(t, err)

	require.NoError(t, client.Schema().AllDeleter().Do(ctx))

	vector := []float32{0.1, 0.2, 0.3}

	tests := []struct {
		name      string
		className string
		id        strfmt.UUID
		create    func(t *testing.T, className string, id strfmt.UUID)
	}{
		{
			name:      "legacy vector field",
			className: "AutoSchemaLegacyVector",
			id:        strfmt.UUID("00000000-0000-0000-0000-00000000000a"),
			create: func(t *testing.T, className string, id strfmt.UUID) {
				_, err := client.Data().Creator().
					WithClassName(className).
					WithID(id.String()).
					WithProperties(map[string]interface{}{"company": "BestOne"}).
					WithVector(vector).
					Do(ctx)
				require.NoError(t, err)
			},
		},
		{
			name:      "default named vector",
			className: "AutoSchemaNamedVector",
			id:        strfmt.UUID("00000000-0000-0000-0000-00000000000b"),
			create: func(t *testing.T, className string, id strfmt.UUID) {
				_, err := client.Data().Creator().
					WithClassName(className).
					WithID(id.String()).
					WithProperties(map[string]interface{}{"company": "BestOne"}).
					WithVectors(map[string]models.Vector{modelsext.DefaultNamedVectorName: vector}).
					Do(ctx)
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.create(t, tt.className, tt.id)

			class, err := client.Schema().ClassGetter().WithClassName(tt.className).Do(ctx)
			require.NoError(t, err)

			// auto-schema creates a single "default" named vector and no legacy vector
			assert.Empty(t, class.Vectorizer)
			assert.Empty(t, class.VectorIndexType)
			assert.Nil(t, class.VectorIndexConfig)
			require.Len(t, class.VectorConfig, 1)
			defaultVector, ok := class.VectorConfig[modelsext.DefaultNamedVectorName]
			require.True(t, ok)
			assert.Equal(t, map[string]interface{}{"none": map[string]interface{}{}}, defaultVector.Vectorizer)
			assert.Equal(t, "hnsw", defaultVector.VectorIndexType)

			objs, err := client.Data().ObjectsGetter().
				WithClassName(tt.className).
				WithID(tt.id.String()).
				WithVector().
				Do(ctx)
			require.NoError(t, err)
			require.Len(t, objs, 1)
			// reads never fill the legacy vector field, the value only comes back
			// under the named vector it was stored as
			assert.Empty(t, objs[0].Vector)
			require.Contains(t, objs[0].Vectors, modelsext.DefaultNamedVectorName)
			assert.Equal(t, models.Vector(vector), objs[0].Vectors[modelsext.DefaultNamedVectorName])

			t.Run("nearVector without target vector", func(t *testing.T) {
				res, err := client.GraphQL().Get().
					WithClassName(tt.className).
					WithNearVector(client.GraphQL().NearVectorArgBuilder().WithVector(vector)).
					WithFields(graphql.Field{Name: "_additional", Fields: []graphql.Field{{Name: "id"}}}).
					WithLimit(1).
					Do(ctx)
				require.NoError(t, err)
				require.Empty(t, res.Errors)
				require.Equal(t, []string{tt.id.String()}, acceptance_with_go_client.GetIds(t, res, tt.className))
			})
		})
	}
}
