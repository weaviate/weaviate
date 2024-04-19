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

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/grpc"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestObjectProperty_Batch(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name   string
		config wvt.Config
	}{
		{
			name:   "single node - rest api",
			config: wvt.Config{Scheme: "http", Host: "localhost:8080"},
		},
		{
			name:   "single node - grpc api",
			config: wvt.Config{Scheme: "http", Host: "localhost:8080", GrpcConfig: &grpc.Config{Host: "localhost:50051"}},
		},
	}
	for _, tt := range tests {
		client, err := wvt.NewClient(tt.config)
		require.Nil(t, err)

		id1 := strfmt.UUID("00000000-0000-0000-0000-000000000001")
		id2 := strfmt.UUID("00000000-0000-0000-0000-000000000002")

		// clean up DB
		err = client.Schema().AllDeleter().Do(context.Background())
		require.Nil(t, err)

		t.Run("batch import object property", func(t *testing.T) {
			resp, err := client.Batch().ObjectsBatcher().
				WithObjects(
					&models.Object{
						Class: "Person",
						ID:    id1,
						Properties: map[string]interface{}{
							"name": "John Doe",
							"json": map[string]interface{}{
								"firstName":   "John",
								"lastName":    "Doe",
								"proffession": "Accountant",
								"birthdate":   "2012-05-05T07:16:30+02:00",
								"phoneNumber": map[string]interface{}{
									"input":                  "020 1234567",
									"defaultCountry":         "nl",
									"internationalFormatted": "+31 20 1234567",
									"countryCode":            31,
									"national":               201234567,
									"nationalFormatted":      "020 1234567",
									"valid":                  true,
								},
								"location": map[string]interface{}{
									"latitude":  52.366667,
									"longitude": 4.9,
								},
							},
						},
					},
					&models.Object{
						Class: "Person",
						ID:    id2,
						Properties: map[string]interface{}{
							"name": "Stacey Spears",
							"json": map[string]interface{}{
								"firstName":   "Stacey",
								"lastName":    "Spears",
								"proffession": "Accountant",
								"birthdate":   "2011-05-05T07:16:30+02:00",
								"phoneNumber": map[string]interface{}{
									"input":                  "020 1555444",
									"defaultCountry":         "nl",
									"internationalFormatted": "+31 20 1555444",
									"countryCode":            31,
									"national":               201555444,
									"nationalFormatted":      "020 1555444",
									"valid":                  true,
								},
								"location": map[string]interface{}{
									"latitude":  51.366667,
									"longitude": 5.9,
								},
							},
						},
					}).
				Do(ctx)
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Len(t, resp, 2)
			objs, err := client.Data().ObjectsGetter().WithClassName("Person").WithID(id1.String()).Do(ctx)
			require.NoError(t, err)
			require.NotNil(t, objs)
			require.Len(t, objs, 1)
			properties, ok := objs[0].Properties.(map[string]interface{})
			require.True(t, ok)
			require.NotNil(t, properties)
			jsonProp, ok := properties["json"].(map[string]interface{})
			require.True(t, ok)
			require.NotNil(t, jsonProp)
			phone, ok := jsonProp["phoneNumber"].(map[string]interface{})
			require.True(t, ok)
			assert.Equal(t, 7, len(phone))
			location, ok := jsonProp["location"].(map[string]interface{})
			require.True(t, ok)
			assert.Equal(t, 2, len(location))
		})

		t.Run("batch import object array property", func(t *testing.T) {
			resp, err := client.Batch().ObjectsBatcher().
				WithObjects(
					&models.Object{
						Class: "People",
						ID:    id1,
						Properties: map[string]interface{}{
							"country": "United Kingdom",
							"people": []interface{}{
								map[string]interface{}{
									"firstName":   "John",
									"lastName":    "Doe",
									"proffession": "Accountant",
									"birthdate":   "2012-05-05T07:16:30+02:00",
									"phoneNumber": map[string]interface{}{
										"input":          "020 1234567",
										"defaultCountry": "nl",
									},
									"location": map[string]interface{}{
										"latitude":  52.366667,
										"longitude": 4.9,
									},
								},
								map[string]interface{}{
									"firstName":   "Stacey",
									"lastName":    "Spears",
									"proffession": "Accountant",
									"birthdate":   "2011-05-05T07:16:30+02:00",
									"phoneNumber": map[string]interface{}{
										"input":          "020 1555444",
										"defaultCountry": "nl",
									},
									"location": map[string]interface{}{
										"latitude":  51.366667,
										"longitude": 5.9,
									},
								},
							},
						},
					},
					&models.Object{
						Class: "People",
						ID:    id2,
						Properties: map[string]interface{}{
							"country": "United States of America",
							"people": []interface{}{
								map[string]interface{}{
									"firstName":   "Robert",
									"lastName":    "Junior",
									"proffession": "Accountant",
									"birthdate":   "2002-05-05T07:16:30+02:00",
									"phoneNumber": map[string]interface{}{
										"input":          "020 1234567",
										"defaultCountry": "nl",
									},
									"location": map[string]interface{}{
										"latitude":  52.366667,
										"longitude": 4.9,
									},
								},
								map[string]interface{}{
									"firstName":   "Steven",
									"lastName":    "Spears",
									"proffession": "Accountant",
									"birthdate":   "2009-05-05T07:16:30+02:00",
									"phoneNumber": map[string]interface{}{
										"input":          "020 1555444",
										"defaultCountry": "nl",
									},
									"location": map[string]interface{}{
										"latitude":  51.366667,
										"longitude": 5.9,
									},
								},
							},
						},
					},
				).
				Do(ctx)
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Len(t, resp, 2)
			objs, err := client.Data().ObjectsGetter().WithClassName("People").Do(ctx)
			require.NoError(t, err)
			require.NotNil(t, objs)
			require.Len(t, objs, 2)
			properties, ok := objs[0].Properties.(map[string]interface{})
			require.True(t, ok)
			require.NotNil(t, properties)
			people, ok := properties["people"].([]interface{})
			require.True(t, ok)
			assert.Equal(t, 2, len(people))
			person, ok := people[0].(map[string]interface{})
			require.True(t, ok)
			assert.Equal(t, 6, len(person))
		})

		t.Run("load json array file using batch api", func(t *testing.T) {
			loadJson := func(t *testing.T) []interface{} {
				jsonFile, err := os.Open("./batch.json")
				require.NoError(t, err)
				require.NotNil(t, jsonFile)
				byteValue, err := io.ReadAll(jsonFile)
				require.NoError(t, err)
				require.NotNil(t, byteValue)
				var result []interface{}
				json.Unmarshal([]byte(byteValue), &result)
				return result
			}
			checkData := func(t *testing.T, className string) {
				res, err := client.Data().ObjectsGetter().WithClassName(className).WithID(id1.String()).Do(ctx)
				require.Nil(t, err)
				require.Len(t, res, 1)
				props, ok := res[0].Properties.(map[string]interface{})
				require.True(t, ok)
				assert.NotNil(t, props)
				assert.Equal(t, 1, len(props))
				objectArrayProp, ok := props["objectArray"].([]interface{})
				require.True(t, ok)
				assert.NotNil(t, objectArrayProp)
				assert.Equal(t, 10, len(objectArrayProp))
				data, ok := objectArrayProp[0].(map[string]interface{})
				require.True(t, ok)
				assert.NotNil(t, data)
				assert.Equal(t, 7, len(data))
				phone, ok := data["phone"].(map[string]interface{})
				require.True(t, ok)
				assert.NotNil(t, phone)
				assert.Equal(t, 2, len(phone))
				location, ok := data["location"].(map[string]interface{})
				require.True(t, ok)
				assert.NotNil(t, location)
				assert.Equal(t, 2, len(location))
			}
			// parse batch.json
			loadedJson := loadJson(t)
			t.Run("with auto schema", func(t *testing.T) {
				className := "BatchArrayJsonAutoSchema"
				resp, err := client.Batch().ObjectsBatcher().
					WithObjects(&models.Object{
						Class: className,
						ID:    id1,
						Properties: map[string]interface{}{
							"objectArray": loadedJson,
						},
					}).Do(ctx)
				require.NoError(t, err)
				require.NotNil(t, resp)
				checkData(t, className)
			})
			t.Run("without auto schema", func(t *testing.T) {
				className := "BatchArrayJsonWithoutAutoSchema"
				err := client.Schema().ClassCreator().WithClass(&models.Class{
					Class: className,
					Properties: []*models.Property{
						{
							Name:     "objectArray",
							DataType: schema.DataTypeObjectArray.PropString(),
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
									Name:     "profession",
									DataType: schema.DataTypeText.PropString(),
								},
								{
									Name:     "description",
									DataType: schema.DataTypeText.PropString(),
								},
								{
									Name:     "phone",
									DataType: schema.DataTypeObject.PropString(),
									NestedProperties: []*models.NestedProperty{
										{
											Name:     "input",
											DataType: schema.DataTypeText.PropString(),
										},
										{
											Name:     "defaultCountry",
											DataType: schema.DataTypeText.PropString(),
										},
									},
								},
								{
									Name:     "location",
									DataType: schema.DataTypeObject.PropString(),
									NestedProperties: []*models.NestedProperty{
										{
											Name:     "latitude",
											DataType: schema.DataTypeNumber.PropString(),
										},
										{
											Name:     "longitude",
											DataType: schema.DataTypeNumber.PropString(),
										},
									},
								},
								{
									Name:     "city",
									DataType: schema.DataTypeText.PropString(),
								},
							},
						},
					},
				}).Do(ctx)
				require.NoError(t, err)
				resp, err := client.Batch().ObjectsBatcher().
					WithObjects(&models.Object{
						Class: className,
						ID:    id1,
						Properties: map[string]interface{}{
							"objectArray": loadedJson,
						},
					}).Do(ctx)
				require.NoError(t, err)
				require.NotNil(t, resp)
				checkData(t, className)
			})
		})
	}
}
