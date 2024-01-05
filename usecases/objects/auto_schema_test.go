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

package objects

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/test_utils"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/objects/validation"
)

func Test_autoSchemaManager_determineType(t *testing.T) {
	type fields struct {
		config config.AutoSchema
	}
	type args struct {
		value interface{}
	}

	autoSchemaEnabledFields := fields{
		config: config.AutoSchema{
			Enabled: true,
		},
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []schema.DataType
		errMsgs []string
	}{
		{
			name: "fail determining type of nested array (1)",
			fields: fields{
				config: config.AutoSchema{
					Enabled:       true,
					DefaultString: schema.DataTypeText.String(),
				},
			},
			args: args{
				value: []interface{}{[]interface{}{"panic"}},
			},
			errMsgs: []string{"unrecognized data type"},
		},
		{
			name: "fail determining type of nested array (2)",
			fields: fields{
				config: config.AutoSchema{
					Enabled:       true,
					DefaultString: schema.DataTypeText.String(),
				},
			},
			args: args{
				value: []interface{}{[]string{}},
			},
			errMsgs: []string{"unrecognized data type"},
		},
		{
			name: "fail determining type of mixed elements in array",
			fields: fields{
				config: config.AutoSchema{
					Enabled:       true,
					DefaultString: schema.DataTypeText.String(),
				},
			},
			args: args{
				value: []interface{}{"something", false},
			},
			errMsgs: []string{"mismatched data type", "'text' expected, got 'boolean'"},
		},
		{
			name: "fail determining type of mixed refs and objects (1)",
			fields: fields{
				config: config.AutoSchema{
					Enabled: true,
				},
			},
			args: args{
				value: []interface{}{
					map[string]interface{}{"beacon": "weaviate://localhost/df48b9f6-ba48-470c-bf6a-57657cb07390"},
					map[string]interface{}{"propOfObject": "something"},
				},
			},
			errMsgs: []string{"mismatched data type", "reference expected, got 'object'"},
		},
		{
			name: "fail determining type of mixed refs and objects (2)",
			fields: fields{
				config: config.AutoSchema{
					Enabled: true,
				},
			},
			args: args{
				value: []interface{}{
					map[string]interface{}{"propOfObject": "something"},
					map[string]interface{}{"beacon": "weaviate://localhost/df48b9f6-ba48-470c-bf6a-57657cb07390"},
				},
			},
			errMsgs: []string{"mismatched data type", "'object' expected, got reference"},
		},
		{
			name: "determine text",
			fields: fields{
				config: config.AutoSchema{
					Enabled:       true,
					DefaultString: schema.DataTypeText.String(),
				},
			},
			args: args{
				value: "string",
			},
			want: []schema.DataType{schema.DataTypeText},
		},
		{
			name: "determine text (implicit)",
			fields: fields{
				config: config.AutoSchema{
					Enabled: true,
				},
			},
			args: args{
				value: "string",
			},
			want: []schema.DataType{schema.DataTypeText},
		},
		{
			name: "determine date",
			fields: fields{
				config: config.AutoSchema{
					Enabled:     true,
					DefaultDate: "date",
				},
			},
			args: args{
				value: "2002-10-02T15:00:00Z",
			},
			want: []schema.DataType{schema.DataTypeDate},
		},
		{
			name: "determine uuid (1)",
			fields: fields{
				config: config.AutoSchema{
					Enabled: true,
				},
			},
			args: args{
				value: "5b2cbe85-c38a-41f7-9e8c-7406ff6d15aa",
			},
			want: []schema.DataType{schema.DataTypeUUID},
		},
		{
			name: "determine uuid (2)",
			fields: fields{
				config: config.AutoSchema{
					Enabled: true,
				},
			},
			args: args{
				value: "5b2cbe85c38a41f79e8c7406ff6d15aa",
			},
			want: []schema.DataType{schema.DataTypeUUID},
		},
		{
			name: "determine int",
			fields: fields{
				config: config.AutoSchema{
					Enabled:       true,
					DefaultNumber: "int",
				},
			},
			args: args{
				value: json.Number("1"),
			},
			want: []schema.DataType{schema.DataTypeInt},
		},
		{
			name: "determine number",
			fields: fields{
				config: config.AutoSchema{
					Enabled:       true,
					DefaultNumber: "number",
				},
			},
			args: args{
				value: json.Number("1"),
			},
			want: []schema.DataType{schema.DataTypeNumber},
		},
		{
			name: "determine boolean",
			fields: fields{
				config: config.AutoSchema{
					Enabled:       true,
					DefaultNumber: "number",
				},
			},
			args: args{
				value: true,
			},
			want: []schema.DataType{schema.DataTypeBoolean},
		},
		{
			name: "determine geoCoordinates",
			fields: fields{
				config: config.AutoSchema{
					Enabled: true,
				},
			},
			args: args{
				value: map[string]interface{}{
					"latitude":  json.Number("1.1"),
					"longitude": json.Number("1.1"),
				},
			},
			want: []schema.DataType{schema.DataTypeGeoCoordinates},
		},
		{
			name: "determine phoneNumber",
			fields: fields{
				config: config.AutoSchema{
					Enabled: true,
				},
			},
			args: args{
				value: map[string]interface{}{
					"input": "020 1234567",
				},
			},
			want: []schema.DataType{schema.DataTypePhoneNumber},
		},
		{
			name: "determine phoneNumber (2)",
			fields: fields{
				config: config.AutoSchema{
					Enabled: true,
				},
			},
			args: args{
				value: map[string]interface{}{
					"input":          "020 1234567",
					"defaultCountry": "nl",
				},
			},
			want: []schema.DataType{schema.DataTypePhoneNumber},
		},
		{
			name: "determine cross reference",
			fields: fields{
				config: config.AutoSchema{
					Enabled: true,
				},
			},
			args: args{
				value: []interface{}{
					map[string]interface{}{"beacon": "weaviate://localhost/df48b9f6-ba48-470c-bf6a-57657cb07390"},
				},
			},
			want: []schema.DataType{schema.DataType("Publication")},
		},
		{
			name: "determine cross references",
			fields: fields{
				config: config.AutoSchema{
					Enabled: true,
				},
			},
			args: args{
				value: []interface{}{
					map[string]interface{}{"beacon": "weaviate://localhost/df48b9f6-ba48-470c-bf6a-57657cb07390"},
					map[string]interface{}{"beacon": "weaviate://localhost/df48b9f6-ba48-470c-bf6a-57657cb07391"},
				},
			},
			want: []schema.DataType{schema.DataType("Publication"), schema.DataType("Article")},
		},
		{
			name: "determine text array",
			fields: fields{
				config: config.AutoSchema{
					Enabled:       true,
					DefaultString: schema.DataTypeText.String(),
				},
			},
			args: args{
				value: []interface{}{"a", "b"},
			},
			want: []schema.DataType{schema.DataTypeTextArray},
		},
		{
			name: "determine text array (implicit)",
			fields: fields{
				config: config.AutoSchema{
					Enabled: true,
				},
			},
			args: args{
				value: []interface{}{"a", "b"},
			},
			want: []schema.DataType{schema.DataTypeTextArray},
		},
		{
			name: "determine int array",
			fields: fields{
				config: config.AutoSchema{
					Enabled:       true,
					DefaultNumber: "int",
				},
			},
			args: args{
				value: []interface{}{json.Number("11"), json.Number("12")},
			},
			want: []schema.DataType{schema.DataTypeIntArray},
		},
		{
			name: "determine number array",
			fields: fields{
				config: config.AutoSchema{
					Enabled:       true,
					DefaultNumber: "number",
				},
			},
			args: args{
				value: []interface{}{json.Number("1.1"), json.Number("1.2")},
			},
			want: []schema.DataType{schema.DataTypeNumberArray},
		},
		{
			name: "determine boolean array",
			fields: fields{
				config: config.AutoSchema{
					Enabled: true,
				},
			},
			args: args{
				value: []interface{}{true, false},
			},
			want: []schema.DataType{schema.DataTypeBooleanArray},
		},
		{
			name: "determine date array",
			fields: fields{
				config: config.AutoSchema{
					Enabled:     true,
					DefaultDate: "date",
				},
			},
			args: args{
				value: []interface{}{"2002-10-02T15:00:00Z", "2002-10-02T15:01:00Z"},
			},
			want: []schema.DataType{schema.DataTypeDateArray},
		},
		{
			name: "determine uuid array (1)",
			fields: fields{
				config: config.AutoSchema{
					Enabled: true,
				},
			},
			args: args{
				value: []interface{}{
					"5b2cbe85-c38a-41f7-9e8c-7406ff6d15aa",
					"57a8564d-089b-4cd9-be39-56681605e0da",
				},
			},
			want: []schema.DataType{schema.DataTypeUUIDArray},
		},
		{
			name: "determine uuid array (2)",
			fields: fields{
				config: config.AutoSchema{
					Enabled: true,
				},
			},
			args: args{
				value: []interface{}{
					"5b2cbe85c38a41f79e8c7406ff6d15aa",
					"57a8564d089b4cd9be3956681605e0da",
				},
			},
			want: []schema.DataType{schema.DataTypeUUIDArray},
		},
		{
			name: "[deprecated string] determine string",
			fields: fields{
				config: config.AutoSchema{
					Enabled:       true,
					DefaultString: schema.DataTypeString.String(),
				},
			},
			args: args{
				value: "string",
			},
			want: []schema.DataType{schema.DataTypeString},
		},
		{
			name: "[deprecated string] determine string array",
			fields: fields{
				config: config.AutoSchema{
					Enabled:       true,
					DefaultString: schema.DataTypeString.String(),
				},
			},
			args: args{
				value: []interface{}{"a", "b"},
			},
			want: []schema.DataType{schema.DataTypeStringArray},
		},
		{
			name:   "determine object",
			fields: autoSchemaEnabledFields,
			args: args{
				value: map[string]interface{}{
					"some_number": 1.23,
					"some_bool":   false,
				},
			},
			want: []schema.DataType{schema.DataTypeObject},
		},
		{
			name:   "determine object array",
			fields: autoSchemaEnabledFields,
			args: args{
				value: []interface{}{
					map[string]interface{}{
						"some_number": 1.23,
						"some_bool":   false,
					},
				},
			},
			want: []schema.DataType{schema.DataTypeObjectArray},
		},
		{
			name:   "determine object, not geoCoordinates (too few props 1)",
			fields: autoSchemaEnabledFields,
			args: args{
				value: map[string]interface{}{
					"latitude": json.Number("1.1"),
				},
			},
			want: []schema.DataType{schema.DataTypeObject},
		},
		{
			name:   "determine object, not geoCoordinates (too few props 2)",
			fields: autoSchemaEnabledFields,
			args: args{
				value: map[string]interface{}{
					"longitude": json.Number("1.1"),
				},
			},
			want: []schema.DataType{schema.DataTypeObject},
		},
		{
			name:   "determine object, not geoCoordinates (too many props)",
			fields: autoSchemaEnabledFields,
			args: args{
				value: map[string]interface{}{
					"latitude":   json.Number("1.1"),
					"longitude":  json.Number("1.1"),
					"unrelevant": "some text",
				},
			},
			want: []schema.DataType{schema.DataTypeObject},
		},
		{
			name:   "determine object, not phoneNumber (too few props)",
			fields: autoSchemaEnabledFields,
			args: args{
				value: map[string]interface{}{
					"defaultCountry": "nl",
				},
			},
			want: []schema.DataType{schema.DataTypeObject},
		},
		{
			name:   "determine object, not phoneNumber (too many props)",
			fields: autoSchemaEnabledFields,
			args: args{
				value: map[string]interface{}{
					"input":                  "020 1234567",
					"defaultCountry":         "nl",
					"internationalFormatted": "+31 20 1234567",
					"countryCode":            31,
					"national":               201234567,
					"nationalFormatted":      "020 1234567",
					"valid":                  true,
				},
			},
			want: []schema.DataType{schema.DataTypeObject},
		},
	}
	for _, tt := range tests {
		vectorRepo := &fakeVectorRepo{}
		vectorRepo.On("ObjectByID", strfmt.UUID("df48b9f6-ba48-470c-bf6a-57657cb07390"), mock.Anything, mock.Anything, mock.Anything).
			Return(&search.Result{ClassName: "Publication"}, nil).Once()
		vectorRepo.On("ObjectByID", strfmt.UUID("df48b9f6-ba48-470c-bf6a-57657cb07391"), mock.Anything, mock.Anything, mock.Anything).
			Return(&search.Result{ClassName: "Article"}, nil).Once()
		m := &autoSchemaManager{
			schemaManager: &fakeSchemaManager{},
			vectorRepo:    vectorRepo,
			config:        tt.fields.config,
		}
		t.Run(tt.name, func(t *testing.T) {
			got, err := m.determineType(tt.args.value, false)
			if len(tt.errMsgs) == 0 {
				require.NoError(t, err)
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("autoSchemaManager.determineType() = %v, want %v", got, tt.want)
				}
			} else {
				for _, errMsg := range tt.errMsgs {
					require.ErrorContains(t, err, errMsg)
				}
				assert.Nil(t, got)
			}
		})
	}
}

func Test_autoSchemaManager_autoSchema_emptyRequest(t *testing.T) {
	// given
	vectorRepo := &fakeVectorRepo{}
	vectorRepo.On("ObjectByID", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&search.Result{ClassName: "Publication"}, nil).Once()
	schemaManager := &fakeSchemaManager{}
	logger, _ := test.NewNullLogger()
	autoSchemaManager := &autoSchemaManager{
		schemaManager: schemaManager,
		vectorRepo:    vectorRepo,
		config: config.AutoSchema{
			Enabled:       true,
			DefaultString: schema.DataTypeText.String(),
			DefaultNumber: "number",
			DefaultDate:   "date",
		},
		logger: logger,
	}

	var obj *models.Object

	err := autoSchemaManager.autoSchema(context.Background(), &models.Principal{}, obj, true)
	assert.EqualError(t, fmt.Errorf(validation.ErrorMissingObject), err.Error())
}

func Test_autoSchemaManager_autoSchema_create(t *testing.T) {
	// given
	vectorRepo := &fakeVectorRepo{}
	vectorRepo.On("ObjectByID", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&search.Result{ClassName: "Publication"}, nil).Once()
	schemaManager := &fakeSchemaManager{}
	logger, _ := test.NewNullLogger()
	autoSchemaManager := &autoSchemaManager{
		schemaManager: schemaManager,
		vectorRepo:    vectorRepo,
		config: config.AutoSchema{
			Enabled:       true,
			DefaultString: schema.DataTypeText.String(),
			DefaultNumber: "number",
			DefaultDate:   "date",
		},
		logger: logger,
	}
	obj := &models.Object{
		Class: "Publication",
		Properties: map[string]interface{}{
			"name":            "Jodie Sparrow",
			"age":             json.Number("30"),
			"publicationDate": "2002-10-02T15:00:00Z",
			"textArray":       []interface{}{"a", "b"},
			"numberArray":     []interface{}{json.Number("30")},
		},
	}
	// when
	schemaBefore := schemaManager.GetSchemaResponse
	err := autoSchemaManager.autoSchema(context.Background(), &models.Principal{}, obj, true)
	schemaAfter := schemaManager.GetSchemaResponse

	// then
	require.Nil(t, schemaBefore.Objects)
	require.Nil(t, err)
	require.NotNil(t, schemaAfter.Objects)
	assert.Equal(t, 1, len(schemaAfter.Objects.Classes))
	assert.Equal(t, "Publication", (schemaAfter.Objects.Classes)[0].Class)
	assert.Equal(t, 5, len((schemaAfter.Objects.Classes)[0].Properties))
	require.NotNil(t, getProperty((schemaAfter.Objects.Classes)[0].Properties, "name"))
	assert.Equal(t, "name", getProperty((schemaAfter.Objects.Classes)[0].Properties, "name").Name)
	assert.Equal(t, "text", getProperty((schemaAfter.Objects.Classes)[0].Properties, "name").DataType[0])
	require.NotNil(t, getProperty((schemaAfter.Objects.Classes)[0].Properties, "age"))
	assert.Equal(t, "age", getProperty((schemaAfter.Objects.Classes)[0].Properties, "age").Name)
	assert.Equal(t, "number", getProperty((schemaAfter.Objects.Classes)[0].Properties, "age").DataType[0])
	require.NotNil(t, getProperty((schemaAfter.Objects.Classes)[0].Properties, "publicationDate"))
	assert.Equal(t, "publicationDate", getProperty((schemaAfter.Objects.Classes)[0].Properties, "publicationDate").Name)
	assert.Equal(t, "date", getProperty((schemaAfter.Objects.Classes)[0].Properties, "publicationDate").DataType[0])
	require.NotNil(t, getProperty((schemaAfter.Objects.Classes)[0].Properties, "textArray"))
	assert.Equal(t, "textArray", getProperty((schemaAfter.Objects.Classes)[0].Properties, "textArray").Name)
	assert.Equal(t, "text[]", getProperty((schemaAfter.Objects.Classes)[0].Properties, "textArray").DataType[0])
	require.NotNil(t, getProperty((schemaAfter.Objects.Classes)[0].Properties, "numberArray"))
	assert.Equal(t, "numberArray", getProperty((schemaAfter.Objects.Classes)[0].Properties, "numberArray").Name)
	assert.Equal(t, "number[]", getProperty((schemaAfter.Objects.Classes)[0].Properties, "numberArray").DataType[0])
}

func Test_autoSchemaManager_autoSchema_update(t *testing.T) {
	// given
	vectorRepo := &fakeVectorRepo{}
	vectorRepo.On("ObjectByID", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&search.Result{ClassName: "Publication"}, nil).Once()
	logger, _ := test.NewNullLogger()
	schemaManager := &fakeSchemaManager{
		GetSchemaResponse: schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{
					{
						Class: "Publication",
						Properties: []*models.Property{
							{
								Name:     "age",
								DataType: []string{"int"},
							},
						},
					},
				},
			},
		},
	}
	autoSchemaManager := &autoSchemaManager{
		schemaManager: schemaManager,
		vectorRepo:    vectorRepo,
		config: config.AutoSchema{
			Enabled:       true,
			DefaultString: schema.DataTypeText.String(),
			DefaultNumber: "int",
			DefaultDate:   "date",
		},
		logger: logger,
	}
	obj := &models.Object{
		Class: "Publication",
		Properties: map[string]interface{}{
			"name":            "Jodie Sparrow",
			"age":             json.Number("30"),
			"publicationDate": "2002-10-02T15:00:00Z",
			"textArray":       []interface{}{"a", "b"},
			"numberArray":     []interface{}{json.Number("30")},
		},
	}
	// when
	// then
	schemaBefore := schemaManager.GetSchemaResponse
	require.NotNil(t, schemaBefore.Objects)
	assert.Equal(t, 1, len(schemaBefore.Objects.Classes))
	assert.Equal(t, "Publication", (schemaBefore.Objects.Classes)[0].Class)
	assert.Equal(t, 1, len((schemaBefore.Objects.Classes)[0].Properties))
	assert.Equal(t, "age", (schemaBefore.Objects.Classes)[0].Properties[0].Name)
	assert.Equal(t, "int", (schemaBefore.Objects.Classes)[0].Properties[0].DataType[0])

	err := autoSchemaManager.autoSchema(context.Background(), &models.Principal{}, obj, true)
	require.Nil(t, err)

	schemaAfter := schemaManager.GetSchemaResponse
	require.NotNil(t, schemaAfter.Objects)
	assert.Equal(t, 1, len(schemaAfter.Objects.Classes))
	assert.Equal(t, "Publication", (schemaAfter.Objects.Classes)[0].Class)
	assert.Equal(t, 5, len((schemaAfter.Objects.Classes)[0].Properties))
	require.NotNil(t, getProperty((schemaAfter.Objects.Classes)[0].Properties, "age"))
	assert.Equal(t, "age", getProperty((schemaAfter.Objects.Classes)[0].Properties, "age").Name)
	assert.Equal(t, "int", getProperty((schemaAfter.Objects.Classes)[0].Properties, "age").DataType[0])
	require.NotNil(t, getProperty((schemaAfter.Objects.Classes)[0].Properties, "name"))
	assert.Equal(t, "name", getProperty((schemaAfter.Objects.Classes)[0].Properties, "name").Name)
	assert.Equal(t, "text", getProperty((schemaAfter.Objects.Classes)[0].Properties, "name").DataType[0])
	require.NotNil(t, getProperty((schemaAfter.Objects.Classes)[0].Properties, "publicationDate"))
	assert.Equal(t, "publicationDate", getProperty((schemaAfter.Objects.Classes)[0].Properties, "publicationDate").Name)
	assert.Equal(t, "date", getProperty((schemaAfter.Objects.Classes)[0].Properties, "publicationDate").DataType[0])
	require.NotNil(t, getProperty((schemaAfter.Objects.Classes)[0].Properties, "textArray"))
	assert.Equal(t, "textArray", getProperty((schemaAfter.Objects.Classes)[0].Properties, "textArray").Name)
	assert.Equal(t, "text[]", getProperty((schemaAfter.Objects.Classes)[0].Properties, "textArray").DataType[0])
	require.NotNil(t, getProperty((schemaAfter.Objects.Classes)[0].Properties, "numberArray"))
	assert.Equal(t, "numberArray", getProperty((schemaAfter.Objects.Classes)[0].Properties, "numberArray").Name)
	assert.Equal(t, "int[]", getProperty((schemaAfter.Objects.Classes)[0].Properties, "numberArray").DataType[0])
}

func Test_autoSchemaManager_getProperties(t *testing.T) {
	type testCase struct {
		name               string
		valProperties      map[string]interface{}
		expectedProperties []*models.Property
	}

	testCases := []testCase{
		{
			name: "mixed 1",
			valProperties: map[string]interface{}{
				"name": "someName",
				"objectProperty": map[string]interface{}{
					"nested_int":  json.Number("123"),
					"nested_text": "some text",
					"nested_objects": []interface{}{
						map[string]interface{}{
							"nested_bool_lvl2": false,
							"nested_numbers_lvl2": []interface{}{
								json.Number("11.11"),
							},
						},
					},
				},
			},
			expectedProperties: []*models.Property{
				{
					Name:     "name",
					DataType: schema.DataTypeText.PropString(),
				},
				{
					Name:     "objectProperty",
					DataType: schema.DataTypeObject.PropString(),
					NestedProperties: []*models.NestedProperty{
						{
							Name:     "nested_int",
							DataType: schema.DataTypeNumber.PropString(),
						},
						{
							Name:     "nested_text",
							DataType: schema.DataTypeText.PropString(),
						},
						{
							Name:     "nested_objects",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{
									Name:     "nested_bool_lvl2",
									DataType: schema.DataTypeBoolean.PropString(),
								},
								{
									Name:     "nested_numbers_lvl2",
									DataType: schema.DataTypeNumberArray.PropString(),
								},
							},
						},
					},
				},
			},
		},
		{
			name: "mixed 2",
			valProperties: map[string]interface{}{
				"name": "someName",
				"objectProperty": map[string]interface{}{
					"nested_number": json.Number("123"),
					"nested_text":   "some text",
					"nested_objects": []interface{}{
						map[string]interface{}{
							"nested_date_lvl2": "2022-01-01T00:00:00+02:00",
							"nested_numbers_lvl2": []interface{}{
								json.Number("11.11"),
							},
						},
					},
				},
			},
			expectedProperties: []*models.Property{
				{
					Name:     "name",
					DataType: schema.DataTypeText.PropString(),
				},
				{
					Name:     "objectProperty",
					DataType: schema.DataTypeObject.PropString(),
					NestedProperties: []*models.NestedProperty{
						{
							Name:     "nested_number",
							DataType: schema.DataTypeNumber.PropString(),
						},
						{
							Name:     "nested_text",
							DataType: schema.DataTypeText.PropString(),
						},
						{
							Name:     "nested_objects",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{
									Name:     "nested_date_lvl2",
									DataType: schema.DataTypeDate.PropString(),
								},
								{
									Name:     "nested_numbers_lvl2",
									DataType: schema.DataTypeNumberArray.PropString(),
								},
							},
						},
					},
				},
			},
		},
		{
			name: "ref",
			valProperties: map[string]interface{}{
				"name": "someName",
				"objectProperty": map[string]interface{}{
					"nested_ref_wannabe": []interface{}{
						map[string]interface{}{
							"beacon": "weaviate://localhost/Soup/8c156d37-81aa-4ce9-a811-621e2702b825",
						},
					},
					"nested_objects": []interface{}{
						map[string]interface{}{
							"nested_ref_wannabe_lvl2": []interface{}{
								map[string]interface{}{
									"beacon": "weaviate://localhost/Soup/8c156d37-81aa-4ce9-a811-621e2702b825",
								},
							},
						},
					},
				},
				"ref": []interface{}{
					map[string]interface{}{
						"beacon": "weaviate://localhost/Soup/8c156d37-81aa-4ce9-a811-621e2702b825",
					},
				},
			},
			expectedProperties: []*models.Property{
				{
					Name:     "name",
					DataType: schema.DataTypeText.PropString(),
				},
				{
					Name:     "objectProperty",
					DataType: schema.DataTypeObject.PropString(),
					NestedProperties: []*models.NestedProperty{
						{
							Name:     "nested_ref_wannabe",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{
									Name:     "beacon",
									DataType: schema.DataTypeText.PropString(),
								},
							},
						},
						{
							Name:     "nested_objects",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{
									Name:     "nested_ref_wannabe_lvl2",
									DataType: schema.DataTypeObjectArray.PropString(),
									NestedProperties: []*models.NestedProperty{
										{
											Name:     "beacon",
											DataType: schema.DataTypeText.PropString(),
										},
									},
								},
							},
						},
					},
				},
				{
					Name:     "ref",
					DataType: []string{"Soup"},
				},
			},
		},
		{
			name: "phone",
			valProperties: map[string]interface{}{
				"name": "someName",
				"objectProperty": map[string]interface{}{
					"nested_phone_wannabe": map[string]interface{}{
						"input":          "020 1234567",
						"defaultCountry": "nl",
					},
					"nested_phone_wannabes": []interface{}{
						map[string]interface{}{
							"input":          "020 1234567",
							"defaultCountry": "nl",
						},
					},
					"nested_objects": []interface{}{
						map[string]interface{}{
							"nested_phone_wannabe_lvl2": map[string]interface{}{
								"input":          "020 1234567",
								"defaultCountry": "nl",
							},
							"nested_phone_wannabes_lvl2": []interface{}{
								map[string]interface{}{
									"input":          "020 1234567",
									"defaultCountry": "nl",
								},
							},
						},
					},
				},
				"phone": map[string]interface{}{
					"input":          "020 1234567",
					"defaultCountry": "nl",
				},
				"phone_wannabes": []interface{}{
					map[string]interface{}{
						"input":          "020 1234567",
						"defaultCountry": "nl",
					},
				},
			},
			expectedProperties: []*models.Property{
				{
					Name:     "name",
					DataType: schema.DataTypeText.PropString(),
				},
				{
					Name:     "objectProperty",
					DataType: schema.DataTypeObject.PropString(),
					NestedProperties: []*models.NestedProperty{
						{
							Name:     "nested_phone_wannabe",
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
							Name:     "nested_phone_wannabes",
							DataType: schema.DataTypeObjectArray.PropString(),
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
							Name:     "nested_objects",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{
									Name:     "nested_phone_wannabe_lvl2",
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
									Name:     "nested_phone_wannabes_lvl2",
									DataType: schema.DataTypeObjectArray.PropString(),
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
							},
						},
					},
				},
				{
					Name:     "phone",
					DataType: schema.DataTypePhoneNumber.PropString(),
				},
				{
					Name:     "phone_wannabes",
					DataType: schema.DataTypeObjectArray.PropString(),
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
			},
		},
		{
			name: "geo",
			valProperties: map[string]interface{}{
				"name": "someName",
				"objectProperty": map[string]interface{}{
					"nested_geo_wannabe": map[string]interface{}{
						"latitude":  json.Number("1.1"),
						"longitude": json.Number("2.2"),
					},
					"nested_geo_wannabes": []interface{}{
						map[string]interface{}{
							"latitude":  json.Number("1.1"),
							"longitude": json.Number("2.2"),
						},
					},
					"nested_objects": []interface{}{
						map[string]interface{}{
							"nested_geo_wannabe_lvl2": map[string]interface{}{
								"latitude":  json.Number("1.1"),
								"longitude": json.Number("2.2"),
							},
							"nested_geo_wannabes_lvl2": []interface{}{
								map[string]interface{}{
									"latitude":  json.Number("1.1"),
									"longitude": json.Number("2.2"),
								},
							},
						},
					},
				},
				"geo": map[string]interface{}{
					"latitude":  json.Number("1.1"),
					"longitude": json.Number("2.2"),
				},
				"geo_wannabes": []interface{}{
					map[string]interface{}{
						"latitude":  json.Number("1.1"),
						"longitude": json.Number("2.2"),
					},
				},
			},
			expectedProperties: []*models.Property{
				{
					Name:     "name",
					DataType: schema.DataTypeText.PropString(),
				},
				{
					Name:     "objectProperty",
					DataType: schema.DataTypeObject.PropString(),
					NestedProperties: []*models.NestedProperty{
						{
							Name:     "nested_geo_wannabe",
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
							Name:     "nested_geo_wannabes",
							DataType: schema.DataTypeObjectArray.PropString(),
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
							Name:     "nested_objects",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{
									Name:     "nested_geo_wannabe_lvl2",
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
									Name:     "nested_geo_wannabes_lvl2",
									DataType: schema.DataTypeObjectArray.PropString(),
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
							},
						},
					},
				},
				{
					Name:     "geo",
					DataType: schema.DataTypeGeoCoordinates.PropString(),
				},
				{
					Name:     "geo_wannabes",
					DataType: schema.DataTypeObjectArray.PropString(),
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
			},
		},
	}

	manager := &autoSchemaManager{
		schemaManager: &fakeSchemaManager{},
		vectorRepo:    &fakeVectorRepo{},
		config: config.AutoSchema{
			Enabled:       true,
			DefaultNumber: schema.DataTypeNumber.String(),
			DefaultString: schema.DataTypeText.String(),
			DefaultDate:   schema.DataTypeDate.String(),
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("testCase_%d", i), func(t *testing.T) {
			properties, _ := manager.getProperties(&models.Object{
				Class:      "ClassWithObjectProps",
				Properties: tc.valProperties,
			})

			assertPropsMatch(t, tc.expectedProperties, properties)
		})
	}
}

func Test_autoSchemaManager_perform_withNested(t *testing.T) {
	logger, _ := test.NewNullLogger()
	className := "ClassWithObjectProps"

	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
			{
				Name:     "objectProperty",
				DataType: schema.DataTypeObject.PropString(),
				NestedProperties: []*models.NestedProperty{
					{
						Name:     "nested_int",
						DataType: schema.DataTypeNumber.PropString(),
					},
					{
						Name:     "nested_text",
						DataType: schema.DataTypeText.PropString(),
					},
					{
						Name:     "nested_objects",
						DataType: schema.DataTypeObjectArray.PropString(),
						NestedProperties: []*models.NestedProperty{
							{
								Name:     "nested_bool_lvl2",
								DataType: schema.DataTypeBoolean.PropString(),
							},
							{
								Name:     "nested_numbers_lvl2",
								DataType: schema.DataTypeNumberArray.PropString(),
							},
						},
					},
				},
			},
		},
	}
	object := &models.Object{
		Class: className,
		Properties: map[string]interface{}{
			"name": "someName",
			"objectProperty": map[string]interface{}{
				"nested_number": json.Number("123"),
				"nested_text":   "some text",
				"nested_objects": []interface{}{
					map[string]interface{}{
						"nested_date_lvl2": "2022-01-01T00:00:00+02:00",
						"nested_numbers_lvl2": []interface{}{
							json.Number("11.11"),
						},
						"nested_phone_wannabe_lvl2": map[string]interface{}{
							"input":          "020 1234567",
							"defaultCountry": "nl",
						},
						"nested_phone_wannabes_lvl2": []interface{}{
							map[string]interface{}{
								"input":          "020 1234567",
								"defaultCountry": "nl",
							},
						},
					},
				},
				"nested_phone_wannabe": map[string]interface{}{
					"input":          "020 1234567",
					"defaultCountry": "nl",
				},
				"nested_phone_wannabes": []interface{}{
					map[string]interface{}{
						"input":          "020 1234567",
						"defaultCountry": "nl",
					},
				},
			},
			"phone": map[string]interface{}{
				"input":          "020 1234567",
				"defaultCountry": "nl",
			},
			"phone_wannabes": []interface{}{
				map[string]interface{}{
					"input":          "020 1234567",
					"defaultCountry": "nl",
				},
			},
			"objectPropertyGeo": map[string]interface{}{
				"nested_objects": []interface{}{
					map[string]interface{}{
						"nested_geo_wannabe_lvl2": map[string]interface{}{
							"latitude":  json.Number("1.1"),
							"longitude": json.Number("2.2"),
						},
						"nested_geo_wannabes_lvl2": []interface{}{
							map[string]interface{}{
								"latitude":  json.Number("1.1"),
								"longitude": json.Number("2.2"),
							},
						},
					},
				},
				"nested_geo_wannabe": map[string]interface{}{
					"latitude":  json.Number("1.1"),
					"longitude": json.Number("2.2"),
				},
				"nested_geo_wannabes": []interface{}{
					map[string]interface{}{
						"latitude":  json.Number("1.1"),
						"longitude": json.Number("2.2"),
					},
				},
			},
			"geo": map[string]interface{}{
				"latitude":  json.Number("1.1"),
				"longitude": json.Number("2.2"),
			},
			"geo_wannabes": []interface{}{
				map[string]interface{}{
					"latitude":  json.Number("1.1"),
					"longitude": json.Number("2.2"),
				},
			},
		},
	}
	expectedClass := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
			{
				Name:     "objectProperty",
				DataType: schema.DataTypeObject.PropString(),
				NestedProperties: []*models.NestedProperty{
					{
						Name:     "nested_int",
						DataType: schema.DataTypeNumber.PropString(),
					},
					{
						Name:     "nested_number",
						DataType: schema.DataTypeNumber.PropString(),
					},
					{
						Name:     "nested_text",
						DataType: schema.DataTypeText.PropString(),
					},
					{
						Name:     "nested_phone_wannabe",
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
						Name:     "nested_phone_wannabes",
						DataType: schema.DataTypeObjectArray.PropString(),
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
						Name:     "nested_objects",
						DataType: schema.DataTypeObjectArray.PropString(),
						NestedProperties: []*models.NestedProperty{
							{
								Name:     "nested_bool_lvl2",
								DataType: schema.DataTypeBoolean.PropString(),
							},
							{
								Name:     "nested_date_lvl2",
								DataType: schema.DataTypeDate.PropString(),
							},
							{
								Name:     "nested_numbers_lvl2",
								DataType: schema.DataTypeNumberArray.PropString(),
							},
							{
								Name:     "nested_phone_wannabe_lvl2",
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
								Name:     "nested_phone_wannabes_lvl2",
								DataType: schema.DataTypeObjectArray.PropString(),
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
						},
					},
				},
			},
			{
				Name:     "phone",
				DataType: schema.DataTypePhoneNumber.PropString(),
			},
			{
				Name:     "phone_wannabes",
				DataType: schema.DataTypeObjectArray.PropString(),
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
				Name:     "objectPropertyGeo",
				DataType: schema.DataTypeObject.PropString(),
				NestedProperties: []*models.NestedProperty{
					{
						Name:     "nested_geo_wannabe",
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
						Name:     "nested_geo_wannabes",
						DataType: schema.DataTypeObjectArray.PropString(),
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
						Name:     "nested_objects",
						DataType: schema.DataTypeObjectArray.PropString(),
						NestedProperties: []*models.NestedProperty{
							{
								Name:     "nested_geo_wannabe_lvl2",
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
								Name:     "nested_geo_wannabes_lvl2",
								DataType: schema.DataTypeObjectArray.PropString(),
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
						},
					},
				},
			},
			{
				Name:     "geo",
				DataType: schema.DataTypeGeoCoordinates.PropString(),
			},
			{
				Name:     "geo_wannabes",
				DataType: schema.DataTypeObjectArray.PropString(),
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
		},
	}

	schemaManager := &fakeSchemaManager{
		GetSchemaResponse: schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{class},
			},
		},
	}
	manager := &autoSchemaManager{
		schemaManager: schemaManager,
		vectorRepo:    &fakeVectorRepo{},
		config: config.AutoSchema{
			Enabled:       true,
			DefaultNumber: schema.DataTypeNumber.String(),
			DefaultString: schema.DataTypeText.String(),
			DefaultDate:   schema.DataTypeDate.String(),
		},
		logger: logger,
	}

	err := manager.autoSchema(context.Background(), &models.Principal{}, object, true)
	require.NoError(t, err)

	schemaAfter := schemaManager.GetSchemaResponse
	require.NotNil(t, schemaAfter.Objects)
	require.Len(t, schemaAfter.Objects.Classes, 1)
	require.Equal(t, className, schemaAfter.Objects.Classes[0].Class)

	assertPropsMatch(t, expectedClass.Properties, schemaAfter.Objects.Classes[0].Properties)
}

func getProperty(properties []*models.Property, name string) *models.Property {
	for _, prop := range properties {
		if prop.Name == name {
			return prop
		}
	}
	return nil
}

func assertPropsMatch(t *testing.T, propsA, propsB []*models.Property) {
	require.Len(t, propsB, len(propsA), "props: different length")

	pMap := map[string]int{}
	for index, p := range propsA {
		pMap[p.Name] = index
	}

	for _, pB := range propsB {
		require.Contains(t, pMap, pB.Name)
		pA := propsA[pMap[pB.Name]]

		assert.Equal(t, pA.DataType, pB.DataType)
		test_utils.AssertNestedPropsMatch(t, pA.NestedProperties, pB.NestedProperties)
	}
}
