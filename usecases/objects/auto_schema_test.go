//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package objects

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func Test_autoSchemaManager_determineType(t *testing.T) {
	type fields struct {
		config config.AutoSchema
	}
	type args struct {
		value interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []schema.DataType
	}{
		{
			name: "determine string",
			fields: fields{
				config: config.AutoSchema{
					Enabled:       true,
					DefaultString: "string",
				},
			},
			args: args{
				value: "string",
			},
			want: []schema.DataType{schema.DataTypeString},
		},
		{
			name: "determine text",
			fields: fields{
				config: config.AutoSchema{
					Enabled:       true,
					DefaultString: "text",
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
					"input":                  "020 1234567",
					"internationalFormatted": "+31 20 1234567",
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
			if got := m.determineType(tt.args.value); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("autoSchemaManager.determineType() = %v, want %v", got, tt.want)
			}
		})
	}
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
			DefaultString: "string",
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
		},
	}
	// when
	schemaBefore := schemaManager.GetSchemaResponse
	err := autoSchemaManager.autoSchema(context.Background(), &models.Principal{}, obj)
	schemaAfter := schemaManager.GetSchemaResponse

	// then
	require.Nil(t, schemaBefore.Objects)
	require.Nil(t, err)
	require.NotNil(t, schemaAfter.Objects)
	assert.Equal(t, 1, len(schemaAfter.Objects.Classes))
	assert.Equal(t, "Publication", (schemaAfter.Objects.Classes)[0].Class)
	assert.Equal(t, 3, len((schemaAfter.Objects.Classes)[0].Properties))
	require.NotNil(t, getProperty((schemaAfter.Objects.Classes)[0].Properties, "name"))
	assert.Equal(t, "name", getProperty((schemaAfter.Objects.Classes)[0].Properties, "name").Name)
	assert.Equal(t, "string", getProperty((schemaAfter.Objects.Classes)[0].Properties, "name").DataType[0])
	require.NotNil(t, getProperty((schemaAfter.Objects.Classes)[0].Properties, "age"))
	assert.Equal(t, "age", getProperty((schemaAfter.Objects.Classes)[0].Properties, "age").Name)
	assert.Equal(t, "number", getProperty((schemaAfter.Objects.Classes)[0].Properties, "age").DataType[0])
	require.NotNil(t, getProperty((schemaAfter.Objects.Classes)[0].Properties, "publicationDate"))
	assert.Equal(t, "publicationDate", getProperty((schemaAfter.Objects.Classes)[0].Properties, "publicationDate").Name)
	assert.Equal(t, "date", getProperty((schemaAfter.Objects.Classes)[0].Properties, "publicationDate").DataType[0])
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
			DefaultString: "string",
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

	err := autoSchemaManager.autoSchema(context.Background(), &models.Principal{}, obj)
	require.Nil(t, err)

	schemaAfter := schemaManager.GetSchemaResponse
	require.NotNil(t, schemaAfter.Objects)
	assert.Equal(t, 1, len(schemaAfter.Objects.Classes))
	assert.Equal(t, "Publication", (schemaAfter.Objects.Classes)[0].Class)
	assert.Equal(t, 3, len((schemaAfter.Objects.Classes)[0].Properties))
	require.NotNil(t, getProperty((schemaAfter.Objects.Classes)[0].Properties, "age"))
	assert.Equal(t, "age", getProperty((schemaAfter.Objects.Classes)[0].Properties, "age").Name)
	assert.Equal(t, "int", getProperty((schemaAfter.Objects.Classes)[0].Properties, "age").DataType[0])
	require.NotNil(t, getProperty((schemaAfter.Objects.Classes)[0].Properties, "name"))
	assert.Equal(t, "name", getProperty((schemaAfter.Objects.Classes)[0].Properties, "name").Name)
	assert.Equal(t, "string", getProperty((schemaAfter.Objects.Classes)[0].Properties, "name").DataType[0])
	require.NotNil(t, getProperty((schemaAfter.Objects.Classes)[0].Properties, "publicationDate"))
	assert.Equal(t, "publicationDate", getProperty((schemaAfter.Objects.Classes)[0].Properties, "publicationDate").Name)
	assert.Equal(t, "date", getProperty((schemaAfter.Objects.Classes)[0].Properties, "publicationDate").DataType[0])
}

func getProperty(properties []*models.Property, name string) *models.Property {
	for _, prop := range properties {
		if prop.Name == name {
			return prop
		}
	}
	return nil
}
