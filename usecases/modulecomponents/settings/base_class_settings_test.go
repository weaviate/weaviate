//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package settings

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/modules"
)

func Test_BaseClassSettings(t *testing.T) {
	targetVector := "targetVector"
	propertyToIndex := "someProp"
	class := &models.Class{
		Class: "MyClass",
		VectorConfig: map[string]models.VectorConfig{
			targetVector: {
				Vectorizer: map[string]any{
					"my-module": map[string]any{
						"vectorizeClassName": false,
						"properties":         []any{propertyToIndex},
					},
				},
				VectorIndexType: "hnsw",
			},
		},
		Properties: []*models.Property{
			{
				Name: propertyToIndex,
				ModuleConfig: map[string]any{
					"my-module": map[string]any{
						"skip":                  true,
						"vectorizePropertyName": true,
					},
				},
			},
			{
				Name: "otherProp",
			},
		},
	}

	cfg := modules.NewClassBasedModuleConfig(class, "my-module", "tenant", targetVector, nil)
	ic := NewBaseClassSettings(cfg, false)

	assert.True(t, ic.PropertyIndexed(propertyToIndex))
	assert.True(t, ic.VectorizePropertyName(propertyToIndex))
	assert.False(t, ic.PropertyIndexed("otherProp"))
	assert.False(t, ic.VectorizePropertyName("otherProp"))
	assert.False(t, ic.VectorizeClassName())
}

func Test_BaseClassSettings_ValidateClassSettings(t *testing.T) {
	targetVector := "targetVector"
	getClass := func(moduleSettings map[string]any) *models.Class {
		settings := map[string]any{
			"vectorizeClassName": false,
		}
		for k, v := range moduleSettings {
			settings[k] = v
		}
		return &models.Class{
			Class: "MyClass",
			VectorConfig: map[string]models.VectorConfig{
				targetVector: {
					Vectorizer: map[string]any{
						"my-module": settings,
					},
					VectorIndexType: "hnsw",
				},
			},
			Properties: []*models.Property{
				{
					Name: "prop1",
				},
				{
					Name: "otherProp",
				},
			},
		}
	}
	tests := []struct {
		name     string
		settings map[string]any
		wantErr  error
	}{
		{
			name:     "without properties",
			settings: nil,
			wantErr:  nil,
		},
		{
			name: "proper properties",
			settings: map[string]any{
				"properties": []any{"prop1"},
			},
			wantErr: nil,
		},
		{
			name: "nil properties",
			settings: map[string]any{
				"properties": nil,
			},
			wantErr: errors.New("properties field needs to be of array type, got: <nil>"),
		},
		{
			name: "at least 1 property",
			settings: map[string]any{
				"properties": []any{},
			},
			wantErr: errors.New("properties field needs to have at least 1 property defined"),
		},
		{
			name: "at least 1 property with []string",
			settings: map[string]any{
				"properties": []string{},
			},
			wantErr: errors.New("properties field needs to have at least 1 property defined"),
		},
		{
			name: "must be an array",
			settings: map[string]any{
				"properties": "string",
			},
			wantErr: errors.New("properties field needs to be of array type, got: string"),
		},
		{
			name: "properties values need to be string",
			settings: map[string]any{
				"properties": []any{"string", 1},
			},
			wantErr: errors.New("properties field value: 1 must be a string"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			class := getClass(tt.settings)
			cfg := modules.NewClassBasedModuleConfig(class, "my-module", "tenant", targetVector, nil)
			s := NewBaseClassSettings(cfg, false)
			err := s.ValidateClassSettings()
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.Equal(t, err.Error(), tt.wantErr.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func Test_BaseClassSettings_Validate(t *testing.T) {
	getDBConfig := func(autoSchemaEnabled bool) *config.Config {
		var enabled *runtime.DynamicValue[bool]
		if autoSchemaEnabled {
			enabled = runtime.NewDynamicValue(autoSchemaEnabled)
		}
		cfg := config.Config{AutoSchema: config.AutoSchema{Enabled: enabled}}
		return &cfg
	}
	classNoProperties := func() *models.Class {
		return &models.Class{
			Class: "MyClass",
			VectorConfig: map[string]models.VectorConfig{
				"targetVector": {
					Vectorizer: map[string]any{
						"my-module": map[string]any{
							"vectorizeClassName": false,
						},
					},
				},
			},
		}
	}
	classOnlySourceProperties := func() *models.Class {
		return &models.Class{
			Class: "MyClass",
			VectorConfig: map[string]models.VectorConfig{
				"targetVector": {
					Vectorizer: map[string]any{
						"my-module": map[string]any{
							"vectorizeClassName": false,
							"properties":         []string{"property_should_be_created_autoschema"},
						},
					},
				},
			},
		}
	}
	classPropertiesAndSourceProperties := func() *models.Class {
		return &models.Class{
			Class: "MyClass",
			VectorConfig: map[string]models.VectorConfig{
				"targetVector": {
					Vectorizer: map[string]any{
						"my-module": map[string]any{
							"vectorizeClassName": false,
							"properties":         []string{"predefined_property"},
						},
					},
				},
			},
			Properties: []*models.Property{
				{
					Name:     "predefined_property",
					DataType: []string{schema.DataTypeText.String()},
				},
				{
					Name:     "otherProp",
					DataType: []string{schema.DataTypeText.String()},
				},
			},
		}
	}
	classPropertiesAndNonExistentSourceProperties := func() *models.Class {
		return &models.Class{
			Class: "MyClass",
			VectorConfig: map[string]models.VectorConfig{
				"targetVector": {
					Vectorizer: map[string]any{
						"my-module": map[string]any{
							"vectorizeClassName": false,
							"properties":         []string{"nonexistent_property"},
						},
					},
				},
			},
			Properties: []*models.Property{
				{
					Name:     "predefined_property",
					DataType: []string{schema.DataTypeText.String()},
				},
				{
					Name:     "otherProp",
					DataType: []string{schema.DataTypeText.String()},
				},
			},
		}
	}
	classAllVectorizablePropertiesAndSourceProperties := func() *models.Class {
		return &models.Class{
			Class: "MyClass",
			VectorConfig: map[string]models.VectorConfig{
				"targetVector": {
					Vectorizer: map[string]interface{}{
						"my-module": map[string]interface{}{
							"vectorizeClassName": false,
							"properties": []string{
								"text_prop", "text_array_prop", "string_prop", "string_array_prop",
								"object_prop", "object_array_prop",
								"int_prop", "int_array_prop",
								"number_prop", "number_array_prop",
								"date_prop", "date_array_prop",
								"boolean_prop", "boolean_array_prop",
							},
						},
					},
				},
			},
			Properties: []*models.Property{
				{
					Name:     "text_prop",
					DataType: []string{schema.DataTypeText.String()},
				},
				{
					Name:     "text_array_prop",
					DataType: []string{schema.DataTypeTextArray.String()},
				},
				{
					Name:     "string_prop",
					DataType: []string{schema.DataTypeString.String()},
				},
				{
					Name:     "string_array_prop",
					DataType: []string{schema.DataTypeStringArray.String()},
				},
				{
					Name:     "object_prop",
					DataType: []string{schema.DataTypeObject.String()},
				},
				{
					Name:     "object_array_prop",
					DataType: []string{schema.DataTypeObjectArray.String()},
				},
				{
					Name:     "int_prop",
					DataType: []string{schema.DataTypeInt.String()},
				},
				{
					Name:     "int_array_prop",
					DataType: []string{schema.DataTypeIntArray.String()},
				},
				{
					Name:     "number_prop",
					DataType: []string{schema.DataTypeNumber.String()},
				},
				{
					Name:     "number_array_prop",
					DataType: []string{schema.DataTypeNumberArray.String()},
				},
				{
					Name:     "date_prop",
					DataType: []string{schema.DataTypeDate.String()},
				},
				{
					Name:     "date_array_prop",
					DataType: []string{schema.DataTypeDateArray.String()},
				},
				{
					Name:     "boolean_prop",
					DataType: []string{schema.DataTypeBoolean.String()},
				},
				{
					Name:     "boolean_array_prop",
					DataType: []string{schema.DataTypeBooleanArray.String()},
				},
			},
		}
	}
	tests := []struct {
		name              string
		class             *models.Class
		autoSchemaEnabled bool
		wantErr           error
	}{
		{
			name:              "class with no properties, auto schema enabled",
			class:             classNoProperties(),
			autoSchemaEnabled: true,
		},
		{
			name:              "class with no properties, auto schema disabled",
			class:             classNoProperties(),
			autoSchemaEnabled: false,
			wantErr:           errInvalidProperties,
		},
		{
			name:              "class with only source properties, auto schema enabled",
			class:             classOnlySourceProperties(),
			autoSchemaEnabled: true,
		},
		{
			name:              "class with only source properties, auto schema disabled",
			class:             classOnlySourceProperties(),
			autoSchemaEnabled: false,
			wantErr:           errInvalidProperties,
		},
		{
			name:              "class with properties and source properties, auto schema enabled",
			class:             classPropertiesAndSourceProperties(),
			autoSchemaEnabled: true,
		},
		{
			name:              "class with properties and source properties, auto schema disabled",
			class:             classPropertiesAndSourceProperties(),
			autoSchemaEnabled: false,
		},
		{
			name:              "class with properties and non existent source properties, auto schema enabled",
			class:             classPropertiesAndNonExistentSourceProperties(),
			autoSchemaEnabled: true,
		},
		{
			name:              "class with properties and non existent source properties, auto schema disabled",
			class:             classPropertiesAndNonExistentSourceProperties(),
			autoSchemaEnabled: false,
			wantErr:           errInvalidProperties,
		},
		{
			name:              "class with all vectorizable property types",
			class:             classAllVectorizablePropertiesAndSourceProperties(),
			autoSchemaEnabled: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := modules.NewClassBasedModuleConfig(tt.class, "my-module", "tenant", "targetVector", getDBConfig(tt.autoSchemaEnabled))
			ic := NewBaseClassSettings(cfg, false)
			err := ic.Validate(tt.class)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.wantErr.Error())
			} else {
				assert.Nil(t, err)
			}
		})
	}
}
