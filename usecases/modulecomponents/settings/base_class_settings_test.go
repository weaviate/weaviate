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

package settings

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/modules"
)

func Test_BaseClassSettings(t *testing.T) {
	targetVector := "targetVector"
	propertyToIndex := "someProp"
	class := &models.Class{
		Class: "MyClass",
		VectorConfig: map[string]models.VectorConfig{
			targetVector: {
				Vectorizer: map[string]interface{}{
					"my-module": map[string]interface{}{
						"vectorizeClassName": false,
						"properties":         []interface{}{propertyToIndex},
					},
				},
				VectorIndexType: "hnsw",
			},
		},
		Properties: []*models.Property{
			{
				Name: propertyToIndex,
				ModuleConfig: map[string]interface{}{
					"my-module": map[string]interface{}{
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

	cfg := modules.NewClassBasedModuleConfig(class, "my-module", "tenant", targetVector)
	ic := NewBaseClassSettings(cfg, false)

	assert.True(t, ic.PropertyIndexed(propertyToIndex))
	assert.True(t, ic.VectorizePropertyName(propertyToIndex))
	assert.False(t, ic.PropertyIndexed("otherProp"))
	assert.False(t, ic.VectorizePropertyName("otherProp"))
	assert.False(t, ic.VectorizeClassName())
}

func Test_BaseClassSettings_Validate(t *testing.T) {
	targetVector := "targetVector"
	getClass := func(moduleSettings map[string]interface{}) *models.Class {
		settings := map[string]interface{}{
			"vectorizeClassName": false,
		}
		for k, v := range moduleSettings {
			settings[k] = v
		}
		return &models.Class{
			Class: "MyClass",
			VectorConfig: map[string]models.VectorConfig{
				targetVector: {
					Vectorizer: map[string]interface{}{
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
		settings map[string]interface{}
		wantErr  error
	}{
		{
			name:     "without properties",
			settings: nil,
			wantErr:  nil,
		},
		{
			name: "proper properties",
			settings: map[string]interface{}{
				"properties": []interface{}{"prop1"},
			},
			wantErr: nil,
		},
		{
			name: "nil properties",
			settings: map[string]interface{}{
				"properties": nil,
			},
			wantErr: errors.New("properties field needs to be of array type, got: <nil>"),
		},
		{
			name: "at least 1 property",
			settings: map[string]interface{}{
				"properties": []interface{}{},
			},
			wantErr: errors.New("properties field needs to have at least 1 property defined"),
		},
		{
			name: "at least 1 property with []string",
			settings: map[string]interface{}{
				"properties": []string{},
			},
			wantErr: errors.New("properties field needs to have at least 1 property defined"),
		},
		{
			name: "must be an array",
			settings: map[string]interface{}{
				"properties": "string",
			},
			wantErr: errors.New("properties field needs to be of array type, got: string"),
		},
		{
			name: "properties values need to be string",
			settings: map[string]interface{}{
				"properties": []interface{}{"string", 1},
			},
			wantErr: errors.New("properties field value: 1 must be a string"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			class := getClass(tt.settings)
			cfg := modules.NewClassBasedModuleConfig(class, "my-module", "tenant", targetVector)
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
