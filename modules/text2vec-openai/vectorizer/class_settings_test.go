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

package vectorizer

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/modules"
)

func Test_classSettings_Validate(t *testing.T) {
	class := &models.Class{
		Class: "test",
		Properties: []*models.Property{
			{
				DataType: []string{schema.DataTypeText.String()},
				Name:     "test",
			},
		},
	}
	tests := []struct {
		name    string
		cfg     moduletools.ClassConfig
		wantErr error
	}{
		{
			name: "text-embedding-3-small",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "text-embedding-3-small",
				},
			},
		},
		{
			name: "text-embedding-3-small, 512 dimensions",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":      "text-embedding-3-small",
					"dimensions": 512,
				},
			},
		},
		{
			name: "text-embedding-3-small, wrong dimensions",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":      "text-embedding-3-small",
					"dimensions": 1,
				},
			},
			wantErr: errors.New("wrong dimensions setting for text-embedding-3-small model, available dimensions are: [512 1536]"),
		},
		{
			name: "text-embedding-3-large",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "text-embedding-3-large",
				},
			},
		},
		{
			name: "text-embedding-3-large, 512 dimensions",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":      "text-embedding-3-large",
					"dimensions": 1024,
				},
			},
		},
		{
			name: "text-embedding-3-large, wrong dimensions",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":      "text-embedding-3-large",
					"dimensions": 512,
				},
			},
			wantErr: errors.New("wrong dimensions setting for text-embedding-3-large model, available dimensions are: [256 1024 3072]"),
		},
		{
			name: "text-embedding-ada-002",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":        "ada",
					"modelVersion": "002",
				},
			},
		},
		{
			name: "text-embedding-ada-002 - dimensions error",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":      "ada",
					"dimensions": 512,
				},
			},
			wantErr: errors.New("dimensions setting can only be used with V3 embedding models: [text-embedding-3-small text-embedding-3-large]"),
		},
		{
			name: "text-embedding-ada-002 - wrong model version",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":        "ada",
					"modelVersion": "003",
				},
			},
			wantErr: errors.New("unsupported version 003"),
		},
		{
			name: "wrong model name",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "unknown-model",
				},
			},
			wantErr: errors.New("wrong OpenAI model name, available model names are: [ada babbage curie davinci text-embedding-3-small text-embedding-3-large]"),
		},
		{
			name: "wrong properties",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":      "text-embedding-3-large",
					"properties": "wrong-properties",
				},
			},
			wantErr: errors.New("properties field needs to be of array type, got: string"),
		},
		{
			name: "wrong apiVersion",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"resourceName": "resource",
					"deploymentId": "deploymentId",
					"apiVersion":   "wrong-api-version",
				},
			},
			wantErr: errors.New("wrong Azure OpenAI apiVersion setting, available api versions are: " +
				"[2022-12-01 2023-03-15-preview 2023-05-15 2023-06-01-preview 2023-07-01-preview 2023-08-01-preview " +
				"2023-09-01-preview 2023-12-01-preview 2024-02-15-preview 2024-03-01-preview 2024-02-01]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := NewClassSettings(tt.cfg)
			err := cs.Validate(class)
			if tt.wantErr != nil {
				assert.EqualError(t, err, tt.wantErr.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func Test_classSettings(t *testing.T) {
	t.Run("with target vector and properties", func(t *testing.T) {
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
		ic := NewClassSettings(cfg)

		assert.True(t, ic.PropertyIndexed(propertyToIndex))
		assert.True(t, ic.VectorizePropertyName(propertyToIndex))
		assert.False(t, ic.PropertyIndexed("otherProp"))
		assert.False(t, ic.VectorizePropertyName("otherProp"))
		assert.False(t, ic.VectorizeClassName())
	})
}
