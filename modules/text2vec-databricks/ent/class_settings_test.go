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

package ent

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
			name: "user supplied serving url",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"endpoint": "https://foo.databricks.com/serving-endpoints/databricks-gte-large-en/invocations",
				},
			},
		},
		{
			name: "user did not supply serving url",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{},
			},
			wantErr: errors.New("endpoint cannot be empty"),
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
