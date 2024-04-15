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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/modules"
)

func TestClassSettings(t *testing.T) {
	t.Run("with all defaults", func(t *testing.T) {
		class := &models.Class{
			Class: "MyClass",
			Properties: []*models.Property{{
				Name: "someProp",
			}},
		}

		cfg := modules.NewClassBasedModuleConfig(class, "my-module", "tenant", "")
		ic := NewClassSettings(cfg)

		assert.True(t, ic.PropertyIndexed("someProp"))
		assert.False(t, ic.VectorizePropertyName("someProp"))
		assert.True(t, ic.VectorizeClassName())
		assert.Equal(t, ic.PoolingStrategy(), "masked_mean")
	})

	t.Run("with a nil config", func(t *testing.T) {
		// this is the case if we were running in a situation such as a
		// cross-class vectorization of search time, as is the case with Explore
		// {}, we then expect all default values

		ic := NewClassSettings(nil)

		assert.True(t, ic.PropertyIndexed("someProp"))
		assert.False(t, ic.VectorizePropertyName("someProp"))
		assert.True(t, ic.VectorizeClassName())
		assert.Equal(t, ic.PoolingStrategy(), "masked_mean")
	})

	t.Run("with all explicit config matching the defaults", func(t *testing.T) {
		class := &models.Class{
			Class: "MyClass",
			ModuleConfig: map[string]interface{}{
				"my-module": map[string]interface{}{
					"vectorizeClassName": true,
					"poolingStrategy":    "masked_mean",
				},
			},
			Properties: []*models.Property{{
				Name: "someProp",
				ModuleConfig: map[string]interface{}{
					"my-module": map[string]interface{}{
						"skip":                  false,
						"vectorizePropertyName": false,
					},
				},
			}},
		}

		cfg := modules.NewClassBasedModuleConfig(class, "my-module", "tenant", "")
		ic := NewClassSettings(cfg)

		assert.True(t, ic.PropertyIndexed("someProp"))
		assert.False(t, ic.VectorizePropertyName("someProp"))
		assert.True(t, ic.VectorizeClassName())
		assert.Equal(t, ic.PoolingStrategy(), "masked_mean")
	})

	t.Run("with all explicit config using non-default values", func(t *testing.T) {
		class := &models.Class{
			Class: "MyClass",
			ModuleConfig: map[string]interface{}{
				"my-module": map[string]interface{}{
					"vectorizeClassName": false,
					"poolingStrategy":    "cls",
				},
			},
			Properties: []*models.Property{{
				Name: "someProp",
				ModuleConfig: map[string]interface{}{
					"my-module": map[string]interface{}{
						"skip":                  true,
						"vectorizePropertyName": true,
					},
				},
			}},
		}

		cfg := modules.NewClassBasedModuleConfig(class, "my-module", "tenant", "")
		ic := NewClassSettings(cfg)

		assert.False(t, ic.PropertyIndexed("someProp"))
		assert.True(t, ic.VectorizePropertyName("someProp"))
		assert.False(t, ic.VectorizeClassName())
		assert.Equal(t, ic.PoolingStrategy(), "cls")
	})

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

	t.Run("with inferenceUrl setting", func(t *testing.T) {
		class := &models.Class{
			Class: "MyClass",
			VectorConfig: map[string]models.VectorConfig{
				"withInferenceUrl": {
					Vectorizer: map[string]interface{}{
						"my-module": map[string]interface{}{
							"vectorizeClassName": false,
							"poolingStrategy":    "cls",
							"inferenceUrl":       "http://inference.url",
						},
					},
				},
				"withPassageAndQueryInferenceUrl": {
					Vectorizer: map[string]interface{}{
						"my-module": map[string]interface{}{
							"vectorizeClassName":  false,
							"poolingStrategy":     "cls",
							"passageInferenceUrl": "http://passage.inference.url",
							"queryInferenceUrl":   "http://query.inference.url",
						},
					},
				},
			},

			Properties: []*models.Property{{
				Name: "someProp",
				ModuleConfig: map[string]interface{}{
					"my-module": map[string]interface{}{
						"skip":                  true,
						"vectorizePropertyName": true,
					},
				},
			}},
		}

		cfg := modules.NewClassBasedModuleConfig(class, "my-module", "tenant", "withInferenceUrl")
		ic := NewClassSettings(cfg)

		assert.False(t, ic.PropertyIndexed("someProp"))
		assert.True(t, ic.VectorizePropertyName("someProp"))
		assert.False(t, ic.VectorizeClassName())
		assert.Equal(t, ic.PoolingStrategy(), "cls")
		assert.Equal(t, ic.InferenceURL(), "http://inference.url")
		assert.Empty(t, ic.PassageInferenceURL())
		assert.Empty(t, ic.QueryInferenceURL())

		cfg = modules.NewClassBasedModuleConfig(class, "my-module", "tenant", "withPassageAndQueryInferenceUrl")
		ic = NewClassSettings(cfg)

		assert.False(t, ic.PropertyIndexed("someProp"))
		assert.True(t, ic.VectorizePropertyName("someProp"))
		assert.False(t, ic.VectorizeClassName())
		assert.Equal(t, ic.PoolingStrategy(), "cls")
		assert.Empty(t, ic.InferenceURL())
		assert.Equal(t, ic.PassageInferenceURL(), "http://passage.inference.url")
		assert.Equal(t, ic.QueryInferenceURL(), "http://query.inference.url")
	})
}

func Test_classSettings_Validate(t *testing.T) {
	tests := []struct {
		name       string
		vectorizer map[string]interface{}
		wantErr    error
	}{
		{
			name: "only inference url",
			vectorizer: map[string]interface{}{
				"vectorizeClassName": false,
				"poolingStrategy":    "cls",
				"inferenceUrl":       "http://inference.url",
			},
		},
		{
			name: "only passage and query inference urls",
			vectorizer: map[string]interface{}{
				"vectorizeClassName":  false,
				"poolingStrategy":     "cls",
				"passageInferenceUrl": "http://passage.inference.url",
				"queryInferenceUrl":   "http://query.inference.url",
			},
		},
		{
			name: "error - all inference urls",
			vectorizer: map[string]interface{}{
				"vectorizeClassName":  false,
				"poolingStrategy":     "cls",
				"inferenceUrl":        "http://inference.url",
				"passageInferenceUrl": "http://passage.inference.url",
				"queryInferenceUrl":   "http://query.inference.url",
			},
			wantErr: errors.New("either inferenceUrl or passageInferenceUrl together with queryInferenceUrl needs to be set, not both"),
		},
		{
			name: "error - all inference urls, without passage",
			vectorizer: map[string]interface{}{
				"vectorizeClassName": false,
				"poolingStrategy":    "cls",
				"inferenceUrl":       "http://inference.url",
				"queryInferenceUrl":  "http://query.inference.url",
			},
			wantErr: errors.New("either inferenceUrl or passageInferenceUrl together with queryInferenceUrl needs to be set, not both"),
		},
		{
			name: "error - all inference urls, without query",
			vectorizer: map[string]interface{}{
				"vectorizeClassName":  false,
				"poolingStrategy":     "cls",
				"inferenceUrl":        "http://inference.url",
				"passageInferenceUrl": "http://passage.inference.url",
			},
			wantErr: errors.New("either inferenceUrl or passageInferenceUrl together with queryInferenceUrl needs to be set, not both"),
		},
		{
			name: "error - passage inference url set but not query",
			vectorizer: map[string]interface{}{
				"vectorizeClassName":  false,
				"poolingStrategy":     "cls",
				"passageInferenceUrl": "http://passage.inference.url",
			},
			wantErr: errors.New("passageInferenceUrl is set but queryInferenceUrl is empty, both needs to be set"),
		},
		{
			name: "error - query inference url set but not passage",
			vectorizer: map[string]interface{}{
				"vectorizeClassName": false,
				"poolingStrategy":    "cls",
				"queryInferenceUrl":  "http://passage.inference.url",
			},
			wantErr: errors.New("queryInferenceUrl is set but passageInferenceUrl is empty, both needs to be set"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			class := &models.Class{
				Class: "MyClass",
				VectorConfig: map[string]models.VectorConfig{
					"namedVector": {
						Vectorizer: map[string]interface{}{
							"my-module": tt.vectorizer,
						},
					},
				},
				Properties: []*models.Property{{
					Name: "someProp",
					ModuleConfig: map[string]interface{}{
						"my-module": map[string]interface{}{
							"skip":                  true,
							"vectorizePropertyName": true,
						},
					},
				}},
			}

			cfg := modules.NewClassBasedModuleConfig(class, "my-module", "tenant", "namedVector")
			ic := NewClassSettings(cfg)
			err := ic.Validate(class)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.wantErr.Error())
			} else {
				assert.Nil(t, err)
			}
		})
	}
}
