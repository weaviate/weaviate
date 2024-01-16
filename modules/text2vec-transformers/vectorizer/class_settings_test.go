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

	"github.com/stretchr/testify/assert"
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

		cfg := modules.NewClassBasedModuleConfig(class, "my-module", "tenant")
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

		cfg := modules.NewClassBasedModuleConfig(class, "my-module", "tenant")
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

		cfg := modules.NewClassBasedModuleConfig(class, "my-module", "tenant")
		ic := NewClassSettings(cfg)

		assert.False(t, ic.PropertyIndexed("someProp"))
		assert.True(t, ic.VectorizePropertyName("someProp"))
		assert.False(t, ic.VectorizeClassName())
		assert.Equal(t, ic.PoolingStrategy(), "cls")
	})
}
