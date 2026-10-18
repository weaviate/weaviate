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

package modules

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestClassBasedModuleConfig(t *testing.T) {
	t.Run("when the prop doesn't exist", func(t *testing.T) {
		class := &models.Class{
			Class: "Test",
		}
		cfg := NewClassBasedModuleConfig(class, "my-module", "tenant", "", nil)
		assert.Equal(t, map[string]interface{}{}, cfg.Property("some-prop"))
	})

	t.Run("without any module-specific config", func(t *testing.T) {
		class := &models.Class{
			Class: "Test",
			Properties: []*models.Property{
				{
					Name: "some-prop",
				},
			},
		}
		cfg := NewClassBasedModuleConfig(class, "my-module", "tenant", "", nil)
		assert.Equal(t, map[string]interface{}{}, cfg.Class())
		assert.Equal(t, map[string]interface{}{}, cfg.Property("some-prop"))
	})

	t.Run("with config for other modules set", func(t *testing.T) {
		class := &models.Class{
			Class: "Test",
			ModuleConfig: map[string]interface{}{
				"other-module": map[string]interface{}{
					"classLevel": "foo",
				},
			},
			Properties: []*models.Property{
				{
					Name: "some-prop",
					ModuleConfig: map[string]interface{}{
						"other-module": map[string]interface{}{
							"propLevel": "bar",
						},
					},
				},
			},
		}
		cfg := NewClassBasedModuleConfig(class, "my-module", "tenant", "", nil)
		assert.Equal(t, map[string]interface{}{}, cfg.Class())
		assert.Equal(t, map[string]interface{}{},
			cfg.Property("some-prop"))
	})

	t.Run("with all config set", func(t *testing.T) {
		class := &models.Class{
			Class: "Test",
			ModuleConfig: map[string]interface{}{
				"my-module": map[string]interface{}{
					"classLevel": "foo",
				},
			},
			Properties: []*models.Property{
				{
					Name: "some-prop",
					ModuleConfig: map[string]interface{}{
						"my-module": map[string]interface{}{
							"propLevel": "bar",
						},
					},
				},
			},
		}
		cfg := NewClassBasedModuleConfig(class, "my-module", "tenant", "", nil)
		assert.Equal(t, map[string]interface{}{"classLevel": "foo"}, cfg.Class())
		assert.Equal(t, map[string]interface{}{"propLevel": "bar"},
			cfg.Property("some-prop"))
	})
}

// The settings a request names are read over the class's for that module
// only; without settings the class config is returned as is.
func TestRequestModuleConfig(t *testing.T) {
	class := &models.Class{Class: "C", ModuleConfig: map[string]interface{}{
		"decisions-a": map[string]interface{}{"model": "class-model", "maxDocuments": 5},
		"decisions-b": map[string]interface{}{"model": "other"},
	}}
	base := NewClassBasedModuleConfig(class, "decisions-a", "", "", &config.Config{})

	assert.Same(t, base, NewRequestModuleConfig(base, "decisions-a", nil), "no settings")

	cfg := NewRequestModuleConfig(base, "decisions-a", map[string]any{"maxDocuments": 7, "images": true})

	assert.Equal(t, map[string]interface{}{"model": "class-model", "maxDocuments": 7, "images": true}, cfg.ClassByModuleName("decisions-a"))
	assert.Equal(t, map[string]interface{}{"model": "class-model", "maxDocuments": 7, "images": true}, cfg.Class())
	assert.Equal(t, map[string]interface{}{"model": "other"}, cfg.ClassByModuleName("decisions-b"), "another module reads the class")
	assert.Equal(t, map[string]interface{}{"model": "class-model", "maxDocuments": 5}, base.ClassByModuleName("decisions-a"), "the class config is not changed")
}
