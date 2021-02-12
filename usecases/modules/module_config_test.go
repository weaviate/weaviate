package modules

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
)

func TestClassBasedModuleConfig(t *testing.T) {
	t.Run("when the prop doesn't exist", func(t *testing.T) {
		class := &models.Class{
			Class: "Test",
		}
		cfg := NewClassBasedModuleConfig(class, "my-module")
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
		cfg := NewClassBasedModuleConfig(class, "my-module")
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
		cfg := NewClassBasedModuleConfig(class, "my-module")
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
		cfg := NewClassBasedModuleConfig(class, "my-module")
		assert.Equal(t, map[string]interface{}{"classLevel": "foo"}, cfg.Class())
		assert.Equal(t, map[string]interface{}{"propLevel": "bar"},
			cfg.Property("some-prop"))
	})
}
