package modules

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/stretchr/testify/assert"
)

func TestSetClassDefaults(t *testing.T) {
	t.Run("no modules", func(t *testing.T) {
		class := &models.Class{
			Class:      "Foo",
			Vectorizer: "none",
		}

		p := NewProvider()
		p.SetClassDefaults(class)

		assert.Equal(t, &models.Class{Class: "Foo", Vectorizer: "none"}, class,
			"the class is not changed")
	})

	t.Run("module is set, but does not have config capability", func(t *testing.T) {
		class := &models.Class{
			Class:      "Foo",
			Vectorizer: "my-module",
		}

		p := NewProvider()
		p.Register(&dummyModuleNoCapabilities{name: "my-module"})
		p.SetClassDefaults(class)

		assert.Equal(t, &models.Class{Class: "Foo", Vectorizer: "my-module"}, class,
			"the class is not changed")
	})

	t.Run("without user-provided values", func(t *testing.T) {
		class := &models.Class{
			Class: "Foo",
			Properties: []*models.Property{{
				Name:     "Foo",
				DataType: []string{"string"},
			}},
			Vectorizer: "my-module",
		}
		expected := &models.Class{
			Class: "Foo",
			ModuleConfig: map[string]interface{}{
				"my-module": map[string]interface{}{
					"per-class-prop-1": "some default value",
					"per-class-prop-2": "some default value",
				},
			},
			Properties: []*models.Property{{
				Name:     "Foo",
				DataType: []string{"string"},
				ModuleConfig: map[string]interface{}{
					"my-module": map[string]interface{}{
						"per-prop-1": "prop default value",
						"per-prop-2": "prop default value",
					},
				},
			}},
			Vectorizer: "my-module",
		}

		p := NewProvider()
		p.Register(&dummyModuleClassConfigurator{
			dummyModuleNoCapabilities{
				name: "my-module",
			},
		})
		p.SetClassDefaults(class)

		assert.Equal(t, expected, class,
			"the defaults were set from config")
	})

	t.Run("with some user-provided values", func(t *testing.T) {
		class := &models.Class{
			Class: "Foo",
			ModuleConfig: map[string]interface{}{
				"my-module": map[string]interface{}{
					"per-class-prop-1": "overwritten by user",
				},
			},
			Properties: []*models.Property{{
				Name:     "Foo",
				DataType: []string{"string"},
				ModuleConfig: map[string]interface{}{
					"my-module": map[string]interface{}{
						"per-prop-1": "prop overwritten by user",
					},
				},
			}},
			Vectorizer: "my-module",
		}
		expected := &models.Class{
			Class: "Foo",
			ModuleConfig: map[string]interface{}{
				"my-module": map[string]interface{}{
					"per-class-prop-1": "overwritten by user",
					"per-class-prop-2": "some default value",
				},
			},
			Properties: []*models.Property{{
				Name:     "Foo",
				DataType: []string{"string"},
				ModuleConfig: map[string]interface{}{
					"my-module": map[string]interface{}{
						"per-prop-1": "prop overwritten by user",
						"per-prop-2": "prop default value",
					},
				},
			}},
			Vectorizer: "my-module",
		}

		p := NewProvider()
		p.Register(&dummyModuleClassConfigurator{
			dummyModuleNoCapabilities{
				name: "my-module",
			},
		})
		p.SetClassDefaults(class)

		assert.Equal(t, expected, class,
			"the defaults were set from config")
	})
}

type dummyModuleClassConfigurator struct {
	dummyModuleNoCapabilities
}

func (d *dummyModuleClassConfigurator) ClassConfigDefaults() map[string]interface{} {
	return map[string]interface{}{
		"per-class-prop-1": "some default value",
		"per-class-prop-2": "some default value",
	}
}

func (d *dummyModuleClassConfigurator) PropertyConfigDefaults(
	dt *schema.DataType) map[string]interface{} {
	return map[string]interface{}{
		"per-prop-1": "prop default value",
		"per-prop-2": "prop default value",
	}
}

func (d *dummyModuleClassConfigurator) ValidateClass(
	class *models.Class, cfg modulecapabilities.ClassConfig) error {
	return nil
}

var _ = modulecapabilities.ClassConfigurator(
	&dummyModuleClassConfigurator{})
