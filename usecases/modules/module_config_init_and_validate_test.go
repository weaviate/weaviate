//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package modules

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			dummyModuleNoCapabilities: dummyModuleNoCapabilities{
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
			dummyModuleNoCapabilities: dummyModuleNoCapabilities{
				name: "my-module",
			},
		})
		p.SetClassDefaults(class)

		assert.Equal(t, expected, class,
			"the defaults were set from config")
	})
}

func TestValidateClass(t *testing.T) {
	ctx := context.Background()
	t.Run("when class has no vectorizer set, it does not check", func(t *testing.T) {
		class := &models.Class{
			Class: "Foo",
			Properties: []*models.Property{{
				Name:     "Foo",
				DataType: []string{"string"},
			}},
			Vectorizer: "none",
		}

		p := NewProvider()
		p.Register(&dummyModuleClassConfigurator{
			validateError: errors.Errorf("if I was used, you'd fail"),
			dummyModuleNoCapabilities: dummyModuleNoCapabilities{
				name: "my-module",
			},
		})
		p.SetClassDefaults(class)

		assert.Nil(t, p.ValidateClass(ctx, class))
	})

	t.Run("when vectorizer does not have capability, it skips validation",
		func(t *testing.T) {
			class := &models.Class{
				Class: "Foo",
				Properties: []*models.Property{{
					Name:     "Foo",
					DataType: []string{"string"},
				}},
				Vectorizer: "my-module",
			}

			p := NewProvider()
			p.Register(&dummyModuleNoCapabilities{
				name: "my-module",
			})
			p.SetClassDefaults(class)

			assert.Nil(t, p.ValidateClass(ctx, class))
		})

	t.Run("the module validates if capable and configured", func(t *testing.T) {
		class := &models.Class{
			Class: "Foo",
			Properties: []*models.Property{{
				Name:     "Foo",
				DataType: []string{"string"},
			}},
			Vectorizer: "my-module",
		}

		p := NewProvider()
		p.Register(&dummyModuleClassConfigurator{
			validateError: errors.Errorf("no can do!"),
			dummyModuleNoCapabilities: dummyModuleNoCapabilities{
				name: "my-module",
			},
		})
		p.SetClassDefaults(class)

		err := p.ValidateClass(ctx, class)
		require.NotNil(t, err)
		assert.Equal(t, "module 'my-module': no can do!", err.Error())
	})
}

type dummyModuleClassConfigurator struct {
	dummyModuleNoCapabilities
	validateError error
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

func (d *dummyModuleClassConfigurator) ValidateClass(ctx context.Context,
	class *models.Class, cfg moduletools.ClassConfig) error {
	return d.validateError
}

var _ = modulecapabilities.ClassConfigurator(
	&dummyModuleClassConfigurator{})
