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
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestSetClassDefaults(t *testing.T) {
	logger, _ := test.NewNullLogger()
	t.Run("no modules", func(t *testing.T) {
		class := &models.Class{
			Class:      "Foo",
			Vectorizer: "none",
		}

		p := NewProvider(logger, config.Config{})
		p.SetClassDefaults(class)

		assert.Equal(t, &models.Class{Class: "Foo", Vectorizer: "none"}, class,
			"the class is not changed")
	})

	t.Run("module is set, but does not have config capability", func(t *testing.T) {
		class := &models.Class{
			Class:      "Foo",
			Vectorizer: "my-module",
		}

		p := NewProvider(logger, config.Config{})
		p.Register(&dummyText2VecModuleNoCapabilities{name: "my-module"})
		p.SetClassDefaults(class)

		assert.Equal(t, &models.Class{Class: "Foo", Vectorizer: "my-module"}, class,
			"the class is not changed")
	})

	t.Run("without user-provided values", func(t *testing.T) {
		class := &models.Class{
			Class: "Foo",
			Properties: []*models.Property{{
				Name:         "Foo",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			}},
			Vectorizer: "my-module",
		}
		expected := &models.Class{
			Class: "Foo",
			ModuleConfig: map[string]any{
				"my-module": map[string]any{
					"per-class-prop-1": "some default value",
					"per-class-prop-2": "some default value",
				},
			},
			Properties: []*models.Property{{
				Name:         "Foo",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				ModuleConfig: map[string]any{
					"my-module": map[string]any{
						"per-prop-1": "prop default value",
						"per-prop-2": "prop default value",
					},
				},
			}},
			Vectorizer: "my-module",
		}

		p := NewProvider(logger, config.Config{})
		p.Register(&dummyModuleClassConfigurator{
			dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
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
			ModuleConfig: map[string]any{
				"my-module": map[string]any{
					"per-class-prop-1": "overwritten by user",
				},
			},
			Properties: []*models.Property{{
				Name:         "Foo",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				ModuleConfig: map[string]any{
					"my-module": map[string]any{
						"per-prop-1": "prop overwritten by user",
					},
				},
			}},
			Vectorizer: "my-module",
		}
		expected := &models.Class{
			Class: "Foo",
			ModuleConfig: map[string]any{
				"my-module": map[string]any{
					"per-class-prop-1": "overwritten by user",
					"per-class-prop-2": "some default value",
				},
			},
			Properties: []*models.Property{{
				Name:         "Foo",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				ModuleConfig: map[string]any{
					"my-module": map[string]any{
						"per-prop-1": "prop overwritten by user",
						"per-prop-2": "prop default value",
					},
				},
			}},
			Vectorizer: "my-module",
		}

		p := NewProvider(logger, config.Config{})
		p.Register(&dummyModuleClassConfigurator{
			dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
				name: "my-module",
			},
		})
		p.SetClassDefaults(class)

		assert.Equal(t, expected, class,
			"the defaults were set from config")
	})

	t.Run("named vector, without user-provided values", func(t *testing.T) {
		class := &models.Class{
			Class: "Foo",
			Properties: []*models.Property{{
				Name:         "Foo",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			}},
			VectorConfig: map[string]models.VectorConfig{
				"vec1": {
					Vectorizer: map[string]any{"my-module": map[string]any{}},
				},
			},
		}
		expected := &models.Class{
			Class: "Foo",
			Properties: []*models.Property{{
				Name:         "Foo",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				ModuleConfig: map[string]any{
					"my-module": map[string]any{
						"per-prop-1": "prop default value",
						"per-prop-2": "prop default value",
					},
				},
			}},
			VectorConfig: map[string]models.VectorConfig{
				"vec1": {
					Vectorizer: map[string]any{
						"my-module": map[string]any{
							"per-class-prop-1": "some default value",
							"per-class-prop-2": "some default value",
						},
					},
				},
			},
		}

		p := NewProvider(logger, config.Config{})
		p.Register(&dummyModuleClassConfigurator{
			dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
				name: "my-module",
			},
		})
		p.SetClassDefaults(class)

		assert.Equal(t, expected, class, "the defaults were set from config")
	})

	t.Run("mixed vector, without user-provided values", func(t *testing.T) {
		class := &models.Class{
			Class: "Foo",
			Properties: []*models.Property{{
				Name:         "Foo",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			}},
			VectorConfig: map[string]models.VectorConfig{
				"vec1": {
					Vectorizer: map[string]any{"my-module": map[string]any{}},
				},
			},
			Vectorizer:        "my-module",
			VectorIndexConfig: hnsw.NewDefaultUserConfig(),
		}
		expected := &models.Class{
			Class: "Foo",
			Properties: []*models.Property{{
				Name:         "Foo",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				ModuleConfig: map[string]any{
					"my-module": map[string]any{
						"per-prop-1": "prop default value",
						"per-prop-2": "prop default value",
					},
				},
			}},
			VectorConfig: map[string]models.VectorConfig{
				"vec1": {
					Vectorizer: map[string]any{
						"my-module": map[string]any{
							"per-class-prop-1": "some default value",
							"per-class-prop-2": "some default value",
						},
					},
				},
			},
			Vectorizer: "my-module",
			ModuleConfig: map[string]any{
				"my-module": map[string]any{
					"per-class-prop-1": "some default value",
					"per-class-prop-2": "some default value",
				},
			},
			VectorIndexConfig: hnsw.NewDefaultUserConfig(),
		}

		p := NewProvider(logger, config.Config{})
		p.Register(&dummyModuleClassConfigurator{
			dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
				name: "my-module",
			},
		})
		p.SetClassDefaults(class)

		assert.Equal(t, expected, class, "the defaults were set from config")
	})
}

func TestValidateClass(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	t.Run("when class has no vectorizer set, it does not check", func(t *testing.T) {
		class := &models.Class{
			Class: "Foo",
			Properties: []*models.Property{{
				Name:         "Foo",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			}},
			Vectorizer: "none",
		}

		p := NewProvider(logger, config.Config{})
		p.Register(&dummyModuleClassConfigurator{
			validateError: errors.Errorf("if I was used, you'd fail"),
			dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
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
					Name:         "Foo",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				}},
				Vectorizer: "my-module",
			}

			p := NewProvider(logger, config.Config{})
			p.Register(&dummyText2VecModuleNoCapabilities{
				name: "my-module",
			})
			p.SetClassDefaults(class)

			assert.Nil(t, p.ValidateClass(ctx, class))
		})

	t.Run("the module validates if capable and configured", func(t *testing.T) {
		class := &models.Class{
			Class: "Foo",
			Properties: []*models.Property{{
				Name:         "Foo",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			}},
			Vectorizer: "my-module",
		}

		p := NewProvider(logger, config.Config{})
		p.Register(&dummyModuleClassConfigurator{
			validateError: errors.Errorf("no can do!"),
			dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
				name: "my-module",
			},
		})
		p.SetClassDefaults(class)

		err := p.ValidateClass(ctx, class)
		require.NotNil(t, err)
		assert.Equal(t, "module 'my-module': no can do!", err.Error())
	})
}

func TestValidateClass_BlobHashNotMixedWithOtherFields(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	type testCase struct {
		name        string
		class       *models.Class
		modules     []dummyModuleClassConfigurator
		expectError string // empty means no error expected
	}

	tests := []testCase{
		{
			name: "single named vector: blobHash alone is allowed",
			class: &models.Class{
				Class: "Foo",
				Properties: []*models.Property{
					{Name: "img", DataType: schema.DataTypeBlobHash.PropString()},
				},
				VectorConfig: map[string]models.VectorConfig{
					"vec1": {
						Vectorizer: map[string]any{
							"my-module": map[string]any{},
						},
					},
				},
			},
			modules: []dummyModuleClassConfigurator{
				{
					dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
						name:            "my-module",
						mediaProperties: []string{"img"},
						vectorizeText:   new(false),
					},
				},
			},
		},
		{
			name: "single named vector: blobHash with other media field is rejected",
			class: &models.Class{
				Class: "Foo",
				Properties: []*models.Property{
					{Name: "img", DataType: schema.DataTypeBlobHash.PropString()},
					{Name: "photo", DataType: schema.DataTypeBlob.PropString()},
				},
				VectorConfig: map[string]models.VectorConfig{
					"vec1": {
						Vectorizer: map[string]any{
							"my-module": map[string]any{},
						},
					},
				},
			},
			modules: []dummyModuleClassConfigurator{
				{
					dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
						name:            "my-module",
						mediaProperties: []string{"img", "photo"},
						vectorizeText:   new(false),
					},
				},
			},
			expectError: "blobHash property cannot be combined with other vectorizable fields",
		},
		{
			name: "single named vector: blobHash with text vectorization enabled is rejected",
			class: &models.Class{
				Class: "Foo",
				Properties: []*models.Property{
					{Name: "img", DataType: schema.DataTypeBlobHash.PropString()},
					{Name: "description", DataType: schema.DataTypeText.PropString()},
				},
				VectorConfig: map[string]models.VectorConfig{
					"vec1": {
						Vectorizer: map[string]any{
							"my-module": map[string]any{},
						},
					},
				},
			},
			modules: []dummyModuleClassConfigurator{
				{
					dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
						name:            "my-module",
						mediaProperties: []string{"img"},
						vectorizeText:   new(true),
					},
				},
			},
			expectError: "blobHash property cannot be combined with other vectorizable fields",
		},
		{
			name: "single named vector: blobHash alone with text vectorization disabled is allowed",
			class: &models.Class{
				Class: "Foo",
				Properties: []*models.Property{
					{Name: "img", DataType: schema.DataTypeBlobHash.PropString()},
					{Name: "description", DataType: schema.DataTypeText.PropString()},
				},
				VectorConfig: map[string]models.VectorConfig{
					"vec1": {
						Vectorizer: map[string]any{
							"my-module": map[string]any{},
						},
					},
				},
			},
			modules: []dummyModuleClassConfigurator{
				{
					dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
						name:            "my-module",
						mediaProperties: []string{"img"},
						vectorizeText:   new(false),
					},
				},
			},
		},
		{
			name: "single named vector: blob (not blobHash) with other fields is allowed",
			class: &models.Class{
				Class: "Foo",
				Properties: []*models.Property{
					{Name: "img", DataType: schema.DataTypeBlob.PropString()},
					{Name: "description", DataType: schema.DataTypeText.PropString()},
				},
				VectorConfig: map[string]models.VectorConfig{
					"vec1": {
						Vectorizer: map[string]any{
							"my-module": map[string]any{},
						},
					},
				},
			},
			modules: []dummyModuleClassConfigurator{
				{
					dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
						name:            "my-module",
						mediaProperties: []string{"img"},
						vectorizeText:   new(true),
					},
				},
			},
		},
		{
			name: "multiple named vectors: only one misconfigured - error references correct target vector",
			class: &models.Class{
				Class: "Foo",
				Properties: []*models.Property{
					{Name: "img", DataType: schema.DataTypeBlobHash.PropString()},
					{Name: "photo", DataType: schema.DataTypeBlob.PropString()},
					{Name: "title", DataType: schema.DataTypeText.PropString()},
				},
				VectorConfig: map[string]models.VectorConfig{
					"vec_text": {
						Vectorizer: map[string]any{
							"text-module": map[string]any{},
						},
					},
					"vec_image": {
						Vectorizer: map[string]any{
							"image-module": map[string]any{},
						},
					},
					"vec_bad": {
						Vectorizer: map[string]any{
							"bad-module": map[string]any{},
						},
					},
				},
			},
			modules: []dummyModuleClassConfigurator{
				{
					dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
						name:          "text-module",
						vectorizeText: new(true),
					},
				},
				{
					dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
						name:            "image-module",
						mediaProperties: []string{"photo"},
						vectorizeText:   new(false),
					},
				},
				{
					// This module mixes blobHash with another media field
					dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
						name:            "bad-module",
						mediaProperties: []string{"img", "photo"},
						vectorizeText:   new(false),
					},
				},
			},
			expectError: "blobHash property cannot be combined with other vectorizable fields",
		},
		{
			name: "multiple named vectors: all correctly configured",
			class: &models.Class{
				Class: "Foo",
				Properties: []*models.Property{
					{Name: "img", DataType: schema.DataTypeBlobHash.PropString()},
					{Name: "photo", DataType: schema.DataTypeBlob.PropString()},
					{Name: "title", DataType: schema.DataTypeText.PropString()},
				},
				VectorConfig: map[string]models.VectorConfig{
					"vec_text": {
						Vectorizer: map[string]any{
							"text-module": map[string]any{},
						},
					},
					"vec_image": {
						Vectorizer: map[string]any{
							"image-module": map[string]any{},
						},
					},
					"vec_hash": {
						Vectorizer: map[string]any{
							"hash-module": map[string]any{},
						},
					},
				},
			},
			modules: []dummyModuleClassConfigurator{
				{
					dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
						name:          "text-module",
						vectorizeText: new(true),
					},
				},
				{
					dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
						name:            "image-module",
						mediaProperties: []string{"photo"},
						vectorizeText:   new(false),
					},
				},
				{
					// blobHash in its own named vector, no other fields
					dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
						name:            "hash-module",
						mediaProperties: []string{"img"},
						vectorizeText:   new(false),
					},
				},
			},
		},
		{
			name: "multiple named vectors: blobHash with text in one vector is rejected",
			class: &models.Class{
				Class: "Foo",
				Properties: []*models.Property{
					{Name: "img", DataType: schema.DataTypeBlobHash.PropString()},
					{Name: "photo", DataType: schema.DataTypeBlob.PropString()},
					{Name: "title", DataType: schema.DataTypeText.PropString()},
				},
				VectorConfig: map[string]models.VectorConfig{
					"vec_ok1": {
						Vectorizer: map[string]any{
							"ok-module-1": map[string]any{},
						},
					},
					"vec_ok2": {
						Vectorizer: map[string]any{
							"ok-module-2": map[string]any{},
						},
					},
					"vec_bad_text": {
						Vectorizer: map[string]any{
							"bad-text-module": map[string]any{},
						},
					},
				},
			},
			modules: []dummyModuleClassConfigurator{
				{
					dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
						name:          "ok-module-1",
						vectorizeText: new(true),
					},
				},
				{
					dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
						name:            "ok-module-2",
						mediaProperties: []string{"photo"},
						vectorizeText:   new(false),
					},
				},
				{
					// blobHash + text vectorization enabled → should fail
					dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
						name:            "bad-text-module",
						mediaProperties: []string{"img"},
						vectorizeText:   new(true),
					},
				},
			},
			expectError: "blobHash property cannot be combined with other vectorizable fields",
		},
		{
			name: "legacy vectorizer: blobHash with other media field is rejected",
			class: &models.Class{
				Class: "Foo",
				Properties: []*models.Property{
					{Name: "img", DataType: schema.DataTypeBlobHash.PropString()},
					{Name: "photo", DataType: schema.DataTypeBlob.PropString()},
				},
				Vectorizer: "my-module",
			},
			modules: []dummyModuleClassConfigurator{
				{
					dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
						name:            "my-module",
						mediaProperties: []string{"img", "photo"},
						vectorizeText:   new(false),
					},
				},
			},
			expectError: "blobHash property cannot be combined with other vectorizable fields",
		},
		{
			name: "legacy vectorizer: blobHash alone is allowed",
			class: &models.Class{
				Class: "Foo",
				Properties: []*models.Property{
					{Name: "img", DataType: schema.DataTypeBlobHash.PropString()},
				},
				Vectorizer: "my-module",
			},
			modules: []dummyModuleClassConfigurator{
				{
					dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
						name:            "my-module",
						mediaProperties: []string{"img"},
						vectorizeText:   new(false),
					},
				},
			},
		},
		{
			name: "no media properties: no blobHash check needed",
			class: &models.Class{
				Class: "Foo",
				Properties: []*models.Property{
					{Name: "title", DataType: schema.DataTypeText.PropString()},
				},
				VectorConfig: map[string]models.VectorConfig{
					"vec1": {
						Vectorizer: map[string]any{
							"my-module": map[string]any{},
						},
					},
				},
			},
			modules: []dummyModuleClassConfigurator{
				{
					dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
						name:          "my-module",
						vectorizeText: new(true),
					},
				},
			},
		},
		{
			name: "single named vector: two blobHash media properties is rejected",
			class: &models.Class{
				Class: "Foo",
				Properties: []*models.Property{
					{Name: "img1", DataType: schema.DataTypeBlobHash.PropString()},
					{Name: "img2", DataType: schema.DataTypeBlobHash.PropString()},
				},
				VectorConfig: map[string]models.VectorConfig{
					"vec1": {
						Vectorizer: map[string]any{
							"my-module": map[string]any{},
						},
					},
				},
			},
			modules: []dummyModuleClassConfigurator{
				{
					dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
						name:            "my-module",
						mediaProperties: []string{"img1", "img2"},
						vectorizeText:   new(false),
					},
				},
			},
			expectError: "blobHash property cannot be combined with other vectorizable fields",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := NewProvider(logger, config.Config{})
			for i := range tc.modules {
				p.Register(&tc.modules[i])
			}
			p.SetClassDefaults(tc.class)

			err := p.ValidateClass(ctx, tc.class)
			if tc.expectError == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectError)
			}
		})
	}
}

func TestSetSinglePropertyDefaults(t *testing.T) {
	class := &models.Class{
		Class: "Foo",
		ModuleConfig: map[string]any{
			"my-module": map[string]any{
				"per-class-prop-1": "overwritten by user",
			},
		},
		Properties: []*models.Property{{
			Name:         "Foo",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
			ModuleConfig: map[string]any{
				"my-module": map[string]any{
					"per-prop-1": "prop overwritten by user",
				},
			},
		}},
		Vectorizer: "my-module",
	}
	prop := &models.Property{
		DataType: []string{"boolean"},
		ModuleConfig: map[string]any{
			"my-module": map[string]any{
				"per-prop-1": "overwritten by user",
			},
		},
		Name: "newProp",
	}
	expected := &models.Property{
		DataType: []string{"boolean"},
		ModuleConfig: map[string]any{
			"my-module": map[string]any{
				"per-prop-1": "overwritten by user",
				"per-prop-2": "prop default value",
			},
		},
		Name: "newProp",
	}

	logger, _ := test.NewNullLogger()
	p := NewProvider(logger, config.Config{})
	p.Register(&dummyModuleClassConfigurator{
		dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
			name: "my-module",
		},
	})
	p.SetSinglePropertyDefaults(class, prop)

	assert.Equal(t, expected, prop,
		"user specified module config is used, for rest the default value is used")
}

func TestSetSinglePropertyDefaults_MixedVectors(t *testing.T) {
	class := &models.Class{
		Class: "Foo",
		Properties: []*models.Property{{
			Name:         "Foo",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
			ModuleConfig: map[string]any{
				"my-module": map[string]any{
					"per-prop-1": "prop overwritten by user",
				},
			},
		}},
		Vectorizer: "my-module",
		ModuleConfig: map[string]any{
			"my-module": map[string]any{
				"per-class-prop-1": "overwritten by user",
			},
		},
		VectorIndexConfig: hnsw.NewDefaultUserConfig(),
		VectorConfig: map[string]models.VectorConfig{
			"vec1": {
				Vectorizer: map[string]any{
					"my-module-2": map[string]any{},
				},
			},
		},
	}
	prop := &models.Property{
		DataType: []string{"boolean"},
		ModuleConfig: map[string]any{
			"my-module": map[string]any{
				"per-prop-1": "overwritten by user",
			},
		},
		Name: "newProp",
	}
	expected := &models.Property{
		DataType: []string{"boolean"},
		ModuleConfig: map[string]any{
			"my-module": map[string]any{
				"per-prop-1": "overwritten by user",
				"per-prop-2": "prop default value",
			},
			"my-module-2": map[string]any{
				"per-prop-1": "prop default value",
				"per-prop-2": "prop default value",
			},
		},
		Name: "newProp",
	}

	logger, _ := test.NewNullLogger()
	p := NewProvider(logger, config.Config{})
	p.Register(&dummyModuleClassConfigurator{
		dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
			name: "my-module",
		},
	})
	p.Register(&dummyModuleClassConfigurator{
		dummyText2VecModuleNoCapabilities: dummyText2VecModuleNoCapabilities{
			name: "my-module-2",
		},
	})
	p.SetSinglePropertyDefaults(class, prop)

	assert.Equal(t, expected, prop,
		"user specified module config is used, for rest the default value is used")
}

type dummyModuleClassConfigurator struct {
	dummyText2VecModuleNoCapabilities
	validateError error
}

func (d *dummyModuleClassConfigurator) ClassConfigDefaults() map[string]any {
	return map[string]any{
		"per-class-prop-1": "some default value",
		"per-class-prop-2": "some default value",
	}
}

func (d *dummyModuleClassConfigurator) PropertyConfigDefaults(
	dt *schema.DataType,
) map[string]any {
	return map[string]any{
		"per-prop-1": "prop default value",
		"per-prop-2": "prop default value",
	}
}

func (d *dummyModuleClassConfigurator) ValidateClass(ctx context.Context,
	class *models.Class, cfg moduletools.ClassConfig,
) error {
	return d.validateError
}

var _ = modulecapabilities.ClassConfigurator(
	&dummyModuleClassConfigurator{})
