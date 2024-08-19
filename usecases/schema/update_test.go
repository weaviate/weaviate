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

package schema

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

// As of now, most class settings are immutable, but we need to allow some
// specific updates, such as the vector index config
func TestClassUpdates(t *testing.T) {
	t.Run("a class which doesn't exist", func(t *testing.T) {
		err := newSchemaManager().UpdateClass(context.Background(),
			nil, "WrongClass", &models.Class{})
		require.NotNil(t, err)
		assert.Equal(t, ErrNotFound, err)
	})

	t.Run("various immutable and mutable fields", func(t *testing.T) {
		type test struct {
			name          string
			initial       *models.Class
			update        *models.Class
			expectedError error
		}

		tests := []test{
			{
				name:    "attempting a name change",
				initial: &models.Class{Class: "InitialName"},
				update:  &models.Class{Class: "UpdatedName"},
				expectedError: errors.Errorf(
					"class name is immutable: " +
						"attempted change from \"InitialName\" to \"UpdatedName\""),
			},
			{
				name:    "attempting to modify the vectorizer",
				initial: &models.Class{Class: "InitialName", Vectorizer: "model1"},
				update:  &models.Class{Class: "InitialName", Vectorizer: "model2"},
				expectedError: errors.Errorf(
					"vectorizer is immutable: " +
						"attempted change from \"model1\" to \"model2\""),
			},
			{
				name:    "attempting to modify the vector index type",
				initial: &models.Class{Class: "InitialName", VectorIndexType: "hnsw"},
				update:  &models.Class{Class: "InitialName", VectorIndexType: "lsh"},
				expectedError: errors.Errorf(
					"vector index type is immutable: " +
						"attempted change from \"hnsw\" to \"lsh\""),
			},
			{
				name:    "attempting to add a property",
				initial: &models.Class{Class: "InitialName"},
				update: &models.Class{
					Class: "InitialName",
					Properties: []*models.Property{
						{
							Name: "newProp",
						},
					},
				},
				expectedError: errors.Errorf(
					"properties cannot be updated through updating the class. Use the add " +
						"property feature (e.g. \"POST /v1/schema/{className}/properties\") " +
						"to add additional properties"),
			},
			{
				name: "leaving properties unchanged",
				initial: &models.Class{
					Class: "InitialName",
					Properties: []*models.Property{
						{
							Name:     "aProp",
							DataType: schema.DataTypeText.PropString(),
						},
					},
				},
				update: &models.Class{
					Class: "InitialName",
					Properties: []*models.Property{
						{
							Name:     "aProp",
							DataType: schema.DataTypeText.PropString(),
						},
					},
				},
				expectedError: nil,
			},
			{
				name: "attempting to rename a property",
				initial: &models.Class{
					Class: "InitialName",
					Properties: []*models.Property{
						{
							Name:     "aProp",
							DataType: schema.DataTypeText.PropString(),
						},
					},
				},
				update: &models.Class{
					Class: "InitialName",
					Properties: []*models.Property{
						{
							Name:     "changedProp",
							DataType: schema.DataTypeText.PropString(),
						},
					},
				},
				expectedError: errors.Errorf(
					"properties cannot be updated through updating the class. Use the add " +
						"property feature (e.g. \"POST /v1/schema/{className}/properties\") " +
						"to add additional properties"),
			},
			{
				name: "attempting to update the inverted index cleanup interval",
				initial: &models.Class{
					Class: "InitialName",
					InvertedIndexConfig: &models.InvertedIndexConfig{
						CleanupIntervalSeconds: 17,
					},
				},
				update: &models.Class{
					Class: "InitialName",
					InvertedIndexConfig: &models.InvertedIndexConfig{
						CleanupIntervalSeconds: 18,
						Bm25: &models.BM25Config{
							K1: config.DefaultBM25k1,
							B:  config.DefaultBM25b,
						},
					},
				},
			},
			{
				name: "attempting to update the inverted index BM25 config",
				initial: &models.Class{
					Class: "InitialName",
					InvertedIndexConfig: &models.InvertedIndexConfig{
						CleanupIntervalSeconds: 18,
						Bm25: &models.BM25Config{
							K1: 1.012,
							B:  0.125,
						},
					},
				},
				update: &models.Class{
					Class: "InitialName",
					InvertedIndexConfig: &models.InvertedIndexConfig{
						CleanupIntervalSeconds: 18,
						Bm25: &models.BM25Config{
							K1: 1.012,
							B:  0.125,
						},
					},
				},
			},
			{
				name: "attempting to update the inverted index Stopwords config",
				initial: &models.Class{
					Class: "InitialName",
					InvertedIndexConfig: &models.InvertedIndexConfig{
						CleanupIntervalSeconds: 18,
						Stopwords: &models.StopwordConfig{
							Preset: "en",
						},
					},
				},
				update: &models.Class{
					Class: "InitialName",
					InvertedIndexConfig: &models.InvertedIndexConfig{
						CleanupIntervalSeconds: 18,
						Stopwords: &models.StopwordConfig{
							Preset:    "none",
							Additions: []string{"banana", "passionfruit", "kiwi"},
							Removals:  []string{"a", "the"},
						},
					},
				},
			},
			{
				name: "attempting to update module config",
				initial: &models.Class{
					Class: "InitialName",
					ModuleConfig: map[string]interface{}{
						"my-module1": map[string]interface{}{
							"my-setting": "some-value",
						},
					},
				},
				update: &models.Class{
					Class: "InitialName",
					ModuleConfig: map[string]interface{}{
						"my-module1": map[string]interface{}{
							"my-setting": "updated-value",
						},
					},
				},
				expectedError: errors.Errorf("module config is immutable"),
			},
			{
				name: "updating vector index config",
				initial: &models.Class{
					Class: "InitialName",
					VectorIndexConfig: map[string]interface{}{
						"some-setting": "old-value",
					},
				},
				update: &models.Class{
					Class: "InitialName",
					VectorIndexConfig: map[string]interface{}{
						"some-setting": "new-value",
					},
				},
				expectedError: nil,
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				sm := newSchemaManager()
				assert.Nil(t, sm.AddClass(context.Background(), nil, test.initial))
				err := sm.UpdateClass(context.Background(), nil, test.initial.Class, test.update)
				if test.expectedError == nil {
					assert.Nil(t, err)
				} else {
					require.NotNil(t, err, "update must error")
					assert.Equal(t, test.expectedError.Error(), err.Error())
				}
			})
		}
	})

	t.Run("update vector index config", func(t *testing.T) {
		t.Run("with a validation error", func(t *testing.T) {
			sm := newSchemaManager()
			migrator := &configMigrator{
				vectorConfigValidationError: errors.Errorf("don't think so!"),
			}
			sm.migrator = migrator

			t.Run("create an initial class", func(t *testing.T) {
				err := sm.AddClass(context.Background(), nil, &models.Class{
					Class: "ClassWithVectorIndexConfig",
					VectorIndexConfig: map[string]interface{}{
						"setting-1": "value-1",
					},
				})

				assert.Nil(t, err)
			})

			t.Run("attempt an update of the vector index config", func(t *testing.T) {
				err := sm.UpdateClass(context.Background(), nil,
					"ClassWithVectorIndexConfig", &models.Class{
						Class: "ClassWithVectorIndexConfig",
						VectorIndexConfig: map[string]interface{}{
							"setting-1": "updated-value",
						},
					})
				expectedErrMsg := "vector index config: don't think so!"
				expectedValidateCalledWith := fakeVectorConfig{
					raw: map[string]interface{}{
						"distance":  "cosine",
						"setting-1": "updated-value",
					},
				}
				expectedUpdateCalled := false

				require.NotNil(t, err)
				assert.Equal(t, expectedErrMsg, err.Error())
				assert.Equal(t, expectedValidateCalledWith, migrator.vectorConfigValidateCalledWith)
				assert.Equal(t, expectedUpdateCalled, migrator.vectorConfigUpdateCalled)
			})
		})

		t.Run("with a valid update", func(t *testing.T) {
			sm := newSchemaManager()
			migrator := &configMigrator{}
			sm.migrator = migrator

			t.Run("create an initial class", func(t *testing.T) {
				err := sm.AddClass(context.Background(), nil, &models.Class{
					Class: "ClassWithVectorIndexConfig",
					VectorIndexConfig: map[string]interface{}{
						"setting-1": "value-1",
					},
				})

				assert.Nil(t, err)
			})

			t.Run("update the vector index config", func(t *testing.T) {
				err := sm.UpdateClass(context.Background(), nil,
					"ClassWithVectorIndexConfig", &models.Class{
						Class: "ClassWithVectorIndexConfig",
						VectorIndexConfig: map[string]interface{}{
							"setting-1": "updated-value",
						},
					})
				expectedValidateCalledWith := fakeVectorConfig{
					raw: map[string]interface{}{
						"distance":  "cosine",
						"setting-1": "updated-value",
					},
				}
				expectedUpdateCalledWith := fakeVectorConfig{
					raw: map[string]interface{}{
						"distance":  "cosine",
						"setting-1": "updated-value",
					},
				}
				expectedUpdateCalled := true

				require.Nil(t, err)
				assert.Equal(t, expectedValidateCalledWith, migrator.vectorConfigValidateCalledWith)
				assert.Equal(t, expectedUpdateCalledWith, migrator.vectorConfigUpdateCalledWith)
				assert.Equal(t, expectedUpdateCalled, migrator.vectorConfigUpdateCalled)
			})

			t.Run("the update is reflected", func(t *testing.T) {
				class := sm.getClassByName("ClassWithVectorIndexConfig")
				require.NotNil(t, class)
				expectedVectorIndexConfig := fakeVectorConfig{
					raw: map[string]interface{}{
						"distance":  "cosine",
						"setting-1": "updated-value",
					},
				}

				assert.Equal(t, expectedVectorIndexConfig, class.VectorIndexConfig)
			})
		})
	})

	t.Run("update sharding config", func(t *testing.T) {
		t.Run("with a validation error (immutable field)", func(t *testing.T) {
			sm := newSchemaManager()
			migrator := &NilMigrator{}
			sm.migrator = migrator

			t.Run("create an initial class", func(t *testing.T) {
				err := sm.AddClass(context.Background(), nil, &models.Class{
					Class: "ClassWithShardingConfig",
				})

				assert.Nil(t, err)
			})

			t.Run("attempt an update of the vector index config", func(t *testing.T) {
				err := sm.UpdateClass(context.Background(), nil,
					"ClassWithShardingConfig", &models.Class{
						Class: "ClassWithShardingConfig",
						ShardingConfig: map[string]interface{}{
							"desiredCount": json.Number("7"),
						},
					})
				expectedErrMsg := "re-sharding not supported yet: shard count is immutable: attempted change from \"1\" to \"7\""
				require.NotNil(t, err)
				assert.Contains(t, err.Error(), expectedErrMsg)
			})
		})
	})
}

func TestClassUpdate_ValidateVectorIndexConfigs(t *testing.T) {
	type testCase struct {
		name           string
		initial        *models.Class
		updated        *models.Class
		expectedErrMsg string
	}

	createClass := func(cfg map[string]models.VectorConfig) *models.Class {
		return &models.Class{
			Class:        "TargetVectors",
			VectorConfig: cfg,
		}
	}

	vcFlatContextionary := models.VectorConfig{
		VectorIndexType: "flat",
		Vectorizer: map[string]interface{}{
			"text2vec-contextionary": "some-settings",
		},
		VectorIndexConfig: map[string]interface{}{
			"setting-flat": "value-flat",
		},
	}
	vcHnswContextionary := models.VectorConfig{
		VectorIndexType: "hnsw",
		Vectorizer: map[string]interface{}{
			"text2vec-contextionary": "some-settings",
		},
		VectorIndexConfig: map[string]interface{}{
			"setting-hnsw": "value-hnsw",
		},
	}

	_ = vcFlatContextionary
	_ = vcHnswContextionary

	testCases := []testCase{
		{
			name:           "same settings with nil vectors config",
			initial:        createClass(nil),
			updated:        createClass(nil),
			expectedErrMsg: "",
		},
		{
			name:           "same settings with nil+empty vectors config",
			initial:        createClass(nil),
			updated:        createClass(map[string]models.VectorConfig{}),
			expectedErrMsg: "",
		},
		{
			name:           "same settings with empty+nil vectors config",
			initial:        createClass(map[string]models.VectorConfig{}),
			updated:        createClass(nil),
			expectedErrMsg: "",
		},
		{
			name:           "same settings with empty vectors config",
			initial:        createClass(map[string]models.VectorConfig{}),
			updated:        createClass(map[string]models.VectorConfig{}),
			expectedErrMsg: "",
		},
		{
			name: "same settings with single vector",
			initial: createClass(map[string]models.VectorConfig{
				"vector": vcFlatContextionary,
			}),
			updated: createClass(map[string]models.VectorConfig{
				"vector": vcFlatContextionary,
			}),
			expectedErrMsg: "",
		},
		{
			name: "same settings with multi vectors",
			initial: createClass(map[string]models.VectorConfig{
				"vector1": vcFlatContextionary,
				"vector2": vcHnswContextionary,
			}),
			updated: createClass(map[string]models.VectorConfig{
				"vector2": vcHnswContextionary,
				"vector1": vcFlatContextionary,
			}),
			expectedErrMsg: "",
		},
		{
			name:    "no initial vectors",
			initial: createClass(nil),
			updated: createClass(map[string]models.VectorConfig{
				"vector2": vcHnswContextionary,
				"vector1": vcFlatContextionary,
			}),
			expectedErrMsg: "additional configs for vectors",
		},
		{
			name: "no updated vectors",
			initial: createClass(map[string]models.VectorConfig{
				"vector1": vcFlatContextionary,
				"vector2": vcHnswContextionary,
			}),
			updated:        createClass(nil),
			expectedErrMsg: "missing configs for vectors",
		},
		{
			name: "more updated vectors",
			initial: createClass(map[string]models.VectorConfig{
				"vector1": vcFlatContextionary,
			}),
			updated: createClass(map[string]models.VectorConfig{
				"vector2": vcHnswContextionary,
				"vector1": vcFlatContextionary,
			}),
			expectedErrMsg: "additional config for vector \"vector2\"",
		},
		{
			name: "more initial vectors",
			initial: createClass(map[string]models.VectorConfig{
				"vector1": vcFlatContextionary,
				"vector2": vcHnswContextionary,
			}),
			updated: createClass(map[string]models.VectorConfig{
				"vector1": vcFlatContextionary,
			}),
			expectedErrMsg: "missing config for vector \"vector2\"",
		},
		{
			name: "index type changed",
			initial: createClass(map[string]models.VectorConfig{
				"vector1": vcFlatContextionary,
				"vector2": vcHnswContextionary,
			}),
			updated: createClass(map[string]models.VectorConfig{
				"vector1": vcFlatContextionary,
				"vector2": vcFlatContextionary,
			}),
			expectedErrMsg: "vector index type of vector \"vector2\" is immutable: attempted change from \"hnsw\" to \"flat\"",
		},
		{
			name: "vectorizer changed",
			initial: createClass(map[string]models.VectorConfig{
				"vector1": vcFlatContextionary,
			}),
			updated: createClass(map[string]models.VectorConfig{
				"vector1": {
					VectorIndexType: "flat",
					Vectorizer: map[string]interface{}{
						"not-contextionary": "some-settings",
					},
				},
			}),
			expectedErrMsg: "vectorizer of vector \"vector1\" is immutable: attempted change from \"text2vec-contextionary\" to \"not-contextionary\"",
		},
		{
			name: "vectorizer config not map",
			initial: createClass(map[string]models.VectorConfig{
				"vector1": vcFlatContextionary,
			}),
			updated: createClass(map[string]models.VectorConfig{
				"vector1": {
					VectorIndexType: "flat",
					Vectorizer:      "not-map",
				},
			}),
			expectedErrMsg: "invalid vectorizer config for vector \"vector1\"",
		},
		{
			name: "vectorizer config multiple keys",
			initial: createClass(map[string]models.VectorConfig{
				"vector1": vcFlatContextionary,
			}),
			updated: createClass(map[string]models.VectorConfig{
				"vector1": {
					VectorIndexType: "flat",
					Vectorizer: map[string]interface{}{
						"text2vec-contextionary": "some-settings",
						"additional-key":         "value",
					},
				},
			}),
			expectedErrMsg: "invalid vectorizer config for vector \"vector1\"",
		},
	}

	t.Run("validation only", func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := validateVectorConfigsParityAndImmutables(tc.initial, tc.updated)

				if tc.expectedErrMsg == "" {
					assert.NoError(t, err)
				} else {
					assert.ErrorContains(t, err, tc.expectedErrMsg)
				}
			})
		}
	})

	t.Run("full update", func(t *testing.T) {
		ctx := context.Background()

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				sm := newSchemaManager()
				m := &configMigrator{}
				sm.migrator = m

				err := sm.AddClass(ctx, nil, tc.initial)
				require.NoError(t, err)

				err = sm.UpdateClass(ctx, nil, tc.updated.Class, tc.updated)

				if tc.expectedErrMsg == "" {
					assert.NoError(t, err)
				} else {
					assert.ErrorContains(t, err, tc.expectedErrMsg)
				}

				// migrator's validation and update are called only for configured target vectors
				if tc.expectedErrMsg == "" && len(tc.updated.VectorConfig) > 0 {
					cfgs := map[string]schema.VectorIndexConfig{}
					for vecName, vecCfg := range tc.updated.VectorConfig {
						cfgs[vecName] = vecCfg.VectorIndexConfig.(schema.VectorIndexConfig)
					}

					assert.True(t, m.vectorConfigsUpdateCalled)
					assert.Equal(t, cfgs, m.vectorConfigsValidateCalledWith)
					assert.Equal(t, cfgs, m.vectorConfigsUpdateCalledWith)
				} else {
					assert.False(t, m.vectorConfigsUpdateCalled)
					assert.Nil(t, m.vectorConfigsValidateCalledWith)
					assert.Nil(t, m.vectorConfigsUpdateCalledWith)
				}
			})
		}
	})
}

type configMigrator struct {
	NilMigrator
	vectorConfigValidationError     error
	vectorConfigValidateCalledWith  schema.VectorIndexConfig
	vectorConfigUpdateCalled        bool
	vectorConfigUpdateCalledWith    schema.VectorIndexConfig
	vectorConfigsValidationError    error
	vectorConfigsValidateCalledWith map[string]schema.VectorIndexConfig
	vectorConfigsUpdateCalled       bool
	vectorConfigsUpdateCalledWith   map[string]schema.VectorIndexConfig
}

func (m *configMigrator) ValidateVectorIndexConfigUpdate(ctx context.Context,
	old, updated schema.VectorIndexConfig,
) error {
	m.vectorConfigValidateCalledWith = updated
	return m.vectorConfigValidationError
}

func (m *configMigrator) UpdateVectorIndexConfig(ctx context.Context,
	className string, updated schema.VectorIndexConfig,
) error {
	m.vectorConfigUpdateCalledWith = updated
	m.vectorConfigUpdateCalled = true
	return nil
}

func (m *configMigrator) ValidateVectorIndexConfigsUpdate(ctx context.Context,
	old, updated map[string]schema.VectorIndexConfig,
) error {
	m.vectorConfigsValidateCalledWith = updated
	return m.vectorConfigsValidationError
}

func (m *configMigrator) UpdateVectorIndexConfigs(ctx context.Context,
	className string, updated map[string]schema.VectorIndexConfig,
) error {
	m.vectorConfigsUpdateCalledWith = updated
	m.vectorConfigsUpdateCalled = true
	return nil
}
