package schema

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// As of now, most class settings are immutable, but we need to allow some
// specific updates, such as the vector index config
func TestClassUpdates(t *testing.T) {
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
							DataType: []string{"string"},
						},
					},
				},
				update: &models.Class{
					Class: "InitialName",
					Properties: []*models.Property{
						{
							Name:     "aProp",
							DataType: []string{"string"},
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
							DataType: []string{"string"},
						},
					},
				},
				update: &models.Class{
					Class: "InitialName",
					Properties: []*models.Property{
						{
							Name:     "changedProp",
							DataType: []string{"string"},
						},
					},
				},
				expectedError: errors.Errorf(
					"properties cannot be updated through updating the class. Use the add " +
						"property feature (e.g. \"POST /v1/schema/{className}/properties\") " +
						"to add additional properties"),
			},
			{
				name: "attempting to update the inverted index config",
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
					},
				},
				expectedError: errors.Errorf("inverted index config is immutable"),
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
						"some-setting": "old-value",
					},
				},
				expectedError: nil,
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				sm := newSchemaManager()
				assert.Nil(t, sm.AddObject(context.Background(), nil, test.initial))
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
}
