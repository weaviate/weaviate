//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objects

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestValidateObjectAndNormalizeNames(t *testing.T) {
	ctx := context.Background()
	cfg := &config.WeaviateConfig{}

	// Mock vectorRepoExists function
	mockExists := func(ctx context.Context, class string, id strfmt.UUID, repl *additional.ReplicationProperties, tenant string) (bool, error) {
		return true, nil
	}

	t.Run("valid object with uppercase properties", func(t *testing.T) {
		incoming := &models.Object{
			Class: "Article",
			ID:    "00000000-0000-0000-0000-000000000001",
			Properties: map[string]interface{}{
				"Title":   "Test Article",
				"Content": "Test content",
			},
		}

		existing := &models.Object{
			Class: "Article",
			ID:    "00000000-0000-0000-0000-000000000001",
		}

		fetchedClasses := map[string]versioned.Class{
			"Article": {
				Class: &models.Class{
					Class: "Article",
					Properties: []*models.Property{
						{Name: "title", DataType: []string{"text"}},
						{Name: "content", DataType: []string{"text"}},
					},
				},
			},
		}

		err := validateObjectAndNormalizeNames(ctx, cfg, mockExists, nil, incoming, existing, fetchedClasses)
		require.NoError(t, err)

		// Check that properties were normalized to lowercase
		props := incoming.Properties.(map[string]interface{})
		assert.Contains(t, props, "title")
		assert.Contains(t, props, "content")
		assert.Equal(t, "Test Article", props["title"])
	})

	t.Run("invalid UUID", func(t *testing.T) {
		incoming := &models.Object{
			Class: "Article",
			ID:    "invalid-uuid",
			Properties: map[string]interface{}{
				"title": "Test",
			},
		}

		fetchedClasses := map[string]versioned.Class{
			"Article": {
				Class: &models.Class{
					Class: "Article",
				},
			},
		}

		err := validateObjectAndNormalizeNames(ctx, cfg, mockExists, nil, incoming, nil, fetchedClasses)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid UUID")
	})

	t.Run("class not found", func(t *testing.T) {
		incoming := &models.Object{
			Class: "NonExistent",
			ID:    "00000000-0000-0000-0000-000000000001",
		}

		fetchedClasses := map[string]versioned.Class{}

		err := validateObjectAndNormalizeNames(ctx, cfg, mockExists, nil, incoming, nil, fetchedClasses)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found in schema")
	})

	t.Run("preserve nil values for deletion", func(t *testing.T) {
		incoming := &models.Object{
			Class: "Article",
			ID:    "00000000-0000-0000-0000-000000000001",
			Properties: map[string]interface{}{
				"Title":   "New Title",
				"Content": nil, // This should be preserved for deletion
			},
		}

		existing := &models.Object{
			Class: "Article",
			ID:    "00000000-0000-0000-0000-000000000001",
		}

		fetchedClasses := map[string]versioned.Class{
			"Article": {
				Class: &models.Class{
					Class: "Article",
					Properties: []*models.Property{
						{Name: "title", DataType: []string{"text"}},
						{Name: "content", DataType: []string{"text"}},
					},
				},
			},
		}

		err := validateObjectAndNormalizeNames(ctx, cfg, mockExists, nil, incoming, existing, fetchedClasses)
		require.NoError(t, err)

		// Note: validation.Object() normalizes properties and removes nil values
		// The caller (batch_patch.go) handles nil detection before calling this function
		props := incoming.Properties.(map[string]interface{})
		assert.Contains(t, props, "title")
		assert.Equal(t, "New Title", props["title"])
	})
}

func TestMergeObjectSchemaAndVectorizeShared(t *testing.T) {
	ctx := context.Background()

	mockLogger := logrus.New()

	t.Run("merge properties from previous and new", func(t *testing.T) {
		// Mock modules provider that does nothing (no vectorization)
		mockModulesProvider := &fakeModulesProvider{}
		mockModulesProvider.On("UpdateVector", mock.Anything, mock.Anything).Return(nil, nil)
		prevProps := map[string]interface{}{
			"title":   "Old Title",
			"content": "Old Content",
			"author":  "John Doe",
		}

		nextProps := map[string]interface{}{
			"title":   "New Title",
			"content": "New Content",
		}

		class := &models.Class{
			Class:      "Article",
			Vectorizer: "none",
		}

		id := strfmt.UUID("00000000-0000-0000-0000-000000000001")

		result, err := mergeObjectSchemaAndVectorizeShared(
			ctx,
			prevProps,
			nextProps,
			[]float32{1.0, 2.0, 3.0}, // prevVec
			nil,                      // nextVec
			nil,                      // prevVecs
			nil,                      // nextVecs
			id,
			class,
			mockModulesProvider,
			nil, // findObject not called in test
			mockLogger,
		)

		require.NoError(t, err)
		assert.NotNil(t, result)

		// Check merged properties
		props := result.Properties.(map[string]interface{})
		assert.Equal(t, "New Title", props["title"])     // Updated
		assert.Equal(t, "New Content", props["content"]) // Updated
		assert.Equal(t, "John Doe", props["author"])     // Preserved

		// Check vector was preserved
		assert.Equal(t, []float32{1.0, 2.0, 3.0}, []float32(result.Vector))
	})

	t.Run("use new vector when provided", func(t *testing.T) {
		// Mock modules provider that does nothing (no vectorization)
		mockModulesProvider := &fakeModulesProvider{}
		mockModulesProvider.On("UpdateVector", mock.Anything, mock.Anything).Return(nil, nil)

		prevProps := map[string]interface{}{
			"title": "Old Title",
		}

		nextProps := map[string]interface{}{
			"title": "New Title",
		}

		class := &models.Class{
			Class:      "Article",
			Vectorizer: "none",
		}

		id := strfmt.UUID("00000000-0000-0000-0000-000000000001")

		result, err := mergeObjectSchemaAndVectorizeShared(
			ctx,
			prevProps,
			nextProps,
			[]float32{1.0, 2.0, 3.0}, // prevVec
			[]float32{4.0, 5.0, 6.0}, // nextVec - new vector
			nil,
			nil,
			id,
			class,
			mockModulesProvider,
			nil, // findObject not called in test
			mockLogger,
		)

		require.NoError(t, err)
		assert.Equal(t, []float32{4.0, 5.0, 6.0}, []float32(result.Vector)) // New vector used
	})

	t.Run("handle nil previous properties", func(t *testing.T) {
		// Mock modules provider that does nothing (no vectorization)
		mockModulesProvider := &fakeModulesProvider{}
		mockModulesProvider.On("UpdateVector", mock.Anything, mock.Anything).Return(nil, nil)

		nextProps := map[string]interface{}{
			"title": "New Title",
		}

		class := &models.Class{
			Class:      "Article",
			Vectorizer: "none",
		}

		id := strfmt.UUID("00000000-0000-0000-0000-000000000001")

		result, err := mergeObjectSchemaAndVectorizeShared(
			ctx,
			nil, // No previous properties
			nextProps,
			nil,
			nil,
			nil,
			nil,
			id,
			class,
			mockModulesProvider,
			nil, // findObject not called in test
			mockLogger,
		)

		require.NoError(t, err)
		props := result.Properties.(map[string]interface{})
		assert.Equal(t, "New Title", props["title"])
	})

	t.Run("handle named vectors", func(t *testing.T) {
		// Mock modules provider that does nothing (no vectorization)
		mockModulesProvider := &fakeModulesProvider{}
		mockModulesProvider.On("UpdateVector", mock.Anything, mock.Anything).Return(nil, nil)

		prevProps := map[string]interface{}{
			"title": "Old Title",
		}

		nextProps := map[string]interface{}{
			"title": "New Title",
		}

		class := &models.Class{
			Class:      "Article",
			Vectorizer: "none",
			VectorConfig: map[string]models.VectorConfig{
				"custom_vector": {
					Vectorizer: map[string]interface{}{
						"none": nil,
					},
				},
			},
		}

		id := strfmt.UUID("00000000-0000-0000-0000-000000000001")

		prevVecs := models.Vectors{
			"custom_vector": []float32{1.0, 2.0, 3.0},
		}

		result, err := mergeObjectSchemaAndVectorizeShared(
			ctx,
			prevProps,
			nextProps,
			nil,
			nil,
			prevVecs, // Previous named vectors
			nil,      // No new named vectors
			id,
			class,
			mockModulesProvider,
			nil, // findObject not called in test
			mockLogger,
		)

		require.NoError(t, err)
		assert.NotNil(t, result.Vectors)
		assert.Contains(t, result.Vectors, "custom_vector")
		assert.Equal(t, []float32{1.0, 2.0, 3.0}, result.Vectors["custom_vector"])
	})
}
