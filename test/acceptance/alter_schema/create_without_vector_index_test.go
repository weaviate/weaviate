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

package alterschema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

func testCreateClassWithoutVectorIndex() func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("bare class with only properties", func(t *testing.T) {
			className := "CreateWithoutVectorIndexBare"

			deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
			helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
			defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:     "name",
						DataType: []string{schema.DataTypeText.String()},
					},
				},
			}

			createParams := clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls)
			resp, err := helper.Client(t).Schema.SchemaObjectsCreate(createParams, nil)
			require.NoError(t, err, "creating a class without any vector index should succeed")
			require.NotNil(t, resp)

			got := helper.GetClass(t, className)
			require.NotNil(t, got)
			assert.NotEqual(t, "none", got.VectorIndexType,
				"server-applied default must not be the dropped-index sentinel")
			assert.Equal(t, "hnsw", got.VectorIndexType,
				"server-applied default must be hnsw (vectorindex.DefaultVectorIndexType)")
		})

		t.Run("class with only class name", func(t *testing.T) {
			className := "CreateWithoutVectorIndexMinimal"

			deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
			helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
			defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

			cls := &models.Class{Class: className}

			createParams := clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls)
			resp, err := helper.Client(t).Schema.SchemaObjectsCreate(createParams, nil)
			require.NoError(t, err, "creating a minimal class should succeed")
			require.NotNil(t, resp)

			got := helper.GetClass(t, className)
			require.NotNil(t, got)
			assert.NotEqual(t, "none", got.VectorIndexType,
				"server-applied default must not be the dropped-index sentinel")
			assert.Equal(t, "hnsw", got.VectorIndexType,
				"server-applied default must be hnsw (vectorindex.DefaultVectorIndexType)")
		})

		// Mirrors the curl repro: a vectorConfig with a "default" named vector
		// and a vectorizer specified, but no vectorIndexType. The named-vector
		// branch of setClassDefaults must default the empty VectorIndexType to
		// hnsw, not leak the "none" sentinel.
		t.Run("named vector default with vectorizer, no vectorIndexType", func(t *testing.T) {
			className := "CreateWithoutVectorIndexNamedDefault"

			deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
			helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
			defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:     "name",
						DataType: []string{schema.DataTypeText.String()},
					},
				},
				VectorConfig: map[string]models.VectorConfig{
					"default": {
						Vectorizer: map[string]any{
							"text2vec-model2vec": map[string]any{},
						},
						// VectorIndexType intentionally omitted.
					},
				},
			}

			createParams := clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls)
			resp, err := helper.Client(t).Schema.SchemaObjectsCreate(createParams, nil)
			require.NoError(t, err, "creating a class with a named vector but no vectorIndexType should succeed")
			require.NotNil(t, resp)

			got := helper.GetClass(t, className)
			require.NotNil(t, got)
			cfg, ok := got.VectorConfig["default"]
			require.True(t, ok, "named vector \"default\" should be present in the persisted schema")
			assert.NotEqual(t, "none", cfg.VectorIndexType,
				"server-applied default must not be the dropped-index sentinel")
			assert.Equal(t, "hnsw", cfg.VectorIndexType,
				"server-applied default must be hnsw (vectorindex.DefaultVectorIndexType)")
		})

		// Same shape as above but with the "none" vectorizer, so the assertion
		// holds independently of any vectorizer module being available.
		t.Run("named vector default with none vectorizer, no vectorIndexType", func(t *testing.T) {
			className := "CreateWithoutVectorIndexNamedDefaultNoneVec"

			deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
			helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
			defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:     "name",
						DataType: []string{schema.DataTypeText.String()},
					},
				},
				VectorConfig: map[string]models.VectorConfig{
					"default": {
						Vectorizer: map[string]any{
							"none": map[string]any{},
						},
						// VectorIndexType intentionally omitted.
					},
				},
			}

			createParams := clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls)
			resp, err := helper.Client(t).Schema.SchemaObjectsCreate(createParams, nil)
			require.NoError(t, err, "creating a class with a named vector but no vectorIndexType should succeed")
			require.NotNil(t, resp)

			got := helper.GetClass(t, className)
			require.NotNil(t, got)
			cfg, ok := got.VectorConfig["default"]
			require.True(t, ok, "named vector \"default\" should be present in the persisted schema")
			assert.NotEqual(t, "none", cfg.VectorIndexType,
				"server-applied default must not be the dropped-index sentinel")
			assert.Equal(t, "hnsw", cfg.VectorIndexType,
				"server-applied default must be hnsw (vectorindex.DefaultVectorIndexType)")
		})
	}
}
