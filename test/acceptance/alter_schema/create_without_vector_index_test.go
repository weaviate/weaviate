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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

// assertVectorless pins the shape of a collection that asked for no vector:
// no named vectors and no legacy fields, not even the server's defaults.
func assertVectorless(t *testing.T, cls *models.Class) {
	t.Helper()
	assert.Empty(t, cls.VectorConfig, "a class that configured no vector must have no named vectors")
	assert.Empty(t, cls.Vectorizer, "the server default vectorizer module must not create a legacy index")
	assert.Empty(t, cls.VectorIndexType, "the server default index type must not create a legacy index")
	assert.Nil(t, cls.VectorIndexConfig)
}

func testCreateClassWithoutVectorIndex() func(t *testing.T) {
	return func(t *testing.T) {
		// A class that configures no vector at all must come back genuinely
		// vector-less: the server defaults must never be what creates an
		// index. The journey continues past the schema to prove the
		// collection is usable and recoverable.
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
			assertVectorless(t, got)

			t.Run("objects without a vector are accepted", func(t *testing.T) {
				require.NoError(t, helper.CreateObject(t, &models.Object{
					ID:         "00000000-0000-0000-0000-00000000ce01",
					Class:      className,
					Properties: map[string]any{"name": "no vector here"},
				}))
			})

			t.Run("objects carrying a vector are rejected", func(t *testing.T) {
				_, err := helper.CreateObjectWithResponse(t, &models.Object{
					ID:         "00000000-0000-0000-0000-00000000ce02",
					Class:      className,
					Properties: map[string]any{"name": "legacy vector"},
					Vector:     []float32{1, 2, 3},
				})
				require.Error(t, err, "a legacy vector must be rejected by a collection without an index")

				_, err = helper.CreateObjectWithResponse(t, &models.Object{
					ID:         "00000000-0000-0000-0000-00000000ce03",
					Class:      className,
					Properties: map[string]any{"name": "named vector"},
					Vectors:    models.Vectors{"whatever": []float32{1, 2, 3}},
				})
				require.Error(t, err, "a named vector must be rejected by a collection without an index")
			})

			t.Run("a vector search reports the missing index", func(t *testing.T) {
				query := fmt.Sprintf(`{Get{%s(nearVector:{vector:[1,2,3]}){name}}}`, className)
				errs := graphqlhelper.ErrorGraphQL(t, nil, query)
				require.NotEmpty(t, errs)
			})

			t.Run("a named vector can be added afterwards", func(t *testing.T) {
				got := helper.GetClass(t, className)
				require.NotNil(t, got)
				got.VectorConfig = map[string]models.VectorConfig{
					"added": {
						Vectorizer:      map[string]any{"none": map[string]any{}},
						VectorIndexType: "hnsw",
					},
				}
				_, err := helper.Client(t).Schema.SchemaObjectsUpdate(
					clschema.NewSchemaObjectsUpdateParams().
						WithClassName(className).WithObjectClass(got), nil)
				require.NoError(t, err, "adding a named vector to a vector-less collection should succeed")

				updated := helper.GetClass(t, className)
				require.NotNil(t, updated)
				require.Contains(t, updated.VectorConfig, "added")
				assert.Empty(t, updated.Vectorizer, "adding a named vector must not introduce a legacy index")
				assert.Empty(t, updated.VectorIndexType)

				require.NoError(t, helper.CreateObject(t, &models.Object{
					ID:         "00000000-0000-0000-0000-00000000ce04",
					Class:      className,
					Properties: map[string]any{"name": "now vectorized"},
					Vectors:    models.Vectors{"added": []float32{1, 2, 3}},
				}))
			})
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
			assertVectorless(t, got)
		})

		t.Run("multi-tenant bare class", func(t *testing.T) {
			const (
				className = "CreateWithoutVectorIndexMT"
				tenant    = "tenant1"
			)

			deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
			helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
			defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "name", DataType: []string{schema.DataTypeText.String()}},
				},
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
			}

			createParams := clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls)
			resp, err := helper.Client(t).Schema.SchemaObjectsCreate(createParams, nil)
			require.NoError(t, err)
			require.NotNil(t, resp)

			got := helper.GetClass(t, className)
			require.NotNil(t, got)
			assertVectorless(t, got)

			helper.CreateTenants(t, className, []*models.Tenant{{Name: tenant}})

			require.NoError(t, helper.CreateObject(t, &models.Object{
				ID:         "00000000-0000-0000-0000-00000000ce21",
				Class:      className,
				Tenant:     tenant,
				Properties: map[string]any{"name": "tenant object"},
			}))
		})

		// The legacy index stays available to anyone who asks for it: a
		// class-level vectorizer alone is enough, and the index type is then
		// filled from the server default.
		t.Run("explicit vectorizer keeps the legacy index", func(t *testing.T) {
			className := "CreateWithoutVectorIndexExplicitLegacy"

			deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
			helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
			defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "name", DataType: []string{schema.DataTypeText.String()}},
				},
				Vectorizer: "none",
			}

			createParams := clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls)
			resp, err := helper.Client(t).Schema.SchemaObjectsCreate(createParams, nil)
			require.NoError(t, err)
			require.NotNil(t, resp)

			got := helper.GetClass(t, className)
			require.NotNil(t, got)
			assert.Equal(t, "none", got.Vectorizer)
			assert.Equal(t, "hnsw", got.VectorIndexType,
				"server-applied default must be hnsw (vectorindex.DefaultVectorIndexType)")
			require.NotNil(t, got.VectorIndexConfig)

			require.NoError(t, helper.CreateObject(t, &models.Object{
				ID:         "00000000-0000-0000-0000-00000000ce11",
				Class:      className,
				Properties: map[string]any{"name": "legacy vector"},
				Vector:     []float32{1, 2, 3},
			}))
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
