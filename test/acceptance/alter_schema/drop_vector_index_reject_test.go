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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

// testRejectNoneVectorIndexType verifies that the "none" sentinel value
// for VectorIndexType cannot be used directly by users to bypass the controlled
// DeleteClassVectorIndex path.
func testRejectNoneVectorIndexType() func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("reject creating class with deleted vector index type", func(t *testing.T) {
			className := "RejectDeletedOnCreate"

			// Clean up in case of previous failed run.
			deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
			helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:     "name",
						DataType: []string{schema.DataTypeText.String()},
					},
				},
				VectorConfig: map[string]models.VectorConfig{
					"v1": {
						Vectorizer: map[string]any{
							"none": map[string]any{},
						},
						VectorIndexType: "none",
					},
				},
			}

			createParams := clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls)
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(createParams, nil)
			require.Error(t, err, "creating a class with vectorIndexType 'none' should fail")

			var createErr *clschema.SchemaObjectsCreateUnprocessableEntity
			require.ErrorAs(t, err, &createErr)
			require.NotNil(t, createErr.Payload)
			require.NotEmpty(t, createErr.Payload.Error)
			assert.Contains(t, createErr.Payload.Error[0].Message, "internal sentinel for dropped indexes")

			// Clean up just in case.
			helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		})

		t.Run("reject creating class with mixed valid and deleted vectors", func(t *testing.T) {
			className := "RejectDeletedMixed"

			deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
			helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:     "name",
						DataType: []string{schema.DataTypeText.String()},
					},
				},
				VectorConfig: map[string]models.VectorConfig{
					"valid_vec": {
						Vectorizer: map[string]any{
							"none": map[string]any{},
						},
						VectorIndexType: "hnsw",
					},
					"bad_vec": {
						Vectorizer: map[string]any{
							"none": map[string]any{},
						},
						VectorIndexType: "none",
					},
				},
			}

			createParams := clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls)
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(createParams, nil)
			require.Error(t, err, "creating a class with any vectorIndexType 'none' should fail")

			// Clean up just in case.
			helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		})

		t.Run("reject re-creating a dropped vector index via update", func(t *testing.T) {
			className := "RejectRecreateDropped"

			deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
			helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:     "name",
						DataType: []string{schema.DataTypeText.String()},
					},
				},
				VectorConfig: map[string]models.VectorConfig{
					"my_vector": {
						Vectorizer: map[string]any{
							"none": map[string]any{},
						},
						VectorIndexType: "hnsw",
					},
				},
			}

			t.Run("create class", func(t *testing.T) {
				createParams := clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls)
				resp, err := helper.Client(t).Schema.SchemaObjectsCreate(createParams, nil)
				helper.AssertRequestOk(t, resp, err, nil)
			})

			t.Run("drop vector index", func(t *testing.T) {
				params := clschema.NewSchemaObjectsVectorsDeleteParams().
					WithClassName(className).
					WithVectorIndexName("my_vector")
				resp, err := helper.Client(t).Schema.SchemaObjectsVectorsDelete(params, nil)
				helper.AssertRequestOk(t, resp, err, nil)
			})

			t.Run("verify vector index is dropped", func(t *testing.T) {
				assert.EventuallyWithT(t, func(collect *assert.CollectT) {
					cls := helper.GetClass(t, className)
					cfg, ok := cls.VectorConfig["my_vector"]
					assert.True(collect, ok, "vector config should still exist")
					if ok {
						assert.Equal(collect, "none", cfg.VectorIndexType)
					}
				}, 15*time.Second, 200*time.Millisecond)
			})

			t.Run("attempt to re-create dropped vector via update is rejected", func(t *testing.T) {
				cls := helper.GetClass(t, className)
				require.NotNil(t, cls)

				// Try to flip the dropped vector back to "hnsw".
				// The parser rejects this as an invalid re-creation of a
				// dropped vector index.
				cls.VectorConfig["my_vector"] = models.VectorConfig{
					Vectorizer: map[string]any{
						"none": map[string]any{},
					},
					VectorIndexType: "hnsw",
				}

				updateParams := clschema.NewSchemaObjectsUpdateParams().
					WithClassName(className).
					WithObjectClass(cls)
				_, err := helper.Client(t).Schema.SchemaObjectsUpdate(updateParams, nil)
				require.Error(t, err, "update should be rejected; re-creating a dropped vector index is not allowed")
			})

			// Clean up.
			t.Run("delete class", func(t *testing.T) {
				deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
				resp, err := helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
				helper.AssertRequestOk(t, resp, err, nil)
			})
		})
	}
}
