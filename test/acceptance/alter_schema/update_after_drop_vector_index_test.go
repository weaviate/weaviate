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

// testUpdateClassAfterDropVectorIndex verifies that updating a class
// after dropping a vector index does not panic. This is a regression test for:
// https://github.com/weaviate/weaviate/pull/10941
func testUpdateClassAfterDropVectorIndex() func(t *testing.T) {
	return func(t *testing.T) {
		className := "UpdateAfterDropVectorIndex"

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

		t.Run("verify vector index dropped", func(t *testing.T) {
			assert.EventuallyWithT(t, func(collect *assert.CollectT) {
				cls := helper.GetClass(t, className)
				cfg, ok := cls.VectorConfig["my_vector"]
				assert.True(collect, ok, "vector config should still exist")
				if ok {
					assert.Equal(collect, "none", cfg.VectorIndexType, "VectorIndexType should be 'none'")
					assert.Nil(collect, cfg.VectorIndexConfig, "VectorIndexConfig should be nil")
				}
			}, 15*time.Second, 200*time.Millisecond)
		})

		t.Run("update class description after drop should not panic", func(t *testing.T) {
			cls := helper.GetClass(t, className)
			require.NotNil(t, cls)

			cls.Description = "updated description after dropping vector index"

			updateParams := clschema.NewSchemaObjectsUpdateParams().
				WithClassName(className).
				WithObjectClass(cls)
			resp, err := helper.Client(t).Schema.SchemaObjectsUpdate(updateParams, nil)
			helper.AssertRequestOk(t, resp, err, nil)
		})

		t.Run("verify description updated", func(t *testing.T) {
			cls := helper.GetClass(t, className)
			require.NotNil(t, cls)
			assert.Equal(t, "updated description after dropping vector index", cls.Description)
		})

		// Clean up.
		t.Run("delete class", func(t *testing.T) {
			deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
			resp, err := helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
			helper.AssertRequestOk(t, resp, err, nil)
		})
	}
}
