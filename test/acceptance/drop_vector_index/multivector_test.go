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

package drop_vector_index

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

// testMultiVector pins the drop lifecycle for a multi-vector (ColBERT-style)
// named vector: its per-object vector LISTS must be stripped from storage the
// same way single vectors are, with a regular sibling untouched.
func testMultiVector() func(t *testing.T) {
	return func(t *testing.T) {
		const (
			className = "DropVectorIndexMultiVector"
			colbert   = "colbert"
			sibling   = "sibling"
			dim       = 16
			count     = 20
		)

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		t.Run("create class and insert multi-vector objects", func(t *testing.T) {
			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "name", DataType: []string{schema.DataTypeText.String()}},
				},
				VectorConfig: map[string]models.VectorConfig{
					colbert: {
						Vectorizer:      map[string]any{"none": map[string]any{}},
						VectorIndexType: "hnsw",
						VectorIndexConfig: map[string]any{
							"multivector": map[string]any{"enabled": true},
						},
					},
					sibling: noneVectorConfig(),
				},
			}
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(
				clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls), nil)
			require.NoError(t, err)

			batch := make([]*models.Object, count)
			for i := range count {
				batch[i] = &models.Object{
					ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000000015%02d", i)),
					Class:      className,
					Properties: map[string]any{"name": fmt.Sprintf("object-%d", i)},
					Vectors: models.Vectors{
						colbert: [][]float32{
							randVec(dim, float32(i)),
							randVec(dim, float32(i+100)),
							randVec(dim, float32(i+200)),
						},
						sibling: randVec(dim, float32(i+300)),
					},
				}
			}
			helper.CreateObjectsBatch(t, batch)
			time.Sleep(3 * time.Second) // past the 1s dirty-flush
		})

		t.Run("objects carry the multi-vector before drop", func(t *testing.T) {
			objs := listAllObjectsWithVectors(t, className)
			require.Len(t, objs, count)
			for _, obj := range objs {
				require.Contains(t, obj.Vectors, colbert)
				require.Contains(t, obj.Vectors, sibling)
			}
		})

		t.Run("drop the multi-vector index", func(t *testing.T) {
			dropTargetVector(t, className, colbert)
		})

		t.Run("entry removed, objects stripped, sibling intact", func(t *testing.T) {
			eventuallyTargetVectorRemoved(t, className, colbert)
			got := helper.GetClass(t, className)
			require.Contains(t, got.VectorConfig, sibling)

			objs := listAllObjectsWithVectors(t, className)
			require.Len(t, objs, count)
			for _, obj := range objs {
				require.NotContains(t, obj.Vectors, colbert,
					"the multi-vector lists must be stripped from storage")
				require.Equal(t, dim, vecDim(t, obj.Vectors[sibling]))
			}
			require.Equal(t, 3, nearVectorResults(t, className, sibling, randVec(dim, 7), 3))
		})
	}
}
