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
	clobjects "github.com/weaviate/weaviate/client/objects"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

// testLifecycle drives the full drop lifecycle end to end: insert -> flush ->
// drop -> cleanup drains -> the VectorConfig entry is removed -> the name is
// re-creatable with a fresh index (even with a new dimensionality).
func testLifecycle() func(t *testing.T) {
	return func(t *testing.T) {
		const (
			className    = "DropVectorIndexLifecycle"
			dropped      = "vec_rq8" // 512-dim, dropped and later re-created with 256 dims
			sibling      = "vec_rq1" // 768-dim, must survive untouched
			droppedDim   = 512
			siblingDim   = 768
			recreatedDim = 256
			initialCount = 50
			newCount     = 5
		)

		rq8Config := models.VectorConfig{
			Vectorizer:      map[string]any{"none": map[string]any{}},
			VectorIndexType: "hnsw",
			VectorIndexConfig: map[string]any{
				"rq": map[string]any{"enabled": true, "bits": 8},
			},
		}

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		t.Run("create class", func(t *testing.T) {
			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "name", DataType: []string{schema.DataTypeText.String()}},
				},
				VectorConfig: map[string]models.VectorConfig{
					dropped: rq8Config,
					sibling: {
						Vectorizer:      map[string]any{"none": map[string]any{}},
						VectorIndexType: "hnsw",
						VectorIndexConfig: map[string]any{
							"rq": map[string]any{"enabled": true, "bits": 1},
						},
					},
				},
			}
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(
				clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls), nil)
			require.NoError(t, err)
		})

		t.Run("insert objects with both vectors", func(t *testing.T) {
			for i := range initialCount {
				obj := &models.Object{
					ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000000001%02d", i)),
					Class:      className,
					Properties: map[string]any{"name": fmt.Sprintf("object-%d", i)},
					Vectors: models.Vectors{
						dropped: randVec(droppedDim, float32(i)),
						sibling: randVec(siblingDim, float32(i)),
					},
				}
				_, err := helper.Client(t).Objects.ObjectsCreate(
					clobjects.NewObjectsCreateParams().WithBody(obj), nil)
				require.NoError(t, err)
			}

			// Past the 1s dirty-flush: cleanup gets real segments to rewrite.
			time.Sleep(5 * time.Second)
		})

		t.Run("nearVector works on both vectors before drop", func(t *testing.T) {
			require.Equal(t, 3, nearVectorResults(t, className, dropped, randVec(droppedDim, 7), 3))
			require.Equal(t, 3, nearVectorResults(t, className, sibling, randVec(siblingDim, 7), 3))
		})

		t.Run("all objects carry both vectors before drop", func(t *testing.T) {
			objs := listAllObjectsWithVectors(t, className)
			require.Len(t, objs, initialCount)
			for _, obj := range objs {
				require.Contains(t, obj.Vectors, dropped)
				require.Contains(t, obj.Vectors, sibling)
				require.Equal(t, droppedDim, vecDim(t, obj.Vectors[dropped]))
				require.Equal(t, siblingDim, vecDim(t, obj.Vectors[sibling]))
			}
		})

		t.Run("drop the vector index", func(t *testing.T) {
			dropTargetVector(t, className, dropped)
		})

		t.Run("entry is removed from the schema after cleanup", func(t *testing.T) {
			eventuallyTargetVectorRemoved(t, className, dropped)
			got := helper.GetClass(t, className)
			require.Contains(t, got.VectorConfig, sibling, "sibling vector must survive the drop")
		})

		t.Run("nearVector on the dropped vector fails", func(t *testing.T) {
			require.NotEmpty(t, nearVectorErrors(t, className, dropped, randVec(droppedDim, 7)))
		})

		t.Run("nearVector on the sibling still works", func(t *testing.T) {
			require.Equal(t, 3, nearVectorResults(t, className, sibling, randVec(siblingDim, 7), 3))
		})

		t.Run("stored objects no longer carry the dropped vector", func(t *testing.T) {
			objs := listAllObjectsWithVectors(t, className)
			require.Len(t, objs, initialCount)
			for _, obj := range objs {
				require.NotContains(t, obj.Vectors, dropped)
				require.Equal(t, siblingDim, vecDim(t, obj.Vectors[sibling]))
			}
		})

		t.Run("re-create the vector under the same name", func(t *testing.T) {
			cls := helper.GetClass(t, className)
			cls.VectorConfig[dropped] = rq8Config
			_, err := helper.Client(t).Schema.SchemaObjectsUpdate(
				clschema.NewSchemaObjectsUpdateParams().WithClassName(className).WithObjectClass(cls), nil)
			require.NoError(t, err, "the name must be re-creatable once the drop finalized")
		})

		t.Run("insert objects with the re-created vector at a new dimensionality", func(t *testing.T) {
			for i := range newCount {
				obj := &models.Object{
					ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000000002%02d", i)),
					Class:      className,
					Properties: map[string]any{"name": fmt.Sprintf("new-object-%d", i)},
					Vectors: models.Vectors{
						dropped: randVec(recreatedDim, float32(i)),
					},
				}
				_, err := helper.Client(t).Objects.ObjectsCreate(
					clobjects.NewObjectsCreateParams().WithBody(obj), nil)
				require.NoError(t, err, "the re-created index is fresh, a new dimensionality is legal")
			}
		})

		t.Run("old objects have no re-created vector, new ones have it at 256 dims", func(t *testing.T) {
			objs := listAllObjectsWithVectors(t, className)
			require.Len(t, objs, initialCount+newCount)
			var withNew int
			for _, obj := range objs {
				if v, ok := obj.Vectors[dropped]; ok {
					withNew++
					require.Equal(t, recreatedDim, vecDim(t, v))
				} else {
					require.Equal(t, siblingDim, vecDim(t, obj.Vectors[sibling]),
						"pre-drop objects keep only the sibling vector")
				}
			}
			require.Equal(t, newCount, withNew)
		})

		t.Run("nearVector works on the re-created vector", func(t *testing.T) {
			require.Equal(t, newCount, nearVectorResults(t, className, dropped, randVec(recreatedDim, 2), newCount))
		})
	}
}
