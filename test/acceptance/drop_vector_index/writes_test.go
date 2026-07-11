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
	clbatch "github.com/weaviate/weaviate/client/batch"
	clobjects "github.com/weaviate/weaviate/client/objects"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

// testWriteMatrix pins write behavior while the marker is set and after
// finalize: carrying writes rejected on every path (single + batch), clean
// writes/PATCH/refs never collateral damage.
func testWriteMatrix() func(t *testing.T) {
	return func(t *testing.T) {
		const (
			className       = "DropVectorIndexWrites"
			targetClassName = "DropVectorIndexWritesTarget"
			dropped         = "vec"
			sibling         = "sibling"
			droppedDim      = 32
			siblingDim      = 64
			baseCount       = 10
		)

		baseID := func(i int) strfmt.UUID {
			return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000000003%02d", i))
		}
		targetID := strfmt.UUID("00000000-0000-0000-0000-000000000400")

		cleanupClasses := func() {
			for _, class := range []string{className, targetClassName} {
				deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(class)
				helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
			}
		}
		cleanupClasses()
		defer cleanupClasses()

		t.Run("create classes", func(t *testing.T) {
			target := &models.Class{
				Class: targetClassName,
				Properties: []*models.Property{
					{Name: "name", DataType: []string{schema.DataTypeText.String()}},
				},
			}
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(
				clschema.NewSchemaObjectsCreateParams().WithObjectClass(target), nil)
			require.NoError(t, err)

			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "name", DataType: []string{schema.DataTypeText.String()}},
					{Name: "toTarget", DataType: []string{targetClassName}},
				},
				VectorConfig: map[string]models.VectorConfig{
					dropped: {
						Vectorizer:      map[string]any{"none": map[string]any{}},
						VectorIndexType: "hnsw",
					},
					sibling: {
						Vectorizer:      map[string]any{"none": map[string]any{}},
						VectorIndexType: "hnsw",
					},
				},
			}
			_, err = helper.Client(t).Schema.SchemaObjectsCreate(
				clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls), nil)
			require.NoError(t, err)
		})

		t.Run("insert base objects", func(t *testing.T) {
			for i := range baseCount {
				obj := &models.Object{
					ID:         baseID(i),
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
			_, err := helper.Client(t).Objects.ObjectsCreate(
				clobjects.NewObjectsCreateParams().WithBody(&models.Object{
					ID:         targetID,
					Class:      targetClassName,
					Properties: map[string]any{"name": "target"},
				}), nil)
			require.NoError(t, err)

			time.Sleep(3 * time.Second) // past the 1s dirty-flush
		})

		t.Run("drop the vector index", func(t *testing.T) {
			dropTargetVector(t, className, dropped)
		})

		// wantErr pins the phase transition: marker set -> dropped-vector reject;
		// after finalize the entry is gone -> plain unknown-vector reject.
		writeMatrix := func(phase string, idOffset int, wantErr string) func(t *testing.T) {
			return func(t *testing.T) {
				mkID := func(i int) strfmt.UUID {
					return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000000005%02d", idOffset+i))
				}

				t.Run("create carrying the dropped vector is rejected", func(t *testing.T) {
					_, err := helper.Client(t).Objects.ObjectsCreate(
						clobjects.NewObjectsCreateParams().WithBody(&models.Object{
							ID:         mkID(0),
							Class:      className,
							Properties: map[string]any{"name": phase},
							Vectors:    models.Vectors{dropped: randVec(droppedDim, 99)},
						}), nil)
					require.Error(t, err)
					require.Contains(t, errorResponseText(err), wantErr)
				})

				t.Run("create carrying only the sibling succeeds", func(t *testing.T) {
					_, err := helper.Client(t).Objects.ObjectsCreate(
						clobjects.NewObjectsCreateParams().WithBody(&models.Object{
							ID:         mkID(1),
							Class:      className,
							Properties: map[string]any{"name": phase},
							Vectors:    models.Vectors{sibling: randVec(siblingDim, 99)},
						}), nil)
					require.NoError(t, err, "an object not carrying the dropped vector must not be collateral damage")
				})

				t.Run("batch mixing carrying and clean objects splits per object", func(t *testing.T) {
					resp, err := helper.Client(t).Batch.BatchObjectsCreate(
						clbatch.NewBatchObjectsCreateParams().WithBody(clbatch.BatchObjectsCreateBody{
							Objects: []*models.Object{
								{
									ID: mkID(2), Class: className,
									Properties: map[string]any{"name": phase},
									Vectors:    models.Vectors{dropped: randVec(droppedDim, 98)},
								},
								{
									ID: mkID(3), Class: className,
									Properties: map[string]any{"name": phase},
									Vectors:    models.Vectors{sibling: randVec(siblingDim, 98)},
								},
							},
						}), nil)
					require.NoError(t, err)
					require.Len(t, resp.Payload, 2)
					require.Contains(t, batchItemError(resp.Payload[0]), wantErr,
						"the carrying batch item must be rejected")
					require.Empty(t, batchItemError(resp.Payload[1]),
						"the clean batch item must succeed")
				})
			}
		}

		t.Run("write matrix while the marker is set", writeMatrix("during-cleanup", 0, "vector index"))

		t.Run("property PATCH on an object that carried the vector succeeds", func(t *testing.T) {
			_, err := helper.Client(t).Objects.ObjectsClassPatch(
				clobjects.NewObjectsClassPatchParams().WithClassName(className).WithID(baseID(0)).
					WithBody(&models.Object{
						Class:      className,
						ID:         baseID(0),
						Properties: map[string]any{"name": "patched"},
					}), nil)
			require.NoError(t, err)

			obj, err := helper.GetObject(t, className, baseID(0), "vector")
			require.NoError(t, err)
			require.Equal(t, "patched", obj.Properties.(map[string]any)["name"])
			require.Contains(t, obj.Vectors, sibling, "the sibling vector must survive a property PATCH")
		})

		t.Run("batch reference add to an object that carried the vector succeeds", func(t *testing.T) {
			resp, err := helper.AddReferences(t, []*models.BatchReference{{
				From: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s/%s/toTarget", className, baseID(1))),
				To:   strfmt.URI(fmt.Sprintf("weaviate://localhost/%s/%s", targetClassName, targetID)),
			}})
			helper.CheckReferencesBatchResponse(t, resp, err)
		})

		t.Run("entry is removed from the schema after cleanup", func(t *testing.T) {
			eventuallyTargetVectorRemoved(t, className, dropped)
		})

		t.Run("write matrix after finalize", writeMatrix("after-finalize", 10, "does not have configuration for vector"))

		t.Run("no surviving object carries the dropped vector", func(t *testing.T) {
			objs := listAllObjectsWithVectors(t, className)
			for _, obj := range objs {
				require.NotContains(t, obj.Vectors, dropped)
			}
		})
	}
}
