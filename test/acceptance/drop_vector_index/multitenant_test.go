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

// testMultiTenant pins the drop lifecycle across many active tenants: finalize
// requires every tenant's cleanup unit to drain, so the entry disappearing
// proves per-tenant coverage, and every tenant's stored objects must come out
// stripped.
func testMultiTenant() func(t *testing.T) {
	return func(t *testing.T) {
		const (
			className   = "DropVectorIndexMultiTenant"
			dropped     = "vec"
			sibling     = "sibling"
			droppedDim  = 32
			siblingDim  = 64
			tenantCount = 20
			objsPerTen  = 20
		)

		tenantName := func(i int) string { return fmt.Sprintf("tenant-%02d", i) }

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		t.Run("create class and tenants", func(t *testing.T) {
			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "name", DataType: []string{schema.DataTypeText.String()}},
				},
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
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
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(
				clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls), nil)
			require.NoError(t, err)

			tenants := make([]*models.Tenant, tenantCount)
			for i := range tenants {
				tenants[i] = &models.Tenant{Name: tenantName(i)}
			}
			helper.CreateTenants(t, className, tenants)
		})

		t.Run("insert objects into every tenant", func(t *testing.T) {
			for ten := range tenantCount {
				batch := make([]*models.Object, objsPerTen)
				for i := range objsPerTen {
					batch[i] = &models.Object{
						ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-00%02d-0000000006%02d", ten, i)),
						Class:      className,
						Tenant:     tenantName(ten),
						Properties: map[string]any{"name": fmt.Sprintf("t%d-object-%d", ten, i)},
						Vectors: models.Vectors{
							dropped: randVec(droppedDim, float32(ten*100+i)),
							sibling: randVec(siblingDim, float32(ten*100+i)),
						},
					}
				}
				helper.CreateObjectsBatch(t, batch)
			}
			time.Sleep(5 * time.Second) // let the 1s dirty-flush turn memtables into segments
		})

		t.Run("all tenants carry both vectors before drop", func(t *testing.T) {
			for ten := range tenantCount {
				objs := listTenantObjectsWithVectors(t, className, tenantName(ten))
				require.Len(t, objs, objsPerTen, "tenant %s", tenantName(ten))
				for _, obj := range objs {
					require.Contains(t, obj.Vectors, dropped)
					require.Equal(t, droppedDim, vecDim(t, obj.Vectors[dropped]))
					require.Equal(t, siblingDim, vecDim(t, obj.Vectors[sibling]))
				}
			}
		})

		t.Run("nearVector works on both vectors before drop", func(t *testing.T) {
			for _, ten := range []int{0, tenantCount - 1} {
				require.Equal(t, 3, nearVectorTenantResults(t, className, tenantName(ten), dropped, randVec(droppedDim, 7), 3))
				require.Equal(t, 3, nearVectorTenantResults(t, className, tenantName(ten), sibling, randVec(siblingDim, 7), 3))
			}
		})

		t.Run("drop the vector index", func(t *testing.T) {
			dropTargetVector(t, className, dropped)
		})

		t.Run("entry is removed from the schema after all tenants drained", func(t *testing.T) {
			eventuallyTargetVectorRemoved(t, className, dropped)
			got := helper.GetClass(t, className)
			require.Contains(t, got.VectorConfig, sibling)
		})

		t.Run("nearVector on the dropped vector fails, sibling still works", func(t *testing.T) {
			for _, ten := range []int{0, tenantCount - 1} {
				require.NotEmpty(t, nearVectorTenantErrors(t, className, tenantName(ten), dropped, randVec(droppedDim, 7)))
				require.Equal(t, 3, nearVectorTenantResults(t, className, tenantName(ten), sibling, randVec(siblingDim, 7), 3))
			}
		})

		t.Run("every tenant's objects are stripped", func(t *testing.T) {
			for ten := range tenantCount {
				objs := listTenantObjectsWithVectors(t, className, tenantName(ten))
				require.Len(t, objs, objsPerTen, "tenant %s", tenantName(ten))
				for _, obj := range objs {
					require.NotContains(t, obj.Vectors, dropped,
						"tenant %s must not retain the dropped vector", tenantName(ten))
					require.Equal(t, siblingDim, vecDim(t, obj.Vectors[sibling]))
				}
			}
		})
	}
}
