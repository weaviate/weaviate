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

// testTenantMutationDuringDrop pins the tenant-mutation guard: deactivating a
// tenant while a drop is in flight is rejected (its shard's cleanup unit would
// lose its data), and the same mutation succeeds once the drop finalized.
func testTenantMutationDuringDrop() func(t *testing.T) {
	return func(t *testing.T) {
		const (
			className = "DropVectorIndexTenantMutation"
			dropped   = "vec"
			sibling   = "sibling"
			dim       = 32
			tenant1   = "tenant-1"
			tenant2   = "tenant-2"
		)

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		t.Run("create class, tenants, and objects", func(t *testing.T) {
			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "name", DataType: []string{schema.DataTypeText.String()}},
				},
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
				VectorConfig: map[string]models.VectorConfig{
					dropped: noneVectorConfig(), sibling: noneVectorConfig(),
				},
			}
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(
				clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls), nil)
			require.NoError(t, err)
			helper.CreateTenants(t, className, []*models.Tenant{{Name: tenant1}, {Name: tenant2}})

			for ten, tenant := range map[int]string{0: tenant1, 1: tenant2} {
				batch := make([]*models.Object, 10)
				for i := range 10 {
					batch[i] = &models.Object{
						ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-00%02d-0000000014%02d", ten, i)),
						Class:      className,
						Tenant:     tenant,
						Properties: map[string]any{"name": fmt.Sprintf("object-%d", i)},
						Vectors: models.Vectors{
							dropped: randVec(dim, float32(i)),
							sibling: randVec(dim, float32(i+100)),
						},
					}
				}
				helper.CreateObjectsBatch(t, batch)
			}
			time.Sleep(3 * time.Second) // past the 1s dirty-flush
		})

		t.Run("deactivating a tenant mid-drop is rejected", func(t *testing.T) {
			dropTargetVector(t, className, dropped)

			// Guaranteed still in flight: the first poll tick is 30s away.
			err := helper.UpdateTenantsReturnError(t, className, []*models.Tenant{
				{Name: tenant2, ActivityStatus: models.TenantActivityStatusCOLD},
			})
			require.Error(t, err, "tenant deactivation must be blocked while the drop is in flight")
			require.Contains(t, errorResponseText(err), "wait for it to complete")
		})

		t.Run("deactivation succeeds after the drop finalized", func(t *testing.T) {
			eventuallyTargetVectorRemoved(t, className, dropped)
			helper.UpdateTenants(t, className, []*models.Tenant{
				{Name: tenant2, ActivityStatus: models.TenantActivityStatusCOLD},
			})
		})

		t.Run("active tenant is stripped with sibling intact", func(t *testing.T) {
			objs := listTenantObjectsWithVectors(t, className, tenant1)
			require.Len(t, objs, 10)
			for _, obj := range objs {
				require.NotContains(t, obj.Vectors, dropped)
				require.Equal(t, dim, vecDim(t, obj.Vectors[sibling]))
			}
		})
	}
}
