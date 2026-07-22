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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

// testColdTenantDeferredFinalize pins the cold-tenant completion contract: a
// drop finalizes only once every tenant's shard has been cleaned. With one
// tenant COLD at enqueue the marker must stay after the hot tenants are done;
// reactivating the tenant lets reconciliation re-enqueue a cleanup that covers
// it, and only then is the VectorConfig entry removed.
func testColdTenantDeferredFinalize() func(t *testing.T) {
	return func(t *testing.T) {
		const (
			className = "DropVectorIndexColdTenant"
			dropped   = "vec"
			sibling   = "sibling"
			dim       = 32
			perTenant = 10
		)
		tenants := []string{"tenant-1", "tenant-2", "tenant-3"}
		coldTenant := tenants[2]

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
			helper.CreateTenants(t, className,
				[]*models.Tenant{{Name: tenants[0]}, {Name: tenants[1]}, {Name: tenants[2]}})

			for ten, tenant := range tenants {
				batch := make([]*models.Object, perTenant)
				for i := range perTenant {
					batch[i] = &models.Object{
						ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-00%02d-0000000022%02d", ten, i)),
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

		t.Run("deactivate one tenant, then drop", func(t *testing.T) {
			helper.UpdateTenants(t, className, []*models.Tenant{
				{Name: coldTenant, ActivityStatus: models.TenantActivityStatusCOLD},
			})
			dropTargetVector(t, className, dropped)
		})

		t.Run("hot tenants are cleaned while the marker stays for the cold one", func(t *testing.T) {
			for _, tenant := range tenants[:2] {
				require.EventuallyWithT(t, func(collect *assert.CollectT) {
					objs := listTenantObjectsWithVectors(t, className, tenant)
					if !assert.Len(collect, objs, perTenant) {
						return
					}
					for _, obj := range objs {
						assert.NotContains(collect, obj.Vectors, dropped)
					}
				}, finalizeTimeout, time.Second, "tenant %s cleaned", tenant)
			}

			// Hold across several reconcile cycles: deferral must be stable,
			// not just slow.
			for range 4 {
				got := helper.GetClass(t, className)
				cfg, ok := got.VectorConfig[dropped]
				require.True(t, ok, "marker must persist while a tenant is cold")
				require.Equal(t, "none", cfg.VectorIndexType)
				time.Sleep(3 * time.Second)
			}
		})

		t.Run("class update removing the marker is rejected while a tenant is uncleaned", func(t *testing.T) {
			// The FSM removal gate must not accept a task that completed with
			// the cold tenant's shard uncovered as a voucher.
			cls := helper.GetClass(t, className)
			delete(cls.VectorConfig, dropped)
			_, err := helper.Client(t).Schema.SchemaObjectsUpdate(
				clschema.NewSchemaObjectsUpdateParams().WithClassName(className).WithObjectClass(cls), nil)
			require.Error(t, err)
			require.Contains(t, errorResponseText(err), "cannot remove dropped vector")

			got := helper.GetClass(t, className)
			require.Equal(t, "none", got.VectorConfig[dropped].VectorIndexType,
				"the marker must survive the rejected update")
		})

		t.Run("activate the cold tenant", func(t *testing.T) {
			// Retry: the mutation guard rejects tenant changes while a
			// re-enqueued cleanup task is momentarily active.
			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				err := helper.UpdateTenantsReturnError(t, className, []*models.Tenant{
					{Name: coldTenant, ActivityStatus: models.TenantActivityStatusHOT},
				})
				assert.NoError(collect, err)
			}, 3*time.Minute, 500*time.Millisecond)
		})

		t.Run("reconciliation cleans the reactivated tenant and the drop finalizes", func(t *testing.T) {
			eventuallyTargetVectorRemoved(t, className, dropped)

			for _, tenant := range tenants {
				objs := listTenantObjectsWithVectors(t, className, tenant)
				require.Len(t, objs, perTenant)
				for _, obj := range objs {
					require.NotContains(t, obj.Vectors, dropped,
						"tenant %s must be stripped after the deferred cleanup", tenant)
					require.Equal(t, dim, vecDim(t, obj.Vectors[sibling]))
				}
				require.Equal(t, 3, nearVectorTenantResults(t, className, tenant, sibling, randVec(dim, 7), 3))
			}
		})
	}
}
