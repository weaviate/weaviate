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

// testPartiallyColdTenants pins the cleaned-shard coverage chain: a drop
// completes once every tenant has been CLEANED, even when one of them is cold
// again at the end (steps in the t.Run names). Requires the suite compose's
// tightened reconcile interval.
func testPartiallyColdTenants() func(t *testing.T) {
	return func(t *testing.T) {
		const (
			className = "DropVectorIndexPartiallyColdTenants"
			dropped   = "vec"
			sibling   = "sibling"
			dim       = 32
			perTenant = 10
		)
		tenants := []string{"tenant-1", "tenant-2", "tenant-3", "tenant-4"}

		// setTenantStatus retries: the tenant-mutation guard rejects changes while
		// a (re-enqueued) cleanup task is momentarily active.
		setTenantStatus := func(t *testing.T, tenant, status string) {
			t.Helper()
			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				err := helper.UpdateTenantsReturnError(t, className, []*models.Tenant{
					{Name: tenant, ActivityStatus: status},
				})
				assert.NoError(collect, err)
			}, 3*time.Minute, 500*time.Millisecond, "set %s to %s", tenant, status)
		}
		requireTenantCleaned := func(t *testing.T, tenant string) {
			t.Helper()
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
		requireMarkerStays := func(t *testing.T) {
			t.Helper()
			// Hold across several reconcile cycles: deferral must be stable.
			for range 3 {
				got := helper.GetClass(t, className)
				cfg, ok := got.VectorConfig[dropped]
				require.True(t, ok, "marker must still be present")
				require.Equal(t, "none", cfg.VectorIndexType)
				time.Sleep(3 * time.Second)
			}
		}

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		t.Run("1 create 4 tenants, pump data, deactivate 2", func(t *testing.T) {
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
			helper.CreateTenants(t, className, []*models.Tenant{
				{Name: tenants[0]}, {Name: tenants[1]}, {Name: tenants[2]}, {Name: tenants[3]},
			})

			for ten, tenant := range tenants {
				batch := make([]*models.Object, perTenant)
				for i := range perTenant {
					batch[i] = &models.Object{
						ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-00%02d-0000000023%02d", ten, i)),
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

			setTenantStatus(t, tenants[2], models.TenantActivityStatusCOLD)
			setTenantStatus(t, tenants[3], models.TenantActivityStatusCOLD)
		})

		t.Run("2 drop cleans the hot tenants, cold ones block completion", func(t *testing.T) {
			dropTargetVector(t, className, dropped)
			requireTenantCleaned(t, tenants[0])
			requireTenantCleaned(t, tenants[1])
			requireMarkerStays(t)
		})

		t.Run("3 activating one cold tenant cleans it but does not complete the drop", func(t *testing.T) {
			setTenantStatus(t, tenants[2], models.TenantActivityStatusHOT)
			requireTenantCleaned(t, tenants[2])
			requireMarkerStays(t)
		})

		t.Run("4 deactivate the cleaned tenant again", func(t *testing.T) {
			setTenantStatus(t, tenants[2], models.TenantActivityStatusCOLD)
		})

		t.Run("5 activate the last cold tenant and wait for its cleanup", func(t *testing.T) {
			setTenantStatus(t, tenants[3], models.TenantActivityStatusHOT)
			requireTenantCleaned(t, tenants[3])
		})

		t.Run("6 with all 4 tenants cleaned (1 cold again) the drop completes", func(t *testing.T) {
			eventuallyTargetVectorRemoved(t, className, dropped)
		})

		t.Run("7 diagnostic: re-activating the cleaned cold tenant heals", func(t *testing.T) {
			got := helper.GetClass(t, className)
			if _, present := got.VectorConfig[dropped]; !present {
				t.Skip("already finalized in step 6")
			}
			setTenantStatus(t, tenants[2], models.TenantActivityStatusHOT)
			eventuallyTargetVectorRemoved(t, className, dropped)
		})
	}
}
