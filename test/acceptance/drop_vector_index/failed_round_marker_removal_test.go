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
	"github.com/weaviate/weaviate/test/helper"
)

// testFailedRoundKeepsMarkerAfterDeferredTenantDelete pins that a cleanup
// round which FAILED on a tenant never counts that tenant as stripped: the
// round also deferred a cold tenant, and deleting that tenant must not let a
// class update remove the marker while the failed tenant is still owed.
func testFailedRoundKeepsMarkerAfterDeferredTenantDelete() func(t *testing.T) {
	return func(t *testing.T) {
		const (
			className    = "DropVectorIndexFailedRoundMarker"
			dropped      = "vec"
			sibling      = "sibling"
			dim          = 32
			perTenant    = 10
			failedTenant = "tenant-1"
			coldTenant   = "tenant-2"
		)

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		t.Run("create class, tenants, and objects", func(t *testing.T) {
			createMTDropClass(t, className, dropped, sibling, failedTenant, coldTenant)

			for ten, tenant := range []string{failedTenant, coldTenant} {
				batch := make([]*models.Object, perTenant)
				for i := range perTenant {
					batch[i] = &models.Object{
						ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-00%02d-0000000377%02d", ten, i)),
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

		t.Run("the round defers the cold tenant and fails on the other", func(t *testing.T) {
			setTenantStatusEventually(t, className, coldTenant, models.TenantActivityStatusCOLD)
			before, err := dropTaskIDsErr()
			require.NoError(t, err)
			dropTargetVector(t, className, dropped)

			// Still in flight: the first poll tick is 30s away.
			require.NoError(t, helper.UpdateTenantsReturnError(t, className, []*models.Tenant{
				{Name: failedTenant, ActivityStatus: models.TenantActivityStatusCOLD},
			}))

			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				tasks, err := dropTaskIDsErr()
				if !assert.NoError(collect, err) {
					return
				}
				var rounds int
				for id, status := range tasks {
					if _, known := before[id]; known {
						continue
					}
					rounds++
					assert.Equalf(collect, "FAILED", status, "task %s", id)
				}
				assert.Positive(collect, rounds, "the drop must have started a round")
			}, 3*time.Minute, time.Second)
		})

		t.Run("deleting the deferred tenant does not make the marker removable", func(t *testing.T) {
			require.NoError(t, helper.DeleteTenants(t, className, []string{coldTenant}))

			cls := helper.GetClass(t, className)
			delete(cls.VectorConfig, dropped)
			_, err := helper.Client(t).Schema.SchemaObjectsUpdate(
				clschema.NewSchemaObjectsUpdateParams().WithClassName(className).WithObjectClass(cls), nil)
			require.Error(t, err)
			require.Contains(t, errorResponseText(err), "cannot remove dropped vector")

			got := helper.GetClass(t, className)
			require.Equal(t, "none", got.VectorConfig[dropped].VectorIndexType,
				"the marker must stand while the failed tenant is still owed a round")
		})

		t.Run("reactivating the failed tenant completes the drop", func(t *testing.T) {
			setTenantStatusEventually(t, className, failedTenant, models.TenantActivityStatusHOT)
			eventuallyTargetVectorRemoved(t, className, dropped)

			objs := listTenantObjectsWithVectors(t, className, failedTenant)
			require.Len(t, objs, perTenant)
			for _, obj := range objs {
				require.NotContains(t, obj.Vectors, dropped)
				require.Equal(t, dim, vecDim(t, obj.Vectors[sibling]))
			}
		})
	}
}
