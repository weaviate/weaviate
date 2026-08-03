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
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

// testTenantDeleteRecreateDuringDrop pins the delete+recreate-mid-drop
// journey: a cleaned tenant is deleted and its NAME reused while the marker
// is still pending. Coverage inheritance keys by shard name, so the final
// round counts the recreated shard as cleaned — which is sound ONLY because
// the write path refuses (receiving node) or strips (owner shard) dropped-
// vector bytes while the marker stands, so the recreated shard can never
// hold anything the cleanup would have to remove. This test pins that whole
// contract end to end.
func testTenantDeleteRecreateDuringDrop() func(t *testing.T) {
	return func(t *testing.T) {
		const (
			className  = "DropVectorIndexTenantRecreate"
			dropped    = "vec"
			sibling    = "sibling"
			dim        = 32
			tenant     = "tenant-re"
			coldTenant = "tenant-cold"
			perTenant  = 10
		)

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		// insert batches perTenant objects and returns the flattened per-item
		// errors ("" = all succeeded).
		insert := func(t *testing.T, ten string, seedBase int, withDropped bool) string {
			batch := make([]*models.Object, perTenant)
			for i := range perTenant {
				vectors := models.Vectors{sibling: randVec(dim, float32(seedBase+i+100))}
				if withDropped {
					vectors[dropped] = randVec(dim, float32(seedBase+i))
				}
				batch[i] = &models.Object{
					ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000001%05d", seedBase+i)),
					Class:      className,
					Tenant:     ten,
					Properties: map[string]any{"name": fmt.Sprintf("object-%d", i)},
					Vectors:    vectors,
				}
			}
			var errs string
			for _, item := range helper.CreateObjectsBatchWithResponse(t, batch) {
				errs += batchItemError(item)
			}
			return errs
		}

		t.Run("create class, tenants, objects; cold tenant holds the marker", func(t *testing.T) {
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
			helper.CreateTenants(t, className, []*models.Tenant{{Name: tenant}, {Name: coldTenant}})
			require.Empty(t, insert(t, tenant, 0, true))
			require.Empty(t, insert(t, coldTenant, 20000, true))
			time.Sleep(3 * time.Second) // past the 1s dirty-flush
			setTenantStatusEventually(t, className, coldTenant, models.TenantActivityStatusCOLD)
		})

		t.Run("drop cleans the hot tenant; its round completes; marker defers", func(t *testing.T) {
			dropTargetVector(t, className, dropped)
			requireTenantStripped(t, className, tenant, dropped, perTenant)
			// Coverage is recorded only by a COMPLETED round.
			waitForNoActiveDropTask(t)
		})

		t.Run("delete the cleaned tenant and reuse its name", func(t *testing.T) {
			require.NoError(t, helper.DeleteTenants(t, className, []string{tenant}))
			helper.CreateTenants(t, className, []*models.Tenant{{Name: tenant}})
		})

		t.Run("writes targeting the dropped vector are refused on the recreated tenant", func(t *testing.T) {
			// THE load-bearing guard: inheritance will count the recreated
			// shard as cleaned, which is only sound if no dropped-vector
			// bytes can enter it while the marker stands. The reject is
			// request-level (400), so issue the batch raw.
			obj := &models.Object{
				ID:     strfmt.UUID("00000000-0000-0000-0000-000000140000"),
				Class:  className,
				Tenant: tenant,
				Vectors: models.Vectors{
					dropped: randVec(dim, 1),
					sibling: randVec(dim, 101),
				},
			}
			params := clbatch.NewBatchObjectsCreateParams().
				WithBody(clbatch.BatchObjectsCreateBody{Objects: []*models.Object{obj}})
			resp, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
			require.NoError(t, err)
			var itemErrs string
			for _, item := range resp.Payload {
				itemErrs += batchItemError(item)
			}
			require.Contains(t, itemErrs, "dropped",
				"dropped-vector writes must be refused per item while the marker stands")

			require.Empty(t, insert(t, tenant, 40000, false),
				"sibling-only writes must pass")
		})

		t.Run("activating the cold tenant completes the drop", func(t *testing.T) {
			setTenantStatusEventually(t, className, coldTenant, models.TenantActivityStatusHOT)
			eventuallyTargetVectorRemoved(t, className, dropped)
		})

		t.Run("recreated tenant is clean; sibling intact everywhere", func(t *testing.T) {
			for ten, want := range map[string]int{tenant: perTenant, coldTenant: perTenant} {
				objs := listTenantObjectsWithVectors(t, className, ten)
				require.Len(t, objs, want)
				for _, obj := range objs {
					require.NotContains(t, obj.Vectors, dropped,
						"no un-stripped bytes may survive on %s", ten)
					require.Equal(t, dim, vecDim(t, obj.Vectors[sibling]))
				}
				require.Equal(t, 3, nearVectorTenantResults(t, className, ten, sibling, randVec(dim, 105), 3))
			}
		})
	}
}
