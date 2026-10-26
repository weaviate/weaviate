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
	"sort"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// testDeletedColdTenantFinalizesWithoutReclean pins the cost of the one
// shard-set change that can complete a drop's coverage with no cleaning work
// happening at all: deleting the tenant whose uncleaned shard was holding the
// marker open.
//
// Round 1 cleans the live tenants and defers the finalize for the cold one —
// correct. Deleting the cold tenant then leaves a recorded chain that covers
// every REMAINING shard, and a complete chain standing next to a marker reads
// as closed-epoch residue (a finalized drop whose name was re-created, or a
// missed finalize), which the enqueuer answers with a fresh epoch: a new op ID,
// a full re-snapshot, and every segment of the surviving tenants rewritten a
// second time. Nothing is dirty here — those tenants were stripped by round 1
// and no write has touched them since — so the drop should finalize on the
// coverage it already recorded.
//
// Deliberately a whole-collection observation rather than a tenant-level one:
// the redundant pass is invisible in the data (it converges either way), so the
// only black-box evidence is a cleanup round that had no work to do.
func testDeletedColdTenantFinalizesWithoutReclean() func(t *testing.T) {
	return func(t *testing.T) {
		const (
			className = "DropVectorIndexDeletedColdTenant"
			dropped   = "vec"
			sibling   = "sibling"
			dim       = 32
			perTenant = 10
		)
		tenants := []string{"tenant-1", "tenant-2", "tenant-3"}
		coldTenant := tenants[2]
		liveTenants := tenants[:2]

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		var before map[string]string

		t.Run("create class, tenants, and objects", func(t *testing.T) {
			createMTDropClass(t, className, dropped, sibling, tenants...)

			for ten, tenant := range tenants {
				batch := make([]*models.Object, perTenant)
				for i := range perTenant {
					batch[i] = &models.Object{
						ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-00%02d-0000000029%02d", ten, i)),
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
			setTenantStatusEventually(t, className, coldTenant, models.TenantActivityStatusCOLD)
			dropTargetVector(t, className, dropped)
		})

		t.Run("round one cleans the live tenants and records its coverage", func(t *testing.T) {
			for _, tenant := range liveTenants {
				requireTenantStripped(t, className, tenant, dropped, perTenant)
			}
			// Coverage is recorded when the ROUND completes, which lags the
			// objects reading as stripped by up to a poll tick.
			waitForNoActiveDropTask(t)

			got := helper.GetClass(t, className)
			require.Equal(t, "none", got.VectorConfig[dropped].VectorIndexType,
				"the marker must still stand while the cold tenant is uncleaned")

			var err error
			before, err = dropTaskIDsErr()
			require.NoError(t, err)
			require.NotEmpty(t, before, "round one must leave a record for the next round to inherit from")
		})

		t.Run("deleting the cold tenant finalizes without re-cleaning", func(t *testing.T) {
			require.NoError(t, helper.DeleteTenants(t, className, []string{coldTenant}))

			eventuallyTargetVectorRemoved(t, className, dropped)

			after, err := dropTaskIDsErr()
			require.NoError(t, err)
			var appeared []string
			for id, status := range after {
				if _, known := before[id]; !known {
					appeared = append(appeared, fmt.Sprintf("%s(%s)", id, status))
				}
			}
			sort.Strings(appeared)
			require.Emptyf(t, appeared,
				"the finalize must run on the coverage round one already recorded; cleanup round(s) %v "+
					"re-strip tenants that were cleaned before the deletion and never written to since",
				appeared)
		})

		t.Run("the surviving tenants are intact", func(t *testing.T) {
			for _, tenant := range liveTenants {
				objs := listTenantObjectsWithVectors(t, className, tenant)
				require.Len(t, objs, perTenant)
				for _, obj := range objs {
					require.NotContains(t, obj.Vectors, dropped,
						"tenant %s must stay stripped", tenant)
					require.Equal(t, dim, vecDim(t, obj.Vectors[sibling]))
				}
				require.Equal(t, 3, nearVectorTenantResults(t, className, tenant, sibling, randVec(dim, 7), 3))
			}
		})
	}
}
