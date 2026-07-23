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

// testRedropAfterRecreate pins the generation token end to end: drop a
// vector, let it finalize, re-create the name and write new vectors, add a
// NEW tenant, then re-drop through an all-cold window so the fresh enqueue
// mints no task while the previous drop's FINISHED records still exist. The
// grown tenant set is the shape that broke record-only epoch inference (the
// stale records look "incomplete" against it); the marker-introduction purge
// removes them before they can be inherited — the re-drop must re-clean every
// tenant, never finalize over the re-created vectors.
func testRedropAfterRecreate() func(t *testing.T) {
	return func(t *testing.T) {
		const (
			className = "DropVectorIndexRedropAfterRecreate"
			dropped   = "vec"
			sibling   = "sibling"
			dim       = 32
			perTenant = 8
			tenant    = "tenant-1"
			tenant2   = "tenant-2" // created between the drops — the grown-set twist
		)

		insert := func(t *testing.T, target string, idBase int) {
			batch := make([]*models.Object, perTenant)
			for i := range perTenant {
				batch[i] = &models.Object{
					ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000000024%02d", idBase+i)),
					Class:      className,
					Tenant:     target,
					Properties: map[string]any{"name": fmt.Sprintf("object-%d", idBase+i)},
					Vectors: models.Vectors{
						dropped: randVec(dim, float32(idBase+i)),
						sibling: randVec(dim, float32(idBase+i+100)),
					},
				}
			}
			helper.CreateObjectsBatch(t, batch)
			time.Sleep(3 * time.Second) // past the 1s dirty-flush
		}

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		t.Run("first drop lifecycle completes", func(t *testing.T) {
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
			helper.CreateTenants(t, className, []*models.Tenant{{Name: tenant}})
			insert(t, tenant, 0)

			dropTargetVector(t, className, dropped)
			eventuallyTargetVectorRemoved(t, className, dropped)
		})

		t.Run("re-create the name, write new vectors, add a tenant", func(t *testing.T) {
			cls := helper.GetClass(t, className)
			cls.VectorConfig[dropped] = noneVectorConfig()
			_, err := helper.Client(t).Schema.SchemaObjectsUpdate(
				clschema.NewSchemaObjectsUpdateParams().WithClassName(className).WithObjectClass(cls), nil)
			require.NoError(t, err)
			insert(t, tenant, 50)

			helper.CreateTenants(t, className, []*models.Tenant{{Name: tenant2}})
			insert(t, tenant2, 80)
		})

		t.Run("re-drop through an all-cold window", func(t *testing.T) {
			// Cold BEFORE the drop: the fresh enqueue then mints no task, and
			// only the previous drop's records exist when reconciliation runs.
			setTenantStatusEventually(t, className, tenant, models.TenantActivityStatusCOLD)
			setTenantStatusEventually(t, className, tenant2, models.TenantActivityStatusCOLD)
			dropTargetVector(t, className, dropped)
			setTenantStatusEventually(t, className, tenant, models.TenantActivityStatusHOT)
			setTenantStatusEventually(t, className, tenant2, models.TenantActivityStatusHOT)
		})

		t.Run("the re-drop re-cleans every tenant instead of adopting stale coverage", func(t *testing.T) {
			eventuallyTargetVectorRemoved(t, className, dropped)
			for tenantName, want := range map[string]int{tenant: 2 * perTenant, tenant2: perTenant} {
				objs := listTenantObjectsWithVectors(t, className, tenantName)
				require.Len(t, objs, want)
				for _, obj := range objs {
					require.NotContains(t, obj.Vectors, dropped,
						"the re-dropped vectors must be stripped, not finalized over")
					require.Equal(t, dim, vecDim(t, obj.Vectors[sibling]))
				}
			}
		})
	}
}
