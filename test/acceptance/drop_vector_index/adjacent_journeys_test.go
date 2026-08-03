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

// testConcurrentDrops pins overlapping drops: a second drop on a DIFFERENT
// vector while the first is still cleaning runs as its own task, a duplicate
// drop of the SAME vector while active is a no-op success, and both drops
// finalize with the surviving vector untouched.
func testConcurrentDrops() func(t *testing.T) {
	return func(t *testing.T) {
		const (
			className = "DropVectorIndexConcurrentDrops"
			vecA      = "vecA"
			vecB      = "vecB"
			sibling   = "sibling"
			dim       = 32
			count     = 20
		)

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		t.Run("create class and insert", func(t *testing.T) {
			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "name", DataType: []string{schema.DataTypeText.String()}},
				},
				VectorConfig: map[string]models.VectorConfig{
					vecA: noneVectorConfig(), vecB: noneVectorConfig(), sibling: noneVectorConfig(),
				},
			}
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(
				clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls), nil)
			require.NoError(t, err)

			batch := make([]*models.Object, count)
			for i := range count {
				batch[i] = &models.Object{
					ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000000012%02d", i)),
					Class:      className,
					Properties: map[string]any{"name": fmt.Sprintf("object-%d", i)},
					Vectors: models.Vectors{
						vecA:    randVec(dim, float32(i)),
						vecB:    randVec(dim, float32(i+100)),
						sibling: randVec(dim, float32(i+200)),
					},
				}
			}
			helper.CreateObjectsBatch(t, batch)
			time.Sleep(3 * time.Second) // past the 1s dirty-flush
		})

		t.Run("drop vecA then vecB while vecA is still cleaning", func(t *testing.T) {
			dropTargetVector(t, className, vecA)
			dropTargetVector(t, className, vecB)
		})

		t.Run("duplicate drop of an active target is a no-op success", func(t *testing.T) {
			// Guaranteed still active: the first poll tick is 30s after the drop.
			dropTargetVector(t, className, vecA)
		})

		t.Run("both drops finalize, sibling survives", func(t *testing.T) {
			eventuallyTargetVectorRemoved(t, className, vecA)
			eventuallyTargetVectorRemoved(t, className, vecB)
			got := helper.GetClass(t, className)
			require.Contains(t, got.VectorConfig, sibling)
			require.Len(t, got.VectorConfig, 1)
		})

		t.Run("objects stripped of both, sibling searchable", func(t *testing.T) {
			objs := listAllObjectsWithVectors(t, className)
			require.Len(t, objs, count)
			for _, obj := range objs {
				require.NotContains(t, obj.Vectors, vecA)
				require.NotContains(t, obj.Vectors, vecB)
				require.Equal(t, dim, vecDim(t, obj.Vectors[sibling]))
			}
			require.Equal(t, 3, nearVectorResults(t, className, sibling, randVec(dim, 7), 3))
		})
	}
}

// testDeleteClassMidDrop pins that DeleteClass wins over an in-flight drop
// (the task is cascade-deleted with the class) and that re-creating the class
// with the same vector names starts from a clean slate — no stale marker,
// task, or edit op may survive to strip the reborn vector.
func testDeleteClassMidDrop() func(t *testing.T) {
	return func(t *testing.T) {
		const (
			className = "DropVectorIndexDeleteClassMidDrop"
			vec       = "vec"
			dim       = 32
			count     = 20
		)

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		mkClass := func() *models.Class {
			return &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "name", DataType: []string{schema.DataTypeText.String()}},
				},
				VectorConfig: map[string]models.VectorConfig{
					// The sibling keeps vec droppable (a drop leaving no live
					// named vector is rejected).
					vec: noneVectorConfig(), "sibling": noneVectorConfig(),
				},
			}
		}
		insert := func(t *testing.T, idBase int) {
			batch := make([]*models.Object, count)
			for i := range count {
				batch[i] = &models.Object{
					ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000000013%02d", idBase+i)),
					Class:      className,
					Properties: map[string]any{"name": fmt.Sprintf("object-%d", idBase+i)},
					Vectors:    models.Vectors{vec: randVec(dim, float32(idBase+i))},
				}
			}
			helper.CreateObjectsBatch(t, batch)
		}

		t.Run("create, insert, drop, then delete the class mid-drop", func(t *testing.T) {
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(
				clschema.NewSchemaObjectsCreateParams().WithObjectClass(mkClass()), nil)
			require.NoError(t, err)
			insert(t, 0)
			time.Sleep(3 * time.Second) // past the 1s dirty-flush

			dropTargetVector(t, className, vec)

			_, err = helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
			require.NoError(t, err, "DeleteClass must not be blocked by an in-flight drop")
		})

		t.Run("re-created class with the same vector name is fully functional", func(t *testing.T) {
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(
				clschema.NewSchemaObjectsCreateParams().WithObjectClass(mkClass()), nil)
			require.NoError(t, err, "the name must not be poisoned by the deleted class's drop")

			insert(t, 50)

			got := helper.GetClass(t, className)
			require.Equal(t, "hnsw", got.VectorConfig[vec].VectorIndexType)
			require.Equal(t, 3, nearVectorResults(t, className, vec, randVec(dim, 7), 3))

			// The reborn vector must keep its data past the old task's poll/cleanup
			// horizon — a surviving edit op or replayed task would strip it.
			time.Sleep(35 * time.Second)
			objs := listAllObjectsWithVectors(t, className)
			require.Len(t, objs, count)
			for _, obj := range objs {
				require.Equal(t, dim, vecDim(t, obj.Vectors[vec]))
			}
			got = helper.GetClass(t, className)
			require.Equal(t, "hnsw", got.VectorConfig[vec].VectorIndexType,
				"no stale marker or replayed task may re-drop the reborn vector")
		})
	}
}

// testDropRejections pins the endpoint's negative contract: a legacy class
// (no named vectors) and an unknown target vector are both rejected.
func testDropRejections() func(t *testing.T) {
	return func(t *testing.T) {
		const className = "DropVectorIndexRejections"

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		t.Run("legacy class without named vectors is rejected", func(t *testing.T) {
			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "name", DataType: []string{schema.DataTypeText.String()}},
				},
				Vectorizer: "none", // class-level legacy vector, no VectorConfig
			}
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(
				clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls), nil)
			require.NoError(t, err)

			_, err = helper.Client(t).Schema.SchemaObjectsVectorsDelete(
				clschema.NewSchemaObjectsVectorsDeleteParams().
					WithClassName(className).WithVectorIndexName("anything"), nil)
			require.Error(t, err)
			require.Contains(t, errorResponseText(err), "no named vector")
		})

		t.Run("unknown target vector is rejected", func(t *testing.T) {
			_, err := helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
			require.NoError(t, err)
			cls := &models.Class{
				Class: className,
				VectorConfig: map[string]models.VectorConfig{
					"vec": noneVectorConfig(),
				},
			}
			_, err = helper.Client(t).Schema.SchemaObjectsCreate(
				clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls), nil)
			require.NoError(t, err)

			_, err = helper.Client(t).Schema.SchemaObjectsVectorsDelete(
				clschema.NewSchemaObjectsVectorsDeleteParams().
					WithClassName(className).WithVectorIndexName("missing"), nil)
			require.Error(t, err)
			require.Contains(t, errorResponseText(err), "not found in class")
		})
	}
}

// testZeroTenantDrop pins the tenant-less escape: dropping a vector on an MT
// collection with NO tenants finalizes directly (no cleanup task can ever
// exist to drive it) instead of leaving an immortal marker. Tenants created
// afterwards see the final schema.
func testZeroTenantDrop() func(t *testing.T) {
	return func(t *testing.T) {
		const className = "DropVectorIndexZeroTenants"

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		cls := &models.Class{
			Class:              className,
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
			VectorConfig: map[string]models.VectorConfig{
				"vec": noneVectorConfig(), "sibling": noneVectorConfig(),
			},
		}
		_, err := helper.Client(t).Schema.SchemaObjectsCreate(
			clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls), nil)
		require.NoError(t, err)

		dropTargetVector(t, className, "vec")
		eventuallyTargetVectorRemoved(t, className, "vec")

		// A tenant created afterwards lives on the finalized schema.
		helper.CreateTenants(t, className, []*models.Tenant{{Name: "late"}})
		cur, err := helper.GetClassWithoutAssert(t, className, "")
		require.NoError(t, err)
		require.NotContains(t, cur.VectorConfig, "vec")
		require.Contains(t, cur.VectorConfig, "sibling")
	}
}

// testAllTenantsDeletedMidDrop pins the OTHER zero-tenant journey: tenants
// existed when the marker landed (a cleanup task is in flight), then every
// tenant is deleted mid-drop. The round's units die with their shards and no
// later task can ever exist, so only the enqueuer's zero-tenant escape can
// remove the marker — without it the collection keeps an immortal marker.
func testAllTenantsDeletedMidDrop() func(t *testing.T) {
	return func(t *testing.T) {
		const (
			className = "DropVectorIndexAllTenantsDeleted"
			dropped   = "vec"
			sibling   = "sibling"
			tenant1   = "tenant1"
			tenant2   = "tenant2"
			dim       = 8
		)

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		createMTDropClass(t, className, dropped, sibling, tenant1, tenant2)
		for ten, tenant := range map[int]string{0: tenant1, 1: tenant2} {
			batch := make([]*models.Object, 5)
			for i := range 5 {
				batch[i] = &models.Object{
					ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-01%02d-0000000015%02d", ten, i)),
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

		dropTargetVector(t, className, dropped)
		// Guaranteed still in flight: the first poll tick is 30s away.
		require.NoError(t, helper.DeleteTenants(t, className, []string{tenant1, tenant2}))

		eventuallyTargetVectorRemoved(t, className, dropped)

		// A tenant created afterwards lives on the finalized schema.
		helper.CreateTenants(t, className, []*models.Tenant{{Name: "late"}})
		cur, err := helper.GetClassWithoutAssert(t, className, "")
		require.NoError(t, err)
		require.NotContains(t, cur.VectorConfig, dropped)
		require.Contains(t, cur.VectorConfig, sibling)
	}
}
