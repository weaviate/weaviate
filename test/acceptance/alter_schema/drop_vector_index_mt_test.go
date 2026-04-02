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

package alterschema

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clobjects "github.com/weaviate/weaviate/client/objects"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func testDropVectorIndexMultiTenant(compose *docker.DockerCompose, verifySchemaAfterDrop bool) func(t *testing.T) {
	return func(t *testing.T) {
		className := "DropVectorIndexMTTest"
		tenant1 := "tenant1"
		tenant2 := "tenant2"
		tenant3 := "tenant3"

		// Clean up in case of previous failed run.
		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		cls := &models.Class{
			Class: className,
			MultiTenancyConfig: &models.MultiTenancyConfig{
				Enabled: true,
			},
			Properties: []*models.Property{
				{
					Name:     "name",
					DataType: []string{schema.DataTypeText.String()},
				},
			},
			VectorConfig: map[string]models.VectorConfig{
				"flat_bq": {
					Vectorizer: map[string]any{
						"none": map[string]any{},
					},
					VectorIndexType: "flat",
					VectorIndexConfig: map[string]any{
						"bq": map[string]any{
							"enabled": true,
						},
					},
				},
				"hnsw_rq8": {
					Vectorizer: map[string]any{
						"none": map[string]any{},
					},
					VectorIndexType: "hnsw",
					VectorIndexConfig: map[string]any{
						"rq": map[string]any{
							"enabled": true,
							"bits":    8,
						},
					},
				},
			},
		}

		t.Run("create class", func(t *testing.T) {
			createParams := clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls)
			resp, err := helper.Client(t).Schema.SchemaObjectsCreate(createParams, nil)
			helper.AssertRequestOk(t, resp, err, nil)
		})

		t.Run("create tenants", func(t *testing.T) {
			helper.CreateTenants(t, className, []*models.Tenant{
				{Name: tenant1},
				{Name: tenant2},
				{Name: tenant3},
			})
		})

		dim := 4
		mkVec := func(val float32) []float32 {
			v := make([]float32, dim)
			for i := range v {
				v[i] = val + float32(i)*0.1
			}
			return v
		}

		t.Run("insert objects", func(t *testing.T) {
			for ti, tenant := range []string{tenant1, tenant2, tenant3} {
				for i := range 5 {
					obj := &models.Object{
						ID:     strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000000%02d%03d", ti, i+1)),
						Class:  className,
						Tenant: tenant,
						Properties: map[string]any{
							"name": fmt.Sprintf("object-%s-%d", tenant, i),
						},
						Vectors: models.Vectors{
							"flat_bq":  mkVec(float32(ti*10 + i)),
							"hnsw_rq8": mkVec(float32(ti*10+i) + 100),
						},
					}
					createResp, err := helper.Client(t).Objects.ObjectsCreate(
						clobjects.NewObjectsCreateParams().WithBody(obj), nil)
					helper.AssertRequestOk(t, createResp, err, nil)
				}
			}
		})

		// Verify schema has both named vectors.
		t.Run("verify schema before drop", func(t *testing.T) {
			cls := helper.GetClass(t, className)
			require.Len(t, cls.VectorConfig, 2)
			for _, name := range []string{"flat_bq", "hnsw_rq8"} {
				cfg, ok := cls.VectorConfig[name]
				assert.True(t, ok, "expected vector config %q", name)
				if ok {
					assert.Nil(t, cfg.Deleted, "Deleted should be nil for active vector index %q", name)
				}
			}
		})

		if compose != nil {
			t.Run("verify files exist before drop", func(t *testing.T) {
				for _, tenant := range []string{tenant1, tenant2, tenant3} {
					// flat_bq stores vectors in LSM buckets
					exists := checkFolderExistence(t, compose, className, tenant, "vectors_flat_bq")
					assert.True(t, exists, "expected vectors_flat_bq bucket to exist for %s", tenant)

					exists = checkFolderExistence(t, compose, className, tenant, "vectors_compressed_flat_bq")
					assert.True(t, exists, "expected vectors_compressed_flat_bq bucket to exist for %s", tenant)

					// hnsw_rq8 uses commit log directories
					exists = checkShardFolderExistence(t, compose, className, tenant, "vectors_hnsw_rq8.hnsw.commitlog.d")
					assert.True(t, exists, "expected vectors_hnsw_rq8.hnsw.commitlog.d to exist for %s", tenant)
				}
			})
		}

		t.Run("deactivate tenant3", func(t *testing.T) {
			helper.UpdateTenants(t, className, []*models.Tenant{
				{
					Name:           tenant3,
					ActivityStatus: models.TenantActivityStatusCOLD,
				},
			})
		})

		// Drop both vector indexes while tenant3 is deactivated.
		t.Run("drop flat_bq vector index", func(t *testing.T) {
			params := clschema.NewSchemaObjectsVectorsDeleteParams().
				WithClassName(className).
				WithVectorIndexName("flat_bq")
			resp, err := helper.Client(t).Schema.SchemaObjectsVectorsDelete(params, nil)
			helper.AssertRequestOk(t, resp, err, nil)
		})

		t.Run("drop hnsw_rq8 vector index", func(t *testing.T) {
			params := clschema.NewSchemaObjectsVectorsDeleteParams().
				WithClassName(className).
				WithVectorIndexName("hnsw_rq8")
			resp, err := helper.Client(t).Schema.SchemaObjectsVectorsDelete(params, nil)
			helper.AssertRequestOk(t, resp, err, nil)
		})

		if verifySchemaAfterDrop {
			t.Run("verify schema after drops", func(t *testing.T) {
				assert.EventuallyWithT(t, func(collect *assert.CollectT) {
					cls := helper.GetClass(t, className)
					if !assert.Len(collect, cls.VectorConfig, 2) {
						return
					}
					for _, name := range []string{"flat_bq", "hnsw_rq8"} {
						cfg, ok := cls.VectorConfig[name]
						assert.True(collect, ok, "vector config %q should still exist", name)
						if ok {
							assert.Empty(collect, cfg.VectorIndexType, "VectorIndexType should be empty for %q", name)
							assert.Nil(collect, cfg.VectorIndexConfig, "VectorIndexConfig should be nil for %q", name)
							assert.NotNil(collect, cfg.Deleted, "Deleted should be set for %q", name)
							if cfg.Deleted != nil {
								assert.True(collect, *cfg.Deleted, "Deleted should be true for %q", name)
							}
						}
					}
				}, 15*time.Second, 200*time.Millisecond, "schema should reflect dropped vector indexes")
			})
		}

		if compose != nil {
			t.Run("verify files removed for active tenants", func(t *testing.T) {
				for _, tenant := range []string{tenant1, tenant2} {
					// flat_bq buckets should be gone
					exists := checkFolderExistence(t, compose, className, tenant, "vectors_flat_bq")
					assert.False(t, exists, "vectors_flat_bq bucket should be removed for %s", tenant)

					exists = checkFolderExistence(t, compose, className, tenant, "vectors_compressed_flat_bq")
					assert.False(t, exists, "vectors_compressed_flat_bq bucket should be removed for %s", tenant)

					// hnsw_rq8 commit log and snapshot should be gone
					exists = checkShardFolderExistence(t, compose, className, tenant, "vectors_hnsw_rq8.hnsw.commitlog.d")
					assert.False(t, exists, "vectors_hnsw_rq8.hnsw.commitlog.d should be removed for %s", tenant)

					exists = checkShardFolderExistence(t, compose, className, tenant, "vectors_hnsw_rq8.hnsw.snapshot.d")
					assert.False(t, exists, "vectors_hnsw_rq8.hnsw.snapshot.d should be removed for %s", tenant)
				}
			})

			t.Run("verify files still exist for deactivated tenant3", func(t *testing.T) {
				exists := checkFolderExistence(t, compose, className, tenant3, "vectors_flat_bq")
				assert.True(t, exists, "vectors_flat_bq bucket should still exist for deactivated %s", tenant3)

				exists = checkFolderExistence(t, compose, className, tenant3, "vectors_compressed_flat_bq")
				assert.True(t, exists, "vectors_compressed_flat_bq bucket should still exist for deactivated %s", tenant3)

				exists = checkShardFolderExistence(t, compose, className, tenant3, "vectors_hnsw_rq8.hnsw.commitlog.d")
				assert.True(t, exists, "vectors_hnsw_rq8.hnsw.commitlog.d should still exist for deactivated %s", tenant3)
			})
		}

		// Activate tenant3 and verify cleanup happens.
		t.Run("activate tenant3", func(t *testing.T) {
			helper.UpdateTenants(t, className, []*models.Tenant{
				{
					Name:           tenant3,
					ActivityStatus: models.TenantActivityStatusHOT,
				},
			})
		})

		if compose != nil {
			t.Run("verify files removed for tenant3 after activation", func(t *testing.T) {
				exists := checkFolderExistence(t, compose, className, tenant3, "vectors_flat_bq")
				assert.False(t, exists, "vectors_flat_bq bucket should be removed for %s after activation", tenant3)

				exists = checkFolderExistence(t, compose, className, tenant3, "vectors_compressed_flat_bq")
				assert.False(t, exists, "vectors_compressed_flat_bq bucket should be removed for %s after activation", tenant3)

				exists = checkShardFolderExistence(t, compose, className, tenant3, "vectors_hnsw_rq8.hnsw.commitlog.d")
				assert.False(t, exists, "vectors_hnsw_rq8.hnsw.commitlog.d should be removed for %s after activation", tenant3)

				exists = checkShardFolderExistence(t, compose, className, tenant3, "vectors_hnsw_rq8.hnsw.snapshot.d")
				assert.False(t, exists, "vectors_hnsw_rq8.hnsw.snapshot.d should be removed for %s after activation", tenant3)
			})
		}

		// Clean up.
		t.Run("delete class", func(t *testing.T) {
			deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
			resp, err := helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
			helper.AssertRequestOk(t, resp, err, nil)
		})
	}
}
