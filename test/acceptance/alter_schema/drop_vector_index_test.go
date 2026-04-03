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

func testDropVectorIndex(compose *docker.DockerCompose, verifySchemaAfterDrop bool) func(t *testing.T) {
	return func(t *testing.T) {
		className := "DropVectorIndexTest"

		// Clean up in case of previous failed run.
		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		cls := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name:     "name",
					DataType: []string{schema.DataTypeText.String()},
				},
			},
			VectorConfig: map[string]models.VectorConfig{
				"hnsw": {
					Vectorizer: map[string]any{
						"none": map[string]any{},
					},
					VectorIndexType: "hnsw",
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
				"flat": {
					Vectorizer: map[string]any{
						"none": map[string]any{},
					},
					VectorIndexType: "flat",
				},
				"flat_rq1": {
					Vectorizer: map[string]any{
						"none": map[string]any{},
					},
					VectorIndexType: "flat",
					VectorIndexConfig: map[string]any{
						"rq": map[string]any{
							"enabled": true,
							"bits":    1,
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

		dim := 4
		mkVec := func(val float32) []float32 {
			v := make([]float32, dim)
			for i := range v {
				v[i] = val + float32(i)*0.1
			}
			return v
		}

		t.Run("insert objects", func(t *testing.T) {
			for i := range 5 {
				obj := &models.Object{
					ID:    strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-00000000000%d", i+1)),
					Class: className,
					Properties: map[string]any{
						"name": fmt.Sprintf("object-%d", i),
					},
					Vectors: models.Vectors{
						"hnsw":     mkVec(float32(i)),
						"hnsw_rq8": mkVec(float32(i) + 10),
						"flat":     mkVec(float32(i) + 20),
						"flat_rq1": mkVec(float32(i) + 30),
					},
				}
				createResp, err := helper.Client(t).Objects.ObjectsCreate(
					clobjects.NewObjectsCreateParams().WithBody(obj), nil)
				helper.AssertRequestOk(t, createResp, err, nil)
			}
		})

		// Retrieve the shard name for folder checks.
		var shardName string
		if compose != nil {
			t.Run("get shard name", func(t *testing.T) {
				shardsParams := clschema.NewSchemaObjectsShardsGetParams().WithClassName(className)
				shardsResp, err := helper.Client(t).Schema.SchemaObjectsShardsGet(shardsParams, nil)
				require.NoError(t, err)
				require.Len(t, shardsResp.Payload, 1)
				shardName = shardsResp.Payload[0].Name
			})
		}

		// Verify schema has all 4 named vectors.
		t.Run("verify schema before drop", func(t *testing.T) {
			cls := helper.GetClass(t, className)
			require.Len(t, cls.VectorConfig, 4)
			for _, name := range []string{"hnsw", "hnsw_rq8", "flat", "flat_rq1"} {
				cfg, ok := cls.VectorConfig[name]
				assert.True(t, ok, "expected vector config %q", name)
				if ok {
					assert.Nil(t, cfg.Deleted, "Deleted should be nil for active vector index %q", name)
				}
			}
		})

		type vectorTestCase struct {
			name string
			// hasBuckets indicates the index type stores vectors in LSM buckets
			// (flat indexes do; HNSW uses commit logs and does not).
			hasBuckets     bool
			hasCompression bool
			// isHNSW indicates the index type uses commit log and snapshot directories.
			isHNSW bool
		}

		testCases := []vectorTestCase{
			{
				name:           "flat",
				hasBuckets:     true,
				hasCompression: false,
			},
			{
				name:           "flat_rq1",
				hasBuckets:     true,
				hasCompression: true,
			},
			{
				name:   "hnsw",
				isHNSW: true,
			},
			{
				name:   "hnsw_rq8",
				isHNSW: true,
			},
		}

		for _, tc := range testCases {
			t.Run(fmt.Sprintf("drop vector index %s", tc.name), func(t *testing.T) {
				vectorIndexID := fmt.Sprintf("vectors_%s", tc.name)

				if compose != nil {
					t.Run("verify files exist before drop", func(t *testing.T) {
						if tc.hasBuckets {
							exists := checkFolderExistence(t, compose, className, shardName, vectorIndexID)
							assert.True(t, exists, "expected %s bucket to exist", vectorIndexID)

							if tc.hasCompression {
								exists = checkFolderExistence(t, compose, className, shardName, fmt.Sprintf("vectors_compressed_%s", tc.name))
								assert.True(t, exists, "expected vectors_compressed_%s bucket to exist", tc.name)
							}
						}

						if tc.isHNSW {
							exists := checkShardFolderExistence(t, compose, className, shardName, vectorIndexID+".hnsw.commitlog.d")
							assert.True(t, exists, "expected %s.hnsw.commitlog.d to exist", vectorIndexID)
						}
					})
				}

				t.Run("call drop API", func(t *testing.T) {
					params := clschema.NewSchemaObjectsVectorsDeleteParams().
						WithClassName(className).
						WithVectorIndexName(tc.name)
					resp, err := helper.Client(t).Schema.SchemaObjectsVectorsDelete(params, nil)
					helper.AssertRequestOk(t, resp, err, nil)
				})

				if verifySchemaAfterDrop {
					t.Run("verify vector index dropped in schema", func(t *testing.T) {
						assert.EventuallyWithT(t, func(collect *assert.CollectT) {
							cls := helper.GetClass(t, className)
							cfg, ok := cls.VectorConfig[tc.name]
							assert.True(collect, ok, "vector config %q should still exist in schema", tc.name)
							if ok {
								assert.Empty(collect, cfg.VectorIndexType, "VectorIndexType should be empty for dropped index %q", tc.name)
								assert.Nil(collect, cfg.VectorIndexConfig, "VectorIndexConfig should be nil for dropped index %q", tc.name)
								assert.NotNil(collect, cfg.Deleted, "Deleted should be set for dropped index %q", tc.name)
								if cfg.Deleted != nil {
									assert.True(collect, *cfg.Deleted, "Deleted should be true for dropped index %q", tc.name)
								}
							}
						}, 15*time.Second, 200*time.Millisecond, "schema should reflect dropped vector index %q", tc.name)
					})
				}

				if compose != nil {
					t.Run("verify files removed from disk", func(t *testing.T) {
						if tc.hasBuckets {
							exists := checkFolderExistence(t, compose, className, shardName, vectorIndexID)
							assert.False(t, exists, "%s bucket should be removed", vectorIndexID)

							if tc.hasCompression {
								exists = checkFolderExistence(t, compose, className, shardName, fmt.Sprintf("vectors_compressed_%s", tc.name))
								assert.False(t, exists, "vectors_compressed_%s bucket should be removed", tc.name)
							}
						}

						if tc.isHNSW {
							exists := checkShardFolderExistence(t, compose, className, shardName, vectorIndexID+".hnsw.commitlog.d")
							assert.False(t, exists, "%s.hnsw.commitlog.d should be removed", vectorIndexID)

							exists = checkShardFolderExistence(t, compose, className, shardName, vectorIndexID+".hnsw.snapshot.d")
							assert.False(t, exists, "%s.hnsw.snapshot.d should be removed", vectorIndexID)
						}
					})
				}
			})
		}

		// After dropping all 4, verify the class still has all vector entries
		// but with dropped (empty) index configuration.
		if verifySchemaAfterDrop {
			t.Run("verify schema after all drops", func(t *testing.T) {
				assert.EventuallyWithT(t, func(collect *assert.CollectT) {
					cls := helper.GetClass(t, className)
					if !assert.Len(collect, cls.VectorConfig, 4) {
						return
					}
					for _, name := range []string{"hnsw", "hnsw_rq8", "flat", "flat_rq1"} {
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
				}, 15*time.Second, 200*time.Millisecond, "schema should reflect all dropped vector indexes")
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
