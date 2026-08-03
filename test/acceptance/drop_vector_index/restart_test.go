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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// runRestartSuite pins crash recovery: a drop must converge (entry gone,
// objects stripped, sibling intact) across an ungraceful restart. The task
// polls on a 30s ticker, so a kill within seconds of the drop is guaranteed to
// interrupt it mid-flight. Killing weaviate-0 (the client's node, on a cluster
// typically the RAFT leader) plus 3 shards means the killed node always owns
// cleanup work; the cluster variant adds leader loss.
func runRestartSuite(t *testing.T, compose *docker.DockerCompose) {
	ctx := context.Background()
	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	// Wider than finalizeTimeout: startup adds a DTM readiness probe (~60s).
	const postRestartFinalizeTimeout = 5 * time.Minute

	killAndRestart := func(t *testing.T) {
		// By name — the numeric node helpers are off by one vs container names,
		// and Stop no-ops silently on an unknown name (Start would error).
		zero := 0 * time.Second
		require.NoError(t, compose.Stop(ctx, docker.Weaviate0, &zero), "SIGKILL node")
		require.NoError(t, compose.Start(ctx, docker.Weaviate0), "restart node")
		helper.SetupClient(compose.GetWeaviate().URI())
	}

	journey := func(className string, idBase int, killDelay time.Duration) func(t *testing.T) {
		return func(t *testing.T) {
			const (
				dropped    = "vec"
				sibling    = "sibling"
				droppedDim = 32
				siblingDim = 64
				count      = 40
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
					ShardingConfig: map[string]any{"desiredCount": 3},
					VectorConfig: map[string]models.VectorConfig{
						dropped: noneVectorConfig(), sibling: noneVectorConfig(),
					},
				}
				_, err := helper.Client(t).Schema.SchemaObjectsCreate(
					clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls), nil)
				require.NoError(t, err)

				// Flushed waves -> several segments -> work left across the kill.
				for wave := range 4 {
					batch := make([]*models.Object, 0, count/4)
					for i := range count / 4 {
						n := wave*(count/4) + i
						batch = append(batch, &models.Object{
							ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-00%02d-0000000008%02d", idBase, n)),
							Class:      className,
							Properties: map[string]any{"name": fmt.Sprintf("object-%d", n)},
							Vectors: models.Vectors{
								dropped: randVec(droppedDim, float32(n)),
								sibling: randVec(siblingDim, float32(n)),
							},
						})
					}
					helper.CreateObjectsBatch(t, batch)
					time.Sleep(1500 * time.Millisecond) // past the 1s dirty-flush
				}
			})

			t.Run("drop, kill, restart", func(t *testing.T) {
				dropTargetVector(t, className, dropped)
				time.Sleep(killDelay)
				killAndRestart(t)
			})

			t.Run("drop converges after restart", func(t *testing.T) {
				// Error-tolerant fetch: the node may still be rejoining.
				require.EventuallyWithT(t, func(collect *assert.CollectT) {
					resp, err := helper.Client(t).Schema.SchemaObjectsGet(
						clschema.NewSchemaObjectsGetParams().WithClassName(className), nil)
					if !assert.NoError(collect, err) {
						return
					}
					_, present := resp.Payload.VectorConfig[dropped]
					assert.False(collect, present, "vector entry should be removed after recovery")
				}, postRestartFinalizeTimeout, time.Second)

				got := helper.GetClass(t, className)
				require.Contains(t, got.VectorConfig, sibling, "sibling must survive the crash + drop")
			})

			t.Run("objects are stripped and sibling intact", func(t *testing.T) {
				objs := listAllObjectsWithVectors(t, className)
				require.Len(t, objs, count)
				for _, obj := range objs {
					require.NotContains(t, obj.Vectors, dropped)
					require.Equal(t, siblingDim, vecDim(t, obj.Vectors[sibling]))
				}
			})

			t.Run("sibling search still works", func(t *testing.T) {
				require.Equal(t, 3, nearVectorResults(t, className, sibling, randVec(siblingDim, 7), 3))
			})
		}
	}

	// Armed but pre-first-poll-tick: recovery must resume the op.
	t.Run("crash mid-cleanup", journey("DropVectorIndexRestartMidCleanup", 1, 3*time.Second))

	// Marker durable, enqueue uncertain: task resume or reconcile re-enqueue.
	t.Run("crash right after drop", journey("DropVectorIndexRestartAfterDrop", 2, 0))

	// A collection whose LAST named vector was dropped restarts with ZERO
	// vector indexes — neither named nor legacy structures exist for its
	// shards to initialize. Pins the shard-load path for the vector-less
	// shape: schema intact, data readable, writes still working.
	t.Run("vector-less collection survives restart", func(t *testing.T) {
		const (
			className = "DropVectorIndexRestartVectorless"
			only      = "onlyvec"
			dim       = 32
			count     = 20
		)

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		t.Run("create, insert, drop the only vector, finalize", func(t *testing.T) {
			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "name", DataType: []string{schema.DataTypeText.String()}},
				},
				VectorConfig: map[string]models.VectorConfig{only: noneVectorConfig()},
			}
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(
				clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls), nil)
			require.NoError(t, err)

			batch := make([]*models.Object, count)
			for i := range count {
				batch[i] = &models.Object{
					ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-000000018%03d", i)),
					Class:      className,
					Properties: map[string]any{"name": fmt.Sprintf("object-%d", i)},
					Vectors:    models.Vectors{only: randVec(dim, float32(i))},
				}
			}
			helper.CreateObjectsBatch(t, batch)
			dropTargetVector(t, className, only)
			eventuallyTargetVectorRemoved(t, className, only)
		})

		t.Run("kill and restart", killAndRestart)

		t.Run("shape, data, and writes survive the restart", func(t *testing.T) {
			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				cls, err := helper.GetClassWithoutAssert(t, className, "")
				if !assert.NoError(collect, err) {
					return
				}
				assert.Empty(collect, cls.VectorConfig)
				assert.Empty(collect, cls.Vectorizer)
			}, postRestartFinalizeTimeout, time.Second)

			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				objs, err := listObjectsWithVectorsErr(className, "", count)
				if !assert.NoError(collect, err) {
					return
				}
				assert.Len(collect, objs, count)
			}, postRestartFinalizeTimeout, time.Second)

			post := []*models.Object{{
				ID:         strfmt.UUID("00000000-0000-0000-0000-000000018999"),
				Class:      className,
				Properties: map[string]any{"name": "post-restart"},
			}}
			helper.CreateObjectsBatch(t, post)
		})
	})
}
