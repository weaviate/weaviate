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

// runRestartSuite pins crash recovery: a drop must converge to the finalized
// state (entry gone, objects stripped, sibling intact) across an ungraceful
// restart, whether the kill lands mid-cleanup or right after the drop call
// (the marker-committed/enqueue-uncertain window). The provider polls pending
// sets on a 30s ticker, so a kill within seconds of the drop is GUARANTEED to
// interrupt a non-finished task — no timing luck involved. Recovery is
// whichever path applies: DTM task resume + edit-op recovery on shard load, or
// startup reconciliation re-enqueueing for a marker with no task.
//
// The killed node is always weaviate-0 — the node the client talks to, and on
// a cluster (typically) the RAFT leader, so the cluster variant additionally
// exercises leader loss mid-drop. The class is created with 3 shards so on a
// cluster every node (including the killed one) owns cleanup work.
func runRestartSuite(t *testing.T, compose *docker.DockerCompose) {
	ctx := context.Background()
	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	// Startup adds the DTM readiness probe (up to ~60s) before reconciliation
	// can re-enqueue, on top of the 30s poll ticker — hence the wider window.
	const postRestartFinalizeTimeout = 5 * time.Minute

	killAndRestart := func(t *testing.T) {
		// By name, not StopNode(1): the node-number helpers are off by one vs
		// the container names ("weaviate-0"...). Stop silently no-ops on an
		// unknown name, but the paired Start errors on it, so a wrong name
		// cannot slip through as a fake restart.
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
						dropped: {
							Vectorizer:      map[string]any{"none": map[string]any{}},
							VectorIndexType: "hnsw",
						},
						sibling: {
							Vectorizer:      map[string]any{"none": map[string]any{}},
							VectorIndexType: "hnsw",
						},
					},
				}
				_, err := helper.Client(t).Schema.SchemaObjectsCreate(
					clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls), nil)
				require.NoError(t, err)

				// Insert in flushed waves so the drop has several real segments
				// to rewrite — an op with work left across the kill.
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
					time.Sleep(1500 * time.Millisecond) // > dirty-flush interval: each wave becomes a segment
				}
			})

			t.Run("drop, kill, restart", func(t *testing.T) {
				dropTargetVector(t, className, dropped)
				time.Sleep(killDelay)
				killAndRestart(t)
			})

			t.Run("drop converges after restart", func(t *testing.T) {
				// Error-tolerant class fetch: right after the restart the node may
				// still be rejoining/loading, and a hard-failing helper here would
				// turn a slow boot into a test failure.
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

	// Killed a few seconds in: the task is armed (edit ops registered in the
	// shard sidecar) but cannot have finished its first 30s poll tick —
	// recovery must resume/re-cover the op, not start from a clean slate.
	t.Run("crash mid-cleanup", journey("DropVectorIndexRestartMidCleanup", 1, 3*time.Second))

	// Killed immediately after the drop call returned: the marker is durable
	// but the task may or may not have been enqueued — either the task resumes
	// or startup reconciliation re-enqueues for the orphaned marker.
	t.Run("crash right after drop", journey("DropVectorIndexRestartAfterDrop", 2, 0))
}
