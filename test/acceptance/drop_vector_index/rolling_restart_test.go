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
	"net/http"
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

// runRollingRestartSuite pins the drop against the two k8s-shaped disruptions:
// a rolling restart landing mid-cleanup, and a leader kill right after
// finalize (a replayed completion must not corrupt the converged schema or a
// re-created vector). Cluster-only: rolling restarts and leader loss have no
// single-node analogue (plain restarts are covered by the restart suite).
func runRollingRestartSuite(t *testing.T, compose *docker.DockerCompose) {
	ctx := context.Background()
	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	const (
		dropped    = "vec"
		sibling    = "sibling"
		droppedDim = 32
		siblingDim = 64
		count      = 60
	)
	// Startup adds the DTM readiness probe before recovery paths can run.
	const postDisruptionTimeout = 5 * time.Minute

	waitNodeReady := func(t *testing.T, n int) {
		uri := compose.GetWeaviateNode(n).URI()
		require.Eventuallyf(t, func() bool {
			resp, err := http.Get(fmt.Sprintf("http://%s/v1/.well-known/ready", uri))
			if err != nil {
				return false
			}
			defer resp.Body.Close()
			return resp.StatusCode == http.StatusOK
		}, 90*time.Second, 250*time.Millisecond, "node %d ready after restart", n)
	}

	setup := func(t *testing.T, className string, idBlock int) {
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

		for wave := range 4 {
			batch := make([]*models.Object, 0, count/4)
			for i := range count / 4 {
				n := wave*(count/4) + i
				batch = append(batch, &models.Object{
					ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-00%02d-0000000017%02d", idBlock, n)),
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
	}

	verifyConverged := func(t *testing.T, className string) {
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			for _, uri := range weaviateNodeURIs(compose) {
				helper.SetupClient(uri)
				resp, err := helper.Client(t).Schema.SchemaObjectsGet(
					clschema.NewSchemaObjectsGetParams().WithClassName(className), nil)
				if !assert.NoError(collect, err) {
					return
				}
				_, present := resp.Payload.VectorConfig[dropped]
				assert.False(collect, present, "entry gone on %s", uri)
				assert.Contains(collect, resp.Payload.VectorConfig, sibling, "sibling on %s", uri)
			}
		}, postDisruptionTimeout, time.Second)
		helper.SetupClient(compose.GetWeaviate().URI())

		// Count-convergence barrier: right after a restart the node serves the
		// schema before its shards finish loading, and cross-node lists return
		// partial results in that window.
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			objs := listAllObjectsWithVectors(t, className)
			if !assert.Len(collect, objs, count) {
				return
			}
			for _, obj := range objs {
				assert.NotContains(collect, obj.Vectors, dropped)
				if v, ok := obj.Vectors[sibling]; assert.True(collect, ok) {
					assert.Len(collect, v, siblingDim)
				}
			}
		}, postDisruptionTimeout, time.Second)
		require.Equal(t, 3, nearVectorResults(t, className, sibling, randVec(siblingDim, 7), 3))
	}

	t.Run("rolling restart mid-cleanup", func(t *testing.T) {
		const className = "DropVectorIndexRollingRestart"
		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		setup(t, className, 1)
		dropTargetVector(t, className, dropped)

		// Roll every node, one at a time, while cleanup is in flight. By name:
		// the numeric StopNode helper is off by one vs the container names.
		for n, name := range []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2} {
			require.NoError(t, compose.Stop(ctx, name, nil), "stop %s", name)
			require.NoError(t, compose.Start(ctx, name), "start %s", name)
			waitNodeReady(t, n+1)
		}
		helper.SetupClient(compose.GetWeaviate().URI())

		verifyConverged(t, className)
	})

	t.Run("leader kill right after finalize", func(t *testing.T) {
		const className = "DropVectorIndexLeaderKillFinalize"
		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		setup(t, className, 2)
		dropTargetVector(t, className, dropped)
		eventuallyTargetVectorRemoved(t, className, dropped)

		// Kill the client's node (typically the RAFT leader) immediately after
		// observing the finalize, mid-propagation of the removal.
		zero := 0 * time.Second
		require.NoError(t, compose.Stop(ctx, docker.Weaviate0, &zero))
		require.NoError(t, compose.Start(ctx, docker.Weaviate0))
		waitNodeReady(t, 1)
		helper.SetupClient(compose.GetWeaviate().URI())

		verifyConverged(t, className)

		t.Run("name is re-creatable after the disruption", func(t *testing.T) {
			resp, err := helper.Client(t).Schema.SchemaObjectsGet(
				clschema.NewSchemaObjectsGetParams().WithClassName(className), nil)
			require.NoError(t, err)
			cls := resp.Payload
			cls.VectorConfig[dropped] = noneVectorConfig()
			_, err = helper.Client(t).Schema.SchemaObjectsUpdate(
				clschema.NewSchemaObjectsUpdateParams().WithClassName(className).WithObjectClass(cls), nil)
			require.NoError(t, err, "a replayed completion must not poison the re-created name")

			_, err = helper.Client(t).Objects.ObjectsCreate(
				clobjects.NewObjectsCreateParams().WithBody(&models.Object{
					ID:         "00000000-0000-0000-0002-000000001800",
					Class:      className,
					Properties: map[string]any{"name": "reborn"},
					Vectors:    models.Vectors{dropped: randVec(16, 1)},
				}), nil)
			require.NoError(t, err)
		})
	})
}
