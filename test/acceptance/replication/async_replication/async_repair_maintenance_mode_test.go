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

package replication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

// TestMaintenanceModePeerIsQuietForAsyncReplication: a peer in maintenance mode is retry-later on the REST transport too — hashbeats keep running without inflating the failure counter.
func TestMaintenanceModePeerIsQuietForAsyncReplication(t *testing.T) {
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("PROMETHEUS_MONITORING_ENABLED", "true").
		WithWeaviateEnv("MAINTENANCE_NODES", "weaviate-2").
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()

	freqMs := int64(1000)

	t.Run("create RF=3 schema and data with one node in maintenance", func(t *testing.T) {
		paragraphClass.Vectorizer = "none"
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: 3,
			AsyncConfig: &models.ReplicationAsyncConfig{
				Frequency:                 &freqMs,
				FrequencyWhilePropagating: &freqMs,
			},
		}
		helper.CreateClass(t, paragraphClass)

		batch := make([]*models.Object, 50)
		for i := range batch {
			batch[i] = articles.NewParagraph().WithContents(fmt.Sprintf("paragraph#%d", i)).Object()
		}
		common.CreateObjectsCL(t, compose.GetWeaviate().URI(), batch, types.ConsistencyLevelOne)
	})

	t.Run("async replication is active on the healthy nodes", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			n, err := shardsAsyncReplicationLen(t, paragraphClass.Class)
			require.NoError(ct, err)
			require.Greater(ct, n, 0)
		}, rebuildEventuallyDeadline, 1*time.Second)
	})

	t.Run("hashbeats against the maintenance peer stay quiet", func(t *testing.T) {
		baselines := make([]float64, 2)
		for i := 1; i <= 2; i++ {
			iterations, err := nodeMetricValue(ctx, compose.GetWeaviateNode(i), "async_replication_iteration_count")
			require.NoError(t, err)
			baselines[i-1] = iterations
		}

		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			for i := 1; i <= 2; i++ {
				iterations, err := nodeMetricValue(ctx, compose.GetWeaviateNode(i), "async_replication_iteration_count")
				require.NoError(ct, err)
				require.GreaterOrEqual(ct, iterations, baselines[i-1]+5, "node %d must keep hashbeating", i)
			}
		}, rebuildEventuallyDeadline, 2*time.Second)

		for i := 1; i <= 2; i++ {
			failures, err := nodeMetricValue(ctx, compose.GetWeaviateNode(i), "async_replication_iteration_failure_count")
			require.NoError(t, err)
			require.Zero(t, failures, "node %d must not count the maintenance peer as a failure", i)
		}
	})
}
