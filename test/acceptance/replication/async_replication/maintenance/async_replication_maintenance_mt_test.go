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

package maintenance

import (
	"context"
	"fmt"
	"net/http"
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

// TestAsyncReplicationIntoMaintenanceNodeTenant: a tenant the maintenance node boots with empty must be refilled too.
func TestAsyncReplicationIntoMaintenanceNodeTenant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("MAINTENANCE_NODES", maintenanceNode).
		WithWeaviateEnv("ASYNC_REPLICATION_IN_MAINTENANCE_ENABLED", "true").
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS", "1").
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_IDLE_AFTER_SECONDS", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	class := articles.ParagraphsClass()
	frequency, whilePropagating, delay := int64(1000), int64(500), int64(500)
	class.Vectorizer = "none"
	class.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
	class.ReplicationConfig = &models.ReplicationConfig{
		Factor: 3,
		AsyncConfig: &models.ReplicationAsyncConfig{
			Frequency:                 &frequency,
			FrequencyWhilePropagating: &whilePropagating,
			PropagationDelay:          &delay,
		},
	}
	helper.CreateClass(t, class)
	const tenant = "tenant0"
	helper.CreateTenants(t, class.Class, []*models.Tenant{{Name: tenant, ActivityStatus: models.TenantActivityStatusHOT}})

	require.NoError(t, compose.StopNode(ctx, 2, nil))

	batch := make([]*models.Object, objectCount)
	for i := range batch {
		batch[i] = articles.NewParagraph().WithContents(fmt.Sprintf("paragraph#%d", i)).WithTenant(tenant).Object()
	}
	common.CreateObjectsCL(t, compose.GetWeaviate().URI(), batch, types.ConsistencyLevelQuorum)

	startCtx, startCancel := context.WithTimeout(ctx, 30*time.Second)
	defer startCancel()
	require.ErrorContains(t, compose.StartNode(startCtx, 2), "readiness check", "a maintenance node never reports ready")
	require.Equal(t, http.StatusServiceUnavailable, readyStatus(t, compose.GetWeaviateNode(3).URI()))
	helper.SetupClient(compose.GetWeaviate().URI())

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		counts, err := nodeObjectCounts(t, class.Class)
		require.NoError(ct, err)
		require.Equal(ct, int64(objectCount), counts["weaviate-0"])
		require.Equal(ct, int64(objectCount), counts["weaviate-1"])
		require.Equal(ct, int64(objectCount), counts[maintenanceNode])
	}, 3*time.Minute, time.Second, "async replication must refill the tenant on the maintenance node")
}
