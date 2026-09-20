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

package selfrecovery

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func setTenantStatus(t *testing.T, class, tenant, status string) error {
	t.Helper()
	params := clschema.NewTenantsUpdateParams().WithClassName(class).
		WithBody([]*models.Tenant{{Name: tenant, ActivityStatus: status}})
	_, err := helper.Client(t).Schema.TenantsUpdate(params, nil)
	return err
}

func waitTenantStatus(t *testing.T, class, tenant, status string) {
	t.Helper()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		params := clschema.NewTenantsGetOneParams().WithClassName(class).WithTenantName(tenant)
		body, err := helper.Client(t).Schema.TenantsGetOne(params, nil)
		require.NoError(ct, err)
		require.Equal(ct, status, body.Payload.ActivityStatus)
	}, 60*time.Second, 1*time.Second, "tenant %s never reached %s", tenant, status)
}

func tenantIDPrefix(i int) string {
	return fmt.Sprintf("%08d-%04d-%04d-%04d", i, i, i, i)
}

func TestSelfRecoveryLazyTenantActivationRecovers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose := startSelfRecoveryCluster(ctx, t, srClusterCfg{asyncDisabled: true, lazyLoading: true, warmupMinObjects: 20, concurrency: 1})

	const (
		mtClass      = "LazyFlip"
		coldTenant   = "coldatwipe"
		queuedTenant = "queued"
		count        = 5
		fillers      = 6
		wipedIdx     = 2
	)
	wipedNode := docker.Weaviate2
	allNodes := []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2}
	tenants := []string{coldTenant, queuedTenant}
	for i := 0; i < fillers; i++ {
		tenants = append(tenants, fmt.Sprintf("filler%d", i))
	}
	flipped := false

	mustRun(t, "wait for cluster to form quorum", func(t *testing.T) {
		waitClusterHealthy(t)
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
	})

	mustRun(t, "create the collection and tenants", func(t *testing.T) {
		ensureClass(t, srTenantedClass(mtClass))
		var tenantModels []*models.Tenant
		for _, name := range tenants {
			tenantModels = append(tenantModels, &models.Tenant{Name: name})
		}
		ensureTenants(t, mtClass, tenantModels)
		waitShardsPresent(t, mtClass, len(tenants))
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
		for _, node := range allNodes {
			waitNoShardRecovering(t, mtClass, node)
		}
	})

	mustRun(t, "ingest and deactivate the cold-at-wipe tenant", func(t *testing.T) {
		for i, name := range tenants {
			submitBatch(t, srParagraphObjects(mtClass, tenantIDPrefix(i+1), count, name), types.ConsistencyLevelQuorum)
		}
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
		require.NoError(t, setTenantStatus(t, mtClass, coldTenant, models.TenantActivityStatusCOLD))
		waitTenantStatus(t, mtClass, coldTenant, models.TenantActivityStatusCOLD)
	})

	mustRun(t, "wipe and restart node-3", func(t *testing.T) {
		wipeAndRestart(ctx, t, compose, wipedIdx)
	})

	mustRun(t, "flip the queued tenant COLD and HOT while its recovery is still queued", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			statuses, err := shardStatusesOnNode(t, mtClass, wipedNode)
			require.NoError(ct, err)
			require.Contains(ct, statuses, queuedTenant)
		}, 3*time.Minute, 500*time.Millisecond, "the wiped node never listed %s", queuedTenant)
		if err := setTenantStatus(t, mtClass, queuedTenant, models.TenantActivityStatusCOLD); err != nil {
			t.Logf("deactivation refused (copy already registered), skipping the queued flip: %v", err)
			return
		}
		waitTenantStatus(t, mtClass, queuedTenant, models.TenantActivityStatusCOLD)
		require.NoError(t, setTenantStatus(t, mtClass, queuedTenant, models.TenantActivityStatusHOT))
		waitTenantStatus(t, mtClass, queuedTenant, models.TenantActivityStatusHOT)
		flipped = true
	})

	mustRun(t, "recovery settles", func(t *testing.T) {
		waitNoShardRecovering(t, mtClass, wipedNode)
		waitForSelfRecoveryToSettle(t, allNodes, 5*time.Minute)
	})

	mustRun(t, "the cold-at-wipe tenant has no folder on the wiped node until activated", func(t *testing.T) {
		statuses, err := shardStatusesOnNode(t, mtClass, wipedNode)
		require.NoError(t, err)
		require.NotContains(t, statuses, coldTenant)
	})

	mustRun(t, "activating it recovers the data from peers", func(t *testing.T) {
		require.NoError(t, setTenantStatus(t, mtClass, coldTenant, models.TenantActivityStatusHOT))
		waitTenantStatus(t, mtClass, coldTenant, models.TenantActivityStatusHOT)
		waitNoShardRecovering(t, mtClass, wipedNode)
		waitForSelfRecoveryToSettle(t, allNodes, 5*time.Minute)
		readTenantObjectsFromNode(t, compose, mtClass, tenantIDPrefix(1), count, wipedNode, coldTenant)
		assertShardObjectCount(t, mtClass, wipedNode, coldTenant, count)
	})

	mustRun(t, "the flipped tenant holds its data on the wiped node", func(t *testing.T) {
		if !flipped {
			t.Skip("queued flip did not land while the recovery was queued")
		}
		readTenantObjectsFromNode(t, compose, mtClass, tenantIDPrefix(2), count, wipedNode, queuedTenant)
		assertShardObjectCount(t, mtClass, wipedNode, queuedTenant, count)
	})

	mustRun(t, "cluster is healthy", func(t *testing.T) {
		waitClusterHealthy(t)
	})
}
