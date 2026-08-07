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

package asyncrep_battle

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestBattleTenantRaces races tenant create/write/delete loops against
// runtime-flag toggles and node cycles. The drop-guard failure modes are a
// panic (fatal here via DISABLE_RECOVERY_ON_PANIC) or a resurrected
// hashtree_uuid dir for a deleted tenant.
func TestBattleTenantRaces(t *testing.T) {
	p := battleProfile()
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Minute)
	defer cancel()

	compose := buildCompose(ctx, t, nil)
	defer func() {
		if t.Failed() {
			compose.DumpWeaviateLogs(ctx, os.Stdout, 400)
		}
		require.NoError(t, compose.Terminate(ctx))
	}()

	const class = "BattleS6"
	uri1 := compose.GetWeaviateNode(1).URI()
	helper.SetupClient(uri1)
	helper.CreateClass(t, battleClass(class, 1, true))

	// Persistent tenants stay HOT and loaded so they remain inside the repair
	// mesh (cold empty tenants are a known gap, issue #12526).
	persistent := []string{"p1", "p2", "p3", "p4", "p5"}
	tenants := make([]*models.Tenant, len(persistent))
	for i, name := range persistent {
		tenants[i] = &models.Tenant{Name: name, ActivityStatus: models.TenantActivityStatusHOT}
	}
	helper.CreateTenants(t, class, tenants)
	probeByTenant := map[string]strfmt.UUID{}
	for _, tenant := range persistent {
		batch := make([]*models.Object, 20)
		for i := 0; i < 20; i++ {
			id := strfmt.UUID(uuid.NewString())
			batch[i] = &models.Object{
				ID: id, Class: class, Tenant: tenant,
				Properties: map[string]interface{}{"contents": fmt.Sprintf("%s-seed-%d", tenant, i), "ver": 1},
			}
			if i == 0 {
				probeByTenant[tenant] = id
			}
		}
		common.CreateTenantObjects(t, uri1, batch)
	}
	for _, tenant := range persistent {
		common.WaitForNodeReadyForTenant(t, uri1, class, probeByTenant[tenant], tenant)
	}

	var ephemeral []string
	disabled := false
	for i := 1; i <= p.tenantRaceIters; i++ {
		name := fmt.Sprintf("eph%03d", i)
		helper.CreateTenants(t, class, []*models.Tenant{{Name: name, ActivityStatus: models.TenantActivityStatusHOT}})
		batch := make([]*models.Object, 10)
		for j := 0; j < 10; j++ {
			batch[j] = &models.Object{
				ID: strfmt.UUID(uuid.NewString()), Class: class, Tenant: name,
				Properties: map[string]interface{}{"contents": fmt.Sprintf("%s-%d", name, j)},
			}
		}
		common.CreateTenantObjects(t, uri1, batch)
		// Let the shard load and a hashbeat tick build real hashtree state to race against.
		time.Sleep(1500 * time.Millisecond)
		require.NoError(t, helper.DeleteTenants(t, class, []string{name}))
		ephemeral = append(ephemeral, name)

		for _, tenant := range persistent {
			obj := &models.Object{
				ID: strfmt.UUID(uuid.NewString()), Class: class, Tenant: tenant,
				Properties: map[string]interface{}{"contents": fmt.Sprintf("%s-iter-%d", tenant, i)},
			}
			common.CreateTenantObjects(t, uri1, []*models.Object{obj})
		}

		if i%4 == 0 {
			disabled = !disabled
			writeAsyncReplicationOverride(ctx, t, compose, disabled)
		}
		if i == p.tenantRaceIters/3 {
			cycleTenantNode(ctx, t, compose, 2, nil, class, persistent[0], probeByTenant[persistent[0]])
			uri1 = compose.GetWeaviateNode(1).URI()
			helper.SetupClient(uri1)
		}
		if i == 2*p.tenantRaceIters/3 {
			cycleTenantNode(ctx, t, compose, 3, &sigkill, class, persistent[0], probeByTenant[persistent[0]])
			uri1 = compose.GetWeaviateNode(1).URI()
			helper.SetupClient(uri1)
		}
	}

	writeAsyncReplicationOverride(ctx, t, compose, false)
	time.Sleep(5 * time.Second)

	for _, uri := range nodeURIs(compose) {
		for _, tenant := range ephemeral[:min(len(ephemeral), 5)] {
			_, err := common.GetTenantObjectCL(t, uri, class, strfmt.UUID(uuid.NewString()), tenant, types.ConsistencyLevelOne)
			require.Error(t, err, "deleted tenant %s still serves reads on %s", tenant, uri)
		}
	}
	for n := 1; n <= 3; n++ {
		requireNoEphemeralTenantDirs(ctx, t, compose, n, class, ephemeral)
	}

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for _, tenant := range persistent {
			var sets []map[string]struct{}
			for _, uri := range nodeURIs(compose) {
				s, err := nodeIDSet(uri, class, tenant, 2000)
				require.NoError(ct, err)
				sets = append(sets, s)
			}
			require.Equal(ct, sets[0], sets[1], "tenant %s: node1 vs node2 diverge", tenant)
			require.Equal(ct, sets[0], sets[2], "tenant %s: node1 vs node3 diverge", tenant)
		}
	}, p.convergeTimeout, 2*time.Second, "persistent tenants did not converge")

	shards := persistent
	clusters := clusterURIs(compose)
	createdAt := time.Now().UTC()
	cutoffMs := createdAt.Add(10 * time.Second).UnixMilli()
	for _, cluster := range clusters {
		asyncCheckpointCreate(t, cluster, class, shards, cutoffMs, createdAt.UnixMilli())
	}
	defer func() {
		for _, cluster := range clusters {
			asyncCheckpointDelete(t, cluster, class, shards)
		}
	}()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for _, shard := range shards {
			var roots []string
			for _, cluster := range clusters {
				statuses := asyncCheckpointStatus(t, cluster, class, []string{shard})
				entry, ok := statuses[shard]
				require.True(ct, ok)
				require.NotZero(ct, entry.CutoffMs)
				roots = append(roots, base64.StdEncoding.EncodeToString(entry.Root))
			}
			require.Equal(ct, roots[0], roots[1], "tenant %s roots diverge", shard)
			require.Equal(ct, roots[0], roots[2], "tenant %s roots diverge", shard)
		}
	}, p.convergeTimeout, 2*time.Second, "persistent tenant hashtree roots did not converge")

	requireCleanLogs(ctx, t, compose)
}

// cycleTenantNode cycles a node and waits for tenant readiness on restart.
func cycleTenantNode(ctx context.Context, t *testing.T, compose *docker.DockerCompose, n int, timeout *time.Duration, class, tenant string, probeID strfmt.UUID) {
	t.Helper()
	stopNode(ctx, t, compose, n, timeout)
	require.NoError(t, compose.StartNode(ctx, n-1))
	require.NoError(t, compose.EnsureRunning(ctx, n-1))
	uri := compose.GetWeaviateNode(n).URI()
	common.WaitForNodeReadyForTenant(t, uri, class, probeID, tenant)
}
