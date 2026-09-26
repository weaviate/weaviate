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
	"io"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func srTenantedClass(name string) *models.Class {
	c := srParagraphClass(name)
	c.ShardingConfig = nil
	c.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
	return c
}

// dumpNodeLogLines prints node idx's log lines matching any needle; for failure diagnosis only.
func dumpNodeLogLines(ctx context.Context, t *testing.T, compose *docker.DockerCompose, idx int, needles ...string) {
	t.Helper()
	node, err := compose.ContainerAt(idx)
	if err != nil {
		t.Logf("node %d: container unavailable: %v", idx, err)
		return
	}
	logs, err := node.Container().Logs(ctx)
	if err != nil {
		t.Logf("node %d: logs unavailable: %v", idx, err)
		return
	}
	defer logs.Close()
	buf, _ := io.ReadAll(logs)
	for _, line := range strings.Split(string(buf), "\n") {
		for _, needle := range needles {
			if strings.Contains(line, needle) {
				t.Logf("node %d: %s", idx, line)
				break
			}
		}
	}
}

func dumpSelfRecoveryOps(t *testing.T, targetNode string) {
	t.Helper()
	body, err := helper.Client(t).Replication.ListReplication(
		replication.NewListReplicationParams().WithTargetNode(&targetNode), nil)
	if err != nil {
		t.Logf("list ops for %s: %v", targetNode, err)
		return
	}
	for _, op := range body.Payload {
		state, opType, shard, coll := "", "", "", ""
		if op.Status != nil {
			state = op.Status.State
		}
		if op.Type != nil {
			opType = *op.Type
		}
		if op.Shard != nil {
			shard = *op.Shard
		}
		if op.Collection != nil {
			coll = *op.Collection
		}
		t.Logf("op %v type=%s collection=%s shard=%s state=%s", op.ID, opType, coll, shard, state)
	}
}

func selfRecoveryOpIDs(t *testing.T, targetNode string) map[string]struct{} {
	t.Helper()
	body, err := helper.Client(t).Replication.ListReplication(
		replication.NewListReplicationParams().WithTargetNode(&targetNode), nil)
	require.NoError(t, err)
	ids := map[string]struct{}{}
	for _, op := range body.Payload {
		if op.Type != nil && *op.Type == "SELF_RECOVERY" && op.ID != nil {
			ids[op.ID.String()] = struct{}{}
		}
	}
	return ids
}

func readTenantObjectsFromNode(t *testing.T, compose *docker.DockerCompose, class, idPrefix string, n int, node, tenant string) {
	t.Helper()
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		for i := 0; i < n; i++ {
			id := strfmt.UUID(fmt.Sprintf("%s-%012d", idPrefix, i+1))
			obj, err := common.GetTenantObjectFromNode(t, compose.GetWeaviate().URI(), class, id, node, tenant)
			assert.NoError(ct, err)
			assert.NotNil(ct, obj, "object %s missing on %s", id, node)
		}
	}, 30*time.Second, 1*time.Second, "objects of tenant %s not readable on %s", tenant, node)
}

func TestSelfRecoveryLazyWipedNode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose := startSelfRecoveryCluster(ctx, t, srClusterCfg{asyncDisabled: true, lazyLoading: true, warmupMinObjects: 20})

	const (
		mtClass     = "LazyTenanted"
		paraClass   = "LazyPara"
		bigTenant   = "big"
		smallTenant = "small"
		neverTenant = "never"
		bigCount    = 100
		smallCount  = 5
		paraCount   = 100
		wipedIdx    = 2
	)
	wipedNode := docker.Weaviate2
	donors := []string{docker.Weaviate0, docker.Weaviate1}
	allNodes := []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2}

	mustRun(t, "wait for cluster to form quorum", func(t *testing.T) {
		waitClusterHealthy(t)
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
	})

	mustRun(t, "create collections and tenants", func(t *testing.T) {
		ensureClass(t, srParagraphClass(paraClass))
		ensureClass(t, srTenantedClass(mtClass))
		ensureTenants(t, mtClass, []*models.Tenant{{Name: bigTenant}, {Name: smallTenant}, {Name: neverTenant}})
		waitShardsPresent(t, paraClass, 1)
		waitShardsPresent(t, mtClass, 3)
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
		for _, node := range allNodes {
			waitNoShardRecovering(t, paraClass, node)
			waitNoShardRecovering(t, mtClass, node)
		}
	})

	mustRun(t, "ingest", func(t *testing.T) {
		submitBatch(t, srParagraphObjects(paraClass, "44444444-4444-4444-4444", paraCount, ""), types.ConsistencyLevelQuorum)
		submitBatch(t, srParagraphObjects(mtClass, "55555555-5555-5555-5555", bigCount, bigTenant), types.ConsistencyLevelQuorum)
		submitBatch(t, srParagraphObjects(mtClass, "66666666-6666-6666-6666", smallCount, smallTenant), types.ConsistencyLevelQuorum)
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
	})

	mustRun(t, "written tenants are loaded and the never-written one is cold on every node", func(t *testing.T) {
		for _, node := range allNodes {
			assertShardLoadedState(t, mtClass, node, map[string]bool{bigTenant: true, smallTenant: true, neverTenant: false})
		}
		waitShardsLoaded(t, paraClass, 1)
	})

	mustRun(t, "wipe and restart node-3", func(t *testing.T) {
		wipeAndRestart(ctx, t, compose, wipedIdx)
	})

	mustRun(t, "recovery fires and settles", func(t *testing.T) {
		waitSelfRecoveryOpFired(t, wipedNode)
		waitNoShardRecovering(t, mtClass, wipedNode)
		waitNoShardRecovering(t, paraClass, wipedNode)
		waitForSelfRecoveryToSettle(t, allNodes, 5*time.Minute)
	})

	mustRun(t, "the recovered node follows the lazy policy", func(t *testing.T) {
		t.Cleanup(func() {
			if !t.Failed() {
				return
			}
			dumpSelfRecoveryOps(t, wipedNode)
			dumpNodeLogLines(ctx, t, compose, wipedIdx, "self_recovery_promote", "warming it up", "failed to count",
				"skip_shard_warmup", "load_all_shards", "failure while", "Unable to load shard", "finalizing",
				"self_recovery.", "\"shard\":\""+smallTenant+"\"", "shard_name\":\""+smallTenant+"\"")
		})
		assertShardLoadedState(t, mtClass, wipedNode, map[string]bool{bigTenant: true, smallTenant: false, neverTenant: false})
		assertShardObjectCount(t, mtClass, wipedNode, bigTenant, bigCount)
		assertNodeRecovered(t, paraClass, wipedNode, 1, paraCount)
	})

	mustRun(t, "donors answered the probe for the never-written tenant without loading it", func(t *testing.T) {
		for _, node := range donors {
			node := node
			assert.Never(t, func() bool {
				statuses, err := shardStatusesOnNode(t, mtClass, node)
				return err == nil && statuses[neverTenant] != nil && statuses[neverTenant].Loaded
			}, 10*time.Second, 1*time.Second, "donor %s loaded the never-written tenant", node)
		}
	})

	mustRun(t, "cold copies load on first access with their full data", func(t *testing.T) {
		readTenantObjectsFromNode(t, compose, mtClass, "66666666-6666-6666-6666", smallCount, wipedNode, smallTenant)
		assertShardObjectCount(t, mtClass, wipedNode, smallTenant, smallCount)
	})

	mustRun(t, "the never-written tenant is writable on the recovered node", func(t *testing.T) {
		obj := srParagraphObjects(mtClass, "77777777-7777-7777-7777", 1, neverTenant)[0]
		require.NoError(t, common.CreateObjectCL(t, compose.ContainerURI(wipedIdx), obj, types.ConsistencyLevelAll))
		readTenantObjectsFromNode(t, compose, mtClass, "77777777-7777-7777-7777", 1, wipedNode, neverTenant)
		assertShardLoadedState(t, mtClass, wipedNode, map[string]bool{neverTenant: true})
	})

	mustRun(t, "cluster is healthy", func(t *testing.T) {
		waitClusterHealthy(t)
	})
}

func TestSelfRecoveryLazyIntactRestartLeavesRegisteredTenantsAlone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	compose := startSelfRecoveryCluster(ctx, t, srClusterCfg{asyncDisabled: true, lazyLoading: true, warmupMinObjects: 20, persistentData: true})

	const (
		mtClass       = "LazyRestart"
		neverTenant   = "never"
		writtenTenant = "written"
		writtenCount  = 5
		restartIdx    = 2
	)
	restartedNode := docker.Weaviate2
	allNodes := []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2}
	var opsBeforeRestart map[string]struct{}

	mustRun(t, "wait for cluster to form quorum", func(t *testing.T) {
		waitClusterHealthy(t)
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
	})

	mustRun(t, "create the collection, one written and one never-written tenant", func(t *testing.T) {
		ensureClass(t, srTenantedClass(mtClass))
		ensureTenants(t, mtClass, []*models.Tenant{{Name: neverTenant}, {Name: writtenTenant}})
		waitShardsPresent(t, mtClass, 2)
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
		for _, node := range allNodes {
			waitNoShardRecovering(t, mtClass, node)
		}
		submitBatch(t, srParagraphObjects(mtClass, "88888888-8888-8888-8888", writtenCount, writtenTenant), types.ConsistencyLevelQuorum)
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
		for _, node := range allNodes {
			assertShardLoadedState(t, mtClass, node, map[string]bool{writtenTenant: true, neverTenant: false})
		}
		opsBeforeRestart = selfRecoveryOpIDs(t, restartedNode)
	})

	mustRun(t, "restart node-3 without wiping it", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, restartIdx)
		common.StartNodeAt(ctx, t, compose, restartIdx)
		helper.SetupClient(compose.GetWeaviate().URI())
		waitClusterHealthy(t)
	})

	mustRun(t, "no recovery fires for tenants whose folder exists", func(t *testing.T) {
		assertNoActiveRecovery(t, []string{restartedNode}, 20*time.Second)
		require.Equal(t, opsBeforeRestart, selfRecoveryOpIDs(t, restartedNode),
			"an intact restart must not self-recover a registered tenant")
	})

	mustRun(t, "both tenants come back cold under the warmup threshold", func(t *testing.T) {
		assertShardLoadedState(t, mtClass, restartedNode, map[string]bool{writtenTenant: false, neverTenant: false})
	})

	mustRun(t, "the written tenant loads on first read with its data", func(t *testing.T) {
		readTenantObjectsFromNode(t, compose, mtClass, "88888888-8888-8888-8888", writtenCount, restartedNode, writtenTenant)
		assertShardLoadedState(t, mtClass, restartedNode, map[string]bool{writtenTenant: true})
	})

	mustRun(t, "the never-written tenant is writable", func(t *testing.T) {
		obj := srParagraphObjects(mtClass, "99999999-9999-9999-9999", 1, neverTenant)[0]
		require.NoError(t, common.CreateObjectCL(t, compose.ContainerURI(restartIdx), obj, types.ConsistencyLevelAll))
		readTenantObjectsFromNode(t, compose, mtClass, "99999999-9999-9999-9999", 1, restartedNode, neverTenant)
	})
}
