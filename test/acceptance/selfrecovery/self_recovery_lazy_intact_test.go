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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
)

func TestSelfRecoveryLazyIntactNodeMissingShardDirs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose := startSelfRecoveryCluster(ctx, t, srClusterCfg{asyncDisabled: true, lazyLoading: true, warmupMinObjects: 20, persistentData: true})

	const (
		mtClass     = "LazyIntactTenanted"
		paraClass   = "LazyIntactPara"
		bigTenant   = "big"
		smallTenant = "small"
		neverTenant = "never"
		bigCount    = 100
		smallCount  = 5
		paraCount   = 100
		victimIdx   = 2
	)
	victim := docker.Weaviate2
	donors := []string{docker.Weaviate0, docker.Weaviate1}
	allNodes := []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2}
	var (
		paraShard string
		opsBefore map[string]string
	)
	dumpVictimOnFailure(t, compose, victimIdx, victim)

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
		submitBatch(t, srParagraphObjects(paraClass, "aaaaaaaa-1111-1111-1111", paraCount, ""), types.ConsistencyLevelAll)
		submitBatch(t, srParagraphObjects(mtClass, "bbbbbbbb-2222-2222-2222", bigCount, bigTenant), types.ConsistencyLevelAll)
		submitBatch(t, srParagraphObjects(mtClass, "cccccccc-3333-3333-3333", smallCount, smallTenant), types.ConsistencyLevelAll)
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
	})

	mustRun(t, "written tenants are loaded and the never-written one is cold on every node", func(t *testing.T) {
		for _, node := range allNodes {
			assertShardLoadedState(t, mtClass, node, map[string]bool{bigTenant: true, smallTenant: true, neverTenant: false})
		}
		waitShardsLoaded(t, paraClass, 1)
		paraShard = largestShard(waitShardObjectCounts(t, paraClass, docker.Weaviate0, paraCount))
		require.NotEmpty(t, paraShard)
		opsBefore = mustSelfRecoveryOpTargets(t, victim)
	})

	mustRun(t, "remove the para shard dir and every tenant dir on node-3, then restart", func(t *testing.T) {
		removePathsAndKill(ctx, t, compose, victimIdx,
			shardDirInContainer(paraClass, paraShard),
			shardDirInContainer(mtClass, bigTenant),
			shardDirInContainer(mtClass, smallTenant),
			shardDirInContainer(mtClass, neverTenant))
		startNode(ctx, t, compose, victimIdx)
		waitNewSelfRecoveryOp(t, victim, opsBefore)
		waitNoShardRecovering(t, mtClass, victim)
		waitNoShardRecovering(t, paraClass, victim)
		waitForSelfRecoveryToSettle(t, allNodes, 5*time.Minute)
	})

	mustRun(t, "the shards with data were copied; the never-written tenant fell back to empty", func(t *testing.T) {
		want := []string{paraClass + "/" + paraShard, mtClass + "/" + bigTenant, mtClass + "/" + smallTenant}
		sort.Strings(want)
		require.Equal(t, want, mustNewSelfRecoveryTargets(t, victim, opsBefore))
	})

	mustRun(t, "the recovered shards follow the lazy policy", func(t *testing.T) {
		assertShardLoadedState(t, mtClass, victim, map[string]bool{bigTenant: true, smallTenant: false, neverTenant: false})
		assertShardObjectCount(t, mtClass, victim, bigTenant, bigCount)
		assertNodeRecovered(t, paraClass, victim, 1, paraCount)
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

	mustRun(t, "the cold copy loads on first access with its full data", func(t *testing.T) {
		readTenantObjectsFromNode(t, compose, mtClass, "cccccccc-3333-3333-3333", smallCount, victim, smallTenant)
		assertShardObjectCount(t, mtClass, victim, smallTenant, smallCount)
	})

	mustRun(t, "the never-written tenant is writable on the recovered node", func(t *testing.T) {
		obj := srParagraphObjects(mtClass, "dddddddd-4444-4444-4444", 1, neverTenant)[0]
		require.NoError(t, common.CreateObjectCL(t, compose.ContainerURI(victimIdx), obj, types.ConsistencyLevelAll))
		readTenantObjectsFromNode(t, compose, mtClass, "dddddddd-4444-4444-4444", 1, victim, neverTenant)
		assertShardLoadedState(t, mtClass, victim, map[string]bool{neverTenant: true})
	})

	mustRun(t, "cluster is healthy with no recovery in flight", func(t *testing.T) {
		waitClusterHealthy(t)
		assertNoActiveRecovery(t, allNodes, 10*time.Second)
	})
}
