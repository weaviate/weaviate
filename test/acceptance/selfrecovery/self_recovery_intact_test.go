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

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
)

func largestShard(counts map[string]int64) string {
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Strings(names)
	best := ""
	for _, name := range names {
		if best == "" || counts[name] > counts[best] {
			best = name
		}
	}
	return best
}

func dumpVictimOnFailure(t *testing.T, compose *docker.DockerCompose, idx int, node string) {
	t.Helper()
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		dumpSelfRecoveryOps(t, node)
		dumpNodeLogLines(ctx, t, compose, idx, "self_recovery", "RECOVERING", "failure while", "Unable to load shard", "restoring schema from snapshot")
	})
}

func TestSelfRecoveryIntactNodeMissingShardDirs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	compose := startSelfRecoveryCluster(ctx, t, srClusterCfg{asyncDisabled: true, persistentData: true})

	const (
		multiClass  = "IntactMulti"
		singleClass = "IntactSingle"
		mtClass     = "IntactTenants"
		hotTenant   = "hot"
		coldTenant  = "cold"
		multiCount  = 300
		singleCount = 100
		hotCount    = 40
		coldCount   = 30
		victimIdx   = 2
	)
	victim := docker.Weaviate2
	allNodes := []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2}
	var (
		multiCounts, singleCounts map[string]int64
		deletedShard, singleShard string
		opsBefore                 map[string]string
	)
	dumpVictimOnFailure(t, compose, victimIdx, victim)

	mustRun(t, "wait for cluster to form quorum", func(t *testing.T) {
		waitClusterHealthy(t)
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
	})

	mustRun(t, "create collections and tenants", func(t *testing.T) {
		ensureClass(t, srMultiShardClass(multiClass, 3))
		ensureClass(t, srParagraphClass(singleClass))
		ensureClass(t, srTenantedClass(mtClass))
		ensureTenants(t, mtClass, []*models.Tenant{{Name: hotTenant}, {Name: coldTenant}})
		waitShardsPresent(t, multiClass, 3)
		waitShardsPresent(t, singleClass, 1)
		waitShardsPresent(t, mtClass, 2)
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
		for _, node := range allNodes {
			for _, class := range []string{multiClass, singleClass, mtClass} {
				waitNoShardRecovering(t, class, node)
			}
		}
		waitShardsLoaded(t, multiClass, 3)
		waitShardsLoaded(t, singleClass, 1)
	})

	mustRun(t, "ingest", func(t *testing.T) {
		submitBatch(t, srParagraphObjects(multiClass, "aaaaaaaa-aaaa-aaaa-aaaa", multiCount, ""), types.ConsistencyLevelAll)
		submitBatch(t, srParagraphObjects(singleClass, "bbbbbbbb-bbbb-bbbb-bbbb", singleCount, ""), types.ConsistencyLevelAll)
		submitBatch(t, srParagraphObjects(mtClass, "cccccccc-cccc-cccc-cccc", hotCount, hotTenant), types.ConsistencyLevelAll)
		submitBatch(t, srParagraphObjects(mtClass, "dddddddd-dddd-dddd-dddd", coldCount, coldTenant), types.ConsistencyLevelAll)
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
	})

	mustRun(t, "record per-shard counts and the ops registered so far", func(t *testing.T) {
		multiCounts = waitShardObjectCounts(t, multiClass, docker.Weaviate0, multiCount)
		singleCounts = waitShardObjectCounts(t, singleClass, docker.Weaviate0, singleCount)
		require.Len(t, multiCounts, 3)
		require.Len(t, singleCounts, 1)
		assertShardObjectCounts(t, multiClass, victim, multiCounts)
		assertShardObjectCounts(t, singleClass, victim, singleCounts)
		assertShardObjectCount(t, mtClass, victim, hotTenant, hotCount)
		assertShardObjectCount(t, mtClass, victim, coldTenant, coldCount)
		deletedShard = largestShard(multiCounts)
		require.Positive(t, multiCounts[deletedShard])
		singleShard = largestShard(singleCounts)
		opsBefore = mustSelfRecoveryOpTargets(t, victim)
	})

	mustRun(t, "remove one shard dir of the multi-shard class and restart", func(t *testing.T) {
		removeShardDirsAndKill(ctx, t, compose, victimIdx, multiClass, deletedShard)
		startNode(ctx, t, compose, victimIdx)
		waitNewSelfRecoveryOp(t, victim, opsBefore)
		waitNoShardRecovering(t, multiClass, victim)
		waitForSelfRecoveryToSettle(t, allNodes, 5*time.Minute)
	})

	mustRun(t, "only the removed shard recovered, with its full data", func(t *testing.T) {
		require.Equal(t, []string{multiClass + "/" + deletedShard}, mustNewSelfRecoveryTargets(t, victim, opsBefore))
		assertShardObjectCounts(t, multiClass, victim, multiCounts)
		assertShardObjectCounts(t, singleClass, victim, singleCounts)
		readObjectsFromNode(t, compose, multiClass, "aaaaaaaa-aaaa-aaaa-aaaa", 20, victim)
		opsBefore = mustSelfRecoveryOpTargets(t, victim)
	})

	mustRun(t, "remove the whole single-shard class dir and restart", func(t *testing.T) {
		removeClassDirAndKill(ctx, t, compose, victimIdx, singleClass)
		startNode(ctx, t, compose, victimIdx)
		waitNewSelfRecoveryOp(t, victim, opsBefore)
		waitNoShardRecovering(t, singleClass, victim)
		waitForSelfRecoveryToSettle(t, allNodes, 5*time.Minute)
	})

	mustRun(t, "the class's shard recovered and nothing else did", func(t *testing.T) {
		require.Equal(t, []string{singleClass + "/" + singleShard}, mustNewSelfRecoveryTargets(t, victim, opsBefore))
		assertShardObjectCounts(t, singleClass, victim, singleCounts)
		assertShardObjectCounts(t, multiClass, victim, multiCounts)
		readObjectsFromNode(t, compose, singleClass, "bbbbbbbb-bbbb-bbbb-bbbb", 20, victim)
		opsBefore = mustSelfRecoveryOpTargets(t, victim)
	})

	mustRun(t, "deactivate one tenant, remove both tenant dirs and restart", func(t *testing.T) {
		require.NoError(t, setTenantStatus(t, mtClass, coldTenant, models.TenantActivityStatusCOLD))
		waitTenantStatus(t, mtClass, coldTenant, models.TenantActivityStatusCOLD)
		removeShardDirsAndKill(ctx, t, compose, victimIdx, mtClass, hotTenant, coldTenant)
		startNode(ctx, t, compose, victimIdx)
		waitNewSelfRecoveryOp(t, victim, opsBefore)
		waitNoShardRecovering(t, mtClass, victim)
		waitForSelfRecoveryToSettle(t, allNodes, 5*time.Minute)
	})

	mustRun(t, "the hot tenant recovered at startup and the cold one waited", func(t *testing.T) {
		require.Equal(t, []string{mtClass + "/" + hotTenant}, mustNewSelfRecoveryTargets(t, victim, opsBefore))
		assertShardObjectCount(t, mtClass, victim, hotTenant, hotCount)
		readTenantObjectsFromNode(t, compose, mtClass, "cccccccc-cccc-cccc-cccc", hotCount, victim, hotTenant)
		waitTenantStatus(t, mtClass, coldTenant, models.TenantActivityStatusCOLD)
		opsBefore = mustSelfRecoveryOpTargets(t, victim)
	})

	mustRun(t, "activating the cold tenant recovers it", func(t *testing.T) {
		require.NoError(t, setTenantStatus(t, mtClass, coldTenant, models.TenantActivityStatusHOT))
		waitTenantStatus(t, mtClass, coldTenant, models.TenantActivityStatusHOT)
		waitNewSelfRecoveryOp(t, victim, opsBefore)
		waitNoShardRecovering(t, mtClass, victim)
		waitForSelfRecoveryToSettle(t, allNodes, 5*time.Minute)
		require.Equal(t, []string{mtClass + "/" + coldTenant}, mustNewSelfRecoveryTargets(t, victim, opsBefore))
		assertShardObjectCount(t, mtClass, victim, coldTenant, coldCount)
		readTenantObjectsFromNode(t, compose, mtClass, "dddddddd-dddd-dddd-dddd", coldCount, victim, coldTenant)
	})

	mustRun(t, "cluster is healthy with no recovery in flight", func(t *testing.T) {
		waitClusterHealthy(t)
		assertNoActiveRecovery(t, allNodes, 10*time.Second)
	})
}

func TestSelfRecoveryIntactNodeSnapshotRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose := startSelfRecoveryCluster(ctx, t, srClusterCfg{asyncDisabled: true, raftTrailingLogs: true, debugPort: true, persistentData: true})

	const (
		class     = "IntactSnapshot"
		count     = 100
		victimIdx = 2
	)
	victim := docker.Weaviate2
	allNodes := []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2}
	var (
		counts    map[string]int64
		shard     string
		opsBefore map[string]string
	)
	dumpVictimOnFailure(t, compose, victimIdx, victim)

	mustRun(t, "wait for cluster to form quorum", func(t *testing.T) {
		waitClusterHealthy(t)
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
	})

	mustRun(t, "create the collection and ingest", func(t *testing.T) {
		ensureClass(t, srParagraphClass(class))
		waitShardsPresent(t, class, 1)
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
		for _, node := range allNodes {
			waitNoShardRecovering(t, class, node)
		}
		waitShardsLoaded(t, class, 1)
		submitBatch(t, srParagraphObjects(class, "eeeeeeee-eeee-eeee-eeee", count, ""), types.ConsistencyLevelAll)
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
		counts = waitShardObjectCounts(t, class, docker.Weaviate0, count)
		require.Len(t, counts, 1)
		shard = largestShard(counts)
		assertShardObjectCounts(t, class, victim, counts)
		opsBefore = mustSelfRecoveryOpTargets(t, victim)
	})

	mustRun(t, "snapshot the victim's RAFT state", func(t *testing.T) {
		forceRaftSnapshot(ctx, t, compose, victimIdx)
	})

	mustRun(t, "remove the shard dir and restart", func(t *testing.T) {
		removeShardDirsAndKill(ctx, t, compose, victimIdx, class, shard)
		startNode(ctx, t, compose, victimIdx)
		waitNewSelfRecoveryOp(t, victim, opsBefore)
		waitNoShardRecovering(t, class, victim)
		waitForSelfRecoveryToSettle(t, allNodes, 5*time.Minute)
	})

	mustRun(t, "the shard recovered with its full data", func(t *testing.T) {
		require.Equal(t, []string{class + "/" + shard}, mustNewSelfRecoveryTargets(t, victim, opsBefore))
		assertShardObjectCounts(t, class, victim, counts)
		readObjectsFromNode(t, compose, class, "eeeeeeee-eeee-eeee-eeee", 20, victim)
		waitClusterHealthy(t)
		assertNoActiveRecovery(t, allNodes, 10*time.Second)
	})
}
