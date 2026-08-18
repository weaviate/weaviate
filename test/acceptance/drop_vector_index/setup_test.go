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
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func TestDropVectorIndex_Cluster(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("ENABLE_EXPERIMENTAL_ALTER_SCHEMA_DROP_VECTOR_INDEX_ENDPOINT", "true").
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS", "1").
		// The cold-tenant test needs the reconcile heal within seconds.
		WithWeaviateEnv("DROP_VECTOR_INDEX_RECONCILE_INTERVAL_SECONDS", "5").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		dumpLogsOnFailure(ctx, t, compose)
		require.NoError(t, compose.Terminate(ctx))
	}()

	runSuite(t, compose)
	t.Run("replicated drop", testReplicatedDrop(compose))
	t.Run("replicated cold tenant", testReplicatedColdTenant(compose))
}

func TestDropVectorIndex_Restart_Cluster(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("ENABLE_EXPERIMENTAL_ALTER_SCHEMA_DROP_VECTOR_INDEX_ENDPOINT", "true").
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		dumpLogsOnFailure(ctx, t, compose)
		require.NoError(t, compose.Terminate(ctx))
	}()

	runRestartSuite(t, compose)
}

func TestDropVectorIndex_RollingRestart_Cluster(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("ENABLE_EXPERIMENTAL_ALTER_SCHEMA_DROP_VECTOR_INDEX_ENDPOINT", "true").
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		dumpLogsOnFailure(ctx, t, compose)
		require.NoError(t, compose.Terminate(ctx))
	}()

	runRollingRestartSuite(t, compose)
}

// dumpLogsOnFailure writes every node's log tail before the compose is torn
// down. These jobs upload no artifacts and go-swagger errors stringify their
// payload as a pointer, so a failure here otherwise carries no server-side
// evidence at all.
func dumpLogsOnFailure(ctx context.Context, t *testing.T, compose *docker.DockerCompose) {
	if !t.Failed() {
		return
	}
	compose.DumpWeaviateLogs(ctx, os.Stderr, 300)
}

func runSuite(t *testing.T, compose *docker.DockerCompose) {
	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	t.Run("lifecycle", testLifecycle())
	t.Run("write matrix", testWriteMatrix())
	t.Run("multi tenant", testMultiTenant())
	t.Run("sustained load", testSustainedLoad(compose))
	t.Run("concurrent drops", testConcurrentDrops())
	t.Run("delete class mid-drop", testDeleteClassMidDrop())
	t.Run("drop rejections", testDropRejections())
	t.Run("zero tenant drop", testZeroTenantDrop())
	t.Run("all tenants deleted mid-drop", testAllTenantsDeletedMidDrop())
	t.Run("last vector drop to vectorless", testLastVectorDropToVectorless())
	t.Run("tenant mutation during drop", testTenantMutationDuringDrop())
	t.Run("cold tenant deferred finalize", testColdTenantDeferredFinalize())
	t.Run("deleted cold tenant finalizes without re-clean", testDeletedColdTenantFinalizesWithoutReclean())
	t.Run("partially cold tenants", testPartiallyColdTenants())
	t.Run("tenant delete recreate during drop", testTenantDeleteRecreateDuringDrop())
	t.Run("re-drop after re-create", testRedropAfterRecreate())
	t.Run("multi vector", testMultiVector())
	t.Run("multi vector drop leaves no files on disk", testMultiVectorLeavesNoFilesOnDisk(compose))
	t.Run("hfresh drop leaves no files on disk", testHFreshLeavesNoFilesOnDisk(compose))
}
