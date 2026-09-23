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

package runtimetoggle

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

// TestAsyncRepairMultiTenancyRuntimeToggle exercises the
// ASYNC_REPLICATION_DISABLED kill-switch on a multi-tenant class: HOT
// tenants drain asyncReplicationStatus on disable, miss writes during the
// disabled window, and are repaired by the reconcile hook on re-enable. A
// COLD-then-activated tenant whose shards load while the flag is disabled
// honors the live flag (no async on those shards) and only gets registered
// once the flag flips back.
//
// This catches a regression class that the existing MT tests do not cover:
// the toggle path through `runtime-overrides → AsyncReplicationDisabled hook
// → DB.ReconcileAsyncReplication → per-tenant-shard apply` on a real
// multi-tenant cluster.
func TestAsyncRepairMultiTenancyRuntimeToggle(t *testing.T) {
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
	mainCtx := context.Background()

	const (
		overridePath  = "/etc/weaviate/runtime-overrides.yaml"
		clusterSize   = 3
		hotTenantA    = "hot-a"
		hotTenantB    = "hot-b"
		coldTenant    = "cold-c"
		baselineCount = 5
	)

	ctx, cancel := context.WithTimeout(mainCtx, 15*time.Minute)
	defer cancel()

	emptyOverride := testcontainers.ContainerFile{
		Reader:            strings.NewReader(""),
		ContainerFilePath: overridePath,
		FileMode:          0o644,
	}

	// Boot with async replication ENABLED by default — no ASYNC_REPLICATION_DISABLED env.
	compose, err := docker.New().
		WithWeaviateCluster(clusterSize).
		WithText2VecContextionary().
		WithWeaviateEnv("RUNTIME_OVERRIDES_ENABLED", "true").
		WithWeaviateEnv("RUNTIME_OVERRIDES_PATH", overridePath).
		WithWeaviateEnv("RUNTIME_OVERRIDES_LOAD_INTERVAL", "1s").
		WithWeaviateFiles(emptyOverride).
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()

	t.Run("create RF=3 multi-tenant class", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{Factor: int64(clusterSize)}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
		helper.CreateClass(t, paragraphClass)
	})

	t.Run("add hot and cold tenants", func(t *testing.T) {
		helper.CreateTenants(t, paragraphClass.Class, []*models.Tenant{
			{Name: hotTenantA, ActivityStatus: "HOT"},
			{Name: hotTenantB, ActivityStatus: "HOT"},
			{Name: coldTenant, ActivityStatus: "COLD"},
		})
	})

	t.Run("insert baseline objects into hot tenants", func(t *testing.T) {
		for _, tn := range []string{hotTenantA, hotTenantB} {
			batch := make([]*models.Object, baselineCount)
			for i := 0; i < baselineCount; i++ {
				batch[i] = articles.NewParagraph().
					WithContents(fmt.Sprintf("%s-baseline-%d", tn, i)).
					WithTenant(tn).
					Object()
			}
			// CL=All so every replica is on disk before we start toggling.
			common.CreateObjectsCL(t, compose.GetWeaviate().URI(), batch, types.ConsistencyLevelAll)
		}
	})

	t.Run("async replication is registered on hot tenant shards", func(t *testing.T) {
		// Gate on a healthy cluster and allow >1 hashbeat frequency (30s): a first cycle before peers are reachable errors and only repopulates asyncReplicationStatus a full frequency later.
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			verbose := verbosity.OutputVerbose
			params := nodes.NewNodesGetClassParams().
				WithClassName(paragraphClass.Class).WithOutput(&verbose)
			body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
			require.NoError(ct, clientErr)
			require.NotNil(ct, body.Payload)
			require.Len(ct, body.Payload.Nodes, clusterSize)
			shards := 0
			for _, node := range body.Payload.Nodes {
				require.NotNil(ct, node.Status)
				require.Equal(ct, "HEALTHY", *node.Status)
				shards += len(node.Shards)
			}
			require.Greater(ct, shards, 0, "tenant shards not reported yet")

			n, err := common.ShardsAsyncReplicationLen(t, paragraphClass.Class)
			require.NoError(ct, err)
			require.Greater(ct, n, 0,
				"asyncReplicationStatus must be populated on hot tenant shards at boot")
		}, 90*time.Second, 1*time.Second)
	})

	t.Run("admin disables async replication via the runtime-overrides file", func(t *testing.T) {
		writeAsyncReplicationOverride(ctx, t, compose, clusterSize, overridePath, true)
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			n, err := common.ShardsAsyncReplicationLen(t, paragraphClass.Class)
			require.NoError(ct, err)
			require.Equal(ct, 0, n,
				"asyncReplicationStatus must drain on all hot tenant shards when disabled")
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Run("stop node 3", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 3)
	})

	t.Run("write probe into hot tenant B while node 3 is down", func(t *testing.T) {
		probe := articles.NewParagraph().
			WithContents("probe written into hot-b while async disabled + node 3 down").
			WithTenant(hotTenantB).
			Object()
		common.CreateObjectsCL(t, compose.GetWeaviate().URI(),
			[]*models.Object{probe}, types.ConsistencyLevelOne)
	})

	t.Run("activate the cold tenant and insert (live flag must keep async off on the new shards)", func(t *testing.T) {
		helper.UpdateTenants(t, paragraphClass.Class, []*models.Tenant{
			{Name: coldTenant, ActivityStatus: "HOT"},
		})
		batch := make([]*models.Object, baselineCount)
		for i := 0; i < baselineCount; i++ {
			batch[i] = articles.NewParagraph().
				WithContents(fmt.Sprintf("%s-late-%d", coldTenant, i)).
				WithTenant(coldTenant).
				Object()
		}
		common.CreateObjectsCL(t, compose.GetWeaviate().URI(), batch, types.ConsistencyLevelOne)
		// All loaded tenant shards (hot-a, hot-b, cold-c on surviving nodes)
		// must observe asyncReplicationStatus=0: Migrator / tenant-activation
		// read the live disabled flag during shard load.
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			n, err := common.ShardsAsyncReplicationLen(t, paragraphClass.Class)
			require.NoError(ct, err)
			require.Equal(ct, 0, n,
				"newly-activated tenant shards must honor the live disabled flag")
		}, 15*time.Second, 500*time.Millisecond)
	})

	t.Run("restart node 3", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 3)
	})

	t.Run("verify all nodes are running", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			verbose := verbosity.OutputVerbose
			params := nodes.NewNodesGetClassParams().WithOutput(&verbose)
			body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
			require.NoError(ct, clientErr)
			require.NotNil(ct, body.Payload)
			require.Len(ct, body.Payload.Nodes, clusterSize)
			for _, n := range body.Payload.Nodes {
				require.NotNil(ct, n.Status)
				require.Equal(ct, "HEALTHY", *n.Status)
			}
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Run("node 3 is behind on hot tenant B while async is disabled", func(t *testing.T) {
		resp := common.GQLTenantGet(t, compose.GetWeaviateNode(3).URI(),
			paragraphClass.Class, types.ConsistencyLevelOne, hotTenantB)
		require.Less(t, len(resp), baselineCount+1,
			"node 3 must be behind on hot-b (missing the disabled-window probe)")
	})

	t.Run("re-enable async replication via the runtime-overrides file", func(t *testing.T) {
		writeAsyncReplicationOverride(ctx, t, compose, clusterSize, overridePath, false)
	})

	t.Run("hot tenant B is repaired on node 3 without a restart", func(t *testing.T) {
		// runtime-overrides → AsyncReplicationDisabled hook →
		// DB.ReconcileAsyncReplication → per-tenant-shard enable → fresh
		// hashtree scan → CollectShardDifferences finds the missing probe →
		// async repair propagates it to node 3.
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp := common.GQLTenantGet(t, compose.GetWeaviateNode(3).URI(),
				paragraphClass.Class, types.ConsistencyLevelOne, hotTenantB)
			require.Len(ct, resp, baselineCount+1,
				"node 3 must catch up to baseline+1 on hot-b once async is re-enabled")
		}, 120*time.Second, 2*time.Second)
	})

	t.Run("reconcile registers async on tenant shards loaded under the disabled flag", func(t *testing.T) {
		// hot-a, hot-b, and cold-c shards all came up while the flag was
		// disabled at some point; the reconcile must register every loaded
		// shard. We assert > 0 globally; the per-tenant repair above already
		// proved hot-b is back.
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			n, err := common.ShardsAsyncReplicationLen(t, paragraphClass.Class)
			require.NoError(ct, err)
			require.Greater(ct, n, 0,
				"reconcile hook must register async replication on tenant shards loaded while disabled")
		}, 30*time.Second, 500*time.Millisecond)
	})
}
