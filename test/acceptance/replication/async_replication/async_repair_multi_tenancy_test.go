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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

// This scenario verifies that async replication — which is on by default for
// any class with replication factor > 1 — repairs a multi-tenant tenant's
// shard on a node that missed writes, and that the hashBeat process starts
// correctly when an inactive tenant is later activated (without force-loading
// shards: it is applied via ForEachLoadedShard once the tenant is active).
//
// The actual scenario is as follows:
//   - Create a class with multi-tenancy enabled, replicated by a factor of 3
//   - Create a tenant, and set it to inactive
//   - Stop the second node
//   - Activate the tenant, and insert objects into the 2 surviving nodes
//   - Restart the second node and ensure it has an object count of 0
//   - Wait for objects to propagate via async replication
//   - Verify that the resurrected node contains all objects
func (suite *AsyncReplicationTestSuite) TestAsyncRepairMultiTenancyScenario() {
	t := suite.T()
	mainCtx := context.Background()

	var (
		clusterSize = 3
		tenantName  = "tenant-0"
		objectCount = 100
	)

	ctx, cancel := context.WithTimeout(mainCtx, 15*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(clusterSize).
		WithText2VecContextionary().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	paragraphClass := articles.ParagraphsClass()

	t.Run("create schema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: int64(clusterSize),
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{
			AutoTenantActivation: true,
			Enabled:              true,
		}

		helper.SetupClient(compose.GetWeaviate().URI())
		helper.CreateClass(t, paragraphClass)
	})

	t.Run("add inactive tenant", func(t *testing.T) {
		tenants := []*models.Tenant{{Name: tenantName, ActivityStatus: "COLD"}}
		helper.CreateTenants(t, paragraphClass.Class, tenants)
	})

	t.Run("stop node 2", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 2)
	})

	// Activate/insert tenants while node 2 is down
	t.Run("activate tenant and insert paragraphs", func(t *testing.T) {
		batch := make([]*models.Object, objectCount)
		for i := 0; i < objectCount; i++ {
			batch[i] = articles.NewParagraph().
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				WithTenant(tenantName).
				Object()
		}
		common.CreateObjectsCL(t, compose.GetWeaviate().URI(), batch, types.ConsistencyLevelOne)
	})

	t.Run("start node 2", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 2)
	})

	t.Run("verify that all nodes are running", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			verbose := verbosity.OutputVerbose
			params := nodes.NewNodesGetClassParams().WithOutput(&verbose)
			body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
			require.NoError(ct, clientErr)
			require.NotNil(ct, body.Payload)

			resp := body.Payload
			require.Len(ct, resp.Nodes, clusterSize)
			for _, n := range resp.Nodes {
				require.NotNil(ct, n.Status)
				require.Equal(ct, "HEALTHY", *n.Status)
			}
		}, 15*time.Second, 500*time.Millisecond)
	})

	t.Run("validate async object propagation", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp := common.GQLTenantGet(t, compose.GetWeaviateNode(2).URI(), paragraphClass.Class, types.ConsistencyLevelOne, tenantName)
			require.Len(ct, resp, objectCount)
		}, 120*time.Second, 5*time.Second, "not all the objects have been asynchronously replicated")
	})
}

// TestAsyncRepairMultiTenancyColdTenantConfigUpdate verifies that a tenant
// created COLD picks up an async replication config update made while it was
// COLD, once activated.
//
// Async repair is push-only — a behind node is repaired by its peers. Phase 1
// throttles async replication (far-future frequency + a propagation delay
// larger than the observation window) while the tenant is COLD, so once the
// peers fetch that config no hashbeat cycle can repair the restarted node
// within the window; stale defaults would, failing the test. Phase 2 restores
// a normal config and restarts the peers, so the node catches up — proving
// repair still works and config is re-fetched on shard load.
func (suite *AsyncReplicationTestSuite) TestAsyncRepairMultiTenancyColdTenantConfigUpdate() {
	t := suite.T()
	mainCtx := context.Background()

	const (
		// throttled frequency set while the tenant is COLD; far above the
		// observation window so peers that fetched it cannot repair in time.
		asyncRepairThrottledFrequency = 1 * time.Hour
		// normal frequency restored before phase 2.
		asyncRepairRestoredFrequency = 5 * time.Second
		// window to watch the restarted node; exceeds the 30s/5s defaults.
		asyncRepairObservationWindow = 90 * time.Second
	)

	var (
		clusterSize = 3
		tenantName  = "tenant-0"
		objectCount = 100
	)

	ctx, cancel := context.WithTimeout(mainCtx, 15*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(clusterSize).
		WithText2VecContextionary().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	paragraphClass := articles.ParagraphsClass()

	// setAsyncFrequency throttles (or restores) the class's async replication,
	// setting Frequency, FrequencyWhilePropagating and PropagationDelay to freq.
	// The delay is what makes "no repair within the window" hold regardless of
	// cycle timing: even the immediate first cycle can't propagate objects
	// younger than it.
	setAsyncFrequency := func(t *testing.T, freq time.Duration) {
		t.Helper()
		getParams := schema.NewSchemaObjectsGetParams().WithClassName(paragraphClass.Class)
		res, err := helper.Client(t).Schema.SchemaObjectsGet(getParams, nil)
		require.NoError(t, err)
		require.NotNil(t, res.Payload)
		require.NotNil(t, res.Payload.ReplicationConfig)

		freqMs := int64(freq / time.Millisecond)
		res.Payload.ReplicationConfig.AsyncConfig = &models.ReplicationAsyncConfig{
			Frequency:                 &freqMs,
			FrequencyWhilePropagating: &freqMs,
			PropagationDelay:          &freqMs,
		}
		helper.UpdateClass(t, res.Payload)
	}

	t.Run("create schema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor: int64(clusterSize),
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{
			AutoTenantActivation: true,
			Enabled:              true,
		}

		helper.SetupClient(compose.GetWeaviate().URI())
		helper.CreateClass(t, paragraphClass)
	})

	t.Run("add tenant in cold state", func(t *testing.T) {
		tenants := []*models.Tenant{{Name: tenantName, ActivityStatus: "COLD"}}
		helper.CreateTenants(t, paragraphClass.Class, tenants)
	})

	t.Run("update async replication config on class", func(t *testing.T) {
		// Throttle async replication (frequency + propagation delay) far above
		// the observation window. The peers that will repair the restarted node
		// fetch this config when the insert activates the tenant, making the
		// config fetch observable.
		setAsyncFrequency(t, asyncRepairThrottledFrequency)
	})

	t.Run("stop node 2", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 2)
	})

	t.Run("insert paragraphs (this will activate the tenant)", func(t *testing.T) {
		batch := make([]*models.Object, objectCount)
		for i := 0; i < objectCount; i++ {
			batch[i] = articles.NewParagraph().
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				WithTenant(tenantName).
				Object()
		}
		common.CreateObjectsCL(t, compose.GetWeaviate().URI(), batch, types.ConsistencyLevelOne)
	})

	t.Run("start node 2", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 2)
	})

	t.Run("verify that all nodes are running", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			verbose := verbosity.OutputVerbose
			params := nodes.NewNodesGetClassParams().WithOutput(&verbose)
			body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
			require.NoError(ct, clientErr)
			require.NotNil(ct, body.Payload)

			resp := body.Payload
			require.Len(ct, resp.Nodes, clusterSize)
			for _, n := range resp.Nodes {
				require.NotNil(ct, n.Status)
				require.Equal(ct, "HEALTHY", *n.Status)
			}
		}, 15*time.Second, 500*time.Millisecond)
	})

	t.Run("validate surviving nodes hold all objects", func(t *testing.T) {
		// Control: confirm the data exists on the nodes that stayed up, so a
		// missing object on the restarted node can only mean missed repair.
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp := common.GQLTenantGet(t, compose.GetWeaviate().URI(), paragraphClass.Class, types.ConsistencyLevelOne, tenantName)
			require.Len(ct, resp, objectCount)
		}, 30*time.Second, 5*time.Second, "surviving nodes are missing objects")
	})

	t.Run("validate restarted node does not repair while throttled", func(t *testing.T) {
		// The peers fetched the throttled config on activation; its propagation
		// delay exceeds this window, so no cycle can repair node 2 regardless of
		// timing. Stale defaults would repair within the window, failing this.
		//
		// Poll synchronously rather than with require.Never, whose condition
		// goroutine can outlive the assertion and race the global helper client.
		assertNotFullyRepaired := func() {
			resp := common.GQLTenantGet(t, compose.GetWeaviateNode(2).URI(), paragraphClass.Class, types.ConsistencyLevelOne, tenantName)
			require.NotEqual(t, objectCount, len(resp),
				"restarted node received all objects despite async replication being throttled — the peers did not pick up the COLD-time config update")
		}
		timer := time.NewTimer(asyncRepairObservationWindow)
		defer timer.Stop()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		assertNotFullyRepaired()
		for {
			select {
			case <-timer.C:
				assertNotFullyRepaired() // also assert at the window boundary, not only on ticks
				return
			case <-ticker.C:
				assertNotFullyRepaired()
			}
		}
	})

	t.Run("restore async replication frequency", func(t *testing.T) {
		setAsyncFrequency(t, asyncRepairRestoredFrequency)
	})

	t.Run("restart the peer nodes", func(t *testing.T) {
		// An in-place config update does not reschedule already-running shards,
		// so restart the peers (nodes 1 and 3, which repair node 2 by pushing):
		// their shards reload and run an immediate cycle under the restored
		// config. Node 3 first, so the helper client (node 1) is only briefly down.
		common.StopNodeAt(ctx, t, compose, 3)
		common.StartNodeAt(ctx, t, compose, 3)
		common.StopNodeAt(ctx, t, compose, 1)
		common.StartNodeAt(ctx, t, compose, 1)
	})

	t.Run("verify that all nodes are running after peer restart", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			verbose := verbosity.OutputVerbose
			params := nodes.NewNodesGetClassParams().WithOutput(&verbose)
			body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
			require.NoError(ct, clientErr)
			require.NotNil(ct, body.Payload)

			resp := body.Payload
			require.Len(ct, resp.Nodes, clusterSize)
			for _, n := range resp.Nodes {
				require.NotNil(ct, n.Status)
				require.Equal(ct, "HEALTHY", *n.Status)
			}
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Run("validate restarted node catches up after peer restart", func(t *testing.T) {
		// Touch the peers so their lazily loaded tenant shards load and start
		// async replication.
		common.GQLTenantGet(t, compose.GetWeaviate().URI(), paragraphClass.Class, types.ConsistencyLevelOne, tenantName)
		common.GQLTenantGet(t, compose.GetWeaviateNode(3).URI(), paragraphClass.Class, types.ConsistencyLevelOne, tenantName)

		// The peers' immediate cycle pushes the missing objects to node 2 —
		// proving repair still works (rules out async replication never starting).
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp := common.GQLTenantGet(t, compose.GetWeaviateNode(2).URI(), paragraphClass.Class, types.ConsistencyLevelOne, tenantName)
			require.Len(ct, resp, objectCount)
		}, 120*time.Second, 5*time.Second, "restarted node did not catch up after the peer nodes were restarted")
	})
}

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
func (suite *AsyncReplicationTestSuite) TestAsyncRepairMultiTenancyRuntimeToggle() {
	t := suite.T()
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
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			n, err := shardsAsyncReplicationLen(t, paragraphClass.Class)
			require.NoError(ct, err)
			require.Greater(ct, n, 0,
				"asyncReplicationStatus must be populated on hot tenant shards at boot")
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Run("admin disables async replication via the runtime-overrides file", func(t *testing.T) {
		writeAsyncReplicationOverride(ctx, t, compose, clusterSize, overridePath, true)
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			n, err := shardsAsyncReplicationLen(t, paragraphClass.Class)
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
			n, err := shardsAsyncReplicationLen(t, paragraphClass.Class)
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
			n, err := shardsAsyncReplicationLen(t, paragraphClass.Class)
			require.NoError(ct, err)
			require.Greater(ct, n, 0,
				"reconcile hook must register async replication on tenant shards loaded while disabled")
		}, 30*time.Second, 500*time.Millisecond)
	})
}
