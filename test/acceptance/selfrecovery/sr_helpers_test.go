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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	batchclient "github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/nodes"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// mustRun stops the pipeline when a prerequisite stage fails, instead of burning later stages' long waits.
func mustRun(t *testing.T, name string, f func(t *testing.T)) {
	t.Helper()
	if !t.Run(name, f) {
		t.FailNow()
	}
}

// ensureClass retries across transient leader-forwarding drops right after formation.
func ensureClass(t *testing.T, c *models.Class) {
	t.Helper()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(c)
		if _, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil); err != nil {
			getParams := clschema.NewSchemaObjectsGetParams().WithClassName(c.Class)
			if _, gerr := helper.Client(t).Schema.SchemaObjectsGet(getParams, nil); gerr != nil {
				require.NoError(ct, err)
			}
		}
	}, 30*time.Second, 1*time.Second, "class %s never created", c.Class)
}

func ensureTenants(t *testing.T, class string, tenants []*models.Tenant) {
	t.Helper()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		params := clschema.NewTenantsCreateParams().WithClassName(class).WithBody(tenants)
		if _, err := helper.Client(t).Schema.TenantsCreate(params, nil); err != nil {
			existing, gerr := helper.Client(t).Schema.TenantsGet(clschema.NewTenantsGetParams().WithClassName(class), nil)
			if gerr == nil && existing.Payload != nil && len(existing.Payload) >= len(tenants) {
				return
			}
			require.NoError(ct, err)
		}
	}, 30*time.Second, 1*time.Second, "tenants on %s never created", class)
}

// srClusterCfg toggles per-test cluster knobs on the shared SELF_RECOVERY base env.
type srClusterCfg struct {
	debugPort        bool // /debug/* endpoints (forceRaftSnapshot, smoke wiring)
	raftTrailingLogs bool // RAFT_TRAILING_LOGS=1 to force snapshot-based rejoin
	asyncDisabled    bool // sync replication (tests that assert exact counts mid-recovery)
	lazyLoading      bool // force lazy shard loading with warmupMinObjects as the sweep threshold
	warmupMinObjects int
	persistentData   bool // keep /data across a stop/start (tmpfs is lost on stop)
	concurrency      int  // SELF_RECOVERY_CONCURRENCY; 0 keeps the suite default of 2
}

// startSelfRecoveryCluster boots a 3-node cluster, registers teardown, points the client at node-0.
func startSelfRecoveryCluster(ctx context.Context, t *testing.T, cfg srClusterCfg) *docker.DockerCompose {
	t.Helper()
	concurrency := cfg.concurrency
	if concurrency == 0 {
		concurrency = 2
	}
	b := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("SELF_RECOVERY_ENABLED", "true").
		WithWeaviateEnv("SELF_RECOVERY_CONCURRENCY", strconv.Itoa(concurrency)).
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true")
	if !cfg.persistentData {
		b = b.WithWeaviateTmpfsData()
	}
	if cfg.lazyLoading {
		b = b.WithWeaviateEnv("LAZY_LOAD_SHARD_COUNT_THRESHOLD", "0").
			WithWeaviateEnv("LAZY_LOAD_SHARD_WARMUP_MIN_OBJECTS", strconv.Itoa(cfg.warmupMinObjects))
	}
	if cfg.asyncDisabled {
		b = b.WithWeaviateEnv("ASYNC_REPLICATION_DISABLED", "true")
	}
	if cfg.raftTrailingLogs {
		b = b.WithWeaviateEnv("RAFT_TRAILING_LOGS", "1")
	}
	if cfg.debugPort {
		b = b.WithWeaviateWithDebugPort()
	}
	compose, err := b.Start(ctx)
	require.NoError(t, err)
	// Fresh ctx: t.Cleanup runs after the test's own defer cancel().
	t.Cleanup(func() {
		termCtx, termCancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer termCancel()
		if err := compose.Terminate(termCtx); err != nil {
			t.Errorf("terminate compose: %v", err)
		}
	})
	helper.SetupClient(compose.GetWeaviate().URI())
	return compose
}

// waitClusterHealthy blocks until all 3 nodes report HEALTHY.
func waitClusterHealthy(t *testing.T) {
	t.Helper()
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		body, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
		require.NoError(ct, err)
		require.NotNil(ct, body.Payload)
		require.Len(ct, body.Payload.Nodes, 3, "expected 3 cluster nodes")
		for _, n := range body.Payload.Nodes {
			require.Equal(ct, "HEALTHY", *n.Status, "node %s status", n.Name)
		}
	}, 3*time.Minute, 1*time.Second)
}

// waitShardsLoaded: write-readiness before a wipe.
func waitShardsLoaded(t *testing.T, class string, shardsPerNode int) {
	t.Helper()
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		verbose := verbosity.OutputVerbose
		body, err := helper.Client(t).Nodes.NodesGetClass(
			nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(class), nil)
		require.NoError(ct, err)
		require.NotNil(ct, body.Payload)
		require.Len(ct, body.Payload.Nodes, 3)
		for _, n := range body.Payload.Nodes {
			require.Len(ct, n.Shards, shardsPerNode, "node %s shard count", n.Name)
			for _, s := range n.Shards {
				require.True(ct, s.Loaded, "node %s shard %s not write-ready", n.Name, s.Name)
			}
		}
	}, 3*time.Minute, 1*time.Second)
}

// waitShardsPresent: every node lists shardsPerNode shards of class, loaded or not.
func waitShardsPresent(t *testing.T, class string, shardsPerNode int) {
	t.Helper()
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		verbose := verbosity.OutputVerbose
		body, err := helper.Client(t).Nodes.NodesGetClass(
			nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(class), nil)
		require.NoError(ct, err)
		require.NotNil(ct, body.Payload)
		require.Len(ct, body.Payload.Nodes, 3)
		for _, n := range body.Payload.Nodes {
			require.Len(ct, n.Shards, shardsPerNode, "node %s shard count", n.Name)
		}
	}, 3*time.Minute, 1*time.Second)
}

// submitBatch retries until the batch succeeds with no per-object errors; cl="" = default.
func submitBatch(t *testing.T, objs []*models.Object, cl types.ConsistencyLevel) {
	t.Helper()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		params := batchclient.NewBatchObjectsCreateParams().
			WithBody(batchclient.BatchObjectsCreateBody{Objects: objs})
		if cl != "" {
			cls := string(cl)
			params = params.WithConsistencyLevel(&cls)
		}
		resp, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
		require.NoError(ct, err)
		require.NotNil(ct, resp)
		for _, o := range resp.Payload {
			if o.Result != nil && o.Result.Errors != nil && len(o.Result.Errors.Error) > 0 {
				require.Failf(ct, "batch ingest had per-object errors", "%v", o.Result.Errors.Error[0].Message)
			}
		}
	}, 60*time.Second, 1*time.Second, "batch ingest never succeeded")
}

// wipeAndRestart erases node idx's data, restarts it, re-points the client at node-0.
func wipeAndRestart(ctx context.Context, t *testing.T, compose *docker.DockerCompose, idx int) {
	t.Helper()
	common.WipeNodeDataAt(ctx, t, compose, idx)
	common.StartNodeAt(ctx, t, compose, idx)
	helper.SetupClient(compose.GetWeaviate().URI())
}

// waitSelfRecoveryOpFired blocks until a SELF_RECOVERY op exists for node.
func waitSelfRecoveryOpFired(t *testing.T, node string) {
	t.Helper()
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		found, err := hasSelfRecoveryOp(t, node)
		require.NoError(ct, err)
		assert.True(ct, found, "expected a SELF_RECOVERY op for %s", node)
	}, 5*time.Minute, 1*time.Second, "no SELF_RECOVERY op observed for the wiped node")
}

// assertNoActiveRecovery: negative control for healthy-cluster schema changes.
func assertNoActiveRecovery(t *testing.T, nodeNames []string, d time.Duration) {
	t.Helper()
	for _, node := range nodeNames {
		node := node
		assert.Never(t, func() bool {
			found, _ := hasActiveSelfRecoveryOp(t, node)
			return found
		}, d, 1*time.Second, "unexpected active SELF_RECOVERY op for %s", node)
	}
}

func shardStatusesOnNode(t *testing.T, class, node string) (map[string]*models.NodeShardStatus, error) {
	t.Helper()
	verbose := verbosity.OutputVerbose
	body, err := helper.Client(t).Nodes.NodesGetClass(
		nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(class), nil)
	if err != nil {
		return nil, err
	}
	statuses := map[string]*models.NodeShardStatus{}
	for _, n := range body.Payload.Nodes {
		if n.Name != node {
			continue
		}
		for _, s := range n.Shards {
			statuses[s.Name] = s
		}
	}
	return statuses, nil
}

// waitNoShardRecovering: queued submissions are invisible to the op list, so wait on shard status too.
func waitNoShardRecovering(t *testing.T, class, node string) {
	t.Helper()
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		statuses, err := shardStatusesOnNode(t, class, node)
		require.NoError(ct, err)
		require.NotEmpty(ct, statuses)
		for shard, s := range statuses {
			require.NotEqual(ct, "RECOVERING", s.VectorIndexingStatus, "node %s shard %s still recovering", node, shard)
		}
	}, 5*time.Minute, 1*time.Second, "node %s still has a recovering shard of %s", node, class)
}

// assertShardLoadedState blocks until node reports each named shard with the wanted Loaded flag.
func assertShardLoadedState(t *testing.T, class, node string, want map[string]bool) {
	t.Helper()
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		statuses, err := shardStatusesOnNode(t, class, node)
		require.NoError(ct, err)
		for shard, loaded := range want {
			require.Contains(ct, statuses, shard, "node %s shard %s", node, shard)
			assert.Equal(ct, loaded, statuses[shard].Loaded, "node %s shard %s loaded", node, shard)
		}
	}, 3*time.Minute, 1*time.Second, "node %s never reported the wanted loaded state", node)
}

// assertShardObjectCount blocks until node reports the shard loaded with wantCount objects.
func assertShardObjectCount(t *testing.T, class, node, shard string, wantCount int64) {
	t.Helper()
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		statuses, err := shardStatusesOnNode(t, class, node)
		require.NoError(ct, err)
		require.Contains(ct, statuses, shard)
		require.True(ct, statuses[shard].Loaded, "node %s shard %s loaded", node, shard)
		assert.Equal(ct, wantCount, statuses[shard].ObjectCount, "node %s shard %s object count", node, shard)
	}, 3*time.Minute, 1*time.Second, "node %s shard %s never reported %d objects", node, shard, wantCount)
}

// assertNodeRecovered blocks until node reports wantShards loaded shards with wantCount objects each.
func assertNodeRecovered(t *testing.T, class, node string, wantShards int, wantCount int64) {
	t.Helper()
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		verbose := verbosity.OutputVerbose
		body, err := helper.Client(t).Nodes.NodesGetClass(
			nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(class), nil)
		require.NoError(ct, err)
		require.NotNil(ct, body.Payload)
		for _, n := range body.Payload.Nodes {
			if n.Name != node {
				continue
			}
			require.Len(ct, n.Shards, wantShards)
			for _, s := range n.Shards {
				assert.True(ct, s.Loaded, "shard %s loaded after recovery", s.Name)
				assert.Equal(ct, wantCount, s.ObjectCount, "shard %s object count after recovery", s.Name)
			}
		}
	}, 5*time.Minute, 2*time.Second, "wiped node did not report full object count after recovery")
}
