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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	batchclient "github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// srClusterCfg toggles the per-test cluster knobs on top of the shared
// SELF_RECOVERY base env.
type srClusterCfg struct {
	debugPort        bool // /debug/* endpoints (forceRaftSnapshot, smoke wiring)
	raftTrailingLogs bool // RAFT_TRAILING_LOGS=1 to force snapshot-based rejoin
	asyncDisabled    bool // sync replication (tests that assert exact counts mid-recovery)
}

// startSelfRecoveryCluster boots a 3-node SELF_RECOVERY cluster, registers
// teardown, and points the shared client at node-0.
func startSelfRecoveryCluster(ctx context.Context, t *testing.T, cfg srClusterCfg) *docker.DockerCompose {
	t.Helper()
	b := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("SELF_RECOVERY_ENABLED", "true").
		WithWeaviateEnv("SELF_RECOVERY_CONCURRENCY", "2").
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		WithWeaviateTmpfsData()
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
	// Fresh ctx: t.Cleanup runs after the test's own `defer cancel()`, so the
	// test ctx is already cancelled by teardown time.
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

// waitShardsLoaded blocks until every node holds shardsPerNode loaded shards
// for the class (write-readiness before a wipe).
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

// submitBatch ingests objs, retrying until the batch succeeds with no
// per-object errors. cl="" uses the default consistency level.
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

// wipeAndRestart erases node idx's data, restarts it, and re-points the client
// at node-0.
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

// assertNoActiveRecovery asserts no node ever shows an active SELF_RECOVERY op
// over d (negative control for healthy-cluster schema changes).
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

// assertNodeRecovered blocks until node reports wantShards loaded shards, each
// with wantCount objects.
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
