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
	"net/http"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	batchclient "github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

// forceRaftSnapshot POSTs to the node-local /debug/raft/snapshot
// endpoint, which calls raft.Snapshot() synchronously. Used by the
// e2e tests to guarantee a snapshot exists before wiping a peer, so
// the wiped node's rejoin goes through InstallSnapshot → Restore →
// reloadDBFromSchema → the SELF_RECOVERY hook.
//
// Requires WithWeaviateWithDebugPort() on the compose builder —
// /debug/* routes are served on port 6060 (the profiling port), not
// on the main API port.
func forceRaftSnapshot(ctx context.Context, t *testing.T, compose *docker.DockerCompose, idx int) {
	t.Helper()
	c, err := compose.ContainerAt(idx)
	require.NoError(t, err, "forceRaftSnapshot[%d]: container", idx)
	uri := c.DebugURI()
	require.NotEmpty(t, uri, "forceRaftSnapshot[%d]: empty DebugURI (did you call WithWeaviateWithDebugPort?)", idx)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://"+uri+"/debug/raft/snapshot", nil)
	require.NoError(t, err, "forceRaftSnapshot[%d]: build request", idx)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "forceRaftSnapshot[%d]: do request", idx)
	defer resp.Body.Close()
	require.Less(t, resp.StatusCode, 300, "forceRaftSnapshot[%d]: status %d", idx, resp.StatusCode)
}

// TestSelfRecoveryEndToEnd boots a 3-node cluster with the SELF_RECOVERY
// feature enabled, ingests data into a single-shard RF=3 collection,
// wipes the data dir on node-3, restarts it, and verifies the shard is
// re-hydrated automatically from a peer with full object count.
//
// It also asserts cluster reads at consistency=ONE keep working
// throughout the recovery window (the recovering replica is excluded
// from the eligible set via the existing FSM filter).
func TestSelfRecoveryEndToEnd(t *testing.T) {
	mainCtx := context.Background()
	ctx, cancel := context.WithTimeout(mainCtx, 15*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("SELF_RECOVERY_ENABLED", "true").
		WithWeaviateEnv("SELF_RECOVERY_CONCURRENCY", "2").
		// Required to expose the /replication/* observability API
		// (without it, list/details endpoints return 501).
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		// SELF_RECOVERY only triggers from the snapshot-Restore path
		// (see docs/self-recovery.md "Limitations"). Trim trailing
		// logs so the leader has to send InstallSnapshot to a
		// far-behind joiner. The test then POSTs to
		// /debug/raft/snapshot before wipe to deterministically
		// produce a snapshot — no timing dependency.
		WithWeaviateEnv("RAFT_TRAILING_LOGS", "1").
		WithWeaviateWithDebugPort(). // /debug/raft/snapshot lives on the profiling port
		// Tmpfs at /data so WipeNodeDataAt's stop+start really
		// empties the data dir (rm -rf via docker exec races with
		// weaviate's own writes; tmpfs is auto-wiped on container
		// stop).
		WithWeaviateTmpfsData().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("terminate compose: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	const (
		objCount = 500 // small enough that the test runs fast; large enough that recovery isn't trivial
		// 0-based index into compose.d.containers; with no aux containers
		// (contextionary etc.) a 3-node cluster has indices 0/1/2.
		wipedIdx = 2
	)
	wipedNodeName := docker.Weaviate2
	paragraphClass := articles.ParagraphsClass()

	t.Run("wait for cluster to form quorum", func(t *testing.T) {
		// docker compose returns once the containers are up, but the
		// RAFT FSM may still be electing/joining. CreateClass with
		// RF=3 fails with "1 available, 3 requested" until all 3
		// nodes have joined.
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			body, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
			require.NoError(ct, err)
			require.NotNil(ct, body.Payload)
			require.Len(ct, body.Payload.Nodes, 3, "expected 3 cluster nodes")
			for _, n := range body.Payload.Nodes {
				require.Equal(ct, "HEALTHY", *n.Status, "node %s status", n.Name)
			}
		}, 3*time.Minute, 1*time.Second)
	})

	t.Run("create RF=3 single-shard collection", func(t *testing.T) {
		paragraphClass.ShardingConfig = map[string]interface{}{"desiredCount": 1}
		// Async replication explicitly OFF so SELF_RECOVERY is the
		// only path that can restore the data after the wipe. If
		// the post-recovery object-count check passes, the recovery
		// went through SELF_RECOVERY (not silent async-rep backfill).
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{Factor: 3, AsyncEnabled: false}
		paragraphClass.Vectorizer = "none" // we don't run vector queries; avoid pulling in a vectorizer module
		helper.CreateClass(t, paragraphClass)
	})

	t.Run("wait for shard placement on all 3 nodes", func(t *testing.T) {
		// CreateClass returns before the shard is routable on every node.
		// Without this wait, the first batch hits "shard not found" during
		// the broadcast-to-all-replicas write path.
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			verbose := verbosity.OutputVerbose
			body, err := helper.Client(t).Nodes.NodesGetClass(
				nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(paragraphClass.Class), nil)
			require.NoError(ct, err)
			require.NotNil(ct, body.Payload)
			require.Len(ct, body.Payload.Nodes, 3)
			for _, n := range body.Payload.Nodes {
				require.Equal(ct, "HEALTHY", *n.Status, "node %s status", n.Name)
				require.Len(ct, n.Shards, 1, "node %s shard count", n.Name)
			}
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Run("ingest objects", func(t *testing.T) {
		batch := make([]*models.Object, objCount)
		for i := 0; i < objCount; i++ {
			batch[i] = articles.NewParagraph().
				WithID(strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1))).
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				Object()
		}
		// Even after the shard appears in /nodes, the broadcast write
		// path can race against per-replica shard registration. Retry
		// on transient "shard not found".
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			params := batchclient.NewBatchObjectsCreateParams().WithBody(batchclient.BatchObjectsCreateBody{Objects: batch})
			resp, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
			require.NoError(ct, err)
			require.NotNil(ct, resp)
			for _, o := range resp.Payload {
				if o.Result != nil && o.Result.Errors != nil && len(o.Result.Errors.Error) > 0 {
					require.Failf(ct, "batch ingest had per-object errors", "%v", o.Result.Errors.Error[0].Message)
				}
			}
		}, 30*time.Second, 1*time.Second, "batch ingest never succeeded")
	})

	t.Run("verify all 3 nodes report shard loaded", func(t *testing.T) {
		// Note: ObjectCount in /nodes is async-updated and unreliable
		// for immediate post-ingest assertions. Loaded=true is the
		// signal we care about pre-wipe; full-count verification
		// happens post-recovery via the recovery_completes subtest.
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			verbose := verbosity.OutputVerbose
			body, err := helper.Client(t).Nodes.NodesGetClass(
				nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(paragraphClass.Class), nil)
			require.NoError(ct, err)
			require.NotNil(ct, body.Payload)
			require.Len(ct, body.Payload.Nodes, 3)
			for _, n := range body.Payload.Nodes {
				require.Len(ct, n.Shards, 1, "node %s shards", n.Name)
				assert.True(ct, n.Shards[0].Loaded, "node %s loaded", n.Name)
			}
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Run("force a RAFT snapshot before wipe", func(t *testing.T) {
		// Self-recovery only fires from the snapshot-Restore path
		// (see docs/self-recovery.md "Limitations"). Force a
		// snapshot on every node so the wiped node's rejoin
		// receives InstallSnapshot (not log replay) deterministically.
		for i := 0; i < 3; i++ {
			forceRaftSnapshot(ctx, t, compose, i)
		}
	})

	t.Run("wipe node-3 data and restart", func(t *testing.T) {
		common.WipeNodeDataAt(ctx, t, compose, wipedIdx)
		common.StartNodeAt(ctx, t, compose, wipedIdx)
		// Re-point the helper client at node-1 since node-3's port may have changed on restart.
		helper.SetupClient(compose.GetWeaviate().URI())
	})

	// With async replication disabled on the class, the only path
	// that can restore data on the wiped node is SELF_RECOVERY.

	t.Run("a SELF_RECOVERY op was registered for node-3", func(t *testing.T) {
		// Asserts the recovery actually went through the SELF_RECOVERY
		// path (not silent async-rep backfill, which is disabled on
		// the class anyway).
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			body, err := helper.Client(t).Replication.ListReplication(
				replication.NewListReplicationParams().WithTargetNode(&wipedNodeName), nil)
			require.NoError(ct, err)
			require.NotNil(ct, body.Payload)
			found := false
			for _, op := range body.Payload {
				if op.Type != nil && *op.Type == "SELF_RECOVERY" {
					found = true
					break
				}
			}
			assert.True(ct, found, "expected at least one SELF_RECOVERY op for %s", wipedNodeName)
		}, 2*time.Minute, 1*time.Second, "no SELF_RECOVERY op observed for the wiped node")
	})

	t.Run("recovery completes and node-3 reports full object count", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			verbose := verbosity.OutputVerbose
			body, err := helper.Client(t).Nodes.NodesGetClass(
				nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(paragraphClass.Class), nil)
			require.NoError(ct, err)
			require.NotNil(ct, body.Payload)
			for _, n := range body.Payload.Nodes {
				if n.Name != wipedNodeName {
					continue
				}
				require.Len(ct, n.Shards, 1)
				assert.True(ct, n.Shards[0].Loaded, "node-3 shard should be loaded after recovery")
				assert.Equal(ct, int64(objCount), n.Shards[0].ObjectCount, "node-3 object count after recovery")
			}
		}, 5*time.Minute, 2*time.Second, "node-3 did not report full object count after recovery")
	})

	t.Run("direct query to node-3 returns full data at consistency=ONE", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			for i := 0; i < 10; i++ { // sample, not exhaustive
				id := strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1))
				exists, err := common.ObjectExistsCL(t, compose.ContainerURI(wipedIdx), paragraphClass.Class, id, types.ConsistencyLevelOne)
				assert.NoError(ct, err)
				assert.True(ct, exists, "object %s missing on node-3", id)
			}
		}, 30*time.Second, 1*time.Second, "node-3 sampled data not available at consistency=ONE")
	})
}

// TestSelfRecoveryReadsContinueAtConsistencyONE complements the e2e
// test: while node-3 is recovering, asserts that consistency=ONE reads
// against the cluster never error (the recovering replica is filtered
// out by the existing FSM-based router filter). Runs as a separate test
// to keep the e2e test focused.
func TestSelfRecoveryReadsContinueAtConsistencyONE(t *testing.T) {
	mainCtx := context.Background()
	ctx, cancel := context.WithTimeout(mainCtx, 15*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("SELF_RECOVERY_ENABLED", "true").
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true"). // for /replication/* endpoints
		// Same as TestSelfRecoveryEndToEnd: trim trailing logs so a
		// far-behind joiner must receive InstallSnapshot. The test
		// hits /debug/raft/snapshot before wipe.
		WithWeaviateEnv("RAFT_TRAILING_LOGS", "1").
		WithWeaviateWithDebugPort().
		WithWeaviateTmpfsData().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("terminate compose: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	const (
		objCount = 200
		wipedIdx = 2 // 0-based: third weaviate
	)
	wipedNodeName := docker.Weaviate2
	paragraphClass := articles.ParagraphsClass()
	paragraphClass.ShardingConfig = map[string]interface{}{"desiredCount": 1}
	// Async replication explicitly OFF: SELF_RECOVERY must be the
	// only path that restores data on the wiped node.
	paragraphClass.ReplicationConfig = &models.ReplicationConfig{Factor: 3, AsyncEnabled: false}
	paragraphClass.Vectorizer = "none"

	// Wait for the cluster to form quorum before issuing schema commands
	// (CreateClass with RF=3 fails with "1 available, 3 requested" until
	// every node has joined the RAFT FSM).
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		body, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
		require.NoError(ct, err)
		require.NotNil(ct, body.Payload)
		require.Len(ct, body.Payload.Nodes, 3, "expected 3 cluster nodes")
		for _, n := range body.Payload.Nodes {
			require.Equal(ct, "HEALTHY", *n.Status, "node %s status", n.Name)
		}
	}, 3*time.Minute, 1*time.Second)

	helper.CreateClass(t, paragraphClass)

	// Wait for the shard to appear on all nodes before ingesting (avoids
	// "shard not found" races against the broadcast write path).
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		verbose := verbosity.OutputVerbose
		body, err := helper.Client(t).Nodes.NodesGetClass(
			nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(paragraphClass.Class), nil)
		require.NoError(ct, err)
		require.NotNil(ct, body.Payload)
		require.Len(ct, body.Payload.Nodes, 3)
		for _, n := range body.Payload.Nodes {
			require.Equal(ct, "HEALTHY", *n.Status, "node %s status", n.Name)
			require.Len(ct, n.Shards, 1, "node %s shard count", n.Name)
		}
	}, 30*time.Second, 500*time.Millisecond)

	ids := make([]strfmt.UUID, objCount)
	batch := make([]*models.Object, objCount)
	for i := 0; i < objCount; i++ {
		ids[i] = strfmt.UUID(fmt.Sprintf("11111111-1111-1111-1111-%012d", i+1))
		batch[i] = articles.NewParagraph().WithID(ids[i]).WithContents(fmt.Sprintf("p#%d", i)).Object()
	}
	common.CreateObjects(t, compose.GetWeaviate().URI(), batch)

	// Force a RAFT snapshot before wipe so the rejoin goes through
	// InstallSnapshot. See forceRaftSnapshot for rationale.
	for i := 0; i < 3; i++ {
		forceRaftSnapshot(ctx, t, compose, i)
	}

	common.WipeNodeDataAt(ctx, t, compose, wipedIdx)
	common.StartNodeAt(ctx, t, compose, wipedIdx)
	helper.SetupClient(compose.GetWeaviate().URI())

	// Hit node-1 with consistency=ONE while node-3 is recovering.
	// Wait until node-3 finishes recovery; throughout, every probe
	// must succeed.
	deadline := time.Now().Add(5 * time.Minute)
	probeCount := 0
	for time.Now().Before(deadline) {
		// Spot-check 5 random IDs; should always succeed via the 2 healthy replicas.
		for _, id := range []strfmt.UUID{ids[0], ids[objCount/4], ids[objCount/2], ids[3*objCount/4], ids[objCount-1]} {
			exists, err := common.ObjectExistsCL(t, compose.GetWeaviate().URI(), paragraphClass.Class, id, types.ConsistencyLevelOne)
			require.NoError(t, err, "consistency=ONE probe failed during recovery (probe #%d, id=%s)", probeCount, id)
			require.True(t, exists, "consistency=ONE probe missing object during recovery (probe #%d, id=%s)", probeCount, id)
		}
		probeCount++

		// Check if node-3 has finished recovering (op READY).
		body, err := helper.Client(t).Replication.ListReplication(
			replication.NewListReplicationParams().WithTargetNode(&wipedNodeName), nil)
		if err == nil && body != nil && body.Payload != nil {
			doneCount := 0
			selfRecoveryCount := 0
			for _, op := range body.Payload {
				if op.Type != nil && *op.Type == "SELF_RECOVERY" {
					selfRecoveryCount++
					if op.Status != nil && op.Status.State == "READY" {
						doneCount++
					}
				}
			}
			if selfRecoveryCount > 0 && doneCount == selfRecoveryCount {
				t.Logf("recovery complete after %d probes", probeCount)
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("recovery did not finish within deadline; %d probes succeeded but op never reached READY", probeCount)
}
