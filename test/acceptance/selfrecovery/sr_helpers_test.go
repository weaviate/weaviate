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
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	batchclient "github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
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
	unlicensed       bool // omit LICENSE_KEY: the flag is on but no new recovery may start
	env              map[string]string
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
	if !cfg.unlicensed {
		b = b.WithWeaviateLicense()
	}
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
	for k, v := range cfg.env {
		b = b.WithWeaviateEnv(k, v)
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

// assertNodeLogContains blocks until node idx's full log contains needle.
func assertNodeLogContains(ctx context.Context, t *testing.T, compose *docker.DockerCompose, idx int, needle string) {
	t.Helper()
	node, err := compose.ContainerAt(idx)
	require.NoError(t, err, "node %d: container", idx)
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		logs, err := node.Container().Logs(ctx)
		require.NoError(ct, err, "node %d: logs", idx)
		buf, err := io.ReadAll(logs)
		if cerr := logs.Close(); cerr != nil {
			t.Logf("node %d: close logs: %v", idx, cerr)
		}
		require.NoError(ct, err, "node %d: read logs", idx)
		require.Contains(ct, string(buf), needle, "node %d log", idx)
	}, time.Minute, 2*time.Second, "node %d log never contained %q", idx, needle)
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

func srMultiShardClass(name string, shards int) *models.Class {
	c := srParagraphClass(name)
	c.ShardingConfig = map[string]interface{}{"desiredCount": shards}
	return c
}

func classDirInContainer(class string) string {
	return "/data/" + strings.ToLower(class)
}

func shardDirInContainer(class, shard string) string {
	return classDirInContainer(class) + "/" + shard
}

// removePathsAndKill refuses a missing path, removes the rest, then SIGKILLs: a graceful stop would flush into them.
func removePathsAndKill(ctx context.Context, t *testing.T, compose *docker.DockerCompose, idx int, paths ...string) {
	t.Helper()
	c, err := compose.ContainerAt(idx)
	require.NoError(t, err, "removePathsAndKill: container at index %d", idx)
	cmd := append([]string{"sh", "-c", `for p in "$@"; do test -d "$p" || exit 3; done; rm -rf "$@"`, "sh"}, paths...)
	code, _, err := c.Container().Exec(ctx, cmd)
	require.NoError(t, err, "removePathsAndKill: exec rm %v", paths)
	require.Equal(t, 0, code, "removePathsAndKill: rm %v on node %d exited %d", paths, idx, code)
	common.StopNodeAtWithTimeout(ctx, t, compose, idx, 0)
}

func removeShardDirsAndKill(ctx context.Context, t *testing.T, compose *docker.DockerCompose, idx int, class string, shards ...string) {
	t.Helper()
	paths := make([]string, len(shards))
	for i, shard := range shards {
		paths[i] = shardDirInContainer(class, shard)
	}
	removePathsAndKill(ctx, t, compose, idx, paths...)
}

func removeClassDirAndKill(ctx context.Context, t *testing.T, compose *docker.DockerCompose, idx int, class string) {
	t.Helper()
	removePathsAndKill(ctx, t, compose, idx, classDirInContainer(class))
}

// startNode starts node idx and re-points the client at node-0.
func startNode(ctx context.Context, t *testing.T, compose *docker.DockerCompose, idx int) {
	t.Helper()
	common.StartNodeAt(ctx, t, compose, idx)
	helper.SetupClient(compose.GetWeaviate().URI())
}

// selfRecoveryOpTargets maps op id to "collection/shard" for node's SELF_RECOVERY ops.
func selfRecoveryOpTargets(t *testing.T, targetNode string) (map[string]string, error) {
	t.Helper()
	body, err := helper.Client(t).Replication.ListReplication(
		replication.NewListReplicationParams().WithTargetNode(&targetNode), nil)
	if err != nil {
		return nil, err
	}
	targets := map[string]string{}
	for _, op := range body.Payload {
		if op.Type == nil || *op.Type != "SELF_RECOVERY" || op.ID == nil || op.Collection == nil || op.Shard == nil {
			continue
		}
		targets[op.ID.String()] = *op.Collection + "/" + *op.Shard
	}
	return targets, nil
}

func mustSelfRecoveryOpTargets(t *testing.T, targetNode string) map[string]string {
	t.Helper()
	targets, err := selfRecoveryOpTargets(t, targetNode)
	require.NoError(t, err)
	return targets
}

// newSelfRecoveryTargets: sorted "collection/shard" of the ops registered since before.
func newSelfRecoveryTargets(t *testing.T, targetNode string, before map[string]string) ([]string, error) {
	t.Helper()
	targets, err := selfRecoveryOpTargets(t, targetNode)
	if err != nil {
		return nil, err
	}
	var out []string
	for id, target := range targets {
		if _, seen := before[id]; !seen {
			out = append(out, target)
		}
	}
	sort.Strings(out)
	return out, nil
}

func mustNewSelfRecoveryTargets(t *testing.T, targetNode string, before map[string]string) []string {
	t.Helper()
	targets, err := newSelfRecoveryTargets(t, targetNode, before)
	require.NoError(t, err)
	return targets
}

func waitNewSelfRecoveryOp(t *testing.T, targetNode string, before map[string]string) {
	t.Helper()
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		targets, err := newSelfRecoveryTargets(t, targetNode, before)
		require.NoError(ct, err)
		assert.NotEmpty(ct, targets, "expected a new SELF_RECOVERY op for %s", targetNode)
	}, 5*time.Minute, 1*time.Second, "no new SELF_RECOVERY op observed for %s", targetNode)
}

// waitShardObjectCounts blocks until node's loaded shards of class total wantTotal objects; the verbose count lags ingest.
func waitShardObjectCounts(t *testing.T, class, node string, wantTotal int64) map[string]int64 {
	t.Helper()
	counts := map[string]int64{}
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		statuses, err := shardStatusesOnNode(t, class, node)
		require.NoError(ct, err)
		require.NotEmpty(ct, statuses, "node %s lists no shards of %s", node, class)
		next := map[string]int64{}
		var total int64
		for name, s := range statuses {
			require.True(ct, s.Loaded, "node %s shard %s loaded", node, name)
			next[name] = s.ObjectCount
			total += s.ObjectCount
		}
		require.Equal(ct, wantTotal, total, "node %s object total for %s", node, class)
		counts = next
	}, 3*time.Minute, 1*time.Second, "node %s never reported %d objects for %s", node, wantTotal, class)
	return counts
}

// assertShardObjectCounts blocks until node reports every shard in want loaded with that count.
func assertShardObjectCounts(t *testing.T, class, node string, want map[string]int64) {
	t.Helper()
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		statuses, err := shardStatusesOnNode(t, class, node)
		require.NoError(ct, err)
		for shard, count := range want {
			require.Contains(ct, statuses, shard, "node %s shard %s", node, shard)
			require.True(ct, statuses[shard].Loaded, "node %s shard %s loaded", node, shard)
			assert.Equal(ct, count, statuses[shard].ObjectCount, "node %s shard %s object count", node, shard)
		}
	}, 3*time.Minute, 1*time.Second, "node %s never reported the wanted object counts for %s", node, class)
}

func readObjectsFromNode(t *testing.T, compose *docker.DockerCompose, class, idPrefix string, n int, node string) {
	t.Helper()
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		for i := 0; i < n; i++ {
			id := strfmt.UUID(fmt.Sprintf("%s-%012d", idPrefix, i+1))
			obj, err := common.GetObjectFromNode(t, compose.GetWeaviate().URI(), class, id, node)
			assert.NoError(ct, err)
			assert.NotNil(ct, obj, "object %s missing on %s", id, node)
		}
	}, 30*time.Second, 1*time.Second, "objects of %s not readable on %s", class, node)
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

type srOp struct {
	id     strfmt.UUID
	source string
	shard  string
}

// findSelfRecoveryOp returns the first SELF_RECOVERY op targeting target.
func findSelfRecoveryOp(t *testing.T, target string) srOp {
	t.Helper()
	var op srOp
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		body, err := helper.Client(t).Replication.ListReplication(
			replication.NewListReplicationParams().WithTargetNode(&target), nil)
		require.NoError(ct, err)
		for _, o := range body.Payload {
			if o.Type != nil && *o.Type == "SELF_RECOVERY" && o.ID != nil && o.SourceNode != nil && o.Shard != nil {
				op = srOp{id: *o.ID, source: *o.SourceNode, shard: *o.Shard}
				return
			}
		}
		require.Fail(ct, "no SELF_RECOVERY op yet")
	}, 5*time.Minute, time.Second)
	return op
}

func srOpDetails(t *testing.T, id strfmt.UUID) (*models.ReplicationReplicateDetailsReplicaResponse, error) {
	t.Helper()
	history := true
	resp, err := helper.Client(t).Replication.ReplicationDetails(
		replication.NewReplicationDetailsParams().WithID(id).WithIncludeHistory(&history), nil)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

func srOpAllStatuses(d *models.ReplicationReplicateDetailsReplicaResponse) []*models.ReplicationReplicateDetailsReplicaStatus {
	out := append([]*models.ReplicationReplicateDetailsReplicaStatus{}, d.StatusHistory...)
	if d.Status != nil {
		out = append(out, d.Status)
	}
	return out
}

func srOpStates(d *models.ReplicationReplicateDetailsReplicaResponse) []string {
	var out []string
	for _, s := range srOpAllStatuses(d) {
		if s != nil {
			out = append(out, s.State)
		}
	}
	return out
}

func srOpErrorsInState(d *models.ReplicationReplicateDetailsReplicaResponse, state string) []string {
	var out []string
	for _, s := range srOpAllStatuses(d) {
		if s == nil || s.State != state {
			continue
		}
		for _, e := range s.Errors {
			if e != nil {
				out = append(out, e.Message)
			}
		}
	}
	return out
}

// pollSrOp reads op id every second until done accepts a status-bearing read or timeout passes; returns the last such read and api error.
func pollSrOp(t *testing.T, id strfmt.UUID, timeout time.Duration,
	done func(d *models.ReplicationReplicateDetailsReplicaResponse) bool,
) (*models.ReplicationReplicateDetailsReplicaResponse, bool, error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last *models.ReplicationReplicateDetailsReplicaResponse
	var lastErr error
	for time.Now().Before(deadline) {
		d, err := srOpDetails(t, id)
		lastErr = err
		if err == nil && d != nil && d.Status != nil {
			last = d
			if done(d) {
				return d, true, nil
			}
		}
		time.Sleep(time.Second)
	}
	return last, false, lastErr
}

// waitSelfRecoveryOpState polls until op id is in want; a CANCELLED op fails fast.
func waitSelfRecoveryOpState(t *testing.T, id strfmt.UUID, want string, timeout time.Duration) *models.ReplicationReplicateDetailsReplicaResponse {
	t.Helper()
	last, ok, lastErr := pollSrOp(t, id, timeout, func(d *models.ReplicationReplicateDetailsReplicaResponse) bool {
		switch d.Status.State {
		case want:
			return true
		case "CANCELLED":
			require.FailNowf(t, "op cancelled", "op %s cancelled while waiting for %s; history %v; errors %v",
				id, want, srOpStates(d), srOpErrorsInState(d, "HYDRATING"))
		case "READY":
			require.FailNowf(t, "op already READY", "op %s reached READY while waiting for %s; history %v", id, want, srOpStates(d))
		}
		return false
	})
	if ok {
		return last
	}
	var states, opErrs []string
	if last != nil {
		states = srOpStates(last)
		opErrs = srOpErrorsInState(last, last.Status.State)
	}
	lastOpErr := ""
	if len(opErrs) > 0 {
		lastOpErr = opErrs[len(opErrs)-1]
	}
	require.FailNowf(t, "op state timeout", "op %s never reached %s within %s; history %v; %d errors in the current state, last %q; last api error %v",
		id, want, timeout, states, len(opErrs), lastOpErr, lastErr)
	return nil
}

var srNodeNames = []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2}

func nodeIndex(t *testing.T, name string) int {
	t.Helper()
	for i, n := range srNodeNames {
		if n == name {
			return i
		}
	}
	require.FailNow(t, "unknown node", name)
	return -1
}

// rewoundAfter reports whether a HYDRATING follows the first after in states.
func rewoundAfter(states []string, after string) bool {
	seen := false
	for _, s := range states {
		switch s {
		case after:
			seen = true
		case "HYDRATING":
			if seen {
				return true
			}
		}
	}
	return false
}

// waitRewoundAfter polls until op id shows a HYDRATING after state after; reaching READY or CANCELLED first fails fast.
func waitRewoundAfter(t *testing.T, id strfmt.UUID, after string, timeout time.Duration) {
	t.Helper()
	var states []string
	_, ok, lastErr := pollSrOp(t, id, timeout, func(d *models.ReplicationReplicateDetailsReplicaResponse) bool {
		states = srOpStates(d)
		if rewoundAfter(states, after) {
			return true
		}
		if d.Status.State == "READY" || d.Status.State == "CANCELLED" {
			require.FailNowf(t, "op finished without a rewind", "op %s reached %s without a rewind after %s; history %v",
				id, d.Status.State, after, states)
		}
		return false
	})
	if !ok {
		require.FailNowf(t, "no rewind", "op %s never rewound after %s within %s; history %v; last api error %v",
			id, after, timeout, states, lastErr)
	}
}

// dumpLogsOnFailure registers a cleanup that dumps every node's log tail when t failed.
func dumpLogsOnFailure(t *testing.T, compose *docker.DockerCompose) {
	t.Helper()
	t.Cleanup(func() {
		if t.Failed() {
			var sb strings.Builder
			compose.DumpWeaviateLogs(context.Background(), &sb, 400)
			t.Log(sb.String())
		}
	})
}

func srIDs(prefix string, from, to int) []strfmt.UUID {
	out := make([]strfmt.UUID, 0, to-from)
	for i := from; i < to; i++ {
		out = append(out, strfmt.UUID(fmt.Sprintf("%s-%012d", prefix, i+1)))
	}
	return out
}

// assertExactObjectsOnNode blocks until node serves every id in present and none in absent, reading via uri.
func assertExactObjectsOnNode(t *testing.T, uri, class, node string, present, absent []strfmt.UUID) {
	t.Helper()
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		var missing, resurrected []strfmt.UUID
		for _, id := range present {
			if _, err := common.GetObjectFromNode(t, uri, class, id, node); err != nil {
				missing = append(missing, id)
			}
		}
		for _, id := range absent {
			if _, err := common.GetObjectFromNode(t, uri, class, id, node); err == nil {
				resurrected = append(resurrected, id)
			}
		}
		assert.Empty(ct, missing, "%d objects missing on %s", len(missing), node)
		assert.Empty(ct, resurrected, "%d deleted objects resurrected on %s", len(resurrected), node)
	}, 2*time.Minute, 2*time.Second, "node %s never served the exact object set of %s", node, class)
}
