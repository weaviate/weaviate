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

package backup_multinode

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/cluster"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	backendName = "s3"
	className   = "BackupTest"
	// Large enough that the upload takes several seconds, so cancel and
	// kill-restart tests can observe mid-upload state reliably.
	numObjects = 5000
)

func start3NodeBackupCluster(ctx context.Context, t *testing.T, extraEnv ...string) (*docker.DockerCompose, func()) {
	t.Helper()
	if len(extraEnv)%2 != 0 {
		t.Fatalf("extraEnv must be (key,value) pairs, got %d items", len(extraEnv))
	}
	b := docker.New().
		WithBackendS3("backups", "us-west-1").
		WithWeaviateEnv("BACKUP_DISTRIBUTED_TASKS_ENABLED", "true").
		WithWeaviateEnv("BACKUP_STALE_TIMEOUT", "2m").
		WithWeaviateEnv("BACKUP_MAX_UNIT_RETRIES", "1").
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		WithWeaviateEnv("DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS", "1").
		WithWeaviateEnv("DISABLE_LAZY_LOAD_SHARDS", "true").
		With3NodeCluster()
	for i := 0; i < len(extraEnv); i += 2 {
		b = b.WithWeaviateEnv(extraEnv[i], extraEnv[i+1])
	}
	compose, err := b.Start(ctx)
	if err != nil {
		if compose != nil {
			dumpLogs(ctx, t, compose)
			compose.Terminate(ctx)
		}
		t.Fatalf("start 3-node backup cluster: %v", err)
	}
	return compose, func() {
		dumpLogs(ctx, t, compose)
		compose.Terminate(ctx)
	}
}

func nodeURI(compose *docker.DockerCompose, n int) string {
	return compose.GetWeaviateNode(n).URI()
}

func setupClient(t *testing.T, compose *docker.DockerCompose, nodeIdx int) {
	t.Helper()
	helper.SetupClient(nodeURI(compose, nodeIdx))
}

func createClassAndData(t *testing.T) {
	t.Helper()
	createClassAndDataN(t, numObjects)
}

func createClassAndDataN(t *testing.T, n int) {
	t.Helper()
	cls := &models.Class{
		Class:             className,
		ReplicationConfig: &models.ReplicationConfig{Factor: 3},
	}
	helper.CreateClass(t, cls)

	for i := range n {
		obj := &models.Object{
			Class: className,
			Properties: map[string]interface{}{
				"text": fmt.Sprintf("object-%d", i),
			},
		}
		require.NoError(t, helper.CreateObject(t, obj))
	}
}

func pollBackupStatus(t *testing.T, backupID string, deadline time.Duration) (string, string) {
	t.Helper()
	start := time.Now()
	for time.Since(start) < deadline {
		resp, err := helper.CreateBackupStatus(t, backendName, backupID, "", "")
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		st := *resp.Payload.Status
		if st == "SUCCESS" || st == "FAILED" || st == "CANCELED" {
			return st, resp.Payload.Error
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("backup %s did not reach terminal state within %s", backupID, deadline)
	return "", ""
}

func dumpLogs(ctx context.Context, t *testing.T, compose *docker.DockerCompose) {
	t.Helper()
	if !t.Failed() {
		return
	}
	for n := 1; n <= 3; n++ {
		c := compose.GetWeaviateNode(n)
		if c == nil {
			continue
		}
		logs, err := c.Container().Logs(ctx)
		if err != nil {
			t.Logf("node %d: failed to get logs: %v", n, err)
			continue
		}
		all, _ := io.ReadAll(logs)
		t.Logf("=== node %d logs (last 200 lines) ===\n%s", n, tail(string(all), 200))
	}
}

func tail(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) <= n {
		return s
	}
	return strings.Join(lines[len(lines)-n:], "\n")
}

// fetchGlobalDescriptor reads backup_config.json straight from the object
// store, so the assertion sees the artifact rather than the API's rendering.
func fetchGlobalDescriptor(ctx context.Context, t *testing.T, compose *docker.DockerCompose, backupID string) map[string]any {
	t.Helper()
	client, err := minio.New(compose.GetMinIO().URI(), &minio.Options{
		Creds:  credentials.NewStaticV4("aws_access_key", "aws_secret_key", ""),
		Secure: false,
	})
	require.NoError(t, err)

	obj, err := client.GetObject(ctx, "backups", backupID+"/backup_config.json", minio.GetObjectOptions{})
	require.NoError(t, err)
	defer obj.Close()

	raw, err := io.ReadAll(obj)
	require.NoError(t, err, "global descriptor must exist for backup %s", backupID)

	var descriptor map[string]any
	require.NoError(t, json.Unmarshal(raw, &descriptor))
	return descriptor
}

// dtmUploadLogged reports whether any node ran the DTM node flow. The provider
// logs that line only on the DTM path, so it tells the two paths apart.
func dtmUploadLogged(ctx context.Context, t *testing.T, compose *docker.DockerCompose) bool {
	t.Helper()
	return anyNodeLogContains(ctx, t, compose, "starting DTM backup upload")
}

// forceTerminateLogged reports whether any surviving node proposed
// force-terminate for a stale task.
func forceTerminateLogged(ctx context.Context, t *testing.T, compose *docker.DockerCompose) bool {
	t.Helper()
	return anyNodeLogContains(ctx, t, compose, "proposing force-terminate")
}

func anyNodeLogContains(ctx context.Context, t *testing.T, compose *docker.DockerCompose, needle string) bool {
	t.Helper()
	for n := 1; n <= 3; n++ {
		c := compose.GetWeaviateNode(n)
		if c == nil {
			continue
		}
		logs, err := c.Container().Logs(ctx)
		if err != nil {
			continue
		}
		all, _ := io.ReadAll(logs)
		if strings.Contains(string(all), needle) {
			return true
		}
	}
	return false
}

// raftLeaderNodeIndex returns the 0-based node index of the current raft
// leader. It fails the test if no leader is found within 30 seconds.
func raftLeaderNodeIndex(t *testing.T, compose *docker.DockerCompose) int {
	t.Helper()
	var leaderName string
	require.Eventually(t, func() bool {
		for n := 1; n <= 3; n++ {
			helper.SetupClient(nodeURI(compose, n))
			resp, err := helper.Client(t).Cluster.ClusterGetStatistics(
				cluster.NewClusterGetStatisticsParams(), nil)
			if err != nil || resp.Payload == nil {
				continue
			}
			for _, stat := range resp.Payload.Statistics {
				id, ok := stat.LeaderID.(string)
				if ok && id != "" {
					leaderName = id
					return true
				}
			}
		}
		return false
	}, 30*time.Second, 500*time.Millisecond, "could not determine the raft leader")

	// Compose node names are 1-based ("node1" etc.); GetWeaviateNode uses
	// 1-based indexing too, so the 0-based index is n-1.
	for n := 1; n <= 3; n++ {
		helper.SetupClient(nodeURI(compose, n))
		resp, err := helper.Client(t).Cluster.ClusterGetStatistics(
			cluster.NewClusterGetStatisticsParams(), nil)
		if err != nil || resp.Payload == nil {
			continue
		}
		for _, stat := range resp.Payload.Statistics {
			if stat.Name == leaderName {
				return n - 1
			}
		}
	}
	t.Fatalf("leader %q found via LeaderID but no node claims that name", leaderName)
	return -1
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// assertDescriptorShape checks the invariants a restore depends on, with the
// gate on or off: structure version, per-node fields, and the total size.
func assertDescriptorShape(t *testing.T, descriptor map[string]any, label string) {
	t.Helper()
	assert.Equal(t, "2.1", descriptor["version"], "%s: artifact structure version is frozen", label)
	assert.Equal(t, "SUCCESS", descriptor["status"], "%s", label)
	assert.NotEmpty(t, descriptor["leader"], "%s: restore prefers the leader's descriptor", label)

	nodes, ok := descriptor["nodes"].(map[string]any)
	require.True(t, ok, "%s: nodes must be an object", label)
	require.Len(t, nodes, 3, "%s", label)

	var perNodeSum float64
	for name, entry := range nodes {
		node, ok := entry.(map[string]any)
		require.True(t, ok, "%s: node %s", label, name)
		assert.Equal(t, "SUCCESS", node["status"], "%s: node %s", label, name)
		assert.NotEmpty(t, node["classes"], "%s: node %s", label, name)
		size, ok := node["preCompressionSizeBytes"].(float64)
		require.True(t, ok, "%s: node %s has no size", label, name)
		assert.Positive(t, size, "%s: node %s", label, name)
		perNodeSum += size
	}

	total, ok := descriptor["preCompressionSizeBytes"].(float64)
	require.True(t, ok, "%s: aggregated size missing", label)
	assert.Equal(t, perNodeSum, total, "%s: the aggregate must be the sum of the per-node sizes", label)
}

// takeBackupAndReadDescriptor runs one backup on its own cluster and returns
// the global descriptor it produced. Both gate-on and gate-off also restore
// the backup they just took.
func takeBackupAndReadDescriptor(ctx context.Context, t *testing.T, backupID string, gateOn bool) map[string]any {
	t.Helper()
	compose, cleanup := start3NodeBackupCluster(ctx, t,
		"BACKUP_DISTRIBUTED_TASKS_ENABLED", fmt.Sprintf("%t", gateOn))
	defer cleanup()

	setupClient(t, compose, 1)
	defer helper.ResetClient()
	// 200 objects is enough: this row compares descriptor structure, not volume.
	createClassAndDataN(t, 200)

	_, err := helper.CreateBackup(t, helper.DefaultBackupConfig(), className, backendName, backupID)
	require.NoError(t, err)
	helper.ExpectBackupEventuallyCreated(t, backupID, backendName, nil,
		helper.WithDeadline(3*time.Minute))

	descriptor := fetchGlobalDescriptor(ctx, t, compose, backupID)

	// Without this the row would compare two legacy backups and pass.
	assert.Equal(t, gateOn, dtmUploadLogged(ctx, t, compose),
		"gate=%t must decide which orchestration ran", gateOn)

	helper.DeleteClass(t, className)
	_, restoreErr := helper.RestoreBackup(t, helper.DefaultRestoreConfig(), className, backendName, backupID, nil, false)
	require.NoError(t, restoreErr, "backup must restore on the %s path", map[bool]string{true: "DTM", false: "legacy"}[gateOn])
	helper.ExpectBackupEventuallyRestored(t, backupID, backendName, nil,
		helper.WithDeadline(3*time.Minute))

	return descriptor
}

var sigkillFast = 1 * time.Second

// Each subtest starts a fresh cluster so node lifecycle events do not
// contaminate each other.
func TestBackupMultinode(t *testing.T) {
	t.Run("raft leader killed and restarted mid-upload, backup completes after leader change", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		// Long stale timeout so the restarted node has time to finish.
		compose, cleanup := start3NodeBackupCluster(ctx, t,
			"BACKUP_STALE_TIMEOUT", "5m")
		defer cleanup()

		setupClient(t, compose, 1)
		defer helper.ResetClient()
		createClassAndData(t)

		leaderIdx := raftLeaderNodeIndex(t, compose)
		t.Logf("raft leader before backup: node index %d", leaderIdx)

		// Use a non-leader node so the client survives the upcoming kill.
		survivorIdx := (leaderIdx + 1) % 3
		helper.SetupClient(nodeURI(compose, survivorIdx+1))

		_, err := helper.CreateBackup(t, helper.DefaultBackupConfig(), className, backendName, "leader-kill")
		require.NoError(t, err)

		// Let the upload start before killing the leader.
		time.Sleep(2 * time.Second)
		require.NoError(t, compose.StopNode(ctx, leaderIdx, &sigkillFast))

		var newLeaderIdx int
		require.Eventually(t, func() bool {
			for n := 1; n <= 3; n++ {
				if n-1 == leaderIdx {
					continue
				}
				helper.SetupClient(nodeURI(compose, n))
				resp, err := helper.Client(t).Cluster.ClusterGetStatistics(
					cluster.NewClusterGetStatisticsParams(), nil)
				if err != nil || resp.Payload == nil {
					continue
				}
				for _, stat := range resp.Payload.Statistics {
					id, ok := stat.LeaderID.(string)
					if ok && id != "" {
						newLeaderIdx = raftLeaderNodeIndex(t, compose)
						return newLeaderIdx != leaderIdx
					}
				}
			}
			return false
		}, 30*time.Second, 500*time.Millisecond, "no new raft leader emerged after the kill")
		assert.NotEqual(t, leaderIdx, newLeaderIdx,
			"leadership must have moved to another node")
		t.Logf("raft leader after kill: node index %d", newLeaderIdx)

		require.NoError(t, compose.StartNode(ctx, leaderIdx))

		helper.SetupClient(nodeURI(compose, survivorIdx+1))
		st, errMsg := pollBackupStatus(t, "leader-kill", 6*time.Minute)
		assert.Equal(t, "SUCCESS", st,
			"backup must complete after leader kill+restart, got: %s error: %s", st, errMsg)
	})

	t.Run("operator cancel mid-upload", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		compose, cleanup := start3NodeBackupCluster(ctx, t)
		defer cleanup()

		setupClient(t, compose, 1)
		defer helper.ResetClient()
		createClassAndData(t)

		_, err := helper.CreateBackup(t, helper.DefaultBackupConfig(), className, backendName, "cancel-mid")
		require.NoError(t, err)

		// Cancel only once the backup is in flight. Otherwise the upload can
		// finish before the cancel arrives, and the cancel returns 422.
		require.Eventually(t, func() bool {
			resp, err := helper.CreateBackupStatus(t, backendName, "cancel-mid", "", "")
			if err != nil {
				return false
			}
			st := *resp.Payload.Status
			return st == "TRANSFERRING" || st == "STARTED"
		}, 30*time.Second, 200*time.Millisecond, "backup must reach in-flight state")

		cancelErr := helper.CancelBackup(t, backendName, "cancel-mid")
		require.NoError(t, cancelErr, "cancel of a STARTED backup must succeed")

		st, _ := pollBackupStatus(t, "cancel-mid", 2*time.Minute)
		assert.Equal(t, "CANCELED", st,
			"backup must be cancelled after operator cancel")
	})

	t.Run("node death mid-upload with stale exit, force-terminate, and terminal descriptor", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		compose, cleanup := start3NodeBackupCluster(ctx, t,
			"BACKUP_STALE_TIMEOUT", "30s")
		defer cleanup()

		setupClient(t, compose, 1)
		defer helper.ResetClient()
		createClassAndData(t)

		_, err := helper.CreateBackup(t, helper.DefaultBackupConfig(), className, backendName, "stale-exit")
		require.NoError(t, err)

		require.NoError(t, compose.StopNode(ctx, 1, &sigkillFast))

		st, _ := pollBackupStatus(t, "stale-exit", 4*time.Minute)
		assert.Equal(t, "FAILED", st, "dead node must trigger stale exit -> FAILED")

		assert.True(t, forceTerminateLogged(ctx, t, compose),
			"the stale detector must have proposed force-terminate")

		descriptor := fetchGlobalDescriptor(ctx, t, compose, "stale-exit")
		assert.Equal(t, "FAILED", descriptor["status"],
			"the global descriptor must be terminal after the stale exit")
	})

	t.Run("backend outage during failed backup still ends with a terminal descriptor", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		compose, cleanup := start3NodeBackupCluster(ctx, t)
		defer cleanup()

		setupClient(t, compose, 1)
		defer helper.ResetClient()
		createClassAndData(t)

		// Start the backup (Initialize contacts S3, so MinIO must be up).
		_, err := helper.CreateBackup(t, helper.DefaultBackupConfig(), className, backendName, "backend-outage")
		require.NoError(t, err)

		// DisconnectFromNetwork is used instead of StopMinIO because the
		// MinIO container has AutoRemove:true. The disconnect races the
		// upload, and an upload still running fails because S3 writes
		// need the backend.
		require.NoError(t, compose.DisconnectFromNetwork(ctx, "test-minio"))

		// Keep MinIO unreachable long enough for the stale detector to
		// fire and fail the task.
		time.Sleep(30 * time.Second)

		// Reconnect MinIO so the terminal descriptor can be written.
		require.NoError(t, compose.ConnectToNetwork(ctx, "test-minio"))

		st, _ := pollBackupStatus(t, "backend-outage", 4*time.Minute)
		assert.Equal(t, "FAILED", st,
			"backend outage must cause FAILED with a terminal descriptor")
	})

	t.Run("concurrent create rejected cluster-wide", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		compose, cleanup := start3NodeBackupCluster(ctx, t)
		defer cleanup()

		setupClient(t, compose, 1)
		defer helper.ResetClient()
		createClassAndData(t)

		_, err := helper.CreateBackup(t, helper.DefaultBackupConfig(), className, backendName, "first")
		require.NoError(t, err)

		helper.SetupClient(nodeURI(compose, 2))
		_, err2 := helper.CreateBackup(t, helper.DefaultBackupConfig(), className, backendName, "second")
		require.Error(t, err2, "concurrent create must be rejected cluster-wide")

		helper.SetupClient(nodeURI(compose, 1))
		helper.ExpectBackupEventuallyCreated(t, "first", backendName, nil,
			helper.WithDeadline(3*time.Minute))
	})

	// Each backup runs on its own cluster, because the object store lives
	// inside the compose. The two descriptors are compared in memory
	// afterwards. Both sides also restore the backup they took.
	t.Run("gate-on and gate-off backups produce the same descriptor structure", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer cancel()

		gateOn := takeBackupAndReadDescriptor(ctx, t, "format-gate-on", true)
		gateOff := takeBackupAndReadDescriptor(ctx, t, "format-gate-off", false)

		assertDescriptorShape(t, gateOn, "gate-on")
		assertDescriptorShape(t, gateOff, "gate-off")

		assert.Equal(t, sortedKeys(gateOff), sortedKeys(gateOn),
			"gate-on and gate-off descriptors must carry the same fields")
		assert.Equal(t, gateOff["compressionType"], gateOn["compressionType"])
		assert.Equal(t, gateOff["serverVersion"], gateOn["serverVersion"],
			"descriptor fields come from the request, not the writing node")

		onNodes := gateOn["nodes"].(map[string]any)
		offNodes := gateOff["nodes"].(map[string]any)
		for name, entry := range onNodes {
			other, ok := offNodes[name]
			require.True(t, ok, "node %s missing from the gate-off descriptor", name)
			assert.Equal(t, sortedKeys(other.(map[string]any)), sortedKeys(entry.(map[string]any)),
				"node %s: per-node fields must match", name)
		}
	})
}
