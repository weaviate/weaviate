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

// Package backup_dedupe_replicas_test proves dedupeReplicas backup and fan-out restore on a real 3-node cluster with minio.
package backup_dedupe_replicas_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	minioCredentials "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/cluster/router/types"
	entbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	ubak "github.com/weaviate/weaviate/usecases/backup"
)

const (
	bucketName = "backups"
	regionName = "us-east-1"
	backendS3  = "s3"
	// Chunk data must dominate descriptor metadata or the size comparison is meaningless.
	numObjects = 2000
)

var nodeNames = []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2}

// Planning (10s cutoff lead + budget) must stay under the helper client's 30s HTTP timeout.
func dedupeBackupConfig() *models.BackupConfig {
	cfg := helper.DefaultBackupConfig()
	cfg.DedupeReplicas = true
	cfg.DedupeConvergenceTimeoutSeconds = 15
	return cfg
}

func clusterURIs(compose *docker.DockerCompose) []string {
	return []string{
		compose.GetWeaviate().ClusterURI(),
		compose.GetWeaviateNode(2).ClusterURI(),
		compose.GetWeaviateNode(3).ClusterURI(),
	}
}

func tryCreateCheckpoint(clusterURI, className string, shards []string, cutoffMs, createdAtMs int64) error {
	body, err := json.Marshal(map[string]any{
		"shards":        shards,
		"cutoff_ms":     cutoffMs,
		"created_at_ms": createdAtMs,
	})
	if err != nil {
		return err
	}
	uri := clusterURI
	if !strings.HasPrefix(uri, "http://") {
		uri = "http://" + uri
	}
	resp, err := http.Post(fmt.Sprintf("%s/replicas/indices/%s/async-checkpoint", uri, className),
		"application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("create checkpoint on %s: %d: %s", clusterURI, resp.StatusCode, raw)
	}
	return nil
}

// waitForCheckpointCapability probes until every node can host a checkpoint for every shard; the throttled hashtree init scan makes creates fail transiently after class creation.
func waitForCheckpointCapability(t *testing.T, compose *docker.DockerCompose, className string, shards []string) {
	t.Helper()
	clusters := clusterURIs(compose)
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		createdAt := time.Now().UTC()
		cutoffMs := createdAt.Add(time.Hour).UnixMilli()
		for _, cluster := range clusters {
			require.NoError(ct, tryCreateCheckpoint(cluster, className, shards, cutoffMs, createdAt.UnixMilli()))
		}
		defer func() {
			for _, cluster := range clusters {
				common.DeleteAsyncCheckpoint(t, cluster, className, shards)
			}
		}()
		for _, cluster := range clusters {
			statuses := common.AsyncCheckpointStatus(t, cluster, className, shards)
			for _, shard := range shards {
				entry, ok := statuses[shard]
				require.True(ct, ok, "no checkpoint entry for shard %q on %s", shard, cluster)
				require.NotZero(ct, entry.CutoffMs, "inactive checkpoint for shard %q on %s", shard, cluster)
			}
		}
	}, 2*time.Minute, time.Second)
}

func minioClient(t *testing.T, minioURI string) *minio.Client {
	t.Helper()
	client, err := minio.New(minioURI, &minio.Options{
		Creds:  minioCredentials.NewStaticV4("aws_access_key", "aws_secret_key", ""),
		Secure: false,
	})
	require.NoError(t, err)
	return client
}

func readJSONObject(t *testing.T, client *minio.Client, key string, out any) bool {
	t.Helper()
	obj, err := client.GetObject(context.Background(), bucketName, key, minio.GetObjectOptions{})
	require.NoError(t, err)
	defer obj.Close()
	raw, err := io.ReadAll(obj)
	if err != nil {
		return false
	}
	require.NoError(t, json.Unmarshal(raw, out))
	return true
}

// shardHolders maps each shard of className to the node prefixes that archived it.
func shardHolders(t *testing.T, client *minio.Client, backupID, className string) (map[string][]string, map[string]*entbackup.BackupDescriptor) {
	t.Helper()
	holders := map[string][]string{}
	metas := map[string]*entbackup.BackupDescriptor{}
	for _, node := range nodeNames {
		var meta entbackup.BackupDescriptor
		if !readJSONObject(t, client, fmt.Sprintf("%s/%s/%s", backupID, node, ubak.BackupFile), &meta) {
			continue
		}
		metas[node] = &meta
		for _, cls := range meta.Classes {
			if cls.Name != className {
				continue
			}
			for _, sd := range cls.Shards {
				require.Equal(t, node, sd.Node, "shard %q under prefix %q claims node %q", sd.Name, node, sd.Node)
				holders[sd.Name] = append(holders[sd.Name], node)
			}
		}
	}
	return holders, metas
}

func backupTotalSize(t *testing.T, client *minio.Client, backupID string) int64 {
	t.Helper()
	total := int64(0)
	for object := range client.ListObjects(context.Background(), bucketName, minio.ListObjectsOptions{Recursive: true}) {
		require.NoError(t, object.Err)
		if strings.HasPrefix(object.Key, backupID+"/") {
			total += object.Size
		}
	}
	return total
}

func seedObjects(t *testing.T, host, className string, n int) []strfmt.UUID {
	t.Helper()
	batch := make([]*models.Object, n)
	ids := make([]strfmt.UUID, n)
	for i := range batch {
		ids[i] = strfmt.UUID(uuid.NewString())
		batch[i] = &models.Object{
			Class:      className,
			ID:         ids[i],
			Properties: map[string]any{"contents": fmt.Sprintf("object#%d %s%s%s", i, uuid.NewString(), uuid.NewString(), uuid.NewString())},
		}
	}
	// CL=ALL makes replicas identical at write time, so roots converge without async-rep lag.
	common.CreateObjectsCL(t, host, batch, types.ConsistencyLevelAll)
	return ids
}

// requireOnEveryNode reads node-locally on all replicas right after restore, proving fan-out rather than async-rep healing.
func requireOnEveryNode(t *testing.T, host, className string, ids []strfmt.UUID) {
	t.Helper()
	step := max(1, len(ids)/50)
	for _, node := range nodeNames {
		for i := 0; i < len(ids); i += step {
			obj, err := common.GetObjectFromNode(t, host, className, ids[i], node)
			require.NoError(t, err, "object %s missing on %s", ids[i], node)
			require.NotNil(t, obj)
		}
	}
}

func restoreErrorMessage(err error) string {
	var payload *models.ErrorResponse
	var uerr *backups.BackupsRestoreUnprocessableEntity
	var ierr *backups.BackupsRestoreInternalServerError
	var nerr *backups.BackupsRestoreNotFound
	switch {
	case errors.As(err, &uerr):
		payload = uerr.Payload
	case errors.As(err, &ierr):
		payload = ierr.Payload
	case errors.As(err, &nerr):
		payload = nerr.Payload
	}
	if payload == nil {
		return err.Error()
	}
	parts := make([]string, 0, len(payload.Error))
	for _, item := range payload.Error {
		parts = append(parts, item.Message)
	}
	return strings.Join(parts, "; ")
}

// dumpNodeLogs surfaces backup-relevant container log lines on failure.
func dumpNodeLogs(t *testing.T, compose *docker.DockerCompose) {
	if !t.Failed() {
		return
	}
	for i := 1; i <= 3; i++ {
		node := compose.GetWeaviateNode(i)
		if node == nil {
			continue
		}
		reader, err := node.Container().Logs(context.Background())
		if err != nil {
			continue
		}
		raw, _ := io.ReadAll(reader)
		reader.Close()
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.Contains(line, "dedupe") || strings.Contains(line, "restore") ||
				strings.Contains(line, "backup") && strings.Contains(line, "error") {
				t.Logf("[%s] %s", node.Name(), line)
			}
		}
	}
}

func startDedupeCluster(ctx context.Context, t *testing.T) *docker.DockerCompose {
	t.Helper()
	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithBackendS3(bucketName, regionName).
		Start(ctx)
	require.NoError(t, err)
	return compose
}

func restoreAndVerify(t *testing.T, host, className, backupID string, ids []strfmt.UUID) {
	t.Helper()
	_, err := helper.RestoreBackup(t, helper.DefaultRestoreConfig(), className, backendS3, backupID, nil, false)
	if err != nil {
		t.Fatalf("restore refused: %s", restoreErrorMessage(err))
	}
	helper.ExpectBackupEventuallyRestored(t, backupID, backendS3, nil, helper.WithDeadline(4*time.Minute))
	requireOnEveryNode(t, host, className, ids)
}

func newReplicatedClass(name string) *models.Class {
	return &models.Class{
		Class:             name,
		Vectorizer:        "none",
		ReplicationConfig: &models.ReplicationConfig{Factor: 3},
		Properties: []*models.Property{
			{Name: "contents", DataType: []string{"text"}},
		},
	}
}

func TestBackupDedupeReplicas(t *testing.T) {
	ctx := context.Background()

	compose := startDedupeCluster(ctx, t)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	host := compose.GetWeaviate().URI()
	helper.SetupClient(host)
	defer helper.ResetClient()
	defer dumpNodeLogs(t, compose)

	minioC := minioClient(t, compose.GetMinIO().URI())

	const (
		className       = "DedupeArticles"
		backupID        = "dedupe-backup-1"
		controlBackupID = "dedupe-control-1"
	)
	var ids []strfmt.UUID

	t.Run("seed replicated class", func(t *testing.T) {
		helper.CreateClass(t, newReplicatedClass(className))
		ids = seedObjects(t, host, className, numObjects)
	})

	shards := common.DiscoverShards(t, host, className)
	require.NotEmpty(t, shards)
	waitForCheckpointCapability(t, compose, className, shards)

	t.Run("deduped backup archives each shard under exactly one node", func(t *testing.T) {
		_, err := helper.CreateBackup(t, dedupeBackupConfig(), className, backendS3, backupID)
		require.NoError(t, err)
		helper.ExpectBackupEventuallyCreated(t, backupID, backendS3, nil, helper.WithDeadline(4*time.Minute))

		var global entbackup.DistributedBackupDescriptor
		require.True(t, readJSONObject(t, minioC, fmt.Sprintf("%s/%s", backupID, ubak.GlobalBackupFile), &global))
		assert.Equal(t, ubak.VersionDedupeReplicas, global.Version)
		assert.True(t, global.DedupeReplicas)

		holders, metas := shardHolders(t, minioC, backupID, className)
		seen := map[string]struct{}{}
		for shard, nodes := range holders {
			assert.Len(t, nodes, 1, "shard %q archived by %v, want exactly one node", shard, nodes)
			seen[shard] = struct{}{}
		}
		for _, shard := range shards {
			assert.Contains(t, seen, shard, "shard %q missing from the artifact", shard)
		}
		for node, meta := range metas {
			assert.Equal(t, ubak.VersionDedupeReplicas, meta.Version, "node %s meta version", node)
			assert.True(t, meta.DedupeReplicas, "node %s meta flag", node)
		}
	})

	t.Run("no checkpoints stay active after planning", func(t *testing.T) {
		for _, cluster := range clusterURIs(compose) {
			for shard, entry := range common.AsyncCheckpointStatus(t, cluster, className, shards) {
				assert.Zero(t, entry.CutoffMs, "shard %q on %s still has an active checkpoint", shard, cluster)
			}
		}
	})

	t.Run("deduped artifact is meaningfully smaller than all-replica", func(t *testing.T) {
		_, err := helper.CreateBackup(t, helper.DefaultBackupConfig(), className, backendS3, controlBackupID)
		require.NoError(t, err)
		helper.ExpectBackupEventuallyCreated(t, controlBackupID, backendS3, nil, helper.WithDeadline(4*time.Minute))

		var control entbackup.DistributedBackupDescriptor
		require.True(t, readJSONObject(t, minioC, fmt.Sprintf("%s/%s", controlBackupID, ubak.GlobalBackupFile), &control))
		assert.Equal(t, ubak.Version, control.Version)
		assert.False(t, control.DedupeReplicas)

		controlHolders, _ := shardHolders(t, minioC, controlBackupID, className)
		for shard, nodes := range controlHolders {
			assert.Len(t, nodes, 3, "control backup should archive shard %q on every replica", shard)
		}

		deduped, allReplica := backupTotalSize(t, minioC, backupID), backupTotalSize(t, minioC, controlBackupID)
		assert.Less(t, deduped, allReplica*60/100,
			"deduped backup (%d bytes) should be well under 60%% of the all-replica one (%d bytes)", deduped, allReplica)
	})

	t.Run("restore with a non-injective node mapping is refused", func(t *testing.T) {
		_, err := helper.RestoreBackup(t, helper.DefaultRestoreConfig(), className, backendS3, backupID,
			map[string]string{docker.Weaviate1: docker.Weaviate0, docker.Weaviate2: docker.Weaviate0}, false)
		require.Error(t, err)
		assert.Contains(t, restoreErrorMessage(err), "injective node_mapping")
	})

	t.Run("restore with an unresolvable replica is refused with a hint", func(t *testing.T) {
		_, err := helper.RestoreBackup(t, helper.DefaultRestoreConfig(), className, backendS3, backupID,
			map[string]string{docker.Weaviate2: "ghost-node"}, false)
		require.Error(t, err)
		msg := restoreErrorMessage(err)
		assert.Contains(t, msg, "ghost-node")
		assert.Contains(t, msg, "node_mapping")
	})

	t.Run("restore fans the single copy out to every replica", func(t *testing.T) {
		helper.DeleteClass(t, className)
		restoreAndVerify(t, host, className, backupID, ids)
	})

	t.Run("backup under continuous writes completes without losing shards", func(t *testing.T) {
		const churnID = "dedupe-churn-1"
		stop := make(chan struct{})
		writerDone := make(chan struct{})
		var writerErr error
		client := helper.Client(t)
		go func() {
			defer close(writerDone)
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
					params := batch.NewBatchObjectsCreateParams().WithBody(batch.BatchObjectsCreateBody{
						Objects: []*models.Object{{
							Class:      className,
							ID:         strfmt.UUID(uuid.NewString()),
							Properties: map[string]any{"contents": fmt.Sprintf("churn#%d", i)},
						}},
					})
					if _, err := client.Batch.BatchObjectsCreate(params, nil); err != nil {
						writerErr = err
						return
					}
					time.Sleep(50 * time.Millisecond)
				}
			}
		}()

		_, err := helper.CreateBackup(t, dedupeBackupConfig(), className, backendS3, churnID)
		require.NoError(t, err)
		helper.ExpectBackupEventuallyCreated(t, churnID, backendS3, nil, helper.WithDeadline(4*time.Minute))
		close(stop)
		<-writerDone
		require.NoError(t, writerErr, "concurrent writer failed during backup")

		holders, _ := shardHolders(t, minioC, churnID, className)
		for _, shard := range shards {
			nodes := holders[shard]
			assert.True(t, len(nodes) == 1 || len(nodes) == 3,
				"shard %q archived by %v, want one node (deduped) or all replicas (fallback)", shard, nodes)
		}
	})

	t.Run("convergence timeout above the maximum is rejected", func(t *testing.T) {
		cfg := dedupeBackupConfig()
		cfg.DedupeConvergenceTimeoutSeconds = 601
		_, err := helper.CreateBackup(t, cfg, className, backendS3, "dedupe-bad-timeout")
		require.Error(t, err)
	})

	t.Run("restored replicas survive a rolling cluster restart", func(t *testing.T) {
		// One node at a time keeps RAFT quorum, so restarted nodes can rejoin.
		for i := 1; i <= 3; i++ {
			common.StopNodeAt(ctx, t, compose, i)
			common.StartNodeAt(ctx, t, compose, i)
		}
		host = compose.GetWeaviate().URI()
		helper.SetupClient(host)
		// Ready nodes may still be loading shards; retry until reads settle.
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			for _, node := range nodeNames {
				_, err := common.GetObjectFromNode(t, host, className, ids[0], node)
				require.NoError(ct, err)
			}
		}, 2*time.Minute, 2*time.Second)
		requireOnEveryNode(t, host, className, ids)
	})

	t.Run("legacy non-deduped backup restores through the untouched path", func(t *testing.T) {
		helper.DeleteClass(t, className)
		restoreAndVerify(t, host, className, controlBackupID, ids)
	})
}

func TestBackupDedupeMultiTenantColdTenantFallback(t *testing.T) {
	ctx := context.Background()

	compose := startDedupeCluster(ctx, t)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	host := compose.GetWeaviate().URI()
	helper.SetupClient(host)
	defer helper.ResetClient()
	defer dumpNodeLogs(t, compose)

	minioC := minioClient(t, compose.GetMinIO().URI())

	const (
		className = "DedupeTenants"
		backupID  = "dedupe-mt-1"
		hotTenant = "tenant-hot"
		coldT     = "tenant-cold"
	)

	class := newReplicatedClass(className)
	class.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
	helper.CreateClass(t, class)
	helper.CreateTenants(t, className, []*models.Tenant{{Name: hotTenant}, {Name: coldT}})

	tenantIDs := map[string][]strfmt.UUID{}
	for _, tenant := range []string{hotTenant, coldT} {
		batch := make([]*models.Object, 10)
		ids := make([]strfmt.UUID, 10)
		for i := range batch {
			ids[i] = strfmt.UUID(uuid.NewString())
			batch[i] = &models.Object{
				Class:      className,
				ID:         ids[i],
				Tenant:     tenant,
				Properties: map[string]any{"contents": fmt.Sprintf("%s#%d", tenant, i)},
			}
		}
		common.CreateObjectsCL(t, host, batch, types.ConsistencyLevelAll)
		tenantIDs[tenant] = ids
	}

	waitForCheckpointCapability(t, compose, className, []string{hotTenant, coldT})

	t.Run("deactivate one tenant", func(t *testing.T) {
		helper.UpdateTenants(t, className, []*models.Tenant{{Name: coldT, ActivityStatus: models.TenantActivityStatusCOLD}})
	})

	t.Run("cold tenant falls back to all replicas, hot tenant dedupes", func(t *testing.T) {
		_, err := helper.CreateBackup(t, dedupeBackupConfig(), className, backendS3, backupID)
		require.NoError(t, err)
		helper.ExpectBackupEventuallyCreated(t, backupID, backendS3, nil, helper.WithDeadline(4*time.Minute))

		holders, _ := shardHolders(t, minioC, backupID, className)
		assert.Len(t, holders[hotTenant], 1, "hot tenant archived by %v, want one node", holders[hotTenant])
		assert.Len(t, holders[coldT], 3, "cold tenant archived by %v, want every replica", holders[coldT])

		tenant, err := helper.GetOneTenant(t, className, coldT)
		require.NoError(t, err)
		assert.Equal(t, models.TenantActivityStatusCOLD, tenant.Payload.ActivityStatus,
			"backup planning must never activate a cold tenant")
	})

	t.Run("restore fans hot tenant out and keeps cold tenant restorable", func(t *testing.T) {
		helper.DeleteClass(t, className)
		_, err := helper.RestoreBackup(t, helper.DefaultRestoreConfig(), className, backendS3, backupID, nil, false)
		require.NoError(t, err)
		helper.ExpectBackupEventuallyRestored(t, backupID, backendS3, nil, helper.WithDeadline(4*time.Minute))

		for _, node := range nodeNames {
			for _, id := range tenantIDs[hotTenant] {
				obj, err := common.GetTenantObjectFromNode(t, host, className, id, node, hotTenant)
				require.NoError(t, err, "hot tenant object %s missing on %s", id, node)
				require.NotNil(t, obj)
			}
		}

		helper.UpdateTenants(t, className, []*models.Tenant{{Name: coldT, ActivityStatus: models.TenantActivityStatusHOT}})
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			for _, node := range nodeNames {
				for _, id := range tenantIDs[coldT] {
					obj, err := common.GetTenantObjectFromNode(t, host, className, id, node, coldT)
					require.NoError(ct, err, "cold tenant object %s missing on %s", id, node)
					require.NotNil(ct, obj)
				}
			}
		}, time.Minute, time.Second)
	})
}
