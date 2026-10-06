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

package backup_dedupe_replicas_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	entbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	ubak "github.com/weaviate/weaviate/usecases/backup"
)

// dtmUploadLogged reports whether any node ran the DTM node flow. The provider
// logs that line only on the DTM path, so it tells the two paths apart.
func dtmUploadLogged(ctx context.Context, t *testing.T, compose *docker.DockerCompose) bool {
	t.Helper()
	return anyNodeLogContains(ctx, t, compose, "starting DTM backup upload")
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
		logs.Close()
		if strings.Contains(string(all), needle) {
			return true
		}
	}
	return false
}

// planningStarted reports whether any node holds a checkpoint for the class
// that planning set for a backup created after createdAt. It accepts cutoffs
// after createdAt and at most one minute from now.
//
// Lower bound: planning sets its cutoff 10s after it starts. It sleeps until
// that cutoff before it polls convergence. So an earlier backup that already
// published a plan has a cutoff before createdAt.
//
// Upper bound: the capability probe sets its cutoff an hour ahead.
func planningStarted(compose *docker.DockerCompose, className string, shards []string, createdAt time.Time) bool {
	maxCutoffMs := time.Now().Add(time.Minute).UnixMilli()
	for _, cluster := range clusterURIs(compose) {
		statuses, err := common.TryAsyncCheckpointStatus(cluster, className, shards)
		if err != nil {
			continue
		}
		for _, entry := range statuses {
			if entry.CutoffMs > createdAt.UnixMilli() && entry.CutoffMs <= maxCutoffMs {
				return true
			}
		}
	}
	return false
}

func TestBackupDedupeDTM(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithBackendS3(bucketName, regionName).
		WithWeaviateEnv("BACKUP_DEDUPE_ENABLED", "true").
		WithWeaviateLicense().
		WithWeaviateEnv("BACKUP_DISTRIBUTED_TASKS_ENABLED", "true").
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	host := compose.GetWeaviate().URI()
	helper.SetupClient(host)
	defer helper.ResetClient()
	defer dumpNodeLogs(t, compose)

	minioC := minioClient(t, compose.GetMinIO().URI())

	const className = "DedupeDTMArticles"
	helper.CreateClass(t, newReplicatedClass(className))
	ids := seedObjects(t, host, className, numObjects)
	shards := common.DiscoverShards(t, host, className)
	require.NotEmpty(t, shards)

	t.Run("dedupe backup runs through DTM and restores", func(t *testing.T) {
		const backupID = "dedupe-dtm-1"
		waitForCheckpointCapability(t, compose, className, shards)

		_, err := helper.CreateBackup(t, dedupeBackupConfig(), className, backendS3, backupID)
		require.NoError(t, err)
		helper.ExpectBackupEventuallyCreated(t, backupID, backendS3, nil, helper.WithDeadline(4*time.Minute))
		require.True(t, dtmUploadLogged(ctx, t, compose), "the backup must run through the DTM node flow")

		global := readGlobalMeta(t, minioC, backupID)
		assert.Equal(t, entbackup.Success, global.Status)
		assert.Equal(t, ubak.VersionDedupeReplicas, global.Version)
		assert.True(t, global.DedupeReplicas)
		require.NotEmpty(t, global.DedupeDesignations[className], "the published plan must designate shards")

		holders, metas := shardHolders(t, minioC, backupID, className)
		for shard, designee := range global.DedupeDesignations[className] {
			assert.Equal(t, []string{designee}, holders[shard], "shard %q must be archived by its designee only", shard)
		}
		for node, meta := range metas {
			assert.Equal(t, ubak.VersionDedupeReplicas, meta.Version, "node %s meta version", node)
			assert.True(t, meta.DedupeReplicas, "node %s meta flag", node)
		}

		helper.DeleteClass(t, className)
		restoreAndVerify(t, host, className, backupID, ids)
	})

	t.Run("cancel during convergence stops planning and uploads", func(t *testing.T) {
		const backupID = "dedupe-dtm-cancel-1"
		waitForCheckpointCapability(t, compose, className, shards)

		createdAt := time.Now()
		_, err := helper.CreateBackup(t, dedupeBackupConfig(), className, backendS3, backupID)
		require.NoError(t, err)
		// After planning creates checkpoints, it waits 10s before any upload. So
		// while this backup's checkpoint is active, the cancel lands before any
		// upload starts.
		require.Eventually(t, func() bool {
			return planningStarted(compose, className, shards, createdAt)
		}, 15*time.Second, 100*time.Millisecond, "planning never created its checkpoints")
		require.NoError(t, helper.CancelBackup(t, backendS3, backupID))

		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			status, err := helper.CreateBackupStatus(t, backendS3, backupID, "", "")
			require.NoError(ct, err)
			require.NotNil(ct, status.Payload)
			require.NotNil(ct, status.Payload.Status)
			require.Equal(ct, string(entbackup.Cancelled), *status.Payload.Status)
		}, time.Minute, time.Second)

		// The task reports CANCELED before the terminal descriptor is written.
		globalKey := fmt.Sprintf("%s/%s", backupID, ubak.GlobalBackupFile)
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			_, err := minioC.StatObject(ctx, bucketName, globalKey, minio.StatObjectOptions{})
			require.NoError(ct, err)
		}, time.Minute, time.Second, "the terminal global descriptor was never written")
		global := readGlobalMeta(t, minioC, backupID)
		assert.Equal(t, entbackup.Cancelled, global.Status)
		assert.Empty(t, global.DedupeDesignations, "no plan is published after a cancel during planning")
		for _, node := range nodeNames {
			var meta entbackup.BackupDescriptor
			assert.False(t, readJSONObject(t, minioC, fmt.Sprintf("%s/%s/%s", backupID, node, ubak.BackupFile), &meta),
				"node %s uploaded after the cancel", node)
		}
	})
}
