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
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	ubak "github.com/weaviate/weaviate/usecases/backup"
)

// startUnlicensedCluster starts a 3-node cluster with neither a license key nor BACKUP_DEDUPE_ENABLED.
func startUnlicensedCluster(ctx context.Context, t *testing.T) *docker.DockerCompose {
	t.Helper()
	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithBackendS3(bucketName, regionName).
		Start(ctx)
	require.NoError(t, err)
	return compose
}

// TestBackupDedupeRestoreWithoutLicense proves a deduped artifact restores on a cluster without a license or the dedupe flag.
func TestBackupDedupeRestoreWithoutLicense(t *testing.T) {
	ctx := context.Background()

	const (
		className = "UnlicensedRestoreArticles"
		backupID  = "dedupe-unlicensed-restore-1"
		numSeeded = 300
	)

	licensed := startDedupeCluster(ctx, t)
	licensedTerminated := false
	defer func() {
		if !licensedTerminated {
			require.NoError(t, licensed.Terminate(ctx))
		}
	}()

	host := licensed.GetWeaviate().URI()
	helper.SetupClient(host)
	defer helper.ResetClient()

	var ids []strfmt.UUID
	t.Run("licensed cluster creates a deduped backup", func(t *testing.T) {
		defer dumpNodeLogs(t, licensed)
		helper.CreateClass(t, newReplicatedClass(className))
		ids = seedObjects(t, host, className, numSeeded)
		shards := common.DiscoverShards(t, host, className)
		require.NotEmpty(t, shards)
		waitForCheckpointCapability(t, licensed, className, shards)

		_, err := helper.CreateBackup(t, dedupeBackupConfig(), className, backendS3, backupID)
		require.NoError(t, err)
		helper.ExpectBackupEventuallyCreated(t, backupID, backendS3, nil, helper.WithDeadline(4*time.Minute))

		global := readGlobalMeta(t, minioClient(t, licensed.GetMinIO().URI()), backupID)
		require.Equal(t, ubak.VersionDedupeReplicas, global.Version)
		require.True(t, global.DedupeReplicas)
		require.Positive(t, global.DedupeDesignatedShards)
	})
	require.NotEmpty(t, ids)

	artifact := downloadBackupPrefix(t, minioClient(t, licensed.GetMinIO().URI()), backupID)
	require.NotEmpty(t, artifact)
	require.NoError(t, licensed.Terminate(ctx))
	licensedTerminated = true

	unlicensed := startUnlicensedCluster(ctx, t)
	defer func() {
		require.NoError(t, unlicensed.Terminate(ctx))
	}()
	defer dumpNodeLogs(t, unlicensed)

	host = unlicensed.GetWeaviate().URI()
	helper.SetupClient(host)
	uploadBackupPrefix(t, minioClient(t, unlicensed.GetMinIO().URI()), artifact)

	t.Run("unlicensed cluster restores it on every replica", func(t *testing.T) {
		restoreAndVerify(t, host, className, backupID, ids)
	})
}
