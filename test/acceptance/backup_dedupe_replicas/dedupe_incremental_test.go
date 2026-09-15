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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	entbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	ubak "github.com/weaviate/weaviate/usecases/backup"
)

func readGlobalMeta(t *testing.T, client *minio.Client, backupID string) entbackup.DistributedBackupDescriptor {
	t.Helper()
	var global entbackup.DistributedBackupDescriptor
	require.True(t, readJSONObject(t, client, fmt.Sprintf("%s/%s", backupID, ubak.GlobalBackupFile), &global))
	return global
}

func countSkipEntries(metas map[string]*entbackup.BackupDescriptor, className string) int {
	n := 0
	for _, meta := range metas {
		for _, cls := range meta.Classes {
			if cls.Name != className {
				continue
			}
			for _, sd := range cls.Shards {
				for _, infos := range sd.IncrementalBackupInfo.FilesPerBackup {
					n += len(infos)
				}
			}
		}
	}
	return n
}

// TestBackupDedupeIncremental proves incremental + dedupe compose: sticky designations keep the skip benefit, every fallback still restores complete.
func TestBackupDedupeIncremental(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithBackendS3(bucketName, regionName).
		WithWeaviateEnv("BACKUP_MIN_CHUNK_SIZE", "4096").
		WithWeaviateEnv("BACKUP_DEDUPE_ENABLED", "true").
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

	const (
		className     = "DedupeIncrArticles"
		baseID        = "dedupe-incr-base"
		dedupedIncrID = "dedupe-incr-sticky"
		plainIncrID   = "dedupe-incr-plain"
		incr2ID       = "dedupe-incr-deeper"
		flagOffID     = "dedupe-incr-flagoff"
		ghostIncrID   = "dedupe-incr-ghost"
	)

	var ids []strfmt.UUID
	t.Run("seed replicated class", func(t *testing.T) {
		helper.CreateClass(t, newReplicatedClass(className))
		ids = seedObjects(t, host, className, numObjects)
	})

	shards := common.DiscoverShards(t, host, className)
	require.NotEmpty(t, shards)
	waitForCheckpointCapability(t, compose, className, shards)

	var baseDesignations map[string]string
	t.Run("deduped base backup designates every shard", func(t *testing.T) {
		_, err := helper.CreateBackup(t, dedupeBackupConfig(), className, backendS3, baseID)
		require.NoError(t, err)
		helper.ExpectBackupEventuallyCreated(t, baseID, backendS3, nil, helper.WithDeadline(4*time.Minute))

		global := readGlobalMeta(t, minioC, baseID)
		require.Equal(t, ubak.VersionDedupeReplicas, global.Version)
		require.True(t, global.DedupeReplicas)
		baseDesignations = global.DedupeDesignations[className]
		require.Len(t, baseDesignations, len(shards))
	})

	t.Run("sticky incremental keeps designees and skips files", func(t *testing.T) {
		ids = append(ids, seedObjects(t, host, className, 400)...)
		_, err := helper.CreateBackupWithBase(t, dedupeBackupConfig(), className, backendS3, dedupedIncrID, baseID)
		require.NoError(t, err)
		helper.ExpectBackupEventuallyCreated(t, dedupedIncrID, backendS3, nil, helper.WithDeadline(4*time.Minute))

		global := readGlobalMeta(t, minioC, dedupedIncrID)
		assert.Equal(t, ubak.VersionDedupeReplicas, global.Version)
		assert.Equal(t, baseID, global.BaseBackupID)
		assert.Equal(t, baseDesignations, global.DedupeDesignations[className])

		_, metas := shardHolders(t, minioC, dedupedIncrID, className)
		assert.Positive(t, countSkipEntries(metas, className), "sticky designees must skip unchanged base files")
	})

	t.Run("deduped incremental never larger than plain incremental", func(t *testing.T) {
		_, err := helper.CreateBackupWithBase(t, helper.DefaultBackupConfig(), className, backendS3, plainIncrID, baseID)
		require.NoError(t, err)
		helper.ExpectBackupEventuallyCreated(t, plainIncrID, backendS3, nil, helper.WithDeadline(4*time.Minute))

		deduped, plain := backupTotalSize(t, minioC, dedupedIncrID), backupTotalSize(t, minioC, plainIncrID)
		assert.LessOrEqual(t, deduped, plain,
			"deduped incremental (%d bytes) must never exceed the plain incremental (%d bytes)", deduped, plain)
	})

	t.Run("second hop stays sticky", func(t *testing.T) {
		_, err := helper.CreateBackupWithBase(t, dedupeBackupConfig(), className, backendS3, incr2ID, dedupedIncrID)
		require.NoError(t, err)
		helper.ExpectBackupEventuallyCreated(t, incr2ID, backendS3, nil, helper.WithDeadline(4*time.Minute))

		global := readGlobalMeta(t, minioC, incr2ID)
		assert.Equal(t, dedupedIncrID, global.BaseBackupID)
		assert.Equal(t, baseDesignations, global.DedupeDesignations[className])
	})

	t.Run("flag-off incremental on deduped base stamps 3.0 without dedupe", func(t *testing.T) {
		_, err := helper.CreateBackupWithBase(t, helper.DefaultBackupConfig(), className, backendS3, flagOffID, baseID)
		require.NoError(t, err)
		helper.ExpectBackupEventuallyCreated(t, flagOffID, backendS3, nil, helper.WithDeadline(4*time.Minute))

		global := readGlobalMeta(t, minioC, flagOffID)
		assert.Equal(t, ubak.VersionDedupeReplicas, global.Version)
		assert.False(t, global.DedupeReplicas)
	})

	t.Run("restore first-hop incremental fans out its base chunks", func(t *testing.T) {
		helper.DeleteClass(t, className)
		restoreAndVerify(t, host, className, dedupedIncrID, ids)
	})

	t.Run("restore second-hop incremental fans out", func(t *testing.T) {
		helper.DeleteClass(t, className)
		restoreAndVerify(t, host, className, incr2ID, ids)
	})

	t.Run("restore flag-off incremental via the legacy path", func(t *testing.T) {
		helper.DeleteClass(t, className)
		restoreAndVerify(t, host, className, flagOffID, ids)
	})

	t.Run("ghost base designee falls back and still works", func(t *testing.T) {
		globalKey := fmt.Sprintf("%s/%s", baseID, ubak.GlobalBackupFile)
		var raw map[string]any
		require.True(t, readJSONObject(t, minioC, globalKey, &raw))
		ghost := map[string]map[string]string{className: {}}
		for shard := range baseDesignations {
			ghost[className][shard] = "ghost-node"
		}
		raw["dedupeDesignations"] = ghost
		rewritten, err := json.Marshal(raw)
		require.NoError(t, err)
		_, err = minioC.PutObject(ctx, bucketName, globalKey, bytes.NewReader(rewritten), int64(len(rewritten)), minio.PutObjectOptions{})
		require.NoError(t, err)

		_, err = helper.CreateBackupWithBase(t, dedupeBackupConfig(), className, backendS3, ghostIncrID, baseID)
		require.NoError(t, err)
		helper.ExpectBackupEventuallyCreated(t, ghostIncrID, backendS3, nil, helper.WithDeadline(4*time.Minute))

		global := readGlobalMeta(t, minioC, ghostIncrID)
		require.Len(t, global.DedupeDesignations[className], len(shards))
		for shard, node := range global.DedupeDesignations[className] {
			assert.NotEqual(t, "ghost-node", node, "shard %q designated to the ghost", shard)
		}

		helper.DeleteClass(t, className)
		restoreAndVerify(t, host, className, ghostIncrID, ids)
	})

	t.Run("v2 base with deduped incremental keeps the benefit", func(t *testing.T) {
		const (
			v2ClassName = "DedupeIncrLegacyBase"
			v2BaseID    = "dedupe-incr-v2base"
			v2IncrID    = "dedupe-incr-on-v2base"
		)
		helper.CreateClass(t, newReplicatedClass(v2ClassName))
		v2IDs := seedObjects(t, host, v2ClassName, numObjects)

		_, err := helper.CreateBackup(t, helper.DefaultBackupConfig(), v2ClassName, backendS3, v2BaseID)
		require.NoError(t, err)
		helper.ExpectBackupEventuallyCreated(t, v2BaseID, backendS3, nil, helper.WithDeadline(4*time.Minute))
		base := readGlobalMeta(t, minioC, v2BaseID)
		require.False(t, base.DedupeReplicas)
		require.Nil(t, base.DedupeDesignations)

		v2Shards := common.DiscoverShards(t, host, v2ClassName)
		require.NotEmpty(t, v2Shards)
		waitForCheckpointCapability(t, compose, v2ClassName, v2Shards)

		v2IDs = append(v2IDs, seedObjects(t, host, v2ClassName, 400)...)
		_, err = helper.CreateBackupWithBase(t, dedupeBackupConfig(), v2ClassName, backendS3, v2IncrID, v2BaseID)
		require.NoError(t, err)
		helper.ExpectBackupEventuallyCreated(t, v2IncrID, backendS3, nil, helper.WithDeadline(4*time.Minute))

		global := readGlobalMeta(t, minioC, v2IncrID)
		assert.Equal(t, ubak.VersionDedupeReplicas, global.Version)
		assert.True(t, global.DedupeReplicas)
		require.Len(t, global.DedupeDesignations[v2ClassName], len(v2Shards))

		holders, metas := shardHolders(t, minioC, v2IncrID, v2ClassName)
		for shard, nodes := range holders {
			assert.Len(t, nodes, 1, "shard %q archived by %v, want exactly one node", shard, nodes)
		}
		assert.Positive(t, countSkipEntries(metas, v2ClassName), "any designee skips against a complete v2 base")

		helper.DeleteClass(t, v2ClassName)
		restoreAndVerify(t, host, v2ClassName, v2IncrID, v2IDs)
	})
}
