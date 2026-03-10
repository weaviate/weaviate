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

// Package compaction_test contains end-to-end tests verifying that LSM
// compaction is not blocked during backup operations.
//
// Running:
//
//	go test ./test/acceptance/compaction/... -v -run TestBackup_CompactionRunsDuringBackup -timeout 15m
package compaction_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	moduleshelper "github.com/weaviate/weaviate/test/helper/modules"
)

const (
	backupCollection = "BackupCompactionTest"
	backupID         = "backup-compaction-test"
)

// importBatchForClass inserts objectsPerBatch objects into the given class.
func importBatchForClass(t *testing.T, className string) {
	t.Helper()
	objects := make([]*models.Object, objectsPerBatch)
	for i := range objects {
		objects[i] = &models.Object{
			Class: className,
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"text": randomText(textSize),
			},
		}
	}
	params := batch.NewBatchObjectsCreateParams().
		WithBody(batch.BatchObjectsCreateBody{Objects: objects})
	resp, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
	require.NoError(t, err)
	require.NotNil(t, resp)
}

// getShardNameForClass returns the first shard name for the given class.
func getShardNameForClass(t *testing.T, className string) string {
	t.Helper()
	res, err := helper.Client(t).Schema.SchemaObjectsShardsGet(
		schema.NewSchemaObjectsShardsGetParams().WithClassName(className), nil)
	require.NoError(t, err)
	require.NotEmpty(t, res.Payload, "no shards found for class")
	return res.Payload[0].Name
}

// TestBackup_CompactionRunsDuringBackup verifies that LSM compaction continues
// to merge segments while a backup is in progress, proving that the backup
// descriptor path (hardlinks) does not hold a long-lived lock on the shard.
func TestBackup_CompactionRunsDuringBackup(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithBackendFilesystem().
		WithWeaviate().
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	container := compose.GetWeaviate().Container()

	// 1. Create collection
	helper.CreateClass(t, &models.Class{
		Class:      backupCollection,
		Vectorizer: "none",
		Properties: []*models.Property{{
			Name:     "text",
			DataType: []string{"text"},
		}},
	})
	defer helper.DeleteClass(t, backupCollection)

	// 2. Import initial data — 1000 batches to make the backup take long enough to observe compactions during its execution
	for i := 0; i < 1000; i++ {
		importBatchForClass(t, backupCollection)
	}

	// 3. Wait for segments to appear on disk
	shardName := getShardNameForClass(t, backupCollection)
	require.Eventually(t, func() bool {
		return totalSegmentFileCount(ctx, container, backupCollection, shardName, "objects") >= 1
	}, 180*time.Second, time.Second, "not enough segments appeared")

	// 4. Record pre-backup object count
	preBackupCount := moduleshelper.GetClassCount(t, backupCollection, "")

	// 5. Start backup (returns immediately, runs async)
	cfg := helper.DefaultBackupConfig()
	_, err = helper.CreateBackup(t, cfg, backupCollection, "filesystem", backupID)
	require.NoError(t, err)

	// 6. Observe compaction during backup
	var (
		sawCompactionDuringBackup bool
		prevMaxLevel              int
		prevSegCount              int
		backupDone                bool
	)

	require.Eventually(t, func() bool {
		// Check backup status
		resp, statusErr := helper.CreateBackupStatus(t, "filesystem", backupID, "", "")
		if statusErr == nil && resp.Payload != nil {
			status := *resp.Payload.Status
			if status == "SUCCESS" || status == "FAILED" {
				backupDone = true
			}
		}

		// Insert more data to keep creating segments that need compaction
		importBatchForClass(t, backupCollection)

		// Snapshot segment state
		files := listBucketFiles(ctx, container, backupCollection, shardName, "objects")
		maxLevel := 0
		segCount := 0
		for _, f := range files {
			if strings.HasSuffix(f, ".db") && !strings.HasSuffix(f, ".deleteme") {
				segCount++
				if lvl := segmentLevel(f); lvl > maxLevel {
					maxLevel = lvl
				}
			}
		}

		fmt.Printf("  [poll] backup_done=%v segments=%d max_level=%d prev_segments=%d prev_max_level=%d\n",
			backupDone, segCount, maxLevel, prevSegCount, prevMaxLevel)

		// Detect compaction: new higher-level segments appeared, OR
		// segment count decreased (merge consumed level-0 segments)
		if !backupDone && (maxLevel > prevMaxLevel || (prevSegCount > 0 && segCount < prevSegCount)) {
			sawCompactionDuringBackup = true
		}
		prevMaxLevel = maxLevel
		prevSegCount = segCount

		return backupDone
	}, 5*time.Minute, 2*time.Second, "backup did not complete")

	// 7. Assert compaction occurred during backup
	require.True(t, sawCompactionDuringBackup,
		"expected compaction to merge segments while backup was in progress")

	// 8. Assert backup succeeded
	resp, err := helper.CreateBackupStatus(t, "filesystem", backupID, "", "")
	require.NoError(t, err)
	require.Equal(t, "SUCCESS", *resp.Payload.Status)

	// 9. Delete + Restore + Validate
	helper.DeleteClass(t, backupCollection)

	restoreCfg := helper.DefaultRestoreConfig()
	_, err = helper.RestoreBackup(t, restoreCfg, backupCollection, "filesystem", backupID, nil, false)
	require.NoError(t, err)
	helper.ExpectBackupEventuallyRestored(t, backupID, "filesystem", nil,
		helper.WithDeadline(2*time.Minute))

	restoredCount := moduleshelper.GetClassCount(t, backupCollection, "")
	require.Equal(t, preBackupCount, restoredCount,
		"restored object count should match pre-backup count")
}
