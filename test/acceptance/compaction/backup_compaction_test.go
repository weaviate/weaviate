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

	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/compactions"
	moduleshelper "github.com/weaviate/weaviate/test/helper/modules"
)

const (
	backupCollection = "BackupCompactionTest"
	backupID         = "backup-compaction-test"
)

// getShardNameForClass returns the first shard name for the given class.
func getShardNameForClass(t *testing.T, className string) string {
	t.Helper()
	res, err := helper.Client(t).Schema.SchemaObjectsShardsGet(
		schema.NewSchemaObjectsShardsGetParams().WithClassName(className), nil)
	require.NoError(t, err)
	require.NotEmpty(t, res.Payload, "no shards found for class")
	return res.Payload[0].Name
}

func backupStatus(t *testing.T) string {
	t.Helper()
	resp, err := helper.CreateBackupStatus(t, "filesystem", backupID, "", "")
	require.NoError(t, err)
	return *resp.Payload.Status
}

// TestBackup_CompactionRunsDuringBackup verifies that LSM compaction merges
// segments while a backup is open, proving that the hardlink snapshot path does
// not keep the shard halted for the upload. The server holds every shard upload
// until the test creates uploadHoldFile, so the backup stays open for as long as
// compaction needs: up to the 60s maximum of the compaction cycle backoff.
func TestBackup_CompactionRunsDuringBackup(t *testing.T) {
	ctx := context.Background()

	const (
		uploadHoldFile = "/tmp/release-backup-upload"
		holdDeadline   = 3 * time.Minute
	)

	compose, err := docker.New().
		WithBackendFilesystem().
		WithWeaviate().
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS", "5").
		WithWeaviateEnv("WEAVIATE_TEST_BACKUP_UPLOAD_HOLD_FILE", uploadHoldFile).
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	container := compose.GetWeaviate().Container()

	helper.CreateClass(t, &models.Class{
		Class:      backupCollection,
		Vectorizer: "none",
		Properties: []*models.Property{{
			Name:     "text",
			DataType: []string{"text"},
		}},
	})
	defer helper.DeleteClass(t, backupCollection)

	// Writes during the backup only overwrite churnIDs, so the object count is
	// the same whether a write lands before or after the snapshot.
	churnIDs := make([]strfmt.UUID, compactions.ObjectsPerBatch)
	for i := range churnIDs {
		churnIDs[i] = strfmt.UUID(uuid.New().String())
	}
	compactions.ImportBatchWithIDs(t, backupCollection, churnIDs)
	for range 9 {
		compactions.ImportBatch(t, backupCollection)
	}

	shardName := getShardNameForClass(t, backupCollection)
	require.Eventually(t, func() bool {
		return compactions.TotalSegmentFileCount(ctx, container, backupCollection, shardName, "objects") >= 1
	}, 180*time.Second, time.Second, "not enough segments appeared")

	preBackupCount := moduleshelper.GetClassCount(t, backupCollection, "")

	_, err = helper.CreateBackup(t, helper.DefaultBackupConfig(), backupCollection, "filesystem", backupID)
	require.NoError(t, err)

	// The hardlinks are made while the shard is halted, so every segment listed
	// for the first time after this point was created during or after the halt.
	for stageDeadline := time.Now().Add(2 * time.Minute); ; time.Sleep(time.Second) {
		require.True(t, time.Now().Before(stageDeadline), "backup never staged a hard-linked snapshot")
		n, err := compactions.StagedSegmentCount(ctx, container, fmt.Sprintf("/data/%s*/%s/%s/lsm/objects",
			backup.BackupStagingPrefix, strings.ToLower(backupCollection), shardName))
		require.NoError(t, err)
		if n > 0 {
			break
		}
	}

	segments, err := compactions.SegmentNames(ctx, container, backupCollection, shardName, "objects")
	require.NoError(t, err)
	var watcher compactions.MergeWatcher
	watcher.Arm(segments)

	for holdEnd := time.Now().Add(holdDeadline); ; {
		require.True(t, time.Now().Before(holdEnd),
			"no segment created after the snapshot was compacted within %s while the backup was held", holdDeadline)
		time.Sleep(time.Second)

		compactions.ImportBatchWithIDs(t, backupCollection, churnIDs)

		segments, err := compactions.SegmentNames(ctx, container, backupCollection, shardName, "objects")
		require.NoError(t, err)
		merged := watcher.Observe(segments)

		// Read after the listing, so a merge it shows happened while the backup was open.
		status := backupStatus(t)
		require.NotContains(t, []string{"SUCCESS", "FAILED"}, status,
			"backup finished while held; upload hold not honoured")
		t.Logf("[poll] backup=%s segments=%d merged=%v", status, len(segments), merged)
		if merged {
			break
		}
	}

	code, _, err := container.Exec(ctx, []string{"touch", uploadHoldFile})
	require.NoError(t, err)
	require.Zero(t, code)

	for doneDeadline := time.Now().Add(5 * time.Minute); ; time.Sleep(time.Second) {
		require.True(t, time.Now().Before(doneDeadline), "backup did not complete after release")
		status := backupStatus(t)
		require.NotEqual(t, "FAILED", status, "backup failed")
		if status == "SUCCESS" {
			break
		}
	}

	helper.DeleteClass(t, backupCollection)

	_, err = helper.RestoreBackup(t, helper.DefaultRestoreConfig(), backupCollection, "filesystem", backupID, nil, false)
	require.NoError(t, err)
	helper.ExpectBackupEventuallyRestored(t, backupID, "filesystem", nil,
		helper.WithDeadline(2*time.Minute))

	restoredCount := moduleshelper.GetClassCount(t, backupCollection, "")
	require.Equal(t, preBackupCount, restoredCount,
		"restored object count should match pre-backup count")
}
