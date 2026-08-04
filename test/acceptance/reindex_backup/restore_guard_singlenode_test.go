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

package reindex_backup_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientbackups "github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestRestoreRefusedDuringInFlightReindex pins that a restore is refused
// (422) by a live reindex on an unrelated collection — the gate is
// cluster-wide because the restored class has no local index of its own.
func TestRestoreRefusedDuringInFlightReindex(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithBackendFilesystem().
		WithWeaviate().
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		// Start on the legacy Map (WAND) strategy so the tokenization change
		// below has real migration work to do.
		WithWeaviateEnv("USE_INVERTED_SEARCHABLE", "false").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)
	defer helper.ResetClient()
	defer dumpWeaviateLogs(ctx, t, compose.GetWeaviate().Container(), "weaviate")

	const (
		restoredClass = "RestoreGuard_Payload"
		reindexClass  = "RestoreGuard_Migrating"
		backupID      = "restore-guard-refuse"
	)

	// Delete after backing up so the restore below is otherwise valid.
	helper.CreateClass(t, &models.Class{
		Class: restoredClass,
		Properties: []*models.Property{
			{Name: "body", DataType: []string{"text"}, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
	importBodies(t, restoredClass, 200)
	_, err = helper.CreateBackup(t, helper.DefaultBackupConfig(), restoredClass, "filesystem", backupID)
	require.NoError(t, err)
	helper.ExpectBackupEventuallyCreated(t, backupID, "filesystem", nil,
		helper.WithDeadline(2*time.Minute))
	helper.DeleteClass(t, restoredClass)

	// 50k objects keep the tokenization change live for several seconds, long
	// enough for the restore call below to land inside the window.
	helper.CreateClass(t, &models.Class{
		Class: reindexClass,
		Properties: []*models.Property{
			{Name: "body", DataType: []string{"text"}, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
	// Not deleted on the way out: the migration is still live at that point and
	// the mutation guard rejects the delete. The container goes away regardless.
	importBodies(t, reindexClass, 50_000)

	taskID := submitChangeTokenization(t, restURI, reindexClass, "body", "lowercase")
	t.Logf("change-tokenization task submitted: %s", taskID)
	reindexhelpers.AwaitReindexLive(t, restURI, taskID,
		reindexhelpers.WithTimeout(30*time.Second))

	statusBefore := reindexTaskStatus(t, restURI, taskID)
	_, restoreErr := helper.RestoreBackup(t, helper.DefaultRestoreConfig(),
		restoredClass, "filesystem", backupID, nil, false)
	statusAfter := reindexTaskStatus(t, restURI, taskID)

	// Judge the window before judging the verdict: a migration that drained
	// early would make any outcome below meaningless.
	require.Truef(t, liveReindexStatus(statusBefore) && liveReindexStatus(statusAfter),
		"reindex task %s must still be live on both sides of the restore attempt "+
			"(before=%q after=%q); grow the imported corpus until the migration "+
			"outlives the restore call", taskID, statusBefore, statusAfter)

	require.Error(t, restoreErr,
		"restore must be refused synchronously while a runtime-reindex is live")
	var refusal *clientbackups.BackupsRestoreUnprocessableEntity
	require.ErrorAsf(t, restoreErr, &refusal,
		"expected 422 BackupsRestoreUnprocessableEntity, got %T: %v", restoreErr, restoreErr)
	require.NotNil(t, refusal.Payload, "422 payload must not be nil")

	errMsg := errorResponseMessage(refusal.Payload)
	require.Contains(t, errMsg, "restore blocked: runtime-reindex in flight in the cluster",
		"error body must name the refused operation and the blocking condition; got: %s", errMsg)
	require.Contains(t, errMsg, "retry after the migration finishes",
		"error body must include an actionable next step; got: %s", errMsg)
	// Both words belong to the per-shard backup gate, not this cluster-wide one.
	require.NotContains(t, errMsg, "backup blocked",
		"a restore refusal must not be worded as a backup refusal; got: %s", errMsg)
	require.NotContains(t, errMsg, "this shard",
		"the gate is cluster-wide; no shard is involved; got: %s", errMsg)

	// The class must stay absent: a refused restore may not partially land.
	require.False(t, classExists(t, restURI, restoredClass),
		"a refused restore must not create %s", restoredClass)
}

// reindexTaskStatus reads one reindex task's DTM status.
func reindexTaskStatus(t *testing.T, restURI, taskID string) string {
	t.Helper()
	tasks, ok := reindexhelpers.TryFetchTasks(restURI)
	require.Truef(t, ok, "GET /v1/tasks failed while probing task %s", taskID)
	for _, task := range tasks["reindex"] {
		if task.ID == taskID {
			return task.Status
		}
	}
	t.Fatalf("reindex task %s is not listed by GET /v1/tasks", taskID)
	return ""
}

// liveReindexStatus mirrors db.IsLiveReindexTaskStatus for wire status strings.
func liveReindexStatus(status string) bool {
	switch status {
	case "STARTED", "PREPARING", "SWAPPING":
		return true
	default:
		return false
	}
}

func classExists(t *testing.T, restURI, className string) bool {
	t.Helper()
	_, ok := reindexhelpers.FetchClass(restURI, className, true)
	return ok
}
