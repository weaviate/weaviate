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
	"github.com/weaviate/weaviate/test/helper"
)

// TestRestoreRefusedDuringInFlightReindex pins both halves of the restore
// gate's contract while a migration is live: a restore that INCLUDES the
// migrating collection is refused synchronously with 422, and a restore of
// any OTHER collection is admitted and completes.
//
// The check is cluster-wide but scoped by collection. Cluster-wide because a
// restoring class has no local index, so no per-shard lookup could ever see
// the task; scoped because a migration can run for days, and answering blind
// would refuse every restore of every collection for that whole time.
//
// Both arms are load-bearing. Without the second one, dropping the scoping
// again would leave this test green.
//
// The contract's third arm — a live task whose payload names no collection at
// all refuses every restore — needs a payload no API can write, so it stays at
// the unit tier in TestAnyReindexActivityLookupScopesUndecodablePayloads.
func TestRestoreRefusedDuringInFlightReindex(t *testing.T) {
	ctx := context.Background()

	compose := startGuardNode(ctx, t)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)
	defer helper.ResetClient()
	defer dumpWeaviateLogs(ctx, t, compose.GetWeaviate().Container(), "weaviate")

	const (
		unrelatedClass = "RestoreGuard_Payload"
		reindexClass   = "RestoreGuard_Migrating"
		backend        = "filesystem"
		backupID       = "restore-guard-refuse"
	)

	createBodyClass(t, unrelatedClass, "body")
	importBodies(t, unrelatedClass, 200)

	// 50k objects keep the tokenization change live for several seconds, long
	// enough for both restore calls below to land inside the window.
	createBodyClass(t, reindexClass, "body")
	// Not deleted on the way out: the migration is still live at that point and
	// the mutation guard rejects the delete. The container goes away regardless.
	importBodies(t, reindexClass, 50_000)

	// One backup holding both, so each arm below names a backup that really
	// contains what it asks to restore.
	createBackupOf(t, backend, backupID, unrelatedClass, reindexClass)
	// Deleted after the backup so the cross-collection restore is otherwise
	// valid. The migrating class has to stay: it is what the reindex runs on.
	helper.DeleteClass(t, unrelatedClass)

	taskID := submitChangeTokenization(t, restURI, reindexClass, "body", "lowercase")
	t.Logf("change-tokenization task submitted: %s", taskID)
	reindexhelpers.AwaitReindexLive(t, restURI, taskID,
		reindexhelpers.WithTimeout(30*time.Second))

	statusBefore := reindexTaskStatus(t, restURI, taskID)
	sameCollectionErr := restoreClasses(t, backend, backupID, unrelatedClass, reindexClass)
	crossCollectionErr := restoreClasses(t, backend, backupID, unrelatedClass)
	statusAfter := reindexTaskStatus(t, restURI, taskID)

	// Judge the window before judging the verdicts: a migration that drained
	// early would make any outcome below meaningless.
	require.Truef(t, liveReindexStatus(statusBefore) && liveReindexStatus(statusAfter),
		"reindex task %s must still be live on both sides of the restore attempts "+
			"(before=%q after=%q); grow the imported corpus until the migration "+
			"outlives both restore calls", taskID, statusBefore, statusAfter)

	// Arm 1: the restore names the migrating collection, so it is refused.
	require.Error(t, sameCollectionErr,
		"a restore including the migrating collection must be refused synchronously")
	var refusal *clientbackups.BackupsRestoreUnprocessableEntity
	require.ErrorAsf(t, sameCollectionErr, &refusal,
		"expected 422 BackupsRestoreUnprocessableEntity, got %T: %v", sameCollectionErr, sameCollectionErr)
	require.NotNil(t, refusal.Payload, "422 payload must not be nil")

	errMsg := errorResponseMessage(refusal.Payload)
	require.Contains(t, errMsg, "restore blocked: runtime-reindex in flight in the cluster",
		"error body must name the refused operation and the blocking condition; got: %s", errMsg)
	require.Contains(t, errMsg, "retry after the migration finishes",
		"error body must include an actionable next step; got: %s", errMsg)
	// "backup blocked" is the per-shard backup gate's own prefix
	// (ErrBackupBlockedByInFlightReindex); seeing it here means the two gates'
	// errors got crossed.
	require.NotContains(t, errMsg, "backup blocked",
		"a restore refusal must not be worded as a backup refusal; got: %s", errMsg)

	// A refused restore may not partially land.
	_, exists := reindexhelpers.FetchClass(restURI, unrelatedClass, true)
	require.Falsef(t, exists, "a refused restore must not create %s", unrelatedClass)

	// Arm 2: same migration, same moment, a collection it does not name. This
	// is what pins the scoping — it passes the coordinator's gate AND every
	// participant's, since the restore's class list reaches both.
	require.NoErrorf(t, crossCollectionErr,
		"a restore of %s must be admitted while the migration on %s is live; "+
			"refusing it is the cluster-wide gate this scoping removed",
		unrelatedClass, reindexClass)
	helper.ExpectBackupEventuallyRestored(t, backupID, backend, nil,
		helper.WithDeadline(2*time.Minute))
	_, exists = reindexhelpers.FetchClass(restURI, unrelatedClass, true)
	require.Truef(t, exists, "the admitted restore must bring %s back", unrelatedClass)
}

// createBackupOf backs up several classes into one backup and waits for it to
// report SUCCESS. helper.CreateBackup only takes a single class.
func createBackupOf(t *testing.T, backend, backupID string, classes ...string) {
	t.Helper()
	params := clientbackups.NewBackupsCreateParams().
		WithBackend(backend).
		WithBody(&models.BackupCreateRequest{
			ID:      backupID,
			Include: classes,
			Config:  helper.DefaultBackupConfig(),
		})
	_, err := helper.Client(t).Backups.BackupsCreate(params, nil)
	require.NoError(t, err)
	helper.ExpectBackupEventuallyCreated(t, backupID, backend, nil,
		helper.WithDeadline(3*time.Minute))
}

// restoreClasses issues one restore naming several classes and returns the
// synchronous outcome. helper.RestoreBackup only takes a single class.
func restoreClasses(t *testing.T, backend, backupID string, classes ...string) error {
	t.Helper()
	params := clientbackups.NewBackupsRestoreParams().
		WithBackend(backend).
		WithID(backupID).
		WithBody(&models.BackupRestoreRequest{
			Include: classes,
			Config:  helper.DefaultRestoreConfig(),
		})
	_, err := helper.Client(t).Backups.BackupsRestore(params, nil)
	return err
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
