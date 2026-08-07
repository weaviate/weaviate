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
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// TestReindexRefusedWhileRestoreRuns is the restore half of the guard's stated
// contract: a migration must not start while this node is part of a backup OR a
// restore. The backup half has its own tests; a restore reaches the same probe
// through a different slot, so a Handler wired with only a backupper would let
// every migration through with nothing to catch it.
//
// The migrating collection is not the one being restored, and cannot be: a
// collection under restore does not exist in the schema yet, so a submission
// against it would be answered by the 404 before any gate runs. The guard is
// node-scoped, which is what makes the two collections the right shape here.
func TestReindexRefusedWhileRestoreRuns(t *testing.T) {
	ctx := context.Background()

	compose := startGuardNode(ctx, t)
	t.Cleanup(func() { require.NoError(t, compose.Terminate(ctx)) })
	t.Cleanup(func() { dumpWeaviateLogs(ctx, t, compose.GetWeaviate().Container(), "weaviate") })

	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)
	t.Cleanup(helper.ResetClient)

	const (
		restoredClass  = "ReindexGuard_Restored"
		migratingClass = "ReindexGuard_MigratedDuringRestore"
		propName       = "body"
		backend        = "filesystem"
		backupID       = "reindex-guard-restore-running"
	)

	// 50k objects so the restore stays in flight for seconds, the same sizing
	// the backup-side guard tests use.
	createBodyClass(t, restoredClass, propName)
	importBodies(t, restoredClass, guardDataset)

	// The collection the migration targets. It is not in the backup, so the
	// restore cannot touch it and the two are only ever coupled by the node.
	createBodyClass(t, migratingClass, propName)
	importBodies(t, migratingClass, 200)

	_, err := helper.CreateBackup(t, slowBackupConfig(), restoredClass, backend, backupID)
	require.NoError(t, err, "backup create must be accepted with no reindex in flight")
	helper.ExpectBackupEventuallyCreated(t, backupID, backend, nil,
		helper.WithDeadline(5*time.Minute))

	// Restoring over a live class is refused for its own reasons; drop it so the
	// restore under test is otherwise valid.
	helper.DeleteClass(t, restoredClass)

	_, err = helper.RestoreBackup(t, helper.DefaultRestoreConfig(), restoredClass, backend, backupID, nil, false)
	require.NoError(t, err, "restore must be accepted: nothing is migrating yet")

	run := probeReindexDuringBackup(t, restURI, migratingClass, propName, "whitespace",
		localRestoreStatus(t, backend, backupID), 5*time.Minute)
	blocked := assertReindexBlocked(t, run, backupID)
	require.Containsf(t, guardMessage(blocked.body), "a restore is running in the cluster",
		"the refusal must name the restore that caused it, not a backup; got: %s", blocked.body)
	t.Logf("reindex refused while restore %s was %s: %s",
		backupID, blocked.backupStatus, blocked.body)

	helper.ExpectBackupEventuallyRestored(t, backupID, backend, nil,
		helper.WithDeadline(5*time.Minute))
	_, exists := reindexhelpers.FetchClass(restURI, restoredClass, true)
	require.Truef(t, exists, "the restore must bring %s back", restoredClass)

	// The refusal is transient: the same submission has to be admitted once the
	// restore releases the node's slot.
	successAt := time.Now()
	taskID := awaitReindexAccepted(t, restURI, migratingClass, propName, "whitespace", 60*time.Second)
	t.Logf("reindex accepted %s after restore %s finished: task %s",
		time.Since(successAt).Round(time.Millisecond), backupID, taskID)
	reindexhelpers.AwaitReindexLive(t, restURI, taskID, reindexhelpers.WithTimeout(60*time.Second))
	reindexhelpers.AwaitReindexViaIndexes(t, restURI, migratingClass, propName,
		"searchable", reindexhelpers.WithTimeout(180*time.Second))
}

// localRestoreStatus reads restore status via the process-global client, the
// restore-side twin of localBackupStatus.
func localRestoreStatus(t *testing.T, backend, backupID string) func() (string, bool) {
	return func() (string, bool) {
		resp, err := helper.RestoreBackupStatus(t, backend, backupID, "", "")
		if err != nil || resp == nil || resp.Payload == nil || resp.Payload.Status == nil {
			return "", false
		}
		return *resp.Payload.Status, true
	}
}
