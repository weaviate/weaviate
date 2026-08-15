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
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// guardedBackup is the state a test continues from once the gate has refused
// a submission. The backup may already be finished by then; only the refusal
// is guaranteed to have happened while it was live.
type guardedBackup struct {
	compose   *docker.DockerCompose
	restURI   string
	className string
	propName  string
	backupID  string
	backend   string
	blocked   reindexProbe
}

// The setup both single-node backup tests continue from: a slow capture, and a
// submission refused while that capture was live.
func proveReindexBlockedDuringBackup(
	ctx context.Context, t *testing.T, className, backupID string,
) guardedBackup {
	t.Helper()

	const (
		propName = "body"
		backend  = "filesystem"
	)

	compose := startGuardNode(ctx, t)
	t.Cleanup(func() { require.NoError(t, compose.Terminate(ctx)) })
	// Cleanups run last-registered-first, so this one still reaches a live
	// container.
	t.Cleanup(func() { dumpWeaviateLogs(ctx, t, compose.GetWeaviate().Container(), "weaviate") })

	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)
	t.Cleanup(helper.ResetClient)

	createBodyClass(t, className, propName)
	importBodies(t, className, guardDataset)

	_, err := helper.CreateBackup(t, slowBackupConfig(), className, backend, backupID)
	require.NoError(t, err, "the capture must be admitted: nothing is migrating yet")

	run := probeReindexDuringBackup(t, restURI, className, propName, "whitespace",
		localBackupStatus(t, backend, backupID), 5*time.Minute)

	return guardedBackup{
		compose:   compose,
		restURI:   restURI,
		className: className,
		propName:  propName,
		backupID:  backupID,
		backend:   backend,
		blocked:   assertReindexBlocked(t, run, backupID),
	}
}

// A submission is refused while this node captures, and admitted once the
// capture releases the slot. The second half is what makes the first mean
// anything — a gate that refused forever would pass on its own.
func TestReindexRefusedWhileBackupRuns(t *testing.T) {
	guarded := proveReindexBlockedDuringBackup(context.Background(), t,
		"ReindexGuard_BackupRunning", "reindex-guard-backup-running")
	t.Logf("reindex refused while backup %s was %s: %s",
		guarded.backupID, guarded.blocked.backupStatus, guarded.blocked.body)

	helper.ExpectBackupEventuallyCreated(t, guarded.backupID, guarded.backend, nil,
		helper.WithDeadline(5*time.Minute))

	// Polled rather than submitted once: see heldBackupSlotRefusal.
	successAt := time.Now()
	taskID := awaitReindexAccepted(t, guarded.restURI, guarded.className, guarded.propName,
		"whitespace", 30*time.Second)
	t.Logf("reindex accepted %s after backup %s reported SUCCESS: task %s",
		time.Since(successAt).Round(time.Millisecond), guarded.backupID, taskID)

	reindexhelpers.AwaitReindexLive(t, guarded.restURI, taskID,
		reindexhelpers.WithTimeout(60*time.Second))
	reindexhelpers.AwaitReindexViaIndexes(t, guarded.restURI, guarded.className, guarded.propName,
		"searchable", reindexhelpers.WithTimeout(180*time.Second))
}

// TestReindexBlockClearsAfterNodeCrash pins that the block lives in process
// memory only, so a node that dies mid-capture releases it with no operator
// action. One node makes it deterministic: a single restart clears both the
// coordinator slot and the participant slot.
func TestReindexBlockClearsAfterNodeCrash(t *testing.T) {
	ctx := context.Background()

	guarded := proveReindexBlockedDuringBackup(ctx, t,
		"ReindexGuard_CrashClearsBlock", "reindex-guard-crash-clears")
	t.Logf("the gate was engaged before the crash: %s", guarded.blocked.body)

	// A graceful shutdown would run Weaviate's own teardown, which may release
	// the slot deliberately — the opposite of what this test claims. A zero
	// grace period gives Docker nothing between SIGTERM and SIGKILL, so the
	// process dies before teardown can act.
	kill := time.Duration(0)
	require.NoError(t, guarded.compose.StopAt(ctx, 0, &kill))
	require.NoError(t, guarded.compose.StartAt(ctx, 0))

	// The dynamic port rebinds on restart.
	restURI := guarded.compose.GetWeaviate().URI()
	helper.SetupClient(restURI)

	awaitNodeServing(t, restURI, guarded.className, 120*time.Second)
	servingAt := time.Now()

	taskID := awaitReindexAccepted(t, restURI, guarded.className, guarded.propName,
		"whitespace", 60*time.Second)
	t.Logf("reindex accepted %s after the node started serving again, with no operator action: task %s",
		time.Since(servingAt).Round(time.Millisecond), taskID)

	reindexhelpers.AwaitReindexLive(t, restURI, taskID, reindexhelpers.WithTimeout(60*time.Second))
	reindexhelpers.AwaitReindexViaIndexes(t, restURI, guarded.className, guarded.propName,
		"searchable", reindexhelpers.WithTimeout(180*time.Second))

	// That 202 on its own is not proof the block cleared. The cluster fan-out
	// admits when the node list is empty, which is exactly the state a booting
	// node passes through, so a gate that never re-wired answers 202 too.
	// Re-arm and make the restarted process refuse again to tell the two apart.
	const rearmedBackupID = "reindex-guard-crash-rearmed"
	_, err := helper.CreateBackup(t, slowBackupConfig(), guarded.className, guarded.backend, rearmedBackupID)
	require.NoError(t, err, "the second capture must be admitted once the migration has finished")

	rearmed := probeReindexDuringBackup(t, restURI, guarded.className, guarded.propName, "word",
		localBackupStatus(t, guarded.backend, rearmedBackupID), 5*time.Minute)
	blocked := assertReindexBlocked(t, rearmed, rearmedBackupID)
	t.Logf("the restarted node's gate refuses again while backup %s is %s: %s",
		rearmedBackupID, blocked.backupStatus, blocked.body)
}

// The restore half of the same contract, on a DIFFERENT collection than the one
// restored: a restoring collection is not in the schema yet and would 404 before
// any gate ran, so the only coupling left is the node. That is the point — this
// gate is node-scoped, not collection-scoped.
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

	createBodyClass(t, restoredClass, propName)
	importBodies(t, restoredClass, guardDataset)

	// Not in the backup, so the restore cannot touch it.
	createBodyClass(t, migratingClass, propName)
	importBodies(t, migratingClass, 200)

	_, err := helper.CreateBackup(t, slowBackupConfig(), restoredClass, backend, backupID)
	require.NoError(t, err, "the capture must be admitted: nothing is migrating yet")
	helper.ExpectBackupEventuallyCreated(t, backupID, backend, nil,
		helper.WithDeadline(5*time.Minute))

	// Restoring over a live class is refused for its own reasons; drop it so
	// the restore under test is otherwise valid.
	helper.DeleteClass(t, restoredClass)

	_, err = helper.RestoreBackup(t, helper.DefaultRestoreConfig(), restoredClass, backend, backupID, nil, false)
	require.NoError(t, err, "the restore must be admitted: nothing is migrating yet")

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

	// The negative arm: the same submission has to be admitted once the restore
	// releases the node's slot.
	successAt := time.Now()
	taskID := awaitReindexAccepted(t, restURI, migratingClass, propName, "whitespace", 60*time.Second)
	t.Logf("reindex accepted %s after restore %s finished: task %s",
		time.Since(successAt).Round(time.Millisecond), backupID, taskID)

	reindexhelpers.AwaitReindexLive(t, restURI, taskID, reindexhelpers.WithTimeout(60*time.Second))
	reindexhelpers.AwaitReindexViaIndexes(t, restURI, migratingClass, propName,
		"searchable", reindexhelpers.WithTimeout(180*time.Second))
}
