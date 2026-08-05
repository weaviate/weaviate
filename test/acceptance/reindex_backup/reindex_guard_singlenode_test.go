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

// guardDataset sizes the corpus so the backup stays in flight for seconds,
// not milliseconds; matches the 50k precedent used elsewhere in this package.
const guardDataset = 50_000

// startGuardNode boots a single node with a filesystem backend.
// USE_INVERTED_SEARCHABLE=false forces the legacy Map strategy so the
// tokenization change has real work to do.
func startGuardNode(ctx context.Context, t *testing.T) *docker.DockerCompose {
	t.Helper()
	compose, err := reindexhelpers.WithReindexEnv(
		docker.New().
			WithBackendFilesystem().
			WithWeaviate(),
	).Start(ctx)
	require.NoError(t, err)
	return compose
}

// guardedBackup is the state both single-node tests continue from once the
// guard has answered 409.
type guardedBackup struct {
	compose   *docker.DockerCompose
	restURI   string
	className string
	propName  string
	backupID  string
	backend   string
	blocked   reindexProbe
}

// proveReindexBlockedDuringBackup shares single-node guard setup for both
// tests below. The backup may already be done by return; only the guard
// probe is guaranteed to have run while it was live.
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
	// t.Cleanup runs last-registered-first, so the dump registered here still
	// reaches a live container.
	t.Cleanup(func() { dumpWeaviateLogs(ctx, t, compose.GetWeaviate().Container(), "weaviate") })

	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)
	t.Cleanup(helper.ResetClient)

	createBodyClass(t, className, propName)
	importBodies(t, className, guardDataset)

	_, err := helper.CreateBackup(t, slowBackupConfig(), className, backend, backupID)
	require.NoError(t, err, "backup create must be accepted with no reindex in flight")

	statusOf := localBackupStatus(t, backend, backupID)
	run := probeReindexDuringBackup(t, restURI, className, propName, "whitespace",
		statusOf, 5*time.Minute)

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

// TestReindexRefusedWhileBackupRuns asserts a reindex submission is refused
// with a 409 naming the running backup, then accepted once it finishes.
func TestReindexRefusedWhileBackupRuns(t *testing.T) {
	guarded := proveReindexBlockedDuringBackup(context.Background(), t,
		"ReindexGuard_BackupRunning", "reindex-guard-backup-running")
	t.Logf("reindex refused while backup %s was %s: %s",
		guarded.backupID, guarded.blocked.backupStatus, guarded.blocked.body)

	helper.ExpectBackupEventuallyCreated(t, guarded.backupID, guarded.backend, nil,
		helper.WithDeadline(5*time.Minute))

	// The refusal is transient: the same submission must succeed once the backup
	// finishes. Acceptance is polled since the slot releases just after SUCCESS.
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

// TestReindexBlockClearsAfterNodeCrash asserts the block lives in process
// memory only: killing the node mid-backup releases it with no operator
// action. A single node makes this deterministic (one restart clears both
// the coordinator and the participant slot).
func TestReindexBlockClearsAfterNodeCrash(t *testing.T) {
	ctx := context.Background()

	guarded := proveReindexBlockedDuringBackup(ctx, t,
		"ReindexGuard_CrashClearsBlock", "reindex-guard-crash-clears")
	t.Logf("guard engaged before the crash: %s", guarded.blocked.body)

	require.NoError(t, guarded.compose.StopAt(ctx, 0, nil))
	require.NoError(t, guarded.compose.StartAt(ctx, 0))

	// The dynamic port rebinds on restart, so the URI must be re-resolved before use.
	restURI := guarded.compose.GetWeaviate().URI()
	helper.SetupClient(restURI)

	awaitNodeServing(t, restURI, guarded.className, 120*time.Second)
	servingAt := time.Now()

	taskID := awaitReindexAccepted(t, restURI, guarded.className, guarded.propName,
		"whitespace", 60*time.Second)
	t.Logf("reindex accepted %s after the node started serving again, with no operator action: task %s",
		time.Since(servingAt).Round(time.Millisecond), taskID)

	reindexhelpers.AwaitReindexLive(t, restURI, taskID,
		reindexhelpers.WithTimeout(60*time.Second))
}
