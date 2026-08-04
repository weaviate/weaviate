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
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// guardDataset sizes the corpus so a single-goroutine best-compression backup
// stays in flight for seconds rather than milliseconds. Matches the 50k
// precedent the rest of this package uses for multi-second windows.
const guardDataset = 50_000

// startGuardNode boots a single node with a filesystem backup backend.
// USE_INVERTED_SEARCHABLE=false starts classes on the legacy Map strategy so
// the tokenization change has real work to do.
func startGuardNode(ctx context.Context, t *testing.T) *docker.DockerCompose {
	t.Helper()
	compose, err := docker.New().
		WithBackendFilesystem().
		WithWeaviate().
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		WithWeaviateEnv("USE_INVERTED_SEARCHABLE", "false").
		Start(ctx)
	require.NoError(t, err)
	return compose
}

// TestReindexRefusedWhileBackupRuns asserts that a runtime-reindex submission
// is refused with a 409 naming the running backup, and is accepted once that
// backup finishes.
func TestReindexRefusedWhileBackupRuns(t *testing.T) {
	ctx := context.Background()

	const (
		className = "ReindexGuard_BackupRunning"
		propName  = "body"
		backupID  = "reindex-guard-backup-running"
		backend   = "filesystem"
	)

	compose := startGuardNode(ctx, t)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()
	defer func() { dumpWeaviateLogs(ctx, t, compose.GetWeaviate().Container(), "weaviate") }()

	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)
	defer helper.ResetClient()

	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: propName, DataType: []string{"text"}, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
	importBodies(t, className, guardDataset)

	_, err := helper.CreateBackup(t, slowBackupConfig(), className, backend, backupID)
	require.NoError(t, err, "backup create must be accepted with no reindex in flight")

	statusOf := localBackupStatus(t, backend, backupID)
	run := probeReindexDuringBackup(t, restURI, className, propName, "whitespace",
		statusOf, 5*time.Minute)
	blocked := assertReindexBlocked(t, run, backupID)
	t.Logf("reindex refused while backup %s was %s: %s",
		backupID, blocked.backupStatus, blocked.body)

	helper.ExpectBackupEventuallyCreated(t, backupID, backend, nil,
		helper.WithDeadline(5*time.Minute))

	// The refusal must be transient: the same submission has to go through once
	// the backup is done, and the task it returns has to actually run. The
	// coordinator releases its slot just after it reports SUCCESS, so the
	// acceptance is polled rather than demanded on the first try.
	successAt := time.Now()
	taskID := awaitReindexAccepted(t, restURI, className, propName, "whitespace", 30*time.Second)
	t.Logf("reindex accepted %s after backup %s reported SUCCESS: task %s",
		time.Since(successAt).Round(time.Millisecond), backupID, taskID)
	reindexhelpers.AwaitReindexLive(t, restURI, taskID,
		reindexhelpers.WithTimeout(60*time.Second))
	reindexhelpers.AwaitReindexViaIndexes(t, restURI, className, propName, "searchable",
		reindexhelpers.WithTimeout(180*time.Second))
}

// TestReindexBlockClearsAfterNodeCrash asserts that the block a backup puts on
// runtime-reindex lives in process memory only: killing the node mid-backup has
// to release it with no operator action, no cancel call and no cleanup.
//
// A single node makes this deterministic, because one restart takes out the
// coordinator and the participant slot at the same time.
func TestReindexBlockClearsAfterNodeCrash(t *testing.T) {
	ctx := context.Background()

	const (
		className = "ReindexGuard_CrashClearsBlock"
		propName  = "body"
		backupID  = "reindex-guard-crash-clears"
		backend   = "filesystem"
	)

	compose := startGuardNode(ctx, t)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()
	defer func() { dumpWeaviateLogs(ctx, t, compose.GetWeaviate().Container(), "weaviate") }()

	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)
	defer helper.ResetClient()

	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: propName, DataType: []string{"text"}, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
	importBodies(t, className, guardDataset)

	_, err := helper.CreateBackup(t, slowBackupConfig(), className, backend, backupID)
	require.NoError(t, err, "backup create must be accepted with no reindex in flight")

	statusOf := localBackupStatus(t, backend, backupID)
	run := probeReindexDuringBackup(t, restURI, className, propName, "whitespace",
		statusOf, 5*time.Minute)
	blocked := assertReindexBlocked(t, run, backupID)
	t.Logf("guard engaged before the crash: %s", blocked.body)

	require.NoError(t, compose.StopAt(ctx, 0, nil))
	require.NoError(t, compose.StartAt(ctx, 0))

	// The dynamic port rebinds on restart, so every address has to be resolved
	// again before it is used.
	restURI = compose.GetWeaviate().URI()
	helper.SetupClient(restURI)

	awaitNodeServing(t, restURI, className, 120*time.Second)
	servingAt := time.Now()

	taskID := awaitReindexAccepted(t, restURI, className, propName, "whitespace", 60*time.Second)
	t.Logf("reindex accepted %s after the node started serving again, with no operator action: task %s",
		time.Since(servingAt).Round(time.Millisecond), taskID)

	reindexhelpers.AwaitReindexLive(t, restURI, taskID,
		reindexhelpers.WithTimeout(60*time.Second))
}
