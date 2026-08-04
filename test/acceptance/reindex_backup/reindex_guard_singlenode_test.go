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

// guardDataset sizes the corpus so the backup stays in flight for seconds,
// not milliseconds; matches the 50k precedent used elsewhere in this package.
const guardDataset = 50_000

// startGuardNode boots a single node with a filesystem backend.
// USE_INVERTED_SEARCHABLE=false forces the legacy Map strategy so the
// tokenization change has real work to do.
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

// TestReindexRefusedWhileBackupRuns asserts a reindex submission is refused
// with a 409 naming the running backup, then accepted once it finishes.
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

	// The refusal is transient: the same submission must succeed once the backup
	// finishes. Acceptance is polled since the slot releases just after SUCCESS.
	successAt := time.Now()
	taskID := awaitReindexAccepted(t, restURI, className, propName, "whitespace", 30*time.Second)
	t.Logf("reindex accepted %s after backup %s reported SUCCESS: task %s",
		time.Since(successAt).Round(time.Millisecond), backupID, taskID)
	reindexhelpers.AwaitReindexLive(t, restURI, taskID,
		reindexhelpers.WithTimeout(60*time.Second))
	reindexhelpers.AwaitReindexViaIndexes(t, restURI, className, propName, "searchable",
		reindexhelpers.WithTimeout(180*time.Second))
}

// TestReindexBlockClearsAfterNodeCrash asserts the block lives in process
// memory only: killing the node mid-backup releases it with no operator
// action. A single node makes this deterministic (one restart clears both
// the coordinator and the participant slot).
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

	// The dynamic port rebinds on restart, so the URI must be re-resolved before use.
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
