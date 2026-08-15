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
	"github.com/weaviate/weaviate/client/nodes"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// guardDataset is sized so a change-tokenization migration over it stays
// live on CI long enough to land a backup or a restore inside.
const guardDataset = 50_000

func startGuardNode(ctx context.Context, t *testing.T) *docker.DockerCompose {
	t.Helper()
	compose, err := reindexhelpers.SingleNodeCompose().
		WithBackendFilesystem().
		Start(ctx)
	require.NoError(t, err)
	return compose
}

// createBodyClass creates a single word-tokenized text property, which is
// what a change-tokenization migration needs to have work to do.
func createBodyClass(t *testing.T, className, propName string) {
	t.Helper()
	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: propName, DataType: []string{"text"}, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
}

// createBackupOf backs up several collections in one operation, which the
// single-class helpers in test/helper cannot express.
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
	require.NoError(t, err, "backup create must be accepted with no migration in flight")
	helper.ExpectBackupEventuallyCreated(t, backupID, backend, nil,
		helper.WithDeadline(2*time.Minute))
}

// restoreClasses restores a named subset of a backup and returns the error
// unjudged: both admitted and refused are legitimate answers to ask for.
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

// requireNoPlacement asserts a published refusal names neither the shard nor
// the node the work is on. The resolved shard name is matched too: a leak
// worded without the quote passes the format assertion above it. An empty
// shardName is for a refusal that is collection-wide and never resolved one.
func requireNoPlacement(t *testing.T, msg, shardName string) {
	t.Helper()
	require.NotContainsf(t, msg, `shard "`, "a refusal names no shard; got: %s", msg)
	if shardName != "" {
		require.NotContainsf(t, msg, shardName, "a refusal names no shard; got: %s", msg)
	}
	clusterNodes, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
	require.NoError(t, err)
	for _, node := range clusterNodes.Payload.Nodes {
		require.NotContainsf(t, msg, node.Name, "a refusal names no node; got: %s", msg)
	}
}

// reindexTaskStatus reads a task's DTM status, so a test can prove the
// migration was still live on both sides of the window it tested.
func reindexTaskStatus(t *testing.T, restURI, taskID string) string {
	t.Helper()
	tasks, ok := reindexhelpers.TryFetchTasks(restURI)
	require.True(t, ok, "tasks endpoint must answer")
	for _, namespaced := range tasks {
		for _, task := range namespaced {
			if task.ID == taskID {
				return task.Status
			}
		}
	}
	t.Fatalf("task %s not found in /v1/tasks", taskID)
	return ""
}

// liveReindexStatus mirrors the server's own liveness predicate: a status
// this build does not recognize counts as live, because the alternative
// reads a newer node's migration as finished.
func liveReindexStatus(status string) bool {
	switch status {
	case "FINISHED", "CANCELLED", "FAILED":
		return false
	default:
		return true
	}
}

// slowBackupConfig throttles the upload so a test has time to submit a
// migration before the backup commits. It does not slow the snapshot, so it
// does not decide which of the two guards sees the migration first.
func slowBackupConfig() *models.BackupConfig {
	return &models.BackupConfig{
		CompressionLevel: models.BackupConfigCompressionLevelBestCompression,
		CPUPercentage:    1,
	}
}

// awaitBackupTerminal polls a backup until it stops moving, returning the
// terminal status and the reason recorded with it.
func awaitBackupTerminal(t *testing.T, backend, backupID string, deadline time.Duration) (string, string) {
	t.Helper()
	var status, reason string
	require.Eventually(t, func() bool {
		resp, err := helper.CreateBackupStatus(t, backend, backupID, "", "")
		if err != nil || resp == nil || resp.Payload == nil || resp.Payload.Status == nil {
			return false
		}
		status = string(*resp.Payload.Status)
		reason = resp.Payload.Error
		return status == string(entitiesbackup.Success) ||
			status == string(entitiesbackup.Failed) ||
			status == string(entitiesbackup.Cancelled)
	}, deadline, 200*time.Millisecond,
		"backup %s never reached a terminal status (last=%q reason=%q)", backupID, status, reason)
	return status, reason
}
