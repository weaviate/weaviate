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

// Sized so a change-tokenization migration stays live on CI long enough to land a call inside it.
const guardDataset = 50_000

func startGuardNode(ctx context.Context, t *testing.T) *docker.DockerCompose {
	t.Helper()
	compose, err := reindexhelpers.SingleNodeCompose().
		WithBackendFilesystem().
		Start(ctx)
	require.NoError(t, err)
	return compose
}

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

// An empty shardName is for a refusal that is collection-wide and never resolved one.
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

// An unrecognized status counts as live: the alternative reads a newer node's task as finished.
func liveReindexStatus(status string) bool {
	switch status {
	case "FINISHED", "CANCELLED", "FAILED":
		return false
	default:
		return true
	}
}

// slowBackupConfig widens the window for submitting a migration mid-backup; it does
// not open it, so pair it with awaitCapturedClassUploaded.
func slowBackupConfig() *models.BackupConfig {
	return &models.BackupConfig{
		CompressionLevel: models.BackupConfigCompressionLevelBestCompression,
		CPUPercentage:    1,
	}
}

// awaitCapturedClassUploaded blocks until the capture writes a class file, which only
// happens once the per-shard gate passed. Exit status alone: no directory is not done.
func awaitCapturedClassUploaded(t *testing.T, ctx context.Context, compose *docker.DockerCompose, backupID string) {
	t.Helper()
	dir := "/tmp/backups/" + backupID
	require.Eventually(t, func() bool {
		code, _, err := compose.GetWeaviate().Container().Exec(ctx,
			[]string{"sh", "-c", "find " + dir + " -name 'chunk-*' 2>/dev/null | grep -q ."})
		return err == nil && code == 0
	}, 90*time.Second, 250*time.Millisecond,
		"no chunk file appeared under "+dir+", so the capture never got past descriptors")
}

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
