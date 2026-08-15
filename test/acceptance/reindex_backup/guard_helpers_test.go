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
