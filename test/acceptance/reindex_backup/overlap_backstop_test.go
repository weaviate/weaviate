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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clientbackups "github.com/weaviate/weaviate/client/backups"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// End to end: a migration the gate never saw fails the backup, one on an uncaptured
// collection does not - without the second row the first passes on a check failing all.
func TestOverlapBackstop(t *testing.T) {
	const backend = "filesystem"

	tests := []struct {
		name           string
		capturedClass  string
		migratingClass string
		backupID       string
		wantStatus     entitiesbackup.Status
	}{
		{
			name:          "a migration that ran through the capture fails it",
			capturedClass: "OverlapBackstop_Overlapped",
			backupID:      "overlap-backstop",
			wantStatus:    entitiesbackup.Failed,
		},
		{
			name:           "a migration on a collection this backup never captured does not",
			capturedClass:  "OverlapBackstop_Captured",
			migratingClass: "OverlapBackstop_Elsewhere",
			backupID:       "overlap-backstop-clean",
			wantStatus:     entitiesbackup.Success,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			compose := startGuardNode(ctx, t)
			t.Cleanup(func() { require.NoError(t, compose.Terminate(ctx)) })

			restURI := compose.GetWeaviate().URI()
			helper.SetupClient(restURI)
			t.Cleanup(helper.ResetClient)

			migrating := tt.capturedClass
			createBodyClass(t, tt.capturedClass, "body")
			importBodies(t, tt.capturedClass, guardDataset)
			if tt.migratingClass != "" {
				migrating = tt.migratingClass
				createBodyClass(t, migrating, "body")
				// Same size, or the migration ends before AwaitReindexLive sees it.
				importBodies(t, migrating, guardDataset)
			}

			_, err := helper.CreateBackup(t, slowBackupConfig(), tt.capturedClass, backend, tt.backupID)
			require.NoError(t, err, "the capture must be admitted: nothing is migrating yet")
			awaitCapturedClassUploaded(t, ctx, compose, tt.backupID)

			taskID := submitChangeTokenization(t, restURI, migrating, "body", "lowercase")
			reindexhelpers.AwaitReindexLive(t, restURI, taskID,
				reindexhelpers.WithTimeout(30*time.Second))

			status, reason := awaitBackupTerminal(t, backend, tt.backupID, 5*time.Minute)

			require.Equalf(t, string(tt.wantStatus), status,
				"captured %q while %q was migrating (reason=%q)",
				tt.capturedClass, migrating, reason)
			if tt.wantStatus != entitiesbackup.Failed {
				return
			}
			// The per-shard gate refuses with near-identical text; only the
			// commit-time check names this sentinel.
			require.Contains(t, reason, entitiesbackup.ErrReindexOverlappedBackup.Error(),
				"the commit-time check has to be what failed this backup; got: %s", reason)
			require.Contains(t, reason, tt.capturedClass,
				"the recorded reason must name the collection; got: %s", reason)
		})
	}
}

// A zero retention window is refused at admission for both include shapes, which
// authorize at different points, so only canCommit sees both.
func TestBackupRefusedWhenOverlapCheckCannotAnswer(t *testing.T) {
	ctx := context.Background()
	compose, err := reindexhelpers.SingleNodeCompose().
		WithWeaviateEnv("DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS", "0").
		WithBackendFilesystem().
		Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, compose.Terminate(ctx)) })
	helper.SetupClient(compose.GetWeaviate().URI())
	t.Cleanup(helper.ResetClient)

	const className = "Unanswerable_Class"
	createBodyClass(t, className, "body")

	for name, include := range map[string][]string{
		"an explicit include": {className},
		"an empty include":    nil,
	} {
		t.Run(name, func(t *testing.T) {
			params := clientbackups.NewBackupsCreateParams().
				WithBackend("filesystem").
				WithBody(&models.BackupCreateRequest{
					ID:      "unanswerable-" + strings.ReplaceAll(name, " ", "-"),
					Include: include, Config: helper.DefaultBackupConfig(),
				})
			_, err := helper.Client(t).Backups.BackupsCreate(params, nil)

			require.Error(t, err, "a window that retains nothing can never clear a capture")
			var refused *clientbackups.BackupsCreateUnprocessableEntity
			require.ErrorAs(t, err, &refused, "a refusal the operator can act on is 422; got %v", err)
			msg := errorResponseMessage(refused.Payload)
			require.Contains(t, msg, entitiesbackup.ErrReindexOverlapCheckUnanswerable.Error())
			require.Contains(t, msg, "DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS",
				"the only answer the operator gets owes them the setting to change")
			requireNoPlacement(t, msg, "")
		})
	}
}
