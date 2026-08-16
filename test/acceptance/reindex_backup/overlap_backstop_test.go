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

	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// TestOverlapBackstop proves end to end that a migration the per-shard gate
// never saw still fails the backup, and that one on a collection the backup
// did not capture leaves it alone - without the second case the first passes
// on a check that fails everything.
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
				// Same size as the captured class: AwaitReindexLive below fails
				// a task that finishes before it looks, and a small class
				// finishes at once.
				importBodies(t, migrating, guardDataset)
			}

			_, err := helper.CreateBackup(t, slowBackupConfig(), tt.capturedClass, backend, tt.backupID)
			require.NoError(t, err, "the capture must be admitted: nothing is migrating yet")

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
			// The per-shard gate can also refuse this backup, and its text also
			// says FAILED, runtime-reindex and the collection. Only the
			// commit-time check says "overlapped this backup", so that is what
			// proves which one fired.
			require.Contains(t, reason, entitiesbackup.ErrReindexOverlappedBackup.Error(),
				"the commit-time check has to be what failed this backup; got: %s", reason)
			require.Contains(t, reason, tt.capturedClass,
				"the recorded reason must name the collection; got: %s", reason)
		})
	}
}
