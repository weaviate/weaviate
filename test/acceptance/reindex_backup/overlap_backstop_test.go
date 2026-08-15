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

// TestBackupFailsWhenAMigrationRanThroughItsCapture proves the commit-time
// check end to end: a migration submitted after the per-shard gate has
// already admitted the capture is invisible to that gate, and the backup
// must not be published.
func TestBackupFailsWhenAMigrationRanThroughItsCapture(t *testing.T) {
	ctx := context.Background()

	compose := startGuardNode(ctx, t)
	t.Cleanup(func() { require.NoError(t, compose.Terminate(ctx)) })

	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)
	t.Cleanup(helper.ResetClient)

	const (
		className = "OverlapBackstop_Spanned"
		backend   = "filesystem"
		backupID  = "overlap-backstop"
	)

	createBodyClass(t, className, "body")
	importBodies(t, className, guardDataset)

	_, err := helper.CreateBackup(t, slowBackupConfig(), className, backend, backupID)
	require.NoError(t, err, "the capture must be admitted: nothing is migrating yet")

	taskID := submitChangeTokenization(t, restURI, className, "body", "lowercase")
	t.Logf("change-tokenization task submitted mid-capture: %s", taskID)
	reindexhelpers.AwaitReindexLive(t, restURI, taskID,
		reindexhelpers.WithTimeout(30*time.Second))

	status, reason := awaitBackupTerminal(t, backend, backupID, 5*time.Minute)

	require.Equalf(t, string(entitiesbackup.Failed), status,
		"a capture a migration ran through must not be published as good (reason=%q)", reason)
	// The per-shard gate can also refuse this backup, and its text also says
	// FAILED, runtime-reindex and the collection. Only the commit-time check
	// says "overlapped this backup", so that is what proves which one fired.
	require.Contains(t, reason, entitiesbackup.ErrReindexOverlappedBackup.Error(),
		"the commit-time check has to be what failed this backup; got: %s", reason)
	require.Contains(t, reason, className,
		"the recorded reason must name the collection; got: %s", reason)
	shardName := reindexhelpers.GetFirstShardName(t, restURI, className)
	require.NotContains(t, reason, shardName,
		"the status API must not name a shard; got: %s", reason)
}

// TestBackupSucceedsWhenNoMigrationTouchesTheCapture is the negative half.
// Without it the test above passes on a commit-time check that fails
// everything.
func TestBackupSucceedsWhenNoMigrationTouchesTheCapture(t *testing.T) {
	ctx := context.Background()

	compose := startGuardNode(ctx, t)
	t.Cleanup(func() { require.NoError(t, compose.Terminate(ctx)) })

	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)
	t.Cleanup(helper.ResetClient)

	const (
		capturedClass  = "OverlapBackstop_Captured"
		migratingClass = "OverlapBackstop_Elsewhere"
		backend        = "filesystem"
		backupID       = "overlap-backstop-clean"
	)

	createBodyClass(t, capturedClass, "body")
	importBodies(t, capturedClass, guardDataset)
	createBodyClass(t, migratingClass, "body")
	// Same size as the captured class: AwaitReindexLive below fails a task
	// that finishes before it looks, and a small class finishes at once.
	importBodies(t, migratingClass, guardDataset)

	_, err := helper.CreateBackup(t, slowBackupConfig(), capturedClass, backend, backupID)
	require.NoError(t, err)

	taskID := submitChangeTokenization(t, restURI, migratingClass, "body", "lowercase")
	reindexhelpers.AwaitReindexLive(t, restURI, taskID,
		reindexhelpers.WithTimeout(30*time.Second))

	status, reason := awaitBackupTerminal(t, backend, backupID, 5*time.Minute)
	require.Equalf(t, string(entitiesbackup.Success), status,
		"a migration on a collection this backup never captured must not fail it (reason=%q)", reason)
}
