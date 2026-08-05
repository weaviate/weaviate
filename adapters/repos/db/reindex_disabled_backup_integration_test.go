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

//go:build integrationTest

package db

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/schema"
)

// TestBackup_RuntimeReindexDisabled_MakesNoGateCalls pins the kill-switch
// contract for the backup path: with runtime reindex off, a backup must
// succeed and must not consult the reindex activity lookup at all.
//
// The installed lookup reports every shard as having a live reindex, so
// without the gate the backup fails outright and the call counter is
// non-zero.
func TestBackup_RuntimeReindexDisabled_MakesNoGateCalls(t *testing.T) {
	ctx := testCtx()
	className := "ReindexDisabledBackupClass"

	db := setupTestDBWithConfig(t, t.TempDir(), func(cfg *Config) {
		cfg.RuntimeReindexDisabled = true
	}, makeTestClass(className))
	defer func() {
		require.Nil(t, db.Shutdown(context.Background()))
	}()

	var lookupCalls atomic.Int64
	db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
		lookupCalls.Add(1)
		return func(string, string) bool { return true }
	})
	db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup {
		lookupCalls.Add(1)
		return func(string, string) bool { return true }
	})

	classes := []string{className}
	require.NoError(t, db.Backupable(ctx, classes),
		"backup precheck must pass with runtime reindex disabled")

	for d := range db.BackupDescriptors(ctx, "reindex-disabled-backup", classes, nil) {
		require.NoError(t, d.Error, "backup descriptor must not carry a reindex refusal")
	}

	require.Zero(t, lookupCalls.Load(),
		"backup path must make no reindex lookup with runtime reindex disabled")

	// The gate is reachable via the shard-level entry point too.
	idx := db.GetIndex(schema.ClassName(className))
	require.NotNil(t, idx)
	require.NoError(t, idx.refuseIfReindexInFlight("any-shard"))
	require.Zero(t, lookupCalls.Load())
}
