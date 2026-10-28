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

package db

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
)

// An inactive shard is read from disk and must neither reach the active halt path nor be loaded.
func TestBackupShardWithoutHardlinks_InactiveShardStaysOnDisk(t *testing.T) {
	cases := []struct {
		name        string
		dropFromMap bool
	}{
		{name: "cold lazy shard in the map"},
		{name: "shard on disk but not in the map", dropFromMap: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			f := newAddPropertyLazyFixture(t, "LegacyBackupInactive", singleShardState())
			var name string
			var lazy *LazyLoadShard
			for n, l := range f.coldShards(t) {
				name, lazy = n, l
			}
			_, release, err := f.index.getOrInitShard(ctx, name)
			release()
			require.NoError(t, err)
			if tc.dropFromMap {
				_, err = f.index.UnloadLocalShard(ctx, name)
			} else {
				err = lazy.Shutdown(ctx)
			}
			require.NoError(t, err)
			_, err = os.Stat(shardPath(f.index.path(), name))
			require.NoError(t, err)

			t.Cleanup(func() { require.NoError(t, f.index.ReleaseBackup(ctx, "legacy-inactive")) })
			sd, err := f.index.backupShardWithoutHardlinks(ctx, name, nil)
			require.NoError(t, err)
			require.NotNil(t, sd)
			require.NotEmpty(t, sd.Files)
			require.False(t, lazy.isLoaded())
			_, protected := f.index.backupProtectedShards.Load(name)
			require.True(t, protected)
		})
	}
}

// An unload between the create-lock release and the halt must not leave the backup halting an orphan rebuilt off the map.
func TestBackupShardWithoutHardlinks_UnloadBeforeHaltLeavesNoOrphan(t *testing.T) {
	ctx := testCtx()
	f := newAddPropertyLazyFixture(t, "LegacyBackupUnloadRace", singleShardState())
	var name string
	for n := range f.coldShards(t) {
		name = n
	}
	_, release, err := f.index.getOrInitShard(ctx, name)
	release()
	require.NoError(t, err)

	var unloadOutcome ShardUnloadOutcome
	f.index.legacyBackupBeforeHalt = func() {
		unloadCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		unloadOutcome, _ = f.index.UnloadLocalShard(unloadCtx, name)
	}
	sd, err := f.index.backupShardWithoutHardlinks(ctx, name, nil)
	f.index.legacyBackupBeforeHalt = nil
	require.NoError(t, err)
	require.NotNil(t, sd)
	require.NoError(t, f.index.ReleaseBackup(ctx, "legacy-unload-race"))
	require.Equal(t, ShardUnloadOutcomeRefusedInUse, unloadOutcome)

	_, err = f.index.UnloadLocalShard(ctx, name)
	require.NoError(t, err)
	db, err := shardmeta.Open(shardPath(f.index.path(), name), time.Second)
	if err == nil {
		err = db.Close()
	}
	require.NoError(t, err, "an orphan instance still holds the shard's metadata db")
}
