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
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// The pre-1.19 fallback mode is set once at startup, so every later load of the shard must restore it.
func TestShardReloadKeepsFallbackToSearchable(t *testing.T) {
	reloads := []struct {
		name   string
		reload func(t *testing.T, f *unloadFixture)
	}{
		{name: "cold then hot", reload: func(t *testing.T, f *unloadFixture) {
			_, err := f.index.UnloadLocalShard(testCtx(), f.name)
			require.NoError(t, err)
			require.NoError(t, f.index.initLocalShard(testCtx(), f.name))
		}},
		{name: "unloaded by the unloader", reload: func(t *testing.T, f *unloadFixture) {
			require.Equal(t, UnloadOutcomeOK, f.lazy.TryUnload(testCtx(), UnloadRequest{Reindex: noReindex}))
		}},
	}
	tests := []struct {
		name    string
		missing bool
	}{
		{name: "class missing filterable indexes", missing: true},
		{name: "class not in the migration state"},
	}
	for _, r := range reloads {
		for _, tt := range tests {
			t.Run(r.name+"/"+tt.name, func(t *testing.T) {
				f := newUnloadFixture(t)
				state := &filterableToSearchableMigrationState{
					MissingFilterableClass2Props: map[string]map[string]struct{}{},
					CreatedFilterableClass2Props: map[string]map[string]struct{}{},
				}
				if tt.missing {
					state.MissingFilterableClass2Props[f.index.Config.ClassName.String()] = map[string]struct{}{"warm": {}}
				}
				require.NoError(t, newFilterableToSearchableMigrationFiles(f.index.Config.RootPath).saveMigrationState(state))
				require.NoError(t, f.migrator.AdjustFilterablePropSettings(testCtx()))
				shard, release, err := f.index.getOrInitShard(testCtx(), f.name)
				require.NoError(t, err)
				require.Equal(t, tt.missing, shard.(*LazyLoadShard).currentShard().isFallbackToSearchable())
				release()

				r.reload(t, f)

				shard, release, err = f.index.getOrInitShard(testCtx(), f.name)
				require.NoError(t, err)
				defer release()
				require.Equal(t, tt.missing, shard.(*LazyLoadShard).currentShard().isFallbackToSearchable(),
					"the reloaded shard lost its fallback mode")
			})
		}
	}
}

// A shard whose filterable index the task already rebuilt must not fall back
// again on a reload, while the rest of its class is still being rebuilt.
func TestShardReloadAfterItsFilterableRebuildKeepsFallbackOff(t *testing.T) {
	f := newUnloadFixture(t)
	state := &filterableToSearchableMigrationState{
		MissingFilterableClass2Props: map[string]map[string]struct{}{f.index.Config.ClassName.String(): {"warm": {}}},
		CreatedFilterableClass2Props: map[string]map[string]struct{}{},
	}
	require.NoError(t, newFilterableToSearchableMigrationFiles(f.index.Config.RootPath).saveMigrationState(state))
	require.NoError(t, f.migrator.AdjustFilterablePropSettings(testCtx()))
	shard, release, err := f.index.getOrInitShard(testCtx(), f.name)
	require.NoError(t, err)
	require.True(t, f.lazy.currentShard().isFallbackToSearchable())
	task := newShardInvertedReindexTaskMissingTextFilterable(f.migrator)
	require.NoError(t, task.OnPostResumeStore(testCtx(), shard))
	require.False(t, f.lazy.currentShard().isFallbackToSearchable())
	release()

	require.Equal(t, UnloadOutcomeOK, f.lazy.TryUnload(testCtx(), UnloadRequest{Reindex: noReindex}))

	_, release, err = f.index.getOrInitShard(testCtx(), f.name)
	require.NoError(t, err)
	defer release()
	require.False(t, f.lazy.currentShard().isFallbackToSearchable(), "the reload re-enabled the fallback over a rebuilt filterable index")
}

func TestFallbackStateIsReparsedOnlyWhenTheFileChanges(t *testing.T) {
	root := t.TempDir()
	files := newFilterableToSearchableMigrationFiles(root)
	cache := &fallbackStateCache{byPath: map[string]fallbackStateEntry{}}

	missing, err := cache.missingFilterable(files.stateFileName)
	require.NoError(t, err)
	require.Nil(t, missing, "no state file, no fallback")

	save := func(classes ...string) {
		state := &filterableToSearchableMigrationState{
			MissingFilterableClass2Props: map[string]map[string]struct{}{},
			CreatedFilterableClass2Props: map[string]map[string]struct{}{},
		}
		for _, c := range classes {
			state.MissingFilterableClass2Props[c] = map[string]struct{}{"p": {}}
		}
		require.NoError(t, files.saveMigrationState(state))
	}
	save("A", "B")
	first, err := cache.missingFilterable(files.stateFileName)
	require.NoError(t, err)
	require.Len(t, first, 2)
	again, err := cache.missingFilterable(files.stateFileName)
	require.NoError(t, err)
	require.Equal(t, reflect.ValueOf(first).Pointer(), reflect.ValueOf(again).Pointer(), "an unchanged file is served from the cache")

	save("B")
	after, err := cache.missingFilterable(files.stateFileName)
	require.NoError(t, err)
	require.Len(t, after, 1, "a class the task finished stops falling back on its next load")
	require.Contains(t, after, "B")
}
