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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

func TestLazyShardUnloadingWrapsEagerPaths(t *testing.T) {
	paths := []struct {
		name string
		init func(t *testing.T, f *addPropertyLazyFixture, name string)
	}{
		{name: "movement target", init: func(t *testing.T, f *addPropertyLazyFixture, name string) {
			require.NoError(t, f.index.LoadLocalShardForMovement(testCtx(), name))
		}},
		{name: "new replica", init: func(t *testing.T, f *addPropertyLazyFixture, name string) {
			require.NoError(t, f.index.LoadLocalShardForNewReplica(testCtx(), name))
		}},
		{name: "tenant process", init: func(t *testing.T, f *addPropertyLazyFixture, name string) {
			require.NoError(t, f.index.LoadLocalShardForTenantProcess(testCtx(), name))
		}},
		{name: "namespace reopen", init: func(t *testing.T, f *addPropertyLazyFixture, name string) {
			require.NoError(t, f.index.db.ReopenShard(testCtx(), f.index.Config.ClassName.String(), name))
		}},
		{name: "write init", init: func(t *testing.T, f *addPropertyLazyFixture, name string) {
			_, release, err := f.index.getOrInitShard(testCtx(), name)
			require.NoError(t, err)
			release()
		}},
	}
	modes := []struct {
		name     string
		opts     func() []func(*Config)
		wantLazy bool
	}{
		{name: "no unloader", opts: func() []func(*Config) { return nil }},
		{
			name: "unloader with lazy loading forced off",
			opts: func() []func(*Config) {
				return []func(*Config){withShardUnloader(newFakeShardUnloader()), func(c *Config) { c.EnableLazyLoadShards = boolPtr(false) }}
			},
		},
		{
			name:     "unloader",
			opts:     func() []func(*Config) { return []func(*Config){withShardUnloader(newFakeShardUnloader())} },
			wantLazy: true,
		},
	}
	for _, mode := range modes {
		for _, path := range paths {
			t.Run(mode.name+"/"+path.name, func(t *testing.T) {
				f := newAddPropertyLazyFixture(t, "Wrap", singleShardState(), mode.opts()...)
				var name string
				f.index.ForEachShard(func(n string, _ ShardLike) error { name = n; return nil })
				require.NotEmpty(t, name)
				_, err := f.index.UnloadLocalShard(testCtx(), name)
				require.NoError(t, err)
				require.Nil(t, f.index.shards.Load(name))

				path.init(t, f, name)

				got := f.index.shards.Load(name)
				require.NotNil(t, got)
				if !mode.wantLazy {
					_, bare := got.(*Shard)
					require.True(t, bare, "today's eager paths build a bare shard, got %T", got)
					return
				}
				lazy, ok := got.(*LazyLoadShard)
				require.True(t, ok, "got %T", got)
				require.True(t, lazy.isLoaded(), "the eager path still loads, into the wrapper")
				require.EqualValues(t, 1, f.index.Config.ShardUnloader.Registry().Loaded())
				require.Equal(t, UnloadOutcomeOK, lazy.TryUnload(testCtx(), UnloadRequest{Reindex: noReindex}))
				require.Same(t, lazy, f.index.shards.Load(name))
			})
		}
	}
}

func TestLazyShardUnloadingEagerPathsStayBareShardsWithoutUnloader(t *testing.T) {
	f := newAddPropertyLazyFixture(t, "WrapParity", singleShardState(), func(c *Config) { c.EnableLazyLoadShards = boolPtr(false) })
	var name string
	f.index.ForEachShard(func(n string, _ ShardLike) error { name = n; return nil })
	_, isBare := f.index.shards.Load(name).(*Shard)
	require.True(t, isBare, "lazy loading off keeps today's eager startup shard")
	_, err := f.index.UnloadLocalShard(testCtx(), name)
	require.NoError(t, err)
	require.NoError(t, f.index.LoadLocalShardForMovement(testCtx(), name))
	_, isBare = f.index.shards.Load(name).(*Shard)
	require.True(t, isBare)
}

func TestLazyShardUnloadingMakesAutoDetectedCollectionsLazy(t *testing.T) {
	tests := []struct {
		name     string
		unloader bool
		wantLazy bool
	}{
		{name: "auto-detect without unloader keeps a single-tenant collection eager"},
		{name: "auto-detect with unloader makes it lazy", unloader: true, wantLazy: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := []func(*Config){func(c *Config) { c.EnableLazyLoadShards = nil }}
			if tt.unloader {
				opts = append(opts, withShardUnloader(newFakeShardUnloader()))
			}
			repo, migrator, _, _ := newLazyLoadRepoWithConfig(t, singleShardState(), true, -1, opts...)
			t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })
			require.NoError(t, migrator.AddClass(testCtx(), newClassWithWarmProp("AutoLazy")))
			index := repo.GetIndex("AutoLazy")
			require.NotNil(t, index)
			require.Equal(t, tt.wantLazy, index.Config.EnableLazyLoadShards)
			index.ForEachShard(func(_ string, shard ShardLike) error {
				_, isLazy := shard.(*LazyLoadShard)
				require.Equal(t, tt.wantLazy, isLazy, "got %T", shard)
				return nil
			})
		})
	}
}

func TestNodesStatusNeverReloadsAnUnloadedShard(t *testing.T) {
	f := newUnloadFixture(t)
	f.put(t)

	var status []*models.NodeShardStatus
	_, count, err := f.index.getShardsNodeStatus(testCtx(), &status, "")
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	require.True(t, status[0].Loaded)

	require.Equal(t, UnloadOutcomeOK, f.lazy.TryUnload(testCtx(), UnloadRequest{Reindex: noReindex}))
	beforeLoads := f.unloader.beforeLoads.Load()
	status = nil
	_, _, err = f.index.getShardsNodeStatus(testCtx(), &status, "")
	require.NoError(t, err)
	require.False(t, status[0].Loaded)
	require.False(t, f.lazy.isLoaded())
	require.Equal(t, beforeLoads, f.unloader.beforeLoads.Load(), "the status walk loaded nothing")
}

func TestPinWhileUnloading(t *testing.T) {
	tests := []struct {
		name     string
		unloader bool
		want     UnloadOutcome
	}{
		{name: "no unloader pins nothing"},
		{name: "unloader pins the shard", unloader: true, want: UnloadOutcomeInUse},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts []func(*Config)
			u := newFakeShardUnloader()
			if tt.unloader {
				opts = append(opts, withShardUnloader(u))
			}
			f := newAddPropertyLazyFixture(t, "PinWalk", singleShardState(), opts...)
			for name, lazy := range f.coldShards(t) {
				release, err := f.index.pinWhileUnloading(name)
				require.NoError(t, err)
				if !tt.unloader {
					require.False(t, lazy.isLoaded(), "without an unloader the walk is left as it was")
					release()
					return
				}
				require.True(t, lazy.isLoaded())
				require.Equal(t, tt.want, lazy.TryUnload(testCtx(), UnloadRequest{Reindex: noReindex}))
				release()
				require.Equal(t, UnloadOutcomeOK, lazy.TryUnload(testCtx(), UnloadRequest{Reindex: noReindex}))
			}
		})
	}
}

func TestFallbackModeSwitchLeavesColdShardsCold(t *testing.T) {
	tests := []struct {
		name       string
		unloader   bool
		wantLoaded bool
	}{
		{name: "no unloader force-loads every shard as before", wantLoaded: true},
		{name: "unloader leaves cold shards to inherit the mode at load", unloader: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts []func(*Config)
			if tt.unloader {
				opts = append(opts, withShardUnloader(newFakeShardUnloader()))
			}
			f := newAddPropertyLazyFixture(t, "FallbackCold", singleShardState(), opts...)
			className := f.index.Config.ClassName.String()
			files := newFilterableToSearchableMigrationFiles(f.index.Config.RootPath)
			require.NoError(t, files.createMigrationSkipFlag())
			require.NoError(t, files.saveMigrationState(&filterableToSearchableMigrationState{
				MissingFilterableClass2Props: map[string]map[string]struct{}{className: {"warm": {}}},
				CreatedFilterableClass2Props: map[string]map[string]struct{}{},
			}))
			for name, lazy := range f.coldShards(t) {
				require.NoError(t, f.migrator.AdjustFilterablePropSettings(testCtx()))
				require.Equal(t, tt.wantLoaded, lazy.isLoaded())
				shard, release, err := f.index.getOrInitShard(testCtx(), name)
				require.NoError(t, err)
				require.True(t, shard.(*LazyLoadShard).currentShard().isFallbackToSearchable())
				release()
			}
		})
	}
}
