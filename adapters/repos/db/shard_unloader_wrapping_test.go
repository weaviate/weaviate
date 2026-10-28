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
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
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
			require.False(t, index.Config.lazyLoadsShards(), "auto-detection still picks eager loading")
			index.ForEachShard(func(_ string, shard ShardLike) error {
				lazy, isLazy := shard.(*LazyLoadShard)
				require.Equal(t, tt.wantLazy, isLazy, "got %T", shard)
				if isLazy {
					require.True(t, lazy.isLoaded(), "an eager collection loads its wrappers at startup")
				}
				return nil
			})
		})
	}
}

func TestLazyShardUnloadingKeepsAutoEagerShardLoadsEager(t *testing.T) {
	paths := []struct {
		name string
		init func(t *testing.T, index *Index, name string)
	}{
		{name: "tenant add", init: func(t *testing.T, index *Index, name string) {
			require.NoError(t, index.LoadLocalShardForTenantAdd(testCtx(), name))
		}},
		{name: "tenant activation", init: func(t *testing.T, index *Index, name string) {
			require.NoError(t, index.LoadLocalShardForTenantActivation(testCtx(), name, false))
		}},
		{name: "schema reload", init: func(t *testing.T, index *Index, name string) {
			require.NoError(t, index.loadLocalShardForReload(testCtx(), name))
		}},
	}
	modes := []struct {
		name       string
		lazy       *bool
		unloader   bool
		wantBare   bool
		wantLoaded bool
	}{
		{name: "auto-detected eager without unloader", wantBare: true, wantLoaded: true},
		{name: "auto-detected eager with unloader", unloader: true, wantLoaded: true},
		{name: "lazy loading on with unloader", lazy: boolPtr(true), unloader: true},
	}
	for _, mode := range modes {
		for _, path := range paths {
			t.Run(mode.name+"/"+path.name, func(t *testing.T) {
				opts := []func(*Config){func(c *Config) { c.EnableLazyLoadShards = mode.lazy }}
				if mode.unloader {
					opts = append(opts, withShardUnloader(newFakeShardUnloader()))
				}
				index := newAddPropertyLazyFixture(t, "AutoEager", singleShardState(), opts...).index
				var name string
				index.ForEachShard(func(n string, _ ShardLike) error { name = n; return nil })
				require.NotEmpty(t, name)
				_, err := index.UnloadLocalShard(testCtx(), name)
				require.NoError(t, err)

				path.init(t, index, name)

				got := index.shards.Load(name)
				require.NotNil(t, got)
				_, bare := got.(*Shard)
				require.Equal(t, mode.wantBare, bare, "got %T", got)
				var status []*models.NodeShardStatus
				_, _, err = index.getShardsNodeStatus(testCtx(), &status, "")
				require.NoError(t, err)
				require.Len(t, status, 1)
				require.Equal(t, mode.wantLoaded, status[0].Loaded)
			})
		}
	}
}

func TestLazyShardUnloadingRestartLoadsAutoEagerShardsUpToTheLimit(t *testing.T) {
	const indexes = 5
	tests := []struct {
		name       string
		limit      int64
		wantLoaded int
	}{
		{name: "no limit loads every shard", wantLoaded: indexes},
		{name: "limit loads that many and leaves the rest cold", limit: 2, wantLoaded: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root, state := t.TempDir(), singleShardState()
			classes := make([]*models.Class, indexes)
			for n := range classes {
				classes[n] = newClassWithWarmProp(fmt.Sprintf("Restart%d", n))
			}
			open := func(u *fakeShardUnloader) *DB {
				repo, migrator, schemaGetter, _ := newLazyLoadRepoWithConfig(t, state, true, -1,
					func(c *Config) { c.RootPath = root; c.EnableLazyLoadShards = nil }, withShardUnloader(u))
				schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: classes}}
				for _, class := range classes {
					require.NoError(t, migrator.AddClass(testCtx(), class))
				}
				return repo
			}
			withShard := func(repo *DB, class string, f func(ShardLike)) {
				index := repo.GetIndex(schema.ClassName(class))
				require.NotNil(t, index)
				var name string
				index.ForEachShard(func(n string, _ ShardLike) error { name = n; return nil })
				shard, release, err := index.GetShard(testCtx(), name)
				require.NoError(t, err)
				defer release()
				f(shard)
			}

			repo := open(newFakeShardUnloader())
			ids := make([]strfmt.UUID, indexes)
			for n, class := range classes {
				obj := testObject(class.Class)
				ids[n] = obj.ID()
				withShard(repo, class.Class, func(s ShardLike) { require.NoError(t, s.PutObject(testCtx(), obj)) })
			}
			require.NoError(t, repo.Shutdown(context.Background()))

			u := newFakeShardUnloader()
			u.warmupLimit.Store(tt.limit)
			repo = open(u)
			t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })
			require.EqualValues(t, tt.wantLoaded, u.registry.Loaded())
			require.Zero(t, u.beforeLoads.Load(), "a startup load never makes room")
			cold := 0
			for _, class := range classes {
				repo.GetIndex(schema.ClassName(class.Class)).ForEachShard(func(_ string, shard ShardLike) error {
					lazy, ok := shard.(*LazyLoadShard)
					require.True(t, ok, "got %T", shard)
					if !lazy.isLoaded() {
						cold++
					}
					return nil
				})
			}
			require.Equal(t, indexes-tt.wantLoaded, cold)

			for n, class := range classes {
				withShard(repo, class.Class, func(s ShardLike) {
					ok, err := s.Exists(testCtx(), ids[n])
					require.NoError(t, err)
					require.True(t, ok, "%s keeps its object", class.Class)
				})
			}
			require.EqualValues(t, indexes, u.registry.Loaded(), "a request loads a cold shard")
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
