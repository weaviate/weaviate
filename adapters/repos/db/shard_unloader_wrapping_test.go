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
	"time"

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
				require.True(t, lazy.TryUnload(testCtx(), UnloadRequest{}))
				require.Same(t, lazy, f.index.shards.Load(name))
			})
		}
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

func TestLazyShardUnloadingRestartLoadsEveryAutoEagerShard(t *testing.T) {
	root, state := t.TempDir(), singleShardState()
	classes := make([]*models.Class, 5)
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
		require.True(t, index.Config.EnableLazyLoadShards, "an unloader makes an auto-detected collection lazy")
		require.False(t, index.Config.lazyLoadsShards(), "auto-detection still picks eager loading")
		var name string
		index.ForEachShard(func(n string, _ ShardLike) error { name = n; return nil })
		shard, release, err := index.GetShard(testCtx(), name)
		require.NoError(t, err)
		defer release()
		_, ok := shard.(*LazyLoadShard)
		require.True(t, ok, "got %T", shard)
		f(shard)
	}

	repo := open(newFakeShardUnloader())
	ids := make([]strfmt.UUID, len(classes))
	for n, class := range classes {
		obj := testObject(class.Class)
		ids[n] = obj.ID()
		withShard(repo, class.Class, func(s ShardLike) { require.NoError(t, s.PutObject(testCtx(), obj)) })
	}
	require.NoError(t, repo.Shutdown(context.Background()))

	u := newFakeShardUnloader()
	repo = open(u)
	t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })
	require.EqualValues(t, len(classes), u.registry.Loaded())
	for n, class := range classes {
		withShard(repo, class.Class, func(s ShardLike) {
			ok, err := s.Exists(testCtx(), ids[n])
			require.NoError(t, err)
			require.True(t, ok, "%s keeps its object", class.Class)
		})
	}
	require.Zero(t, u.beforeLoads.Load(), "every shard loaded at startup, which never calls BeforeLoad")
}

func TestStartupWarmupAcrossIndexesLoadsEveryShardWithoutBeforeLoad(t *testing.T) {
	const indexes = 6
	u := newFakeShardUnloader()
	repo, migrator, _, _ := newLazyLoadRepoWithConfig(t, singleShardState(), true, 0, withShardUnloader(u))
	t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })
	for n := range indexes {
		require.NoError(t, migrator.AddClass(testCtx(), newClassWithWarmProp(fmt.Sprintf("Warmup%d", n))))
	}
	for n := range indexes {
		index := repo.GetIndex(schema.ClassName(fmt.Sprintf("Warmup%d", n)))
		require.NotNil(t, index)
		require.Eventually(t, index.allShardsReady.Load, 20*time.Second, 10*time.Millisecond)
	}
	require.EqualValues(t, indexes, u.registry.Loaded(), "warmup loads every shard")
	require.Zero(t, u.beforeLoads.Load(), "a warmup load never calls BeforeLoad")
}
