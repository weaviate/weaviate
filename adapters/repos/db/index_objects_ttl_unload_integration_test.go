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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backgroundaccess"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func setupTTLUnloaderRepo(t *testing.T, shardState *sharding.State, class *models.Class,
	tenant string, expiredCount int,
) (*Index, *LazyLoadShard) {
	t.Helper()

	repo := setupTestDBWithShardState(t, t.TempDir(), shardState, func(cfg *Config) {
		cfg.EnableLazyLoadShards = boolPtr(true)
		cfg.ShardUnloader = newFakeShardUnloader()
	}, nil, class)
	t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })

	insertTTLObjects(t, repo, tenant, expiredCount, 2)

	index, _ := ttlIndex(t, repo)
	var lazy *LazyLoadShard
	require.NoError(t, index.ForEachShard(func(_ string, shard ShardLike) error {
		l, ok := shard.(*LazyLoadShard)
		require.True(t, ok, "shard should be a lazy wrapper, got %T", shard)
		lazy = l
		return nil
	}))
	require.NotNil(t, lazy)
	require.True(t, lazy.isLoaded())
	return index, lazy
}

func runTTLSweep(t *testing.T, index *Index) int32 {
	t.Helper()

	var deleted atomic.Int32
	eg := enterrors.NewErrorGroupWrapper(index.logger)
	ec := errorcompounder.NewSafe()
	index.incomingDeleteObjectsExpired(context.Background(), eg, ec, batchDeleteTTLProp,
		time.Now(), time.Now(), func(n int32) { deleted.Add(n) }, 1)
	eg.Wait()
	require.NoError(t, ec.ToError())
	return deleted.Load()
}

func TestTTLSweepCountsAsShardAccessOnlyWhenItDeletes(t *testing.T) {
	tests := []struct {
		name         string
		shardState   *sharding.State
		class        *models.Class
		tenant       string
		expiredCount int
		unload       bool
		wantDeleted  int32
	}{
		{name: "single tenant, nothing expired", shardState: singleShardState(), class: batchDeleteTTLClass()},
		{name: "single tenant, expired objects", shardState: singleShardState(), class: batchDeleteTTLClass(), expiredCount: 3, wantDeleted: 3},
		{name: "single tenant, unloaded", shardState: singleShardState(), class: batchDeleteTTLClass(), expiredCount: 3, unload: true},
		{name: "multi tenant, nothing expired", shardState: batchDeleteTenantShardState(batchDeleteTenant), class: ttlMultiTenantClass(), tenant: batchDeleteTenant},
		{name: "multi tenant, expired objects", shardState: batchDeleteTenantShardState(batchDeleteTenant), class: ttlMultiTenantClass(), tenant: batchDeleteTenant, expiredCount: 3, wantDeleted: 3},
		{name: "multi tenant, unloaded", shardState: batchDeleteTenantShardState(batchDeleteTenant), class: ttlMultiTenantClass(), tenant: batchDeleteTenant, expiredCount: 3, unload: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index, lazy := setupTTLUnloaderRepo(t, tt.shardState, tt.class, tt.tenant, tt.expiredCount)
			if tt.unload {
				require.True(t, lazy.TryUnload(testCtx(), UnloadRequest{}))
			}
			lazy.lastAccess.Store(0)

			require.Equal(t, tt.wantDeleted, runTTLSweep(t, index))

			require.Equal(t, !tt.unload, lazy.isLoaded())
			require.Equal(t, tt.wantDeleted > 0, lazy.LastAccess() != 0)
		})
	}
}

func TestTTLSweepCountsAsAccessBeforeItsPinIsReleased(t *testing.T) {
	tests := []struct {
		name       string
		shardState *sharding.State
		class      *models.Class
		tenant     string
	}{
		{name: "single tenant", shardState: singleShardState(), class: batchDeleteTTLClass()},
		{name: "multi tenant", shardState: batchDeleteTenantShardState(batchDeleteTenant), class: ttlMultiTenantClass(), tenant: batchDeleteTenant},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index, lazy := setupTTLUnloaderRepo(t, tt.shardState, tt.class, tt.tenant, 3)
			unloaded := map[string]bool{}
			index.afterTTLBatchDelete = func(shardName string) {
				unloaded[shardName] = lazy.TryUnload(testCtx(), UnloadRequest{})
			}
			lazy.lastAccess.Store(0)

			require.Equal(t, int32(3), runTTLSweep(t, index))

			require.Len(t, unloaded, 1)
			for _, outcome := range unloaded {
				require.True(t, outcome)
			}
			require.False(t, lazy.isLoaded())
			require.NotZero(t, lazy.LastAccess())
		})
	}
}

func ttlExpiredFilter() *filters.LocalFilter {
	return &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorLessThanEqual,
		Value:    &filters.Value{Value: time.Now(), Type: schema.DataTypeDate},
		On:       &filters.Path{Class: schema.ClassName(batchDeleteTTLClassName), Property: schema.PropertyName(batchDeleteTTLProp)},
	}}
}

func setupUnlicensedColdTTLRepo(t *testing.T, expiredCount int) (*Index, *LazyLoadShard) {
	t.Helper()

	dir, state := t.TempDir(), singleShardState()
	override := func(cfg *Config) {
		cfg.EnableLazyLoadShards = boolPtr(true)
		cfg.LazyLoadShardWarmupMinObjects = -1
	}
	seed := setupTestDBWithShardState(t, dir, state, override, nil, batchDeleteTTLClass())
	insertTTLObjects(t, seed, "", expiredCount, 2)
	require.NoError(t, seed.Shutdown(context.Background()))

	repo := setupTestDBWithShardState(t, dir, state, override, nil, batchDeleteTTLClass())
	t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })
	index, _ := ttlIndex(t, repo)
	var lazy *LazyLoadShard
	require.NoError(t, index.ForEachShard(func(_ string, shard ShardLike) error {
		l, ok := shard.(*LazyLoadShard)
		require.True(t, ok, "shard should be a lazy wrapper, got %T", shard)
		lazy = l
		return nil
	}))
	require.NotNil(t, lazy)
	require.False(t, lazy.isLoaded())
	return index, lazy
}

func TestIncomingFindUUIDsFromBackgroundWork(t *testing.T) {
	tests := []struct {
		name       string
		background bool
		unload     bool
		wantLoaded bool
		wantFound  int
		wantAccess bool
	}{
		{name: "request, loaded", wantLoaded: true, wantFound: 3, wantAccess: true},
		{name: "request, unloaded", unload: true, wantLoaded: true, wantFound: 3, wantAccess: true},
		{name: "background, loaded", background: true, wantLoaded: true, wantFound: 3},
		{name: "background, unloaded", background: true, unload: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index, lazy := setupTTLUnloaderRepo(t, singleShardState(), batchDeleteTTLClass(), "", 3)
			if tt.unload {
				require.True(t, lazy.TryUnload(testCtx(), UnloadRequest{}))
			}
			lazy.lastAccess.Store(0)
			ctx := context.Background()
			if tt.background {
				ctx = backgroundaccess.With(ctx)
			}

			uuids, err := index.IncomingFindUUIDs(ctx, lazy.Name(), ttlExpiredFilter(), 100)
			require.NoError(t, err)

			require.Len(t, uuids, tt.wantFound)
			require.Equal(t, tt.wantLoaded, lazy.isLoaded())
			require.Equal(t, tt.wantAccess, lazy.LastAccess() != 0)
		})
	}
}

func TestBackgroundWorkLoadsColdShardsWithoutAnUnloader(t *testing.T) {
	t.Run("ttl sweep", func(t *testing.T) {
		index, lazy := setupUnlicensedColdTTLRepo(t, 3)
		require.Equal(t, int32(3), runTTLSweep(t, index))
		require.True(t, lazy.isLoaded())
	})
	t.Run("incoming find uuids", func(t *testing.T) {
		index, lazy := setupUnlicensedColdTTLRepo(t, 3)
		uuids, err := index.IncomingFindUUIDs(backgroundaccess.With(context.Background()), lazy.Name(), ttlExpiredFilter(), 100)
		require.NoError(t, err)
		require.Len(t, uuids, 3)
		require.True(t, lazy.isLoaded())
	})
}

func TestBackgroundWorkLoadsColdShardsWhileTheCollectorIsIdle(t *testing.T) {
	setup := func(t *testing.T) (*Index, *LazyLoadShard) {
		index, lazy := setupTTLUnloaderRepo(t, singleShardState(), batchDeleteTTLClass(), "", 3)
		require.True(t, lazy.TryUnload(testCtx(), UnloadRequest{}))
		u, ok := index.Config.ShardUnloader.(*fakeShardUnloader)
		require.True(t, ok)
		u.idle.Store(true)
		return index, lazy
	}
	t.Run("ttl sweep", func(t *testing.T) {
		index, lazy := setup(t)
		require.Equal(t, int32(3), runTTLSweep(t, index))
		require.True(t, lazy.isLoaded())
	})
	t.Run("incoming find uuids", func(t *testing.T) {
		index, lazy := setup(t)
		uuids, err := index.IncomingFindUUIDs(backgroundaccess.With(context.Background()), lazy.Name(), ttlExpiredFilter(), 100)
		require.NoError(t, err)
		require.Len(t, uuids, 3)
		require.True(t, lazy.isLoaded())
	})
}
