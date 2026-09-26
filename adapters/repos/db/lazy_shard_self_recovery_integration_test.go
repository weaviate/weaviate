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
	"errors"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/indexcounter"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func warmupShardPath(dirName, tenant string) string {
	return shardPath(path.Join(dirName, indexID(schema.ClassName(warmupClassName))), tenant)
}

func stashShardDir(t *testing.T, dirName, tenant string) (restore func()) {
	t.Helper()
	src := warmupShardPath(dirName, tenant)
	dst := filepath.Join(t.TempDir(), tenant)
	require.NoError(t, os.Rename(src, dst))
	return func() { require.NoError(t, os.Rename(dst, src)) }
}

func awaitSweep(t *testing.T, index *Index) {
	t.Helper()
	require.Eventually(t, index.allShardsReady.Load, 30*time.Second, 50*time.Millisecond)
}

func TestLazyShardSelfRecoveryStartupPass(t *testing.T) {
	ctx := context.Background()
	missing := []string{"missing-never", "missing-small", "missing-large"}
	seeds := map[string]warmupSeed{
		"missing-never": {}, "missing-small": {counted: 2}, "missing-large": {counted: 5},
		"intact-never": {}, "intact-small": {counted: 2}, "intact-large": {counted: 5},
	}

	t.Run("submitted", func(t *testing.T) {
		dirName := t.TempDir()
		tenants := seedWarmupTenants(t, dirName, seeds)
		for _, tenant := range missing {
			stashShardDir(t, dirName, tenant)
		}
		orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
		index, hook := newWarmupIndexWithOpts(t, dirName, 3, nil, warmupIndexOpts{ctx: schemaReloadCtx(), orch: orch}, tenants...)
		defer index.Shutdown(ctx)

		require.Equal(t, 3, orch.submitCalls)
		require.False(t, orch.gotStartedWithoutRaftState)
		requireSweepTally(t, hook, map[monitoring.WarmupOutcome]int{
			monitoring.WarmupLoaded:                1,
			monitoring.WarmupSkippedEmpty:          1,
			monitoring.WarmupSkippedBelowThreshold: 1,
			monitoring.WarmupSkippedRecovering:     3,
		})
		for _, tenant := range missing {
			rec, ok := index.shards.Load(tenant).(*RecoveringShard)
			require.True(t, ok, tenant)
			require.True(t, rec.IsRecovering(), tenant)
			require.NoDirExists(t, warmupShardPath(dirName, tenant))
		}
		require.True(t, index.shards.Load("intact-large").(*LazyLoadShard).isLoaded())
		coldWarmupShard(t, index, "intact-small")
		coldWarmupShard(t, index, "intact-never")

		shard, release, err := index.getShardForDirectLocalOperation(ctx, "missing-small", "missing-small", localShardOperationRead, 0)
		release()
		require.NoError(t, err)
		require.Nil(t, shard)

		_, release, err = index.getOrInitShardForReplication(ctx, "missing-small")
		release()
		var unprocessable enterrors.ErrUnprocessable
		require.True(t, errors.As(err, &unprocessable), "got %T: %v", err, err)
		require.True(t, enterrors.IsShardRecovering(err))

		outcome, err := index.loadLocalShardIfActive("missing-small")
		require.NoError(t, err)
		require.Equal(t, monitoring.WarmupOutcome(""), outcome)
		require.NoDirExists(t, warmupShardPath(dirName, "missing-small"))
	})

	t.Run("declined", func(t *testing.T) {
		dirName := t.TempDir()
		tenants := seedWarmupTenants(t, dirName, seeds)
		for _, tenant := range missing {
			stashShardDir(t, dirName, tenant)
		}
		orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: false}
		index, hook := newWarmupIndexWithOpts(t, dirName, 3, nil, warmupIndexOpts{ctx: schemaReloadCtx(), orch: orch}, tenants...)
		defer index.Shutdown(ctx)

		require.Equal(t, 3, orch.submitCalls)
		requireSweepTally(t, hook, map[monitoring.WarmupOutcome]int{
			monitoring.WarmupLoaded:                1,
			monitoring.WarmupSkippedEmpty:          4,
			monitoring.WarmupSkippedBelowThreshold: 1,
		})
		for _, tenant := range missing {
			coldWarmupShard(t, index, tenant)
			require.DirExists(t, warmupShardPath(dirName, tenant))
			count, err := indexcounter.Read(warmupShardPath(dirName, tenant))
			require.NoError(t, err)
			require.Zero(t, count)
		}
	})
}

type recoveringWarmupFixture struct {
	dirName string
	index   *Index
	orch    *fakeSelfRecoveryOrch
	restore func()
}

func newRecoveringWarmupFixture(t *testing.T, tenant string, counted int, minObjects int64, eager bool) *recoveringWarmupFixture {
	t.Helper()
	dirName := t.TempDir()
	seedWarmupTenants(t, dirName, map[string]warmupSeed{tenant: {counted: counted}})
	restore := stashShardDir(t, dirName, tenant)
	orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: true}
	index, _ := newWarmupIndexWithOpts(t, dirName, minObjects, nil, warmupIndexOpts{ctx: schemaReloadCtx(), orch: orch, eager: eager}, tenant)
	t.Cleanup(func() { index.Shutdown(context.Background()) })
	require.Equal(t, 1, orch.submitCalls)
	_, ok := index.shards.Load(tenant).(*RecoveringShard)
	require.True(t, ok)
	awaitSweep(t, index)
	return &recoveringWarmupFixture{dirName: dirName, index: index, orch: orch, restore: restore}
}

func TestLazyShardSelfRecoveryPromotePolicy(t *testing.T) {
	const tenant = "t"
	cases := []struct {
		name        string
		counted     int
		minObjects  int64
		eager       bool
		restore     bool
		mkdir       bool
		wantLoaded  bool
		wantBlocked bool
	}{
		{name: "small restored stays cold", counted: 2, minObjects: 3, restore: true},
		{name: "large restored loads", counted: 5, minObjects: 3, restore: true, wantLoaded: true},
		{name: "empty fallback stays cold", counted: 2, minObjects: 3, mkdir: true},
		{name: "threshold zero loads", counted: 2, minObjects: 0, restore: true, wantLoaded: true},
		{name: "warmup disabled stays cold", counted: 5, minObjects: -1, restore: true},
		{name: "eager restored loads", counted: 2, minObjects: 3, eager: true, restore: true, wantLoaded: true},
		{name: "eager empty fallback loads", counted: 2, minObjects: 3, eager: true, mkdir: true, wantLoaded: true},
		{name: "no live dir keeps the block", counted: 2, minObjects: 3, wantBlocked: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newRecoveringWarmupFixture(t, tenant, tc.counted, tc.minObjects, tc.eager)
			shardDir := warmupShardPath(f.dirName, tenant)
			switch {
			case tc.restore:
				f.restore()
			case tc.mkdir:
				require.NoError(t, os.MkdirAll(shardDir, os.ModePerm))
			}

			require.NoError(t, f.index.PromoteRecoveringLocalShard(context.Background(), tenant))

			entry := f.index.shards.Load(tenant)
			if tc.wantBlocked {
				rec, ok := entry.(*RecoveringShard)
				require.True(t, ok)
				require.True(t, rec.isLoadBlocked())
				require.NoDirExists(t, shardDir)
				return
			}
			lazy, ok := entry.(*LazyLoadShard)
			require.True(t, ok, "got %T", entry)
			require.False(t, lazy.isLoadBlocked())
			require.Equal(t, tc.wantLoaded, lazy.isLoaded())
			require.DirExists(t, shardDir)
			if tc.mkdir && !tc.wantLoaded {
				require.NoDirExists(t, filepath.Join(shardDir, "lsm"))
			}
		})
	}
}

func TestLazyShardSelfRecoveryEmptyFallbackPromoteFollowsCountZeroRule(t *testing.T) {
	const tenant = "t"
	cases := []struct {
		name        string
		singleShard bool
		minObjects  int64
		restore     bool
		wantLoaded  bool
		wantOutcome monitoring.WarmupOutcome
	}{
		{name: "single tenant empty fallback stays cold", singleShard: true, minObjects: 100, wantOutcome: monitoring.WarmupSkippedBelowThreshold},
		{name: "single tenant empty fallback with threshold zero loads", singleShard: true, minObjects: 0, wantLoaded: true},
		{name: "multi tenant empty fallback stays cold", minObjects: 100, wantOutcome: monitoring.WarmupSkippedEmpty},
		{name: "single tenant recovered copy loads", singleShard: true, minObjects: 3, restore: true, wantLoaded: true},
		{name: "single tenant small recovered copy stays cold", singleShard: true, minObjects: 100, restore: true, wantOutcome: monitoring.WarmupSkippedBelowThreshold},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newRecoveringWarmupFixture(t, tenant, 5, tc.minObjects, false)
			if tc.singleShard {
				f.index.partitioningEnabled = false
			}
			shardDir := warmupShardPath(f.dirName, tenant)
			if tc.restore {
				f.restore()
			} else {
				require.NoError(t, os.MkdirAll(shardDir, os.ModePerm))
			}

			require.NoError(t, f.index.PromoteRecoveringLocalShard(context.Background(), tenant))

			lazy, ok := f.index.shards.Load(tenant).(*LazyLoadShard)
			require.True(t, ok)
			require.Equal(t, tc.wantLoaded, lazy.isLoaded())
			if tc.wantLoaded {
				return
			}
			shouldWarm, outcome := f.index.warmupCandidate(tenant)
			require.False(t, shouldWarm)
			require.Equal(t, tc.wantOutcome, outcome)
			if !tc.restore {
				require.NoDirExists(t, filepath.Join(shardDir, "lsm"))
			}
		})
	}
}

func TestLazyShardSelfRecoveryPromotedColdShardLoadsOnDemand(t *testing.T) {
	const tenant = "t"
	ctx := context.Background()
	cases := []struct {
		name       string
		then       func(t *testing.T, index *Index)
		wantLoaded bool
	}{
		{
			name: "tenant activation",
			then: func(t *testing.T, index *Index) {
				require.NoError(t, index.LoadLocalShardForTenantActivation(ctx, tenant, false))
			},
			wantLoaded: true,
		},
		{
			name:       "replica movement",
			then:       func(t *testing.T, index *Index) { require.NoError(t, index.LoadLocalShardForMovement(ctx, tenant)) },
			wantLoaded: true,
		},
		{
			name: "first read",
			then: func(t *testing.T, index *Index) {
				shard, release, err := index.getShardForDirectLocalOperation(ctx, tenant, tenant, localShardOperationRead, 0)
				defer release()
				require.NoError(t, err)
				require.NotNil(t, shard)
			},
			wantLoaded: true,
		},
		{
			name: "next sweep decision",
			then: func(t *testing.T, index *Index) {
				shouldWarm, outcome := index.warmupCandidate(tenant)
				require.False(t, shouldWarm)
				require.Equal(t, monitoring.WarmupSkippedBelowThreshold, outcome)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newRecoveringWarmupFixture(t, tenant, 2, 3, false)
			f.restore()
			require.NoError(t, f.index.PromoteRecoveringLocalShard(ctx, tenant))
			coldWarmupShard(t, f.index, tenant)

			tc.then(t, f.index)

			lazy, ok := f.index.shards.Load(tenant).(*LazyLoadShard)
			require.True(t, ok)
			require.Equal(t, tc.wantLoaded, lazy.isLoaded())
		})
	}
}

func TestLazyShardTenantActivationRecoversMissingDir(t *testing.T) {
	const tenant = "t"
	ctx := context.Background()
	cases := []struct {
		name            string
		replicas        int64
		dirPresent      bool
		submitOK        bool
		activate        func(index *Index) error
		wantRecovering  bool
		wantActivations int
	}{
		{
			name: "activation with folder missing recovers", replicas: 3, submitOK: true, wantRecovering: true, wantActivations: 1,
			activate: func(index *Index) error { return index.LoadLocalShardForTenantActivation(ctx, tenant, false) },
		},
		{
			name: "activation with folder present loads", replicas: 3, dirPresent: true, submitOK: true,
			activate: func(index *Index) error { return index.LoadLocalShardForTenantActivation(ctx, tenant, false) },
		},
		{
			name: "single replica materialises", replicas: 1, submitOK: true,
			activate: func(index *Index) error { return index.LoadLocalShardForTenantActivation(ctx, tenant, false) },
		},
		{
			name: "declined submission materialises", replicas: 3, submitOK: false, wantActivations: 1,
			activate: func(index *Index) error { return index.LoadLocalShardForTenantActivation(ctx, tenant, false) },
		},
		{
			name: "tenant add never recovers", replicas: 3, submitOK: true,
			activate: func(index *Index) error { return index.LoadLocalShardForTenantAdd(ctx, tenant) },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dirName := t.TempDir()
			orch := &fakeSelfRecoveryOrch{enabled: true, submitOK: tc.submitOK}
			index, _ := newWarmupIndexWithOpts(t, dirName, 3, memwatch.NewDummyMonitor(), warmupIndexOpts{orch: orch})
			t.Cleanup(func() { index.Shutdown(context.Background()) })
			index.Config.ReplicationFactor = tc.replicas
			if tc.dirPresent {
				require.NoError(t, os.MkdirAll(shardPath(index.path(), tenant), 0o755))
			}

			require.NoError(t, tc.activate(index))

			require.Equal(t, tc.wantActivations, orch.activationSubmitCalls)
			require.Zero(t, orch.submitCalls)
			_, recovering := index.shards.Load(tenant).(*RecoveringShard)
			require.Equal(t, tc.wantRecovering, recovering)
			if tc.wantRecovering {
				require.NoDirExists(t, shardPath(index.path(), tenant))
			} else {
				require.DirExists(t, shardPath(index.path(), tenant))
			}
		})
	}
}
