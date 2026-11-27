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
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// scanHookSchemaGetter runs onScan once, on the first class lookup a startup reindex scan makes per object.
type scanHookSchemaGetter struct {
	*fakeSchemaGetter
	onScan atomic.Pointer[func()]
}

func (g *scanHookSchemaGetter) ReadOnlyClass(class string) *models.Class {
	if fn := g.onScan.Load(); fn != nil && inStartupReindexScan() && g.onScan.CompareAndSwap(fn, nil) {
		(*fn)()
	}
	return g.fakeSchemaGetter.ReadOnlyClass(class)
}

func inStartupReindexScan() bool {
	pcs := make([]uintptr, 64)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	for {
		f, more := frames.Next()
		if strings.Contains(f.Function, "(*ShardInvertedReindexer).reindexProperties") {
			return true
		}
		if !more {
			return false
		}
	}
}

type startupReindexFixture struct {
	migrator *Migrator
	index    *Index
	getter   *scanHookSchemaGetter
	class    string
	name     string
	ids      []strfmt.UUID
}

func newStartupReindexFixture(t *testing.T, lazy bool, opts ...func(*Config)) *startupReindexFixture {
	t.Helper()
	ctx := testCtx()
	shardState := singleShardState()
	repo, migrator, fake, _ := newLazyLoadRepoWithConfig(t, shardState, lazy, -1, opts...)
	t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })
	getter := &scanHookSchemaGetter{fakeSchemaGetter: fake}
	repo.SetSchemaGetter(getter)

	const className = "StartupReindexPin"
	class := newClassWithWarmProp(className)
	class.InvertedIndexConfig.UsingBlockMaxWAND = false
	require.NoError(t, migrator.AddClass(ctx, class))
	fake.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}

	index := repo.GetIndex(schema.ClassName(className))
	require.NotNil(t, index)
	f := &startupReindexFixture{migrator: migrator, index: index, getter: getter, class: className}
	index.shards.Range(func(name string, _ ShardLike) error {
		f.name = name
		return nil
	})
	require.NotEmpty(t, f.name)

	shard, release, err := index.GetShard(ctx, f.name)
	require.NoError(t, err)
	defer release()
	for range 3 {
		obj := coldTestObject(className)
		require.NoError(t, shard.PutObject(ctx, obj))
		f.ids = append(f.ids, obj.ID())
	}
	return f
}

func (f *startupReindexFixture) loaded(t *testing.T) *Shard {
	t.Helper()
	switch s := f.index.shards.Load(f.name).(type) {
	case *Shard:
		return s
	case *LazyLoadShard:
		require.NotNil(t, s.currentShard())
		return s.currentShard()
	default:
		t.Fatalf("unexpected shard type %T", s)
		return nil
	}
}

func (f *startupReindexFixture) requireIntact(t *testing.T) {
	t.Helper()
	ctx := testCtx()
	shard, release, err := f.index.GetShard(ctx, f.name)
	require.NoError(t, err)
	defer release()
	for _, id := range f.ids {
		ok, err := shard.Exists(ctx, id)
		require.NoError(t, err)
		require.True(t, ok, "object %s survives the reindex", id)
	}
	bucket := shard.Store().Bucket(helpers.BucketFromPropNameLSM("warm"))
	require.NotNil(t, bucket)
	require.Equal(t, lsmkv.StrategyRoaringSet, bucket.Strategy())
}

type startupReindexLoop struct {
	name  string
	setup func(t *testing.T, f *startupReindexFixture)
	run   func(ctx context.Context, f *startupReindexFixture) error
}

func startupReindexLoops() []startupReindexLoop {
	return []startupReindexLoop{
		{
			name:  "inverted reindex",
			setup: func(*testing.T, *startupReindexFixture) {},
			run: func(ctx context.Context, f *startupReindexFixture) error {
				return f.migrator.doInvertedReindex(ctx, map[string]any{
					"ShardInvertedReindexTask_SpecifiedIndex": map[string][]string{f.class: {"warm"}},
				})
			},
		},
		{
			name: "missing text filterable",
			setup: func(t *testing.T, f *startupReindexFixture) {
				require.NoError(t, newFilterableToSearchableMigrationFiles(f.migrator.db.config.RootPath).saveMigrationState(
					&filterableToSearchableMigrationState{
						MissingFilterableClass2Props: map[string]map[string]struct{}{f.class: {"warm": {}}},
						CreatedFilterableClass2Props: map[string]map[string]struct{}{},
					}))
			},
			run: func(ctx context.Context, f *startupReindexFixture) error {
				return f.migrator.doInvertedIndexMissingTextFilterable(ctx,
					map[string]any{"ShardInvertedReindexTaskMissingTextFilterable": nil})
			},
		},
	}
}

type midScanOutcome struct {
	ran, pinned, unloaded bool
	shutdownReturned      bool
	shutdownErr           error
}

func TestStartupReindexPinsTheShardForTheWholeScan(t *testing.T) {
	tests := []struct {
		name    string
		lazy    bool
		opts    []func(*Config)
		midScan func(f *startupReindexFixture, out *midScanOutcome)
		check   func(t *testing.T, f *startupReindexFixture, inner *Shard, out *midScanOutcome)
	}{
		{
			name: "collector configured, idle unload mid-scan",
			lazy: true,
			opts: []func(*Config){withShardUnloader(newFakeShardUnloader())},
			midScan: func(f *startupReindexFixture, out *midScanOutcome) {
				lazy, _ := f.index.shards.Load(f.name).(*LazyLoadShard)
				out.unloaded = lazy.TryUnload(context.Background(), UnloadRequest{})
			},
			check: func(t *testing.T, f *startupReindexFixture, inner *Shard, out *midScanOutcome) {
				require.False(t, out.unloaded, "an idle unload must refuse a shard the startup reindex scans")
				require.False(t, inner.shut.Load())
			},
		},
		{
			name: "no collector, lazy shard shut down mid-scan",
			lazy: true,
			midScan: func(f *startupReindexFixture, out *midScanOutcome) {
				lazy, _ := f.index.shards.Load(f.name).(*LazyLoadShard)
				done := make(chan error, 1)
				enterrors.GoWrapper(func() { done <- lazy.Shutdown(context.Background()) }, f.index.logger)
				select {
				case out.shutdownErr = <-done:
					out.shutdownReturned = true
				case <-time.After(10 * time.Second):
				}
			},
			check: func(t *testing.T, f *startupReindexFixture, inner *Shard, out *midScanOutcome) {
				require.True(t, out.shutdownReturned, "a shutdown mid-scan must return instead of waiting on the scan")
				require.ErrorIs(t, out.shutdownErr, errShardStillInUse)
				require.Eventually(t, inner.shut.Load, 10*time.Second, 10*time.Millisecond,
					"the deferred shutdown completes once the scan releases the shard")
				require.NoError(t, inner.teardownError())
				lazy, _ := f.index.shards.Load(f.name).(*LazyLoadShard)
				lazy.mutex.Lock()
				lazy.shard.Store(nil)
				lazy.mutex.Unlock()
			},
		},
		{
			name:    "no collector, eager shard",
			midScan: func(*startupReindexFixture, *midScanOutcome) {},
			check: func(t *testing.T, f *startupReindexFixture, inner *Shard, out *midScanOutcome) {
				require.False(t, inner.shut.Load())
			},
		},
	}
	for _, loop := range startupReindexLoops() {
		for _, tt := range tests {
			t.Run(loop.name+"/"+tt.name, func(t *testing.T) {
				f := newStartupReindexFixture(t, tt.lazy, tt.opts...)
				loop.setup(t, f)
				inner := f.loaded(t)
				out := &midScanOutcome{}
				hook := func() {
					out.ran = true
					out.pinned = inner.inUseCounter.Load() > 0
					if out.pinned {
						tt.midScan(f, out)
					}
				}
				f.getter.onScan.Store(&hook)

				require.NoError(t, loop.run(testCtx(), f))

				require.True(t, out.ran, "the startup reindex scanned the shard")
				require.True(t, out.pinned, "the startup reindex holds a shard pin while it scans")
				tt.check(t, f, inner, out)
				require.Zero(t, inner.inUseCounter.Load(), "the startup reindex releases its pin")
				f.requireIntact(t)
			})
		}
	}
}
