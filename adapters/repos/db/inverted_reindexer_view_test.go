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
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

const (
	specifiedIndexTask        = "ShardInvertedReindexTask_SpecifiedIndex"
	setToRoaringSetTask       = "ShardInvertedReindexTaskSetToRoaringSet"
	missingTextFilterableTask = "ShardInvertedReindexTaskMissingTextFilterable"
)

// serveAt makes the view serve title's index types at gens and waits for the
// retire of the folders it replaced.
func (f *viewFixture) serveAt(gens map[string]uint64) {
	for it, gen := range gens {
		require.NoError(f.t, os.MkdirAll(filepath.Join(f.shard.pathLSM(), reindexBucketName("title", it, gen)), 0o755))
	}
	f.setLive(gens)
	require.NoError(f.t, f.shard.refreshView(context.Background()))
	f.shard.view.retires.Wait()
}

// legacyFolder rewrites a folder of title with one segment of strategy, which
// the folder keeps when it opens with its usual options.
func (f *viewFixture) legacyFolder(indexType string, gen uint64, strategy string) {
	ctx := context.Background()
	name := reindexBucketName("title", indexType, gen)
	if f.shard.store.Bucket(name) != nil {
		require.NoError(f.t, f.shard.store.ShutdownBucket(ctx, name))
	}
	dir := filepath.Join(f.shard.pathLSM(), name)
	require.NoError(f.t, os.RemoveAll(dir))
	b, err := lsmkv.NewBucketCreator().NewBucket(ctx, dir, "", f.idx.logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), lsmkv.WithStrategy(strategy))
	require.NoError(f.t, err)
	if strategy == lsmkv.StrategySetCollection {
		require.NoError(f.t, b.SetAdd([]byte("stale"), [][]byte{make([]byte, 8)}))
	} else {
		require.NoError(f.t, b.MapSet([]byte("stale"), lsmkv.MapPair{Key: make([]byte, 8), Value: make([]byte, 8)}))
	}
	require.NoError(f.t, b.FlushAndSwitch())
	require.NoError(f.t, b.Shutdown(ctx))
	if gen == 0 {
		opts := f.shard.propertyIndexOptions(f.class.Properties[0], indexType, f.shard.makeDefaultBucketOptions)
		require.NoError(f.t, f.shard.store.CreateOrLoadBucket(ctx, name, opts...))
		require.Equal(f.t, strategy, f.shard.store.Bucket(name).Strategy())
	}
}

func (f *viewFixture) reload() {
	require.NoError(f.t, f.shard.Shutdown(context.Background()))
	sl, err := f.idx.initShard(context.Background(), f.shard.name, f.class, nil, true, true)
	require.NoError(f.t, err)
	f.idx.shards.Store(f.shard.name, sl)
	f.shard = sl.(*Shard)
}

func (f *viewFixture) dropPlainFilterable() {
	require.NoError(f.t, f.shard.store.ShutdownBucket(context.Background(), plainTitle))
	require.NoError(f.t, os.RemoveAll(filepath.Join(f.shard.pathLSM(), plainTitle)))
}

func (f *viewFixture) markMissingFilterable() {
	f.shard.setFallbackToSearchable(true)
	require.NoError(f.t, newFilterableToSearchableMigrationFiles(f.idx.db.config.RootPath).saveMigrationState(
		&filterableToSearchableMigrationState{
			MissingFilterableClass2Props: map[string]map[string]struct{}{f.class.Class: {"title": {}}},
			CreatedFilterableClass2Props: map[string]map[string]struct{}{},
		}))
}

func requireMissingFilterableDone(t *testing.T, f *viewFixture) {
	state, err := newFilterableToSearchableMigrationFiles(f.idx.db.config.RootPath).loadMigrationState()
	require.NoError(t, err)
	require.Contains(t, state.CreatedFilterableClass2Props, f.class.Class)
	require.NotContains(t, state.MissingFilterableClass2Props, f.class.Class)
}

func logsOf(logs *test.Hook, level logrus.Level, field, value string) []*logrus.Entry {
	var out []*logrus.Entry
	for _, e := range logs.AllEntries() {
		if e.Level == level && e.Data[field] == value {
			out = append(out, e)
		}
	}
	return out
}

// requirePlainFilterableRebuilt checks a rebuilt plain filterable folder through
// the view, then that a switch past generation 0 retires it cleanly.
func requirePlainFilterableRebuilt(t *testing.T, f *viewFixture, logs *test.Hook) {
	require.Equal(t, lsmkv.StrategyRoaringSet, f.shard.store.Bucket(plainTitle).Strategy())
	require.Equal(t, uint64(5), countHello(t, f.shard.loadView()))
	f.putObjects(1)
	require.Equal(t, uint64(6), countHello(t, f.shard.loadView()), "a write after the rebuild")

	f.serveAt(map[string]uint64{indexTypeSearchable: 2, indexTypeFilterable: 3})
	require.NoDirExists(t, filepath.Join(f.shard.pathLSM(), plainTitle))
	require.Empty(t, logsOf(logs, logrus.ErrorLevel, "action", "reindex_retire"))
}

func genFiles(t *testing.T, f *viewFixture) []string {
	var out []string
	for _, it := range []string{indexTypeFilterable, indexTypeSearchable} {
		entries, err := os.ReadDir(filepath.Join(f.shard.pathLSM(), reindexBucketName("title", it, 2)))
		require.NoError(t, err)
		for _, e := range entries {
			out = append(out, it+"/"+e.Name())
		}
	}
	return out
}

// Startup reindex tasks work on the store's generation-0 folders and leave
// every folder a reindex run serves past generation 0 alone.
func TestInvertedReindexerView(t *testing.T) {
	var gen2Files []string
	tests := []struct {
		name  string
		task  string
		setup func(f *viewFixture)
		check func(t *testing.T, f *viewFixture, logs *test.Hook)
	}{
		{
			name: "a plain filterable rebuilt while searchable is served at generation 2, after a load",
			task: specifiedIndexTask,
			setup: func(f *viewFixture) {
				f.serveAt(map[string]uint64{indexTypeSearchable: 2})
				f.reload()
			},
			check: requirePlainFilterableRebuilt,
		},
		{
			name: "a set-collection plain filterable converted while searchable is served at generation 2",
			task: setToRoaringSetTask,
			setup: func(f *viewFixture) {
				f.legacyFolder(indexTypeFilterable, 0, lsmkv.StrategySetCollection)
				f.serveAt(map[string]uint64{indexTypeSearchable: 2})
			},
			check: requirePlainFilterableRebuilt,
		},
		{
			name: "both index types served at generation 2 are skipped with a warning",
			task: specifiedIndexTask,
			setup: func(f *viewFixture) {
				f.serveAt(map[string]uint64{indexTypeFilterable: 2, indexTypeSearchable: 2})
				f.putObjects(2)
				require.NoError(f.t, f.shard.store.FlushMemtables(context.Background()))
				gen2Files = genFiles(f.t, f)
			},
			check: func(t *testing.T, f *viewFixture, logs *test.Hook) {
				require.NoDirExists(t, filepath.Join(f.shard.pathLSM(), plainTitle))
				require.NoDirExists(t, filepath.Join(f.shard.pathLSM(), reindexBucketName("title", indexTypeSearchable, 0)))
				require.Equal(t, gen2Files, genFiles(t, f))
				warned := logsOf(logs, logrus.WarnLevel, "property", "title")
				require.Len(t, warned, 1)
				require.Contains(t, warned[0].Message, "REINDEX_INDEXES_AT_STARTUP skips indexes a reindex run serves")
			},
		},
		{
			name: "a missing filterable built from a searchable served at generation 2",
			task: missingTextFilterableTask,
			setup: func(f *viewFixture) {
				f.legacyFolder(indexTypeSearchable, 2, lsmkv.StrategyMapCollection)
				f.serveAt(map[string]uint64{indexTypeSearchable: 2})
				f.dropPlainFilterable()
				f.markMissingFilterable()
			},
			check: func(t *testing.T, f *viewFixture, _ *test.Hook) {
				require.NotNil(t, f.shard.loadView().Bucket(plainTitle))
				require.Equal(t, uint64(5), countHello(t, f.shard.loadView()))
				require.False(t, f.shard.isFallbackToSearchable())
				requireMissingFilterableDone(t, f)
			},
		},
		{
			name: "a filterable a run serves at generation 2 is not built again",
			task: missingTextFilterableTask,
			setup: func(f *viewFixture) {
				f.legacyFolder(indexTypeSearchable, 0, lsmkv.StrategyMapCollection)
				f.serveAt(map[string]uint64{indexTypeFilterable: 2})
				f.markMissingFilterable()
			},
			check: func(t *testing.T, f *viewFixture, _ *test.Hook) {
				require.NoDirExists(t, filepath.Join(f.shard.pathLSM(), plainTitle))
				requireMissingFilterableDone(t, f)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newViewFixture(t, "ViewTest")
			logger, logs := test.NewNullLogger()
			f.idx.logger = logger
			f.putObjects(5)
			tc.setup(f)
			f.idx.db.registerIndex(f.idx)
			args := map[string]any{tc.task: nil}
			if tc.task == specifiedIndexTask {
				args[tc.task] = map[string][]string{f.class.Class: {"title"}}
			}
			require.NoError(t, NewMigrator(f.idx.db, logger, "node1").InvertedReindex(context.Background(), args))
			tc.check(t, f, logs)
		})
	}
}
