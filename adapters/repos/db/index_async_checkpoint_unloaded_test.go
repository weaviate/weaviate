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
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/replica/hashtree"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

type unloadedCheckpointFixture struct {
	*addPropertyLazyFixture
	name string
	lazy *LazyLoadShard
	dir  string
}

func newUnloadedCheckpointFixture(t *testing.T, className string, replicated bool) *unloadedCheckpointFixture {
	t.Helper()
	f := newAddPropertyLazyFixture(t, className, singleShardState())
	if replicated {
		m, ok := f.index.schemaReader.(*schemaUC.MockSchemaReader)
		require.True(t, ok)
		for _, c := range m.ExpectedCalls {
			if c.Method == "ShardReplicas" {
				c.ReturnArguments = mock.Arguments{[]string{"node1", "node2"}, error(nil)}
			}
		}
	}
	var name string
	var lazy *LazyLoadShard
	for n, l := range f.coldShards(t) {
		name, lazy = n, l
	}
	return &unloadedCheckpointFixture{
		addPropertyLazyFixture: f,
		name:                   name,
		lazy:                   lazy,
		dir:                    f.index.shardPathHashTree(name),
	}
}

func (f *unloadedCheckpointFixture) evict(t *testing.T) {
	t.Helper()
	f.index.shardCreateLocks.Lock(f.name)
	defer f.index.shardCreateLocks.Unlock(f.name)
	_, ok := f.index.shards.LoadAndDelete(f.name)
	require.True(t, ok)
}

func (f *unloadedCheckpointFixture) status(t *testing.T, ctx context.Context) (hashtree.Digest, int64, time.Time, bool) {
	t.Helper()
	statuses, err := f.index.getAsyncCheckpointShardStatus(ctx, []string{f.name})
	require.NoError(t, err)
	s, ok := statuses[f.name]
	return s.Root, s.CutoffMs, s.CreatedAt, ok
}

func TestUnloadedAsyncCheckpoint_AnswersFromPersistedHashtree(t *testing.T) {
	ctx := testCtx()
	createdAt := time.Now().UTC()
	cutoffMs := createdAt.Add(time.Hour).UnixMilli()

	for _, inMap := range []bool{true, false} {
		name := "in-map lazy shard"
		if !inMap {
			name = "shard absent from the map"
		}
		t.Run(name, func(t *testing.T) {
			f := newUnloadedCheckpointFixture(t, "UnloadedCkpt", true)
			if !inMap {
				f.evict(t)
			}
			_, wantRoot := writePersistedHashtree(t, f.dir, "hashtree-0000000000000001.ht", 7)

			require.NoError(t, f.index.createAsyncCheckpoint(ctx, f.name, cutoffMs, createdAt))
			root, gotCutoff, gotCreatedAt, ok := f.status(t, ctx)
			require.True(t, ok)
			require.Equal(t, wantRoot, root)
			require.Equal(t, cutoffMs, gotCutoff)
			require.Equal(t, createdAt, gotCreatedAt)
			require.FileExists(t, filepath.Join(f.dir, "hashtree-0000000000000001.ht"))

			require.NoError(t, f.index.deleteAsyncCheckpoint(ctx, f.name))
			_, _, _, ok = f.status(t, ctx)
			require.False(t, ok)
			require.False(t, f.lazy.isLoaded())
		})
	}
}

func TestUnloadedAsyncCheckpoint_UnusableSnapshotKeepsTodaysAnswer(t *testing.T) {
	ctx := testCtx()
	createdAt := time.Now().UTC()
	cutoffMs := createdAt.Add(time.Hour).UnixMilli()

	tests := []struct {
		name       string
		replicated bool
		setup      func(t *testing.T, f *unloadedCheckpointFixture)
	}{
		{name: "no snapshot", replicated: true, setup: func(*testing.T, *unloadedCheckpointFixture) {}},
		{name: "corrupt snapshot", replicated: true, setup: func(t *testing.T, f *unloadedCheckpointFixture) {
			require.NoError(t, os.MkdirAll(f.dir, os.ModePerm))
			require.NoError(t, os.WriteFile(filepath.Join(f.dir, "hashtree-0000000000000001.ht"), []byte("garbage"), 0o600))
		}},
		{name: "async replication disabled", replicated: false, setup: func(t *testing.T, f *unloadedCheckpointFixture) {
			writePersistedHashtree(t, f.dir, "hashtree-0000000000000001.ht", 7)
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name+" in map is 412", func(t *testing.T) {
			f := newUnloadedCheckpointFixture(t, "UnloadedCkptInMap", tc.replicated)
			tc.setup(t, f)
			require.ErrorIs(t, f.index.createAsyncCheckpoint(ctx, f.name, cutoffMs, createdAt), errAsyncReplicationNotActive)
			_, _, _, ok := f.status(t, ctx)
			require.False(t, ok)
			require.False(t, f.lazy.isLoaded())
		})
		t.Run(tc.name+" off map is nil", func(t *testing.T) {
			f := newUnloadedCheckpointFixture(t, "UnloadedCkptOffMap", tc.replicated)
			f.evict(t)
			tc.setup(t, f)
			require.NoError(t, f.index.createAsyncCheckpoint(ctx, f.name, cutoffMs, createdAt))
			_, _, _, ok := f.status(t, ctx)
			require.False(t, ok)
		})
	}
}

func TestUnloadedAsyncCheckpoint_MirrorsShardCreateChecks(t *testing.T) {
	ctx := testCtx()
	f := newUnloadedCheckpointFixture(t, "UnloadedCkptChecks", true)
	writePersistedHashtree(t, f.dir, "hashtree-0000000000000001.ht", 7)
	createdAt := time.Now().UTC()
	cutoffMs := createdAt.Add(time.Hour).UnixMilli()

	require.ErrorIs(t, f.index.createAsyncCheckpoint(ctx, f.name, createdAt.Add(-time.Second).UnixMilli(), createdAt), errAsyncCheckpointCutoffInPast)
	require.NoError(t, f.index.createAsyncCheckpoint(ctx, f.name, cutoffMs, createdAt))
	require.ErrorIs(t, f.index.createAsyncCheckpoint(ctx, f.name, cutoffMs, createdAt), errAsyncCheckpointStale)
	require.NoError(t, f.index.createAsyncCheckpoint(ctx, f.name, cutoffMs, createdAt.Add(time.Millisecond)))
	_, _, gotCreatedAt, ok := f.status(t, ctx)
	require.True(t, ok)
	require.Equal(t, createdAt.Add(time.Millisecond), gotCreatedAt)
	require.False(t, f.lazy.isLoaded())
}

func TestUnloadedAsyncCheckpoint_NewestSnapshotWins(t *testing.T) {
	ctx := testCtx()
	f := newUnloadedCheckpointFixture(t, "UnloadedCkptNewest", true)
	writePersistedHashtree(t, f.dir, "hashtree-0000000000000001.ht", 1)
	_, wantRoot := writePersistedHashtree(t, f.dir, "hashtree-0000000000000002.ht", 2)
	createdAt := time.Now().UTC()

	require.NoError(t, f.index.createAsyncCheckpoint(ctx, f.name, createdAt.Add(time.Hour).UnixMilli(), createdAt))
	root, _, _, ok := f.status(t, ctx)
	require.True(t, ok)
	require.Equal(t, wantRoot, root)
}

func TestUnloadedAsyncCheckpoint_SnapshotGoneBeforeStatusIsOmitted(t *testing.T) {
	ctx := testCtx()
	f := newUnloadedCheckpointFixture(t, "UnloadedCkptGone", true)
	filename, _ := writePersistedHashtree(t, f.dir, "hashtree-0000000000000001.ht", 7)
	createdAt := time.Now().UTC()

	require.NoError(t, f.index.createAsyncCheckpoint(ctx, f.name, createdAt.Add(time.Hour).UnixMilli(), createdAt))
	require.NoError(t, os.Remove(filename))
	_, _, _, ok := f.status(t, ctx)
	require.False(t, ok)
	require.False(t, f.lazy.isLoaded())
}

func TestUnloadedAsyncCheckpoint_LoadBeforeStatusTakesLoadedPath(t *testing.T) {
	ctx := testCtx()
	f := newUnloadedCheckpointFixture(t, "UnloadedCkptLoaded", true)
	writePersistedHashtree(t, f.dir, "hashtree-0000000000000001.ht", 7)
	createdAt := time.Now().UTC()
	cutoffMs := createdAt.Add(time.Hour).UnixMilli()

	require.NoError(t, f.index.createAsyncCheckpoint(ctx, f.name, cutoffMs, createdAt))
	require.NoError(t, f.lazy.Load(ctx))
	t.Cleanup(func() { require.NoError(t, f.lazy.Shutdown(context.Background())) })

	_, gotCutoff, _, ok := f.status(t, ctx)
	require.True(t, ok, "a loaded shard always reports")
	require.Zero(t, gotCutoff, "no live checkpoint was created")
	_, registered := f.index.unloadedCheckpoints.get(f.name)
	require.False(t, registered)
}
