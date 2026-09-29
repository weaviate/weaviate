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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	esync "github.com/weaviate/weaviate/entities/sync"
)

type stubShutdownShard struct {
	ShardLike
	shutdowns   int
	shutdownErr error
}

func (s *stubShutdownShard) Shutdown(context.Context) error {
	s.shutdowns++
	return s.shutdownErr
}

func newDemoteIndex(t *testing.T) *Index {
	t.Helper()
	idx := newRecoveringIndexWith(t, func(i *Index) {
		i.Config.EnableLazyLoadShards = true
		i.Config.LazyLoadShardWarmupMinObjects = -1
	})
	idx.closeRequestedCtx = context.Background()
	idx.metrics = &Metrics{}
	return idx
}

func writeShardFile(t *testing.T, dir, name, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, os.ModePerm))
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644))
}

func requireShardFile(t *testing.T, dir, name, content string) {
	t.Helper()
	got, err := os.ReadFile(filepath.Join(dir, name))
	require.NoError(t, err)
	require.Equal(t, content, string(got))
}

func TestDemoteRecoveredLocalShard(t *testing.T) {
	tests := []struct {
		name          string
		prepare       func(t *testing.T, idx *Index, live, recovery string) ShardLike
		wantErr       bool
		wantRecovery  bool
		wantBlocked   bool
		wantSameEntry bool
		wantNoEntry   bool
	}{
		{
			name: "promoted cold lazy shard moves back behind the load block",
			prepare: func(t *testing.T, idx *Index, live, _ string) ShardLike {
				writeShardFile(t, live, "segment.db", "copy")
				require.NoError(t, idx.PromoteRecoveringLocalShard(context.Background(), "S"))
				_, ok := idx.shards.Load("S").(*LazyLoadShard)
				require.True(t, ok)
				return nil
			},
			wantRecovery: true,
			wantBlocked:  true,
		},
		{
			name: "loaded shard is shut down before the move",
			prepare: func(t *testing.T, idx *Index, live, _ string) ShardLike {
				writeShardFile(t, live, "segment.db", "copy")
				stub := &stubShutdownShard{}
				idx.shards.Store("S", stub)
				return stub
			},
			wantRecovery: true,
			wantBlocked:  true,
		},
		{
			name: "stale staging dir is replaced by the promoted copy",
			prepare: func(t *testing.T, idx *Index, live, recovery string) ShardLike {
				writeShardFile(t, recovery, "stale.db", "stale")
				writeShardFile(t, live, "segment.db", "copy")
				require.NoError(t, idx.PromoteRecoveringLocalShard(context.Background(), "S"))
				return nil
			},
			wantRecovery: true,
			wantBlocked:  true,
		},
		{
			name: "folder promoted but shard still blocked keeps its entry",
			prepare: func(t *testing.T, idx *Index, live, _ string) ShardLike {
				writeShardFile(t, live, "segment.db", "copy")
				return idx.shards.Load("S")
			},
			wantRecovery:  true,
			wantBlocked:   true,
			wantSameEntry: true,
		},
		{
			name: "unregistered shard of a cold tenant stays unregistered",
			prepare: func(t *testing.T, idx *Index, live, _ string) ShardLike {
				idx.shards.LoadAndDelete("S")
				writeShardFile(t, live, "segment.db", "copy")
				return nil
			},
			wantRecovery: true,
			wantNoEntry:  true,
		},
		{
			name: "never promoted is a no-op",
			prepare: func(t *testing.T, idx *Index, _, _ string) ShardLike {
				return idx.shards.Load("S")
			},
			wantBlocked:   true,
			wantSameEntry: true,
		},
		{
			name: "failed shutdown keeps the live dir and the shard",
			prepare: func(t *testing.T, idx *Index, live, _ string) ShardLike {
				writeShardFile(t, live, "segment.db", "copy")
				stub := &stubShutdownShard{shutdownErr: errors.New("in use")}
				idx.shards.Store("S", stub)
				return stub
			},
			wantErr:       true,
			wantSameEntry: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idx := newDemoteIndex(t)
			live := shardPath(idx.path(), "S")
			recovery := live + api.RecoveryFolderSuffix
			before := tc.prepare(t, idx, live, recovery)

			err := idx.DemoteRecoveredLocalShard(context.Background(), "S")
			if tc.wantErr {
				require.Error(t, err)
				requireShardFile(t, live, "segment.db", "copy")
				require.Same(t, before, idx.shards.Load("S"))
				return
			}
			require.NoError(t, err)
			require.NoDirExists(t, live)
			if tc.wantRecovery {
				requireShardFile(t, recovery, "segment.db", "copy")
				require.NoFileExists(t, filepath.Join(recovery, "stale.db"))
			} else {
				require.NoDirExists(t, recovery)
			}
			if stub, ok := before.(*stubShutdownShard); ok {
				require.Equal(t, 1, stub.shutdowns)
			}
			entry := idx.shards.Load("S")
			if tc.wantNoEntry {
				require.Nil(t, entry)
				return
			}
			rec, ok := entry.(*RecoveringShard)
			require.True(t, ok)
			require.Equal(t, tc.wantBlocked, rec.isLoadBlocked())
			if tc.wantSameEntry {
				require.Same(t, before, entry)
			}
		})
	}
}

func TestDemoteThenRepromoteAcrossRounds(t *testing.T) {
	idx := newDemoteIndex(t)
	live := shardPath(idx.path(), "S")
	recovery := live + api.RecoveryFolderSuffix

	for round, content := range []string{"first", "second", "third"} {
		writeShardFile(t, recovery, "segment.db", content)
		require.NoError(t, os.Rename(recovery, live))
		require.NoError(t, idx.PromoteRecoveringLocalShard(context.Background(), "S"))
		lazy, ok := idx.shards.Load("S").(*LazyLoadShard)
		require.True(t, ok, "round %d", round)
		require.False(t, lazy.isLoadBlocked())
		requireShardFile(t, live, "segment.db", content)

		require.NoError(t, idx.DemoteRecoveredLocalShard(context.Background(), "S"))
		require.NoDirExists(t, live)
		rec, ok := idx.shards.Load("S").(*RecoveringShard)
		require.True(t, ok, "round %d", round)
		require.True(t, rec.isLoadBlocked())
	}
}

func TestDemoteRecoveredLocalShardOnClosedIndex(t *testing.T) {
	idx := newTestIndexForRecovery(t, &fakeSelfRecoveryOrch{enabled: true, submitOK: true})
	idx.shardCreateLocks = esync.NewKeyRWLocker()
	idx.closed = true
	require.NoError(t, os.MkdirAll(shardPath(idx.path(), "S"), os.ModePerm))

	require.ErrorIs(t, idx.DemoteRecoveredLocalShard(context.Background(), "S"), errAlreadyShutdown)
	require.DirExists(t, shardPath(idx.path(), "S"))
}
