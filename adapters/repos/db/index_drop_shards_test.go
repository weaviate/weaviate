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
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/schema"
	esync "github.com/weaviate/weaviate/entities/sync"
)

func TestIndexDropShards(t *testing.T) {
	const (
		shardName = "shard1"
		// what ShardLike.ID() reports for a loaded shard of class "Abc"
		loadedShardID = "abc_" + shardName
	)

	tests := []struct {
		name string
		// loaded stores a mock shard under shardName, so drop() runs instead of
		// removing the shard directory
		loaded       bool
		shardDropErr error
		// blockRemoval makes the index directory read-only so os.RemoveAll fails
		blockRemoval    bool
		wantErrContains string
		wantShardLog    string
	}{
		{
			name: "unloaded shard is removed from disk",
		},
		{
			name:            "unloaded shard removal failure is reported",
			blockRemoval:    true,
			wantErrContains: "permission denied",
			wantShardLog:    shardName,
		},
		{
			name:   "loaded shard is dropped",
			loaded: true,
		},
		{
			name:            "loaded shard drop failure is reported",
			loaded:          true,
			shardDropErr:    errors.New("drop failed"),
			wantErrContains: "drop failed",
			wantShardLog:    loadedShardID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			idx := &Index{
				logger:           logger,
				shards:           shardMap{},
				backupLock:       esync.NewKeyRWLocker(),
				shardCreateLocks: esync.NewKeyRWLocker(),
				Config:           IndexConfig{RootPath: t.TempDir(), ClassName: schema.ClassName("Abc")},
			}

			shardDir := shardPath(idx.path(), shardName)
			require.NoError(t, os.MkdirAll(shardDir, 0o755))

			if tt.loaded {
				shard := NewMockShardLike(t)
				shard.EXPECT().ID().Return(loadedShardID).Maybe()
				shard.EXPECT().drop(false).Return(tt.shardDropErr).Once()
				idx.shards.Store(shardName, shard)
			}

			if tt.blockRemoval {
				blockRemovalIn(t, idx.path())
			}

			err := idx.dropShards([]string{shardName})

			for _, entry := range hook.AllEntries() {
				require.NotContains(t, entry.Message, "Recovered from panic")
			}

			if tt.wantErrContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErrContains)
				require.Equal(t, tt.wantShardLog, dropShardLogShard(t, hook))
			}

			require.Nil(t, idx.shards.Load(shardName))
			if !tt.loaded {
				_, err := os.Stat(shardDir)
				require.Equal(t, tt.blockRemoval, err == nil)
			}
		})
	}
}

// TestIndexDropShardsCollectsConcurrentFailures pins that no shard error is lost
// when many shards are dropped in parallel. Run with -race.
func TestIndexDropShardsCollectsConcurrentFailures(t *testing.T) {
	const shardCount = 32

	tests := []struct {
		name string
		drop func(t *testing.T, idx *Index, names []string) error
		// appears once per failed shard in the returned error
		wantErrPerShard string
	}{
		{
			name: "dropShards",
			drop: func(t *testing.T, idx *Index, names []string) error {
				blockRemovalIn(t, idx.path())
				return idx.dropShards(names)
			},
			wantErrPerShard: "permission denied",
		},
		{
			name: "dropCloudShards",
			drop: func(t *testing.T, idx *Index, names []string) error {
				cloud := fakeOffloadCloud{deleteErrs: map[string]error{}}
				for _, name := range names {
					cloud.deleteErrs[name] = errors.New("cloud delete failed")
				}
				return idx.dropCloudShards(context.Background(), cloud, names, "node1")
			},
			wantErrPerShard: "cloud delete failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			idx := &Index{
				logger:           logger,
				shards:           shardMap{},
				backupLock:       esync.NewKeyRWLocker(),
				shardCreateLocks: esync.NewKeyRWLocker(),
				Config:           IndexConfig{RootPath: t.TempDir(), ClassName: schema.ClassName("Abc")},
			}

			names := make([]string, shardCount)
			for i := range names {
				names[i] = fmt.Sprintf("shard%d", i)
				require.NoError(t, os.MkdirAll(shardPath(idx.path(), names[i]), 0o755))
			}

			err := tt.drop(t, idx, names)

			require.Error(t, err)
			require.Equal(t, shardCount, strings.Count(err.Error(), tt.wantErrPerShard))
		})
	}
}

func TestIndexDropCloudShards(t *testing.T) {
	tests := []struct {
		name            string
		names           []string
		deleteErrs      map[string]error
		wantErrContains string
	}{
		{
			name:  "no shard to drop",
			names: []string{},
		},
		{
			name:  "every shard is deleted",
			names: []string{"shard1", "shard2", "shard3"},
		},
		{
			name:            "only the failing shard is reported",
			names:           []string{"shard1", "shard2", "shard3"},
			deleteErrs:      map[string]error{"shard2": errors.New("cloud delete failed")},
			wantErrContains: "cloud delete failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			idx := &Index{
				logger:           logger,
				shards:           shardMap{},
				backupLock:       esync.NewKeyRWLocker(),
				shardCreateLocks: esync.NewKeyRWLocker(),
				Config:           IndexConfig{RootPath: t.TempDir(), ClassName: schema.ClassName("Abc")},
			}

			cloud := fakeOffloadCloud{deleteErrs: tt.deleteErrs}
			err := idx.dropCloudShards(context.Background(), cloud, tt.names, "node1")

			if tt.wantErrContains == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErrContains)
			require.Equal(t, len(tt.deleteErrs), strings.Count(err.Error(), tt.wantErrContains))
		})
	}
}

// blockRemovalIn makes dir read-only so os.RemoveAll fails for its contents.
func blockRemovalIn(t *testing.T, dir string) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions, so os.RemoveAll cannot be made to fail")
	}
	require.NoError(t, os.Chmod(dir, 0o555))
	t.Cleanup(func() { os.Chmod(dir, 0o755) })
}

// fakeOffloadCloud fails Delete for the shards named in deleteErrs and succeeds
// for the rest.
type fakeOffloadCloud struct {
	deleteErrs map[string]error
}

func (c fakeOffloadCloud) VerifyBucket(ctx context.Context) error { return nil }

func (c fakeOffloadCloud) Upload(ctx context.Context, className, shardName, nodeName string) error {
	return nil
}

func (c fakeOffloadCloud) Download(ctx context.Context, className, shardName, nodeName string) error {
	return nil
}

func (c fakeOffloadCloud) Delete(ctx context.Context, className, shardName, nodeName string) error {
	return c.deleteErrs[shardName]
}

func dropShardLogShard(t *testing.T, hook *test.Hook) any {
	t.Helper()
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.ErrorLevel && entry.Data["action"] == "drop_shard" {
			return entry.Data["shard"]
		}
	}
	t.Fatalf("no drop_shard error log entry, got %d entries", len(hook.AllEntries()))
	return nil
}
