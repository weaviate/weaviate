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
	"errors"
	"os"
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
			if tt.blockRemoval && os.Geteuid() == 0 {
				t.Skip("root ignores directory permissions, so os.RemoveAll cannot be made to fail")
			}

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
				require.NoError(t, os.Chmod(idx.path(), 0o555))
				t.Cleanup(func() { os.Chmod(idx.path(), 0o755) })
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
