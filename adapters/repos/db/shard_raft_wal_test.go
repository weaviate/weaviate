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
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/shard"
)

func TestIsRaftReplicated(t *testing.T) {
	dummyRaft := shard.NewRaft(shard.RaftConfig{
		Logger: logrus.New(),
	})

	tests := []struct {
		name     string
		config   bool
		raft     *shard.Raft
		expected bool
	}{
		{
			name:     "both config and raft set",
			config:   true,
			raft:     dummyRaft,
			expected: true,
		},
		{
			name:     "config true but raft nil",
			config:   true,
			raft:     nil,
			expected: false,
		},
		{
			name:     "config false but raft set",
			config:   false,
			raft:     dummyRaft,
			expected: false,
		},
		{
			name:     "both config and raft unset",
			config:   false,
			raft:     nil,
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Shard{
				index: &Index{
					Config: IndexConfig{
						RaftReplicationEnabled: tc.config,
					},
					raft: tc.raft,
				},
			}
			assert.Equal(t, tc.expected, s.isRaftReplicated())
		})
	}
}

func TestWriteWALsGuard(t *testing.T) {
	ctx := testCtx()

	t.Run("non-raft shard calls store WriteWALs", func(t *testing.T) {
		shd, _ := testShard(t, ctx, "NonRaftClass")
		concreteShard := shd.(*Shard)

		// For a non-RAFT shard, writeWALs should succeed (delegates to store)
		err := concreteShard.writeWALs()
		require.NoError(t, err)

		// Verify isRaftReplicated is false
		assert.False(t, concreteShard.isRaftReplicated())
	})

	t.Run("raft shard skips store WriteWALs", func(t *testing.T) {
		shd, _ := testShard(t, ctx, "RaftClass", func(idx *Index) {
			idx.Config.RaftReplicationEnabled = true
			idx.raft = shard.NewRaft(shard.RaftConfig{
				Logger: logrus.New(),
			})
		})
		concreteShard := shd.(*Shard)

		// For a RAFT shard, writeWALs should be a no-op
		err := concreteShard.writeWALs()
		require.NoError(t, err)

		// Verify isRaftReplicated is true
		assert.True(t, concreteShard.isRaftReplicated())
	})
}

func TestRaftShardNoWALFiles(t *testing.T) {
	ctx := testCtx()

	t.Run("raft shard creates no WAL files", func(t *testing.T) {
		shd, _ := testShard(t, ctx, "RaftWALClass", func(idx *Index) {
			idx.Config.RaftReplicationEnabled = true
			idx.raft = shard.NewRaft(shard.RaftConfig{
				Logger: logrus.New(),
			})
		})
		concreteShard := shd.(*Shard)

		// Write an object to trigger bucket creation
		obj := testObject("RaftWALClass")
		err := concreteShard.PutObject(ctx, obj)
		require.NoError(t, err)

		// Walk the LSM directory and verify no .wal files exist
		lsmPath := concreteShard.pathLSM()
		walFiles := findWALFiles(t, lsmPath)
		assert.Empty(t, walFiles, "RAFT shard should not create .wal files, found: %v", walFiles)
	})

	t.Run("non-raft shard creates WAL files", func(t *testing.T) {
		shd, _ := testShard(t, ctx, "NonRaftWALClass")
		concreteShard := shd.(*Shard)

		// Write an object to trigger WAL writes
		obj := testObject("NonRaftWALClass")
		err := concreteShard.PutObject(ctx, obj)
		require.NoError(t, err)

		// Walk the LSM directory and verify .wal files exist
		lsmPath := concreteShard.pathLSM()
		walFiles := findWALFiles(t, lsmPath)
		assert.NotEmpty(t, walFiles, "non-RAFT shard should create .wal files")
	})
}

// findWALFiles walks the directory tree and returns paths of all .wal files.
func findWALFiles(t *testing.T, dir string) []string {
	t.Helper()
	var walFiles []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(info.Name()) == ".wal" {
			walFiles = append(walFiles, path)
		}
		return nil
	})
	require.NoError(t, err)
	return walFiles
}
