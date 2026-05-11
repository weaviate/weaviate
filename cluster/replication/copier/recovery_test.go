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

package copier

import (
	"errors"
	"io/fs"
	"os"
	"path"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
)

// TestRewriteRelPathToLocalShard exercises the path-rewriter used by
// CopyReplicaFilesToLocalShard so files reported by the source under
// "<lower(coll)>/<srcShard>/..." land at "<lower(coll)>/<dstShard>/..."
// when an override is supplied (e.g. SELF_RECOVERY's ".recovering"
// sibling directory).
func TestRewriteRelPathToLocalShard(t *testing.T) {
	c := &Copier{rootDataPath: "/data", logger: logrus.New()}

	tests := []struct {
		name          string
		src, srcShard string
		localShard    string
		want          string
	}{
		{
			name:       "no_override_passes_through",
			src:        "myclass/shard1/segment-1.db",
			srcShard:   "shard1",
			localShard: "shard1",
			want:       "myclass/shard1/segment-1.db",
		},
		{
			name:       "rewrites_shard_segment",
			src:        "myclass/shard1/segment-1.db",
			srcShard:   "shard1",
			localShard: "shard1.recovering",
			want:       "myclass/shard1.recovering/segment-1.db",
		},
		{
			name:       "deep_subpath_preserved",
			src:        "myclass/shard1/lsm/objects/segment-3.db",
			srcShard:   "shard1",
			localShard: "shard1.recovering",
			want:       "myclass/shard1.recovering/lsm/objects/segment-3.db",
		},
		{
			name:       "non_matching_segment_unchanged",
			src:        "myclass/different/file.db",
			srcShard:   "shard1",
			localShard: "shard1.recovering",
			want:       "myclass/different/file.db",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := c.rewriteRelPathToLocalShard(tc.src, tc.srcShard, tc.localShard)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestPromoteRecoveryFolder verifies the rename + parent fsync path and
// the idempotent variants the recovery flow relies on across crashes.
func TestPromoteRecoveryFolder(t *testing.T) {
	root := t.TempDir()
	c := &Copier{rootDataPath: root, logger: logrus.New()}

	collection := "MyClass"
	shard := "shard1"
	livePath := c.shardPath(collection, shard)
	recoveryPath := c.shardPath(collection, api.RecoveryFolderName(shard))

	t.Run("happy_path_renames_recovery_into_place", func(t *testing.T) {
		require.NoError(t, os.MkdirAll(recoveryPath, 0o755))
		require.NoError(t, os.WriteFile(path.Join(recoveryPath, "marker"), []byte("ok"), 0o644))

		require.NoError(t, c.PromoteRecoveryFolder(collection, shard))

		_, err := os.Stat(recoveryPath)
		require.True(t, errors.Is(err, fs.ErrNotExist), "recovery dir should be gone, got %v", err)
		info, err := os.Stat(livePath)
		require.NoError(t, err)
		require.True(t, info.IsDir())
		// Marker file survived the rename.
		_, err = os.Stat(path.Join(livePath, "marker"))
		require.NoError(t, err)

		require.NoError(t, os.RemoveAll(livePath))
	})

	t.Run("erases_stale_recovery_dir_when_live_dir_already_exists", func(t *testing.T) {
		// Setup: both dirs present (e.g. an earlier promote partially
		// succeeded — rename happened, parent fsync survived, then
		// orchestrator crashed before clearing local state, and on the
		// next attempt the recovery dir is rebuilt from another retry).
		require.NoError(t, os.MkdirAll(recoveryPath, 0o755))
		require.NoError(t, os.MkdirAll(livePath, 0o755))
		require.NoError(t, os.WriteFile(path.Join(livePath, "live-marker"), []byte("live"), 0o644))

		require.NoError(t, c.PromoteRecoveryFolder(collection, shard))

		// Stale recovery dir is erased; the live dir is the canonical
		// state and remains untouched.
		_, statErr := os.Stat(recoveryPath)
		require.True(t, errors.Is(statErr, fs.ErrNotExist), "stale recovery dir should be erased, got %v", statErr)
		_, err := os.Stat(path.Join(livePath, "live-marker"))
		require.NoError(t, err, "live dir contents must not be touched")

		require.NoError(t, os.RemoveAll(livePath))
	})

	t.Run("idempotent_when_live_exists_and_recovery_missing", func(t *testing.T) {
		// Crash-after-rename scenario: a previous attempt successfully
		// renamed the recovery dir into place but the consumer crashed
		// before the op reached READY. RAFT log replay re-invokes
		// PromoteRecoveryFolder; this must be a no-op success.
		require.NoError(t, os.MkdirAll(livePath, 0o755))
		require.NoError(t, os.WriteFile(path.Join(livePath, "live-marker"), []byte("live"), 0o644))

		require.NoError(t, c.PromoteRecoveryFolder(collection, shard))

		// Live dir must be untouched; no recovery dir should appear.
		_, err := os.Stat(path.Join(livePath, "live-marker"))
		require.NoError(t, err, "live dir contents must not be touched")
		_, err = os.Stat(recoveryPath)
		require.True(t, errors.Is(err, fs.ErrNotExist), "no recovery dir should appear, got %v", err)

		require.NoError(t, os.RemoveAll(livePath))
	})

	t.Run("errors_when_both_dirs_missing", func(t *testing.T) {
		err := c.PromoteRecoveryFolder(collection, shard)
		require.Error(t, err)
	})
}
