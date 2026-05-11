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
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
)

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
		_, err = os.Stat(path.Join(livePath, "marker"))
		require.NoError(t, err)

		require.NoError(t, os.RemoveAll(livePath))
	})

	t.Run("erases_stale_recovery_dir_when_live_dir_already_exists", func(t *testing.T) {
		require.NoError(t, os.MkdirAll(recoveryPath, 0o755))
		require.NoError(t, os.MkdirAll(livePath, 0o755))
		require.NoError(t, os.WriteFile(path.Join(livePath, "live-marker"), []byte("live"), 0o644))

		require.NoError(t, c.PromoteRecoveryFolder(collection, shard))

		_, statErr := os.Stat(recoveryPath)
		require.True(t, errors.Is(statErr, fs.ErrNotExist), "stale recovery dir should be erased, got %v", statErr)
		_, err := os.Stat(path.Join(livePath, "live-marker"))
		require.NoError(t, err, "live dir contents must not be touched")

		require.NoError(t, os.RemoveAll(livePath))
	})

	t.Run("erases_populated_recovery_when_live_exists_which_the_startup_gate_must_prevent", func(t *testing.T) {
		require.NoError(t, os.MkdirAll(recoveryPath, 0o755))
		require.NoError(t, os.WriteFile(path.Join(recoveryPath, "recovered-data"), []byte("data"), 0o644))
		require.NoError(t, os.MkdirAll(livePath, 0o755))

		require.NoError(t, c.PromoteRecoveryFolder(collection, shard))

		_, statErr := os.Stat(recoveryPath)
		require.True(t, errors.Is(statErr, fs.ErrNotExist),
			"an existing live dir is treated as canonical and the recovery is erased — so the startup gate MUST NOT plant an empty live dir")
		_, err := os.Stat(path.Join(livePath, "recovered-data"))
		require.True(t, errors.Is(err, fs.ErrNotExist), "recovered data is not moved into a pre-existing live dir")

		require.NoError(t, os.RemoveAll(livePath))
	})

	t.Run("idempotent_when_live_exists_and_recovery_missing", func(t *testing.T) {
		require.NoError(t, os.MkdirAll(livePath, 0o755))
		require.NoError(t, os.WriteFile(path.Join(livePath, "live-marker"), []byte("live"), 0o644))

		require.NoError(t, c.PromoteRecoveryFolder(collection, shard))

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

	t.Run("errors_when_path_is_a_file_not_a_directory", func(t *testing.T) {
		parent := filepath.Dir(livePath)
		require.NoError(t, os.MkdirAll(parent, 0o755))
		require.NoError(t, os.WriteFile(livePath, []byte("oops"), 0o644))
		require.NoError(t, os.MkdirAll(recoveryPath, 0o755))
		require.NoError(t, os.WriteFile(path.Join(recoveryPath, "marker"), []byte("recovery"), 0o644))

		err := c.PromoteRecoveryFolder(collection, shard)
		require.Error(t, err, "must error when a non-dir occupies the live path")

		_, statErr := os.Stat(path.Join(recoveryPath, "marker"))
		require.NoError(t, statErr, "recovery dir must be left intact on stat error")

		require.NoError(t, os.Remove(livePath))
		require.NoError(t, os.RemoveAll(recoveryPath))
	})
}
