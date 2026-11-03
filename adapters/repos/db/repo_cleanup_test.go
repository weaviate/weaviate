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
	"path/filepath"
	"testing"

	logrusTest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// cleanupRootPathOnStartup removes staging dirs a crash left behind. Replica
// staging holds hardlinks that pin compaction reclamation. Backup staging holds
// file.CopyFile copies, which skip fsync because no restart keeps them.
func TestStartupRemovesOrphanedStagingDirs(t *testing.T) {
	tests := []struct {
		name       string
		stagingDir func(root string) string
	}{
		{
			name:       "replica staging",
			stagingDir: func(root string) string { return replicaStagingDir(root, "op1", "MyClass") },
		},
		{
			name:       "backup staging",
			stagingDir: func(root string) string { return backupStagingDir(root, "backup1", "MyClass") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()

			keepClassDir := filepath.Join(root, "MyClass")
			require.NoError(t, os.MkdirAll(keepClassDir, 0o755))

			orphanDir := tt.stagingDir(root)
			require.NoError(t, os.MkdirAll(filepath.Join(orphanDir, "lsm", "objects"), 0o755))
			require.NoError(t, os.WriteFile(
				filepath.Join(orphanDir, "lsm", "objects", "segment-x.db"),
				[]byte("staged-content"), 0o644))

			logger, _ := logrusTest.NewNullLogger()
			require.NoError(t, cleanupRootPathOnStartup(root, logger))

			_, err := os.Stat(orphanDir)
			require.Truef(t, errors.Is(err, os.ErrNotExist),
				"orphan staging dir was not removed: %v", err)

			_, err = os.Stat(keepClassDir)
			require.NoError(t, err, "legit class dir was removed by cleanup")
		})
	}
}

// Initial startup: missing rootPath isn't an error.
func TestStartupCleanupIsNoOpOnMissingRootPath(t *testing.T) {
	root := filepath.Join(t.TempDir(), "never-created")
	logger, _ := logrusTest.NewNullLogger()
	require.NoError(t, cleanupRootPathOnStartup(root, logger))
}
