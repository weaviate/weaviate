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
	"github.com/weaviate/weaviate/entities/backup"
)

// Hardlinks inside an orphan staging dir pin compaction reclamation until the
// dir is removed; the startup GC must take care of crashes/cancellations that
// escaped the release defer. The sweep matches on the staging prefixes, not
// reconstructed names, which is what lets the backup staging dir carry a
// per-operation-instance fence.
func TestStartupRemovesOrphanedStagingDirs(t *testing.T) {
	tests := []struct {
		name       string
		stagingDir func(root string) string
	}{
		{
			name: "replica staging dir",
			stagingDir: func(root string) string {
				return filepath.Join(root, ".replica-staging-op1-myclass-deadbeef")
			},
		},
		{
			name: "fenced backup staging dir",
			stagingDir: func(root string) string {
				return backupStagingDir(root, backup.NewOp("op1"), "MyClass")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()

			keepClassDir := filepath.Join(root, "MyClass")
			require.NoError(t, os.MkdirAll(keepClassDir, 0o755))

			staging := tt.stagingDir(root)
			require.NoError(t, os.MkdirAll(filepath.Join(staging, "lsm", "objects"), 0o755))
			require.NoError(t, os.WriteFile(
				filepath.Join(staging, "lsm", "objects", "segment-x.db"),
				[]byte("hardlink-content"), 0o644))

			logger, _ := logrusTest.NewNullLogger()
			require.NoError(t, cleanupRootPathOnStartup(root, logger))

			_, err := os.Stat(staging)
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
