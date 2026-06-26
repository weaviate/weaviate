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

// Hardlinks inside an orphan staging dir pin compaction reclamation until
// the dir is removed; the startup GC must take care of crashes/cancellations
// that escaped the release defer.
func TestStartupRemovesOrphanedReplicaStagingDirs(t *testing.T) {
	root := t.TempDir()

	keepClassDir := filepath.Join(root, "MyClass")
	require.NoError(t, os.MkdirAll(keepClassDir, 0o755))

	orphanReplicaDir := filepath.Join(root, ".replica-staging-op1-myclass-deadbeef")
	require.NoError(t, os.MkdirAll(filepath.Join(orphanReplicaDir, "lsm", "objects"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(orphanReplicaDir, "lsm", "objects", "segment-x.db"),
		[]byte("hardlink-content"), 0o644))

	logger, _ := logrusTest.NewNullLogger()
	require.NoError(t, cleanupRootPathOnStartup(root, logger))

	_, err := os.Stat(orphanReplicaDir)
	require.Truef(t, errors.Is(err, os.ErrNotExist),
		"orphan replica staging dir was not removed: %v", err)

	_, err = os.Stat(keepClassDir)
	require.NoError(t, err, "legit class dir was removed by cleanup")
}

// Initial startup: missing rootPath isn't an error.
func TestStartupCleanupIsNoOpOnMissingRootPath(t *testing.T) {
	root := filepath.Join(t.TempDir(), "never-created")
	logger, _ := logrusTest.NewNullLogger()
	require.NoError(t, cleanupRootPathOnStartup(root, logger))
}
