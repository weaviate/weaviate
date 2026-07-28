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
	"io/fs"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// Pins: markMigrationDone records the flag where the next startup looks for
// it, so the compressed-vectors migration is not re-run.
func TestMarkMigrationDone_WritesFlag(t *testing.T) {
	lsmDir := t.TempDir()
	m := newCompressedVectorsMigrator(logrus.New())

	require.NoError(t, m.markMigrationDone(lsmDir))

	require.DirExists(t, m.migrationDirectory(lsmDir))
	require.FileExists(t, m.migrationPerformedFlagFile(lsmDir))

	// Second call must not fail on the already-present dir and flag.
	require.NoError(t, m.markMigrationDone(lsmDir))
}

// Pins: both durability steps in markMigrationDone propagate their failure
// instead of swallowing it. This is a behaviour change, not just a durability
// one: the pre-PR code used os.Create with a deferred, unchecked Close, so a
// failure here returned nil and the flag was reported as written. A silently
// missing flag makes the next startup re-run the whole migration.
//
// A successful fsync leaves no trace, so each case observes it through its
// failure: 0o311 is write+execute, enough for Mkdir and for the flag write,
// but not for the O_RDONLY open inside diskio.Fsync.
func TestMarkMigrationDone_PropagatesFsyncFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-based test cannot run as root")
	}

	const noReadDir = 0o311

	tests := []struct {
		name string
		// setup returns the dir whose fsync must fail.
		setup func(t *testing.T, m compressedVectorsMigrator, lsmDir string) string
	}{
		{
			// Mkdir creates .migrations, so <lsm> is the level that must be
			// fsynced to make that new dir entry durable.
			name: ".migrations created, <lsm> level fsynced",
			setup: func(t *testing.T, _ compressedVectorsMigrator, lsmDir string) string {
				require.NoError(t, os.Chmod(lsmDir, noReadDir))
				t.Cleanup(func() { _ = os.Chmod(lsmDir, 0o777) })
				return lsmDir
			},
		},
		{
			// .migrations already exists, so Mkdir is skipped entirely and the
			// only fsync left is the flag file's parent inside WriteFileSync.
			name: ".migrations exists, flag file parent fsynced",
			setup: func(t *testing.T, m compressedVectorsMigrator, lsmDir string) string {
				migDir := m.migrationDirectory(lsmDir)
				require.NoError(t, os.Mkdir(migDir, 0o755))
				require.NoError(t, os.Chmod(migDir, noReadDir))
				t.Cleanup(func() { _ = os.Chmod(migDir, 0o777) })
				return migDir
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lsmDir := t.TempDir()
			m := newCompressedVectorsMigrator(logrus.New())
			wantFsyncedDir := test.setup(t, m, lsmDir)

			err := m.markMigrationDone(lsmDir)

			require.Error(t, err,
				"markMigrationDone must fsync %s and propagate its failure", wantFsyncedDir)
			require.ErrorIs(t, err, fs.ErrPermission)

			var pathErr *fs.PathError
			require.ErrorAs(t, err, &pathErr)
			require.Equal(t, wantFsyncedDir, pathErr.Path)
		})
	}
}
