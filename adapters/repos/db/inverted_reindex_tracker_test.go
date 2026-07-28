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
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestReindexTracker(t *testing.T) *fileReindexTracker {
	t.Helper()
	tr := NewFileReindexTracker(t.TempDir(), "test_migration_1", &UuidKeyParser{})
	require.NoError(t, tr.init())
	return tr
}

// writeTornProgressFile marks progress once (advancing the real checkpoint
// to index 1), then overwrites the on-disk checkpoint file with the given
// (deliberately malformed) content, mirroring a torn/interrupted write.
// Returns the checkpoint path.
func writeTornProgressFile(t *testing.T, tr *fileReindexTracker, key indexKey, torn string) string {
	t.Helper()
	require.NoError(t, tr.markProgress(key, 10, 5))

	progressPath := filepath.Join(tr.config.migrationPath, "progress.mig.000000001")
	require.NoError(t, os.WriteFile(progressPath, []byte(torn), 0o600))
	return progressPath
}

// Pins: a torn progress checkpoint must resume from scratch, not panic or misparse into a stale key.
func TestFileReindexTracker_GetProgressTornSentinel(t *testing.T) {
	parser := &UuidKeyParser{}
	key, err := parser.FromString("11111111-1111-1111-1111-111111111111")
	require.NoError(t, err)

	tornVariants := map[string]string{
		"zero length":            "",
		"only whitespace":        "   ",
		"single line no newline": "2026-07-16T10:00:00Z",
		"two lines truncated":    "2026-07-16T10:00:00Z\n11111111-1111-1111-1111-111111111111",
	}

	for name, torn := range tornVariants {
		t.Run(name, func(t *testing.T) {
			tr := newTestReindexTracker(t)
			writeTornProgressFile(t, tr, key, torn)

			// A fresh tracker mirrors a post-restart read with no in-memory state.
			tr2 := NewFileReindexTracker(filepath.Dir(filepath.Dir(tr.config.migrationPath)),
				"test_migration_1", parser)

			require.NotPanics(t, func() {
				gotKey, tm, err := tr2.GetProgress()
				require.NoError(t, err)
				require.Nil(t, tm)
				require.Empty(t, gotKey.Bytes(), "torn checkpoint must resume from scratch, not a stale key")
			})

			// Counter advanced past the torn checkpoint, so the next write is .000000002.
			require.NoError(t, tr2.markProgress(key, 20, 10))
			require.FileExists(t, filepath.Join(tr2.config.migrationPath, "progress.mig.000000002"))
		})
	}
}

// Pins: init() must fsync both directory levels MkdirAll can create
// (<lsm>/.migrations and <lsm> itself), not just the immediate parent.
//
// Durability is not observable from the resulting tree — the directories are
// there whether or not they were fsynced — so each case observes the syscall
// through its error instead: it strips read permission from exactly one level.
// diskio.Fsync opens O_RDONLY and fails with EACCES on that level, while
// MkdirAll needs only write+execute and still creates the tree. Dropping
// either Fsync call therefore turns the expected error into a nil.
func TestFileReindexTracker_InitFsyncsNewParentLevels(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-based test cannot run as root")
	}

	// 0o311 is write+execute: enough for MkdirAll to create and traverse,
	// but not enough for the O_RDONLY open inside diskio.Fsync.
	const noReadDir = 0o311

	tests := []struct {
		name string
		// setup returns the level whose fsync must fail.
		setup func(t *testing.T, lsmPath string) string
	}{
		{
			name: "child <name> dir created, .migrations level fsynced",
			setup: func(t *testing.T, lsmPath string) string {
				migrationsDir := filepath.Join(lsmPath, ".migrations")
				require.NoError(t, os.Mkdir(migrationsDir, 0o777))
				require.NoError(t, os.Chmod(migrationsDir, noReadDir))
				t.Cleanup(func() { _ = os.Chmod(migrationsDir, 0o777) })
				return migrationsDir
			},
		},
		{
			name: ".migrations created, <lsm> level fsynced",
			setup: func(t *testing.T, lsmPath string) string {
				require.NoError(t, os.Chmod(lsmPath, noReadDir))
				t.Cleanup(func() { _ = os.Chmod(lsmPath, 0o777) })
				return lsmPath
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lsmPath := t.TempDir()
			wantFsyncedDir := test.setup(t, lsmPath)

			tr := NewFileReindexTracker(lsmPath, "test_migration_1", &UuidKeyParser{})
			err := tr.init()

			// MkdirAll got through, so the error can only come from the fsync.
			require.DirExists(t, tr.config.migrationPath,
				"init() must create the tracker dir before fsyncing")
			require.Error(t, err, "init() must fsync %s and propagate its failure", wantFsyncedDir)
			require.ErrorIs(t, err, fs.ErrPermission)

			var pathErr *fs.PathError
			require.ErrorAs(t, err, &pathErr)
			require.Equal(t, wantFsyncedDir, pathErr.Path,
				"init() must fsync this level, not just the immediate parent")
		})
	}
}

// Pins: init() creates both levels and stays idempotent over an existing tree.
func TestFileReindexTracker_InitCreatesTreeIdempotently(t *testing.T) {
	lsmPath := t.TempDir()
	migrationsDir := filepath.Join(lsmPath, ".migrations")
	require.NoDirExists(t, migrationsDir,
		".migrations must not pre-exist, so init() must create it")

	tr := NewFileReindexTracker(lsmPath, "test_migration_1", &UuidKeyParser{})
	require.NoError(t, tr.init())

	require.DirExists(t, migrationsDir, "init() must create <lsm>/.migrations")
	require.DirExists(t, tr.config.migrationPath, "init() must create the <name> child dir")

	require.NoError(t, tr.init())
}

// Pins: a torn checkpoint with a half-written count field must not panic
// parseProgressFile / GetMigratedCount — it counts as no progress.
func TestFileReindexTracker_GetMigratedCountTornProgress(t *testing.T) {
	parser := &UuidKeyParser{}
	keyStr := "11111111-1111-1111-1111-111111111111"
	timeStr := "2026-07-16T10:00:00Z"

	// Both shapes pass the len!=4 guard (exactly 4 lines) but leave the trailing
	// count field with no space to split on, triggering the [1]-index panic.
	tornVariants := map[string]string{
		"ends after all N newline": timeStr + "\n" + keyStr + "\nall 5\n",
		"ends mid idx field":       timeStr + "\n" + keyStr + "\nall 5\nidx",
	}

	for name, torn := range tornVariants {
		t.Run(name, func(t *testing.T) {
			tr := newTestReindexTracker(t)
			key, err := parser.FromString(keyStr)
			require.NoError(t, err)
			progressPath := writeTornProgressFile(t, tr, key, torn)

			require.NotPanics(t, func() {
				_, _, allCount, idxCount, perr := tr.parseProgressFile(progressPath)
				require.NoError(t, perr)
				require.Zero(t, allCount, "torn checkpoint counts as no progress")
				require.Zero(t, idxCount, "torn checkpoint counts as no progress")
			})

			require.NotPanics(t, func() {
				total, snapshots, gerr := tr.GetMigratedCount()
				require.NoError(t, gerr)
				require.Zero(t, total, "torn checkpoint contributes 0 to the total")
				require.Len(t, snapshots, 1)
			})
		})
	}
}

func TestFileReindexTracker_GetProgressValidRoundTrip(t *testing.T) {
	parser := &UuidKeyParser{}
	key, err := parser.FromString("22222222-2222-2222-2222-222222222222")
	require.NoError(t, err)

	tr := newTestReindexTracker(t)
	require.NoError(t, tr.markProgress(key, 42, 7))

	tr2 := NewFileReindexTracker(filepath.Dir(filepath.Dir(tr.config.migrationPath)),
		"test_migration_1", parser)
	gotKey, tm, err := tr2.GetProgress()
	require.NoError(t, err)
	require.NotNil(t, tm)
	require.Equal(t, key.String(), gotKey.String())
}

// Pins: the durable write path preserves O_EXCL semantics.
func TestFileReindexTracker_createFileExclusiveAndReadable(t *testing.T) {
	tr := newTestReindexTracker(t)

	started := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, tr.markStarted(started))
	require.True(t, tr.IsStarted())

	// Second create on the same sentinel must fail (exclusive create).
	require.Error(t, tr.markStarted(started))

	got, err := tr.getStarted()
	require.NoError(t, err)
	require.WithinDuration(t, started, got, time.Second)
}

// Pins: removal clears the sentinel, and removing an absent file is a no-op.
func TestFileReindexTracker_removeFileDurable(t *testing.T) {
	tr := newTestReindexTracker(t)

	require.NoError(t, tr.markReindexed())
	require.True(t, tr.IsReindexed())

	require.NoError(t, tr.markProgress(tr.keyParser.FromBytes(nil), 1, 1))
	require.FileExists(t, filepath.Join(tr.config.migrationPath, "progress.mig.000000001"))

	// unmarkReindexed removes the sentinel AND every progress checkpoint.
	require.NoError(t, tr.unmarkReindexed())
	require.False(t, tr.IsReindexed())
	require.NoFileExists(t, filepath.Join(tr.config.migrationPath, "progress.mig.000000001"))

	// Removing an already-absent sentinel is not an error.
	require.NoError(t, tr.unmarkSwapped())
}
