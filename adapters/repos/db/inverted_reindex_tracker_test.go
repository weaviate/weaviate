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
	"time"

	"github.com/stretchr/testify/require"
)

func newTestReindexTracker(t *testing.T) *fileReindexTracker {
	t.Helper()
	tr := NewFileReindexTracker(t.TempDir(), "test_migration_1", &UuidKeyParser{})
	require.NoError(t, tr.init())
	return tr
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
			require.NoError(t, tr.markProgress(key, 10, 5))

			progressPath := filepath.Join(tr.config.migrationPath, "progress.mig.000000001")
			require.NoError(t, os.WriteFile(progressPath, []byte(torn), 0o600))

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

// Pins: a torn progress checkpoint whose trailing count field is half-written
// must not panic parseProgressFile / GetMigratedCount; it counts as no progress.
func TestFileReindexTracker_GetMigratedCountTornProgress(t *testing.T) {
	parser := &UuidKeyParser{}
	keyStr := "11111111-1111-1111-1111-111111111111"
	timeStr := "2026-07-16T10:00:00Z"

	// Both shapes split into exactly 4 lines, so the len!=4 guard passes while the
	// trailing count field has no space to split on: the index-out-of-range panic.
	tornVariants := map[string]string{
		"ends after all N newline": timeStr + "\n" + keyStr + "\nall 5\n",
		"ends mid idx field":       timeStr + "\n" + keyStr + "\nall 5\nidx",
	}

	for name, torn := range tornVariants {
		t.Run(name, func(t *testing.T) {
			tr := newTestReindexTracker(t)
			key, err := parser.FromString(keyStr)
			require.NoError(t, err)
			require.NoError(t, tr.markProgress(key, 10, 5))

			progressPath := filepath.Join(tr.config.migrationPath, "progress.mig.000000001")
			require.NoError(t, os.WriteFile(progressPath, []byte(torn), 0o600))

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
