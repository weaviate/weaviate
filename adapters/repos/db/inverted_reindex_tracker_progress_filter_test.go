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

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regression test for weaviate/weaviate#12657: GetMigratedCount must select
// progress checkpoints with the same strict rule as findLastProgressFile and
// clearProgressFiles, so files that merely share the "progress.mig." prefix
// are neither parsed (aborting the call) nor double-counted.
func TestFileReindexTracker_ProgressFileFilterIsConsistent(t *testing.T) {
	tracker := NewFileReindexTracker(t.TempDir(), "test_migration", &UuidKeyParser{})
	require.NoError(t, tracker.init())

	key, err := (&UuidKeyParser{}).FromString(uuid.NewString())
	require.NoError(t, err)
	require.NoError(t, tracker.markProgress(key, 10, 7))
	require.NoError(t, tracker.markProgress(key, 5, 3))

	dir := tracker.config.migrationPath
	published := filepath.Join(dir, "progress.mig.000000001")
	content, err := os.ReadFile(published)
	require.NoError(t, err)

	// Lookalikes: a parseable temp copy (would double-count), an unparseable
	// temp file (would abort), and a non-numeric suffix of the right length.
	lookalikes := map[string][]byte{
		"progress.mig.000000001.123456.tmp": content,
		"progress.mig.000000002.garbage":    []byte("not a progress file"),
		"progress.mig.00000000x":            []byte("not a progress file"),
	}
	for name, b := range lookalikes {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), b, 0o600))
	}

	total, snapshots, err := tracker.GetMigratedCount()
	require.NoError(t, err)
	assert.Equal(t, 15, total)
	require.Len(t, snapshots, 2)
	assert.Equal(t, "000000001", snapshots[0]["checkpoint"])
	assert.Equal(t, "000000002", snapshots[1]["checkpoint"])

	last, err := tracker.findLastProgressFile()
	require.NoError(t, err)
	assert.Equal(t, "progress.mig.000000002", last)

	require.NoError(t, tracker.clearProgressFiles())
	for name := range lookalikes {
		assert.FileExists(t, filepath.Join(dir, name), "lookalike %q must not be removed", name)
	}
	assert.NoFileExists(t, published)
	assert.NoFileExists(t, filepath.Join(dir, "progress.mig.000000002"))
}
