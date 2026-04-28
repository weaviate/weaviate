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

package compact

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCompactorSnapshotNameCollision reproduces the data-loss bug where
// createSnapshot overwrites and then deletes its own output when an existing
// snapshot and a sorted file share the same timestamp.
//
// Scenario (mirrors a real upgrade from 1.37.1 → compactv2):
//  1. Directory contains `{ts}.snapshot` (existing snapshot) and `{ts}.sorted`
//     (freshly converted from a .condensed that shared the snapshot's TS).
//  2. createSnapshot merges both into a new snapshot.
//  3. BuildMergedFilename(start=ts, end=ts, Snapshot) → `{ts}.snapshot`
//     — identical to the input snapshot path.
//  4. SafeFileWriter.Commit() atomically renames `{ts}.snapshot.tmp` →
//     `{ts}.snapshot`, replacing the input.
//  5. The cleanup loop at compactor.go:546 then iterates `allInputFiles` and
//     removes each by path — including the just-written output.
//
// Expected after fix: `{ts}.snapshot` exists on disk, is non-empty, and is a
// readable V3 snapshot.
func TestCompactorSnapshotNameCollision(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// --- Step 1: write a valid V3 snapshot at {ts}.snapshot -----------------
	snapshotPath := filepath.Join(dir, "1000.snapshot")
	writeValidSnapshot(t, snapshotPath, logger, func(sw *SnapshotWriter) {
		sw.SetEntrypoint(0, 1)
		// Node 0 with connections at level 0 and level 1.
		sw.AddNode(0, 1, [][]uint64{{1, 2}, {1}}, false)
		sw.AddNode(1, 0, [][]uint64{{0, 2}}, false)
		sw.AddNode(2, 0, [][]uint64{{0, 1}}, false)
	})

	// --- Step 2: write a valid .sorted file at the SAME timestamp -----------
	// The .sorted file carries an additional node; after the merge the new
	// snapshot must contain nodes 0..3.
	sortedPath := filepath.Join(dir, "1000.sorted")
	writeValidSorted(t, sortedPath, logger, func(w *WALWriter) {
		// Add a new node 3 and link it to the existing graph.
		require.NoError(t, w.WriteAddNode(3, 0))
		require.NoError(t, w.WriteReplaceLinksAtLevel(3, 0, []uint64{0, 1, 2}))
	})

	// Sanity-check the precondition: both files exist before we run the
	// compactor. They share the same timestamp, which is the root cause.
	beforeSnap, err := os.Stat(snapshotPath)
	require.NoError(t, err, "pre-existing snapshot must exist before RunCycle")
	require.Greater(t, beforeSnap.Size(), int64(0))

	beforeSorted, err := os.Stat(sortedPath)
	require.NoError(t, err, "pre-existing sorted file must exist before RunCycle")
	require.Greater(t, beforeSorted.Size(), int64(0))

	// --- Step 3: run one compactor cycle ------------------------------------
	// Use a tiny SnapshotThreshold so decideAction picks ActionCreateSnapshot
	// even though our tiny test sorted file is a small fraction of the V3
	// snapshot (which is padded to a 4 MB block).
	config := DefaultCompactorConfig(dir)
	config.SnapshotThreshold = 1e-12
	compactor := NewCompactor(config, logger, nil)

	action, err := compactor.RunCycle()
	require.NoError(t, err)
	require.Equal(t, ActionCreateSnapshot, action,
		"with 1 snapshot + 1 sorted file at the same TS, we expect ActionCreateSnapshot")

	// --- Step 4: assert the output survives ---------------------------------
	// BuildMergedFilename(start=1000, end=1000, Snapshot) -> "1000.snapshot",
	// which collides with the input snapshot path. With the bug, the cleanup
	// loop removes the freshly committed output; with the fix, it must remain.
	afterSnap, err := os.Stat(snapshotPath)
	require.NoError(t, err,
		"merged snapshot MUST exist at %s after RunCycle — if it was deleted "+
			"by the cleanup loop the HNSW graph is lost on next startup",
		snapshotPath)
	assert.Greater(t, afterSnap.Size(), int64(0), "merged snapshot must be non-empty")

	// The input .sorted file must be gone (consumed by the merge).
	_, err = os.Stat(sortedPath)
	assert.True(t, os.IsNotExist(err),
		"input sorted file should have been deleted after successful merge")

	// --- Step 5: the merged snapshot must be a readable V3 snapshot ---------
	// and must contain the union of the inputs (nodes 0..3).
	reader := NewSnapshotReader(logger)
	result, err := reader.ReadFromFile(snapshotPath)
	require.NoError(t, err, "merged snapshot must be a readable V3 snapshot")
	require.NotNil(t, result)
	require.NotNil(t, result.Graph)
	assert.Equal(t, uint64(0), result.Graph.Entrypoint)

	// Verify node 3 from the sorted file is present in the merged snapshot.
	require.GreaterOrEqual(t, len(result.Graph.Nodes), 4,
		"merged snapshot must contain nodes from both the input snapshot and the sorted file")
	require.NotNil(t, result.Graph.Nodes[3], "node 3 (from sorted file) must be present after merge")
}

// writeValidSnapshot creates a valid V3 snapshot at path, using the provided
// writer-configuration callback. It commits the file atomically via
// SafeFileWriter so the resulting file is indistinguishable from one
// produced by the compactor itself.
func writeValidSnapshot(t *testing.T, path string, _ logrus.FieldLogger, configure func(*SnapshotWriter)) {
	t.Helper()

	sfw, err := NewSafeFileWriter(path, DefaultBufferSize)
	require.NoError(t, err)
	defer sfw.Abort()

	sw := NewSnapshotWriter(sfw.Writer())
	configure(sw)
	require.NoError(t, sw.Flush())
	require.NoError(t, sfw.Commit())
}

// writeValidSorted creates a valid .sorted file. A .sorted file shares its
// on-disk format with a WAL (it is a WAL whose commits have been sorted by
// node ID), so we can produce one by writing commits via WALWriter.
func writeValidSorted(t *testing.T, path string, _ logrus.FieldLogger, configure func(*WALWriter)) {
	t.Helper()

	sfw, err := NewSafeFileWriter(path, DefaultBufferSize)
	require.NoError(t, err)
	defer sfw.Abort()

	w := NewWALWriter(sfw.Writer())
	configure(w)
	require.NoError(t, sfw.Commit())
}
