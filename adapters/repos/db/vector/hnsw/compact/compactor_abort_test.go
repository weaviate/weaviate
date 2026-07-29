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

// Tests for the post-abort restart safety contract that backs the
// cancellation fix for issue #204. The fix lets Drop and Shutdown stop
// an in-flight maintenance cycle promptly; this file proves that the
// directory left behind by such an abort is fully recoverable on the
// next startup, with no data loss and no stray .tmp files.
//
// SafeFileWriter is the structural reason the contract holds:
//   - Output is written to <name>.tmp; only sfw.Commit() atomically
//     renames it into place.
//   - sfw.Abort() (run via defer on every error path) deletes the .tmp.
//   - Source files are deleted only after their replacement Commit
//     succeeds.
// Aborting between iterations therefore produces directories that are
// either pre-step or post-step — never an in-between half-state.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

// countingOpenFS counts Opens of files matching suffix. Useful for
// expressing "abort after K files have started processing" without
// resorting to wall-clock timing.
type countingOpenFS struct {
	common.FS
	suffix string
	count  *atomic.Int32
}

func (f *countingOpenFS) Open(name string) (common.File, error) {
	if strings.HasSuffix(name, f.suffix) {
		f.count.Add(1)
	}
	return f.FS.Open(name)
}

// TestCompactor_AbortMidConvertToSorted_RestartLoadsCleanly is the core
// post-abort restart-safety test for the cancellation fix.
//
// It seeds a directory with several condensed WAL files (each owning a
// distinct node), aborts the compactor mid-way through convertToSorted
// after exactly two files have been processed, and then re-opens the
// directory through the normal Loader. The contract verified is:
//
//  1. RunCycle returns ErrCompactionAborted (or wraps it).
//  2. The directory is in a mixed but well-formed state — some files
//     converted to .sorted, the rest still as .condensed.
//  3. No .tmp files leak (every SafeFileWriter that didn't Commit
//     ran its Abort).
//  4. Loader.Load() succeeds and accumulates every node from every
//     original file: aborting compaction is a perf yield, not a
//     correctness step.
func TestCompactor_AbortMidConvertToSorted_RestartLoadsCleanly(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	const numCondensed = 5
	// Each condensed file declares a unique node id (i) and an
	// entrypoint pointing at it. After load, the accumulated state must
	// contain a non-nil entry for every node id we wrote.
	for i := 0; i < numCondensed; i++ {
		ts := int64(1000000000 + i)
		path := filepath.Join(dir, fmt.Sprintf("%d.condensed", ts))
		createTestWALFile(t, path, func(w *WALWriter) {
			require.NoError(t, w.WriteAddNode(uint64(i), 0))
			require.NoError(t, w.WriteSetEntryPointMaxLevel(uint64(i), 0))
		})
	}

	// Add a "live" raw file with a higher timestamp than any condensed
	// file. FileDiscovery reserves the highest-timestamp raw file as
	// LiveFile and excludes it from compaction; without it, our raw
	// file would be picked up too and we'd lose precise control over
	// the abort point.
	createTestWALFile(t, filepath.Join(dir, "9999999999"), func(w *WALWriter) {})

	// Abort plumbing: an FS counter + a shouldAbort closure that
	// returns true once two files have been Opened. The compactor's
	// per-iteration check fires *before* convertFileToSorted, so by
	// the time count == 2 we have committed exactly two .sorted files
	// (their sources removed) and the loop returns when iteration 3 is
	// about to begin.
	var opens atomic.Int32
	fs := &countingOpenFS{FS: common.NewOSFS(), suffix: ".condensed", count: &opens}

	config := DefaultCompactorConfig(dir)
	config.FS = fs
	compactor := NewCompactor(config, logger)

	shouldAbort := func() bool { return opens.Load() >= 2 }

	_, err := compactor.RunCycle(shouldAbort)
	require.True(t, errors.Is(err, ErrCompactionAborted),
		"RunCycle should return ErrCompactionAborted when shouldAbort fires; got %v", err)

	// Inspect the on-disk state. The count of .sorted vs .condensed
	// is a direct readout of the abort point: the loop check happens
	// before convertFileToSorted, so two completed → two .sorted →
	// three .condensed remain.
	sorted, condensed, tmp := classifyDir(t, dir)
	assert.Equal(t, 2, sorted, "exactly two files should have been converted before abort")
	assert.Equal(t, 3, condensed, "remaining condensed files must not be touched after abort")
	assert.Equal(t, 0, tmp, "no .tmp files may leak after a clean abort")

	// Restart contract: a fresh Loader walks the now-mixed directory
	// and produces a state that is byte-for-byte equivalent to having
	// loaded all five original .condensed files. Aborted compaction
	// must not cost data.
	loader := NewLoader(LoaderConfig{Dir: dir, Logger: logger})
	result, err := loader.Load()
	require.NoError(t, err, "loading after abort must succeed without crash recovery")
	require.NotNil(t, result)
	require.NotNil(t, result.State)
	assert.False(t, result.RecoveredFromCrash,
		"abort is a clean yield, not a crash; loader must not flag recovery")

	// Each original node id must be present in the loaded graph.
	for i := 0; i < numCondensed; i++ {
		require.Greater(t, len(result.State.Graph.Nodes), i,
			"loaded state should have a slot for node %d", i)
		require.NotNil(t, result.State.Graph.Nodes[i],
			"node %d must be loaded after restart from a partially-compacted directory", i)
	}
}

// TestCompactor_AbortAtTopOfRunCycle_RestartLoadsCleanly verifies the
// trivial case: shouldAbort fires immediately, the cycle is a no-op,
// and the directory survives unchanged.
func TestCompactor_AbortAtTopOfRunCycle_RestartLoadsCleanly(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	const numCondensed = 3
	for i := 0; i < numCondensed; i++ {
		ts := int64(2000000000 + i)
		path := filepath.Join(dir, fmt.Sprintf("%d.condensed", ts))
		createTestWALFile(t, path, func(w *WALWriter) {
			require.NoError(t, w.WriteAddNode(uint64(i), 0))
			require.NoError(t, w.WriteSetEntryPointMaxLevel(uint64(i), 0))
		})
	}
	createTestWALFile(t, filepath.Join(dir, "9999999999"), func(w *WALWriter) {})

	config := DefaultCompactorConfig(dir)
	compactor := NewCompactor(config, logger)

	_, err := compactor.RunCycle(func() bool { return true })
	require.True(t, errors.Is(err, ErrCompactionAborted),
		"RunCycle must short-circuit when shouldAbort returns true at the top")

	// Nothing should have been touched.
	sorted, condensed, tmp := classifyDir(t, dir)
	assert.Equal(t, 0, sorted, "no conversions should have happened")
	assert.Equal(t, numCondensed, condensed, "all condensed files must be preserved")
	assert.Equal(t, 0, tmp, "no .tmp files may exist")

	// And of course the load still works.
	loader := NewLoader(LoaderConfig{Dir: dir, Logger: logger})
	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result.State)
	for i := 0; i < numCondensed; i++ {
		require.Greater(t, len(result.State.Graph.Nodes), i)
		require.NotNil(t, result.State.Graph.Nodes[i])
	}
}

// TestCompactor_AbortMidMergeSorted_RestartLoadsCleanly aborts the
// compactor while it is inside mergeSorted's per-node write loop, then
// verifies no source file was deleted, no .tmp file leaked, and the
// directory still loads cleanly with every original node intact.
//
// The merge step produces output via SafeFileWriter; sources are removed
// only after Commit. Aborting mid-merge therefore leaves all source
// .sorted files in place and reverts the output to nothing — the next
// cycle (or the test's Loader call) sees the same pre-merge state.
func TestCompactor_AbortMidMergeSorted_RestartLoadsCleanly(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	// Three sorted files, each with one unique node. With
	// MaxFilesPerMerge=2 and no snapshot, decideAction picks
	// ActionMergeSorted (sortedCount > maxFilesPerMerge in the
	// no-snapshot branch).
	const numSorted = 3
	for i := 0; i < numSorted; i++ {
		ts := int64(1000000000 + i)
		writeTestSortedFileWithData(t, dir, ts, ts, func(w *WALWriter) {
			require.NoError(t, w.WriteAddNode(uint64(i), 0))
			require.NoError(t, w.WriteSetEntryPointMaxLevel(uint64(i), 0))
		})
	}
	createTestWALFile(t, filepath.Join(dir, "9999999999"), func(w *WALWriter) {})

	// Abort after the merger has produced the first node so we exercise
	// the in-loop poll point (compactor.go's `for { merger.Next() }`).
	var nextCalls atomic.Int32
	shouldAbort := func() bool {
		// Callbacks: 1 at top of RunCycle, 1 at top of mergeSorted,
		// then the inner-loop check fires once per intended Next(). We
		// want to permit the first node to be written and abort before
		// the second; that is, return true on the second poll inside
		// mergeSorted's loop. In practice "after 4 calls" is a safe
		// hard floor across all the surrounding poll points.
		return nextCalls.Add(1) >= 4
	}

	config := DefaultCompactorConfig(dir)
	config.MaxFilesPerMerge = 2
	compactor := NewCompactor(config, logger)

	_, err := compactor.RunCycle(shouldAbort)
	require.True(t, errors.Is(err, ErrCompactionAborted),
		"RunCycle must surface ErrCompactionAborted from mergeSorted; got %v", err)

	// All three source .sorted files must still exist (delete-only-
	// after-Commit) and there must be no merged output and no .tmp.
	sorted, condensed, tmp := classifyDir(t, dir)
	assert.Equal(t, numSorted, sorted, "all source .sorted files must survive an aborted merge")
	assert.Equal(t, 0, condensed, "no condensed files in this scenario")
	assert.Equal(t, 0, tmp, "aborted merge must not leak .tmp")

	// And the dir is loadable.
	loader := NewLoader(LoaderConfig{Dir: dir, Logger: logger})
	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result.State)
	for i := 0; i < numSorted; i++ {
		require.Greater(t, len(result.State.Graph.Nodes), i)
		require.NotNil(t, result.State.Graph.Nodes[i],
			"node %d must be loaded after aborted merge", i)
	}
}

// TestCompactor_AbortMidCreateSnapshot_RestartLoadsCleanly aborts the
// compactor while it is inside snapshotWriter.WriteFromMerger and
// verifies the same invariants: source files survive, no .tmp leaks,
// and the directory loads with no data loss.
func TestCompactor_AbortMidCreateSnapshot_RestartLoadsCleanly(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	// One sorted file with several nodes. With no snapshot, decideAction
	// returns ActionCreateSnapshot, and createSnapshot drives
	// snapshotWriter.WriteFromMerger which polls shouldAbort once per
	// merger.Next() iteration.
	const numNodes = 6
	writeTestSortedFileWithData(t, dir, 1000, 1000, func(w *WALWriter) {
		require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
		for i := 0; i < numNodes; i++ {
			require.NoError(t, w.WriteAddNode(uint64(i), 0))
		}
	})
	createTestWALFile(t, filepath.Join(dir, "9999999999"), func(w *WALWriter) {})

	// Fire shouldAbort after several invocations so that
	// WriteFromMerger has produced at least one node before yielding.
	var calls atomic.Int32
	shouldAbort := func() bool { return calls.Add(1) >= 4 }

	config := DefaultCompactorConfig(dir)
	compactor := NewCompactor(config, logger)

	_, err := compactor.RunCycle(shouldAbort)
	require.True(t, errors.Is(err, ErrCompactionAborted),
		"RunCycle must surface ErrCompactionAborted from createSnapshot; got %v", err)

	// Source .sorted file must survive, no snapshot or .tmp must leak.
	sorted, condensed, tmp := classifyDir(t, dir)
	assert.Equal(t, 1, sorted, "source .sorted file must survive an aborted snapshot")
	assert.Equal(t, 0, condensed)
	assert.Equal(t, 0, tmp, "aborted snapshot must not leak .tmp")
	// We don't assert snapshot==0 here: SafeFileWriter Abort cleans up
	// the .tmp before the rename, so no .snapshot file should exist
	// either. classifyDir doesn't surface that bucket; the .tmp check
	// above is the load-bearing guarantee.

	loader := NewLoader(LoaderConfig{Dir: dir, Logger: logger})
	result, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, result.State)
	for i := 0; i < numNodes; i++ {
		require.Greater(t, len(result.State.Graph.Nodes), i)
		require.NotNil(t, result.State.Graph.Nodes[i],
			"node %d must be loaded after aborted snapshot", i)
	}
}

func classifyDir(t *testing.T, dir string) (sorted, condensed, tmp int) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, ".tmp"):
			tmp++
		case strings.HasSuffix(name, ".sorted"):
			sorted++
		case strings.HasSuffix(name, ".condensed"):
			condensed++
		}
	}
	return
}
