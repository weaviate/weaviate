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

// Tests for the allocChecker memory-pressure gate added to convertFileToSorted.
//
// The contract: when CheckAlloc returns an error the file is silently skipped
// (convertFileToSorted returns nil, not an error), the loop continues, and the
// file is retried on the next cycle. This means a cycle can finish with some
// files converted and others left as .condensed — including non-contiguous gaps
// — without violating any correctness invariant.
//
// The Loader handles all file types regardless of ordering, so a directory with
// interleaved .sorted and .condensed files is perfectly valid and fully
// recoverable with no data loss.

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// alwaysRejectAllocChecker simulates a system that is permanently under memory
// pressure: every CheckAlloc call returns ErrNotEnoughMemory.
type alwaysRejectAllocChecker struct{}

func (c *alwaysRejectAllocChecker) CheckAlloc(sizeInBytes int64) error {
	return enterrors.ErrNotEnoughMemory
}

func (c *alwaysRejectAllocChecker) CheckMappingAndReserve(numberMappings int64, reservationTimeInS int) error {
	return nil
}

func (c *alwaysRejectAllocChecker) Refresh(updateMappings bool) {}

// nthRejectAllocChecker rejects every N-th CheckAlloc call (1-indexed).
// For N=2: the 2nd, 4th, 6th, … calls return ErrNotEnoughMemory; all others
// return nil. This simulates bursty memory pressure that affects alternating
// files in a convertToSorted loop.
type nthRejectAllocChecker struct {
	n     int32
	count atomic.Int32
}

func (c *nthRejectAllocChecker) CheckAlloc(sizeInBytes int64) error {
	if c.count.Add(1)%c.n == 0 {
		return enterrors.ErrNotEnoughMemory
	}
	return nil
}

func (c *nthRejectAllocChecker) CheckMappingAndReserve(numberMappings int64, reservationTimeInS int) error {
	return nil
}

func (c *nthRejectAllocChecker) Refresh(updateMappings bool) {}

// TestCompactor_AllocCheckerRejectsAll_NoFilesConverted verifies the basic
// allocChecker contract: when every CheckAlloc call fails, convertToSorted
// skips all files, RunCycle returns nil (pressure is not a hard error), and
// the directory is unchanged and still loadable.
func TestCompactor_AllocCheckerRejectsAll_NoFilesConverted(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	const numCondensed = 4
	for i := 0; i < numCondensed; i++ {
		ts := int64(1000000000 + i)
		path := filepath.Join(dir, fmt.Sprintf("%d.condensed", ts))
		createTestWALFile(t, path, func(w *WALWriter) {
			// Use level=1 so SortedWriter preserves the explicit WriteAddNode
			// (it omits WriteAddNode for level-0 nodes, relying on implicit
			// creation through links — but test nodes have no links).
			require.NoError(t, w.WriteAddNode(uint64(i), 1))
			require.NoError(t, w.WriteSetEntryPointMaxLevel(uint64(i), 1))
		})
	}
	createTestWALFile(t, filepath.Join(dir, "9999999999"), func(w *WALWriter) {})

	config := DefaultCompactorConfig(dir)
	compactor := NewCompactor(config, logger, &alwaysRejectAllocChecker{})

	_, err := compactor.RunCycle(func() bool { return false })
	require.NoError(t, err, "memory pressure must not cause RunCycle to return an error")

	sorted, condensed, tmp := classifyDir(t, dir)
	assert.Equal(t, 0, sorted, "no files should be converted under total memory pressure")
	assert.Equal(t, numCondensed, condensed, "all condensed files must be preserved")
	assert.Equal(t, 0, tmp, "no .tmp files may leak")

	loader := NewLoader(LoaderConfig{Dir: dir, Logger: logger})
	result, err := loader.Load()
	require.NoError(t, err, "directory must load cleanly after a fully-skipped cycle")
	require.NotNil(t, result.State)
	assert.False(t, result.RecoveredFromCrash, "skipping is a clean yield, not a crash")
	for i := 0; i < numCondensed; i++ {
		require.Greater(t, len(result.State.Graph.Nodes), i)
		require.NotNil(t, result.State.Graph.Nodes[i],
			"node %d must be present after a fully-skipped cycle", i)
	}
}

// TestCompactor_AllocCheckerSkipsEveryOtherFile_MixedDirLoadsCleanly is the
// key safety test requested in code review: the allocChecker rejects every 2nd
// file, leaving the directory in a mixed state with alternating .sorted and
// .condensed files. The test verifies:
//
//  1. RunCycle returns nil — intermittent pressure is not an error.
//  2. Exactly 3 files are converted and 2 are skipped (non-contiguous gaps).
//  3. No .tmp files leak.
//  4. The mixed directory loads cleanly with all nodes present.
//  5. A second cycle without pressure converts the remaining files and the
//     resulting directory still contains all nodes — skipped files are retried.
func TestCompactor_AllocCheckerSkipsEveryOtherFile_MixedDirLoadsCleanly(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	const numCondensed = 5
	for i := 0; i < numCondensed; i++ {
		ts := int64(1000000000 + i)
		path := filepath.Join(dir, fmt.Sprintf("%d.condensed", ts))
		createTestWALFile(t, path, func(w *WALWriter) {
			// Use level=1 so SortedWriter preserves the explicit WriteAddNode
			// (it omits WriteAddNode for level-0 nodes, relying on implicit
			// creation through links — but test nodes have no links).
			require.NoError(t, w.WriteAddNode(uint64(i), 1))
			require.NoError(t, w.WriteSetEntryPointMaxLevel(uint64(i), 1))
		})
	}
	createTestWALFile(t, filepath.Join(dir, "9999999999"), func(w *WALWriter) {})

	// Reject calls 2 and 4 → files at loop indices 1 and 3 are skipped.
	// Files at indices 0, 2, 4 are converted. Result: 3 sorted, 2 condensed.
	config := DefaultCompactorConfig(dir)
	compactor := NewCompactor(config, logger, &nthRejectAllocChecker{n: 2})

	_, err := compactor.RunCycle(func() bool { return false })
	require.NoError(t, err, "intermittent memory pressure must not cause RunCycle to return an error")

	sorted, condensed, tmp := classifyDir(t, dir)
	assert.Equal(t, 3, sorted, "files at indices 0, 2, 4 should be converted")
	assert.Equal(t, 2, condensed, "files at indices 1, 3 should remain .condensed (skipped)")
	assert.Equal(t, 0, tmp, "no .tmp files may leak after partial conversion")

	// The mixed directory — with non-contiguous .sorted and .condensed files —
	// must load cleanly. This is the correctness invariant the reviewer asked to verify.
	loader := NewLoader(LoaderConfig{Dir: dir, Logger: logger})
	result, err := loader.Load()
	require.NoError(t, err, "mixed directory must load without crash recovery")
	require.NotNil(t, result.State)
	assert.False(t, result.RecoveredFromCrash,
		"non-contiguous skips are a clean partial state, not a crash")
	for i := 0; i < numCondensed; i++ {
		require.Greater(t, len(result.State.Graph.Nodes), i)
		require.NotNil(t, result.State.Graph.Nodes[i],
			"node %d must be present in the mixed directory", i)
	}

	// Second cycle with no allocChecker: the two remaining condensed files
	// must be converted. After this all original data is still accessible.
	compactor2 := NewCompactor(config, logger, nil)
	_, err = compactor2.RunCycle(func() bool { return false })
	require.NoError(t, err, "second cycle must succeed")

	_, condensedAfter, tmpAfter := classifyDir(t, dir)
	assert.Equal(t, 0, condensedAfter, "all condensed files must be converted on the second cycle")
	assert.Equal(t, 0, tmpAfter, "no .tmp files after second cycle")

	result2, err := loader.Load()
	require.NoError(t, err, "directory must load cleanly after second cycle")
	require.NotNil(t, result2.State)
	for i := 0; i < numCondensed; i++ {
		require.Greater(t, len(result2.State.Graph.Nodes), i)
		require.NotNil(t, result2.State.Graph.Nodes[i],
			"node %d must still be present after second cycle", i)
	}
}
