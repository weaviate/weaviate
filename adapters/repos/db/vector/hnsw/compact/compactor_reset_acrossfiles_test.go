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
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests pin the contract that, after compaction, a ResetIndex in a later
// file makes everything older than it disappear — matching a no-compaction load.

func compactToFixedPoint(t *testing.T, dir string) {
	t.Helper()
	c := NewCompactor(DefaultCompactorConfig(dir), quietLogger())
	for i := 0; i < 50; i++ {
		action, err := c.RunCycle(nil)
		require.NoError(t, err, "RunCycle %d", i)
		if action == ActionNone {
			return
		}
	}
	t.Fatal("compaction did not converge")
}

func TestCompact_ResetAcrossFiles_MatchesNoCompaction(t *testing.T) {
	type file struct {
		ts  int64
		ops func(w *WALWriter)
	}

	tests := []struct {
		name  string
		files []file
	}{
		{
			name: "reset clears tombstone from older file",
			files: []file{
				{1000, func(w *WALWriter) {
					require.NoError(t, w.WriteAddNode(1, 1))
					require.NoError(t, w.WriteAddTombstone(1))
				}},
				{2000, func(w *WALWriter) {
					require.NoError(t, w.WriteResetIndex())
					require.NoError(t, w.WriteAddNode(2, 1))
					require.NoError(t, w.WriteSetEntryPointMaxLevel(2, 1))
				}},
				{3000, func(w *WALWriter) { require.NoError(t, w.WriteAddNode(3, 0)) }},
			},
		},
		{
			name: "reset clears nodes and links from older files",
			files: []file{
				{1000, func(w *WALWriter) {
					require.NoError(t, w.WriteAddNode(10, 2))
					require.NoError(t, w.WriteAddNode(11, 1))
					require.NoError(t, w.WriteAddLinkAtLevel(10, 0, 11))
					require.NoError(t, w.WriteSetEntryPointMaxLevel(10, 2))
				}},
				{2000, func(w *WALWriter) {
					require.NoError(t, w.WriteAddNode(12, 1))
					require.NoError(t, w.WriteAddLinkAtLevel(12, 0, 10))
				}},
				{3000, func(w *WALWriter) {
					require.NoError(t, w.WriteResetIndex())
					require.NoError(t, w.WriteAddNode(20, 2))
					require.NoError(t, w.WriteAddNode(21, 1))
					require.NoError(t, w.WriteAddLinkAtLevel(20, 0, 21))
					require.NoError(t, w.WriteSetEntryPointMaxLevel(20, 2))
				}},
				{4000, func(w *WALWriter) { require.NoError(t, w.WriteAddNode(22, 0)) }},
			},
		},
		{
			name: "reset in oldest non-live file",
			files: []file{
				{1000, func(w *WALWriter) {
					require.NoError(t, w.WriteAddNode(1, 1))
					require.NoError(t, w.WriteResetIndex())
					require.NoError(t, w.WriteAddNode(5, 1))
					require.NoError(t, w.WriteSetEntryPointMaxLevel(5, 1))
				}},
				{2000, func(w *WALWriter) {
					require.NoError(t, w.WriteAddNode(6, 1))
					require.NoError(t, w.WriteAddLinkAtLevel(6, 0, 5))
				}},
				{3000, func(w *WALWriter) { require.NoError(t, w.WriteAddNode(7, 0)) }},
			},
		},
		{
			name: "two resets in different files - last wins",
			files: []file{
				{1000, func(w *WALWriter) {
					require.NoError(t, w.WriteAddNode(1, 1))
					require.NoError(t, w.WriteAddTombstone(1))
				}},
				{2000, func(w *WALWriter) {
					require.NoError(t, w.WriteResetIndex())
					require.NoError(t, w.WriteAddNode(2, 1))
					require.NoError(t, w.WriteAddTombstone(2))
				}},
				{3000, func(w *WALWriter) {
					require.NoError(t, w.WriteResetIndex())
					require.NoError(t, w.WriteAddNode(3, 2))
					require.NoError(t, w.WriteSetEntryPointMaxLevel(3, 2))
				}},
				{4000, func(w *WALWriter) { require.NoError(t, w.WriteAddNode(4, 0)) }},
			},
		},
		{
			name: "post-reset data in newer files survives",
			files: []file{
				{1000, func(w *WALWriter) {
					require.NoError(t, w.WriteAddNode(1, 1))
					require.NoError(t, w.WriteAddTombstone(1))
					require.NoError(t, w.WriteSetEntryPointMaxLevel(1, 1))
				}},
				{2000, func(w *WALWriter) {
					require.NoError(t, w.WriteResetIndex())
					require.NoError(t, w.WriteAddNode(2, 2))
					require.NoError(t, w.WriteSetEntryPointMaxLevel(2, 2))
				}},
				{3000, func(w *WALWriter) {
					require.NoError(t, w.WriteAddNode(3, 1))
					require.NoError(t, w.WriteAddLinkAtLevel(2, 0, 3))
					require.NoError(t, w.WriteAddTombstone(3))
				}},
				{4000, func(w *WALWriter) { require.NoError(t, w.WriteAddNode(4, 0)) }},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			baseDir := t.TempDir()
			workDir := t.TempDir()
			for _, f := range tc.files {
				name := fmt.Sprintf("%d", f.ts)
				createTestWALFile(t, filepath.Join(baseDir, name), f.ops)
				createTestWALFile(t, filepath.Join(workDir, name), f.ops)
			}

			baseline := loadGraph(t, baseDir)
			compactToFixedPoint(t, workDir)
			compacted := loadGraph(t, workDir)

			assertGraphEqual(t, baseline, compacted)
		})
	}
}

// TestCompact_ResetDiscardsOlderSnapshot verifies a reset also obsoletes a
// snapshot built from pre-reset data once the reset file is compacted.
func TestCompact_ResetDiscardsOlderSnapshot(t *testing.T) {
	baseDir := t.TempDir()
	workDir := t.TempDir()

	files := []struct {
		ts  int64
		ops func(w *WALWriter)
	}{
		{1000, func(w *WALWriter) {
			require.NoError(t, w.WriteAddNode(1, 2))
			require.NoError(t, w.WriteAddNode(2, 1))
			require.NoError(t, w.WriteAddLinkAtLevel(1, 0, 2))
			require.NoError(t, w.WriteAddTombstone(2))
			require.NoError(t, w.WriteSetEntryPointMaxLevel(1, 2))
		}},
		{2000, func(w *WALWriter) {
			require.NoError(t, w.WriteAddNode(3, 1))
			require.NoError(t, w.WriteAddLinkAtLevel(1, 0, 3))
		}},
	}
	for _, f := range files {
		name := fmt.Sprintf("%d", f.ts)
		createTestWALFile(t, filepath.Join(baseDir, name), f.ops)
		createTestWALFile(t, filepath.Join(workDir, name), f.ops)
	}

	// Compact the first batch into a snapshot while file 2000 is still live.
	compactToFixedPoint(t, workDir)
	require.NotNil(t, NewLoader(LoaderConfig{Dir: workDir, Logger: quietLogger()}))

	resetFile := func(w *WALWriter) {
		require.NoError(t, w.WriteResetIndex())
		require.NoError(t, w.WriteAddNode(9, 2))
		require.NoError(t, w.WriteSetEntryPointMaxLevel(9, 2))
	}
	liveFile := func(w *WALWriter) { require.NoError(t, w.WriteAddNode(10, 0)) }
	createTestWALFile(t, filepath.Join(baseDir, "3000"), resetFile)
	createTestWALFile(t, filepath.Join(workDir, "3000"), resetFile)
	createTestWALFile(t, filepath.Join(baseDir, "4000"), liveFile)
	createTestWALFile(t, filepath.Join(workDir, "4000"), liveFile)

	baseline := loadGraph(t, baseDir)
	compactToFixedPoint(t, workDir)
	compacted := loadGraph(t, workDir)

	assertGraphEqual(t, baseline, compacted)

	require.NotContains(t, compacted.Graph.Tombstones, uint64(2),
		"pre-reset tombstone resurrected after compaction")
	require.True(t, effectivelyAbsent(nodeAt(compacted, 1)),
		"pre-reset node 1 resurrected after compaction")
}
