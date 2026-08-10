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
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

func TestCompactor_StatsPublishing(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	createWALFile(t, filepath.Join(dir, "1000"))
	createEmptyFile(t, dir, "2000") // live file

	compactor := NewCompactor(DefaultCompactorConfig(dir), logger)

	assert.Nil(t, compactor.Stats(), "no stats before the first cycle")

	// First cycle converts raw → sorted → snapshot; the published stats must
	// reflect the state after the action, not the mid-cycle scan.
	action, err := compactor.RunCycle(nil)
	require.NoError(t, err)
	require.Equal(t, ActionCreateSnapshot, action)

	stats := compactor.Stats()
	require.NotNil(t, stats)
	assert.Equal(t, 0, stats.RawFiles)
	assert.Equal(t, 0, stats.CondensedFiles)
	assert.Equal(t, 0, stats.SortedFiles)
	assert.Equal(t, int64(1000), stats.SnapshotTimestamp)
	assert.Equal(t, uint64(1), stats.Cycles)

	snapInfo, err := os.Stat(filepath.Join(dir, "1000.snapshot"))
	require.NoError(t, err)
	assert.Equal(t, snapInfo.Size(), stats.TotalSizeBytes,
		"total size is the snapshot plus the empty live file")

	// An idle cycle publishes too, so the cycle counter reports liveness.
	action, err = compactor.RunCycle(nil)
	require.NoError(t, err)
	require.Equal(t, ActionNone, action)

	stats = compactor.Stats()
	require.NotNil(t, stats)
	assert.Equal(t, uint64(2), stats.Cycles)
	assert.Equal(t, int64(1000), stats.SnapshotTimestamp)
}

// failOnSnapshotFS fails every ReadDir that observes a .snapshot file while
// armed. In a raw → sorted → snapshot cycle the first such ReadDir is the
// post-action stats re-scan, so arming it makes exactly that scan fail.
type failOnSnapshotFS struct {
	common.FS
	armed atomic.Bool
}

func (f *failOnSnapshotFS) ReadDir(name string) ([]os.DirEntry, error) {
	entries, err := f.FS.ReadDir(name)
	if err != nil || !f.armed.Load() {
		return entries, err
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".snapshot") {
			return nil, assert.AnError
		}
	}
	return entries, nil
}

func TestCompactor_CycleCounterSurvivesFailedStatsScan(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	createWALFile(t, filepath.Join(dir, "1000"))
	createEmptyFile(t, dir, "2000") // live file

	fs := &failOnSnapshotFS{FS: common.NewOSFS()}
	fs.armed.Store(true)

	config := DefaultCompactorConfig(dir)
	config.FS = fs
	compactor := NewCompactor(config, logger)

	// Cycle 1 creates a snapshot; the post-action stats re-scan fails.
	// The cycle itself succeeded, so it must be counted anyway.
	action, err := compactor.RunCycle(nil)
	require.NoError(t, err)
	require.Equal(t, ActionCreateSnapshot, action)
	assert.Nil(t, compactor.Stats(), "a failed re-scan skips the publish")

	fs.armed.Store(false)

	// Cycle 2 is idle and publishes. It must report both completed cycles,
	// not just the ones whose publish succeeded.
	action, err = compactor.RunCycle(nil)
	require.NoError(t, err)
	require.Equal(t, ActionNone, action)

	stats := compactor.Stats()
	require.NotNil(t, stats)
	assert.Equal(t, uint64(2), stats.Cycles)
	assert.Equal(t, int64(1000), stats.SnapshotTimestamp)
}

func TestStatsFromState(t *testing.T) {
	tests := []struct {
		name   string
		state  *DirectoryState
		cycles uint64
		want   Stats
	}{
		{
			name:   "empty directory",
			state:  &DirectoryState{},
			cycles: 1,
			want:   Stats{Cycles: 1},
		},
		{
			name: "counts and sizes summed across file types",
			state: &DirectoryState{
				Snapshot:       &FileInfo{StartTS: 1000, EndTS: 3000, Size: 100},
				SortedFiles:    []FileInfo{{Size: 10}, {Size: 20}},
				RawFiles:       []FileInfo{{Size: 1}},
				CondensedFiles: []FileInfo{{Size: 2}, {Size: 3}, {Size: 4}},
				LiveFile:       &FileInfo{Size: 7},
			},
			cycles: 42,
			want: Stats{
				RawFiles:          1,
				CondensedFiles:    3,
				SortedFiles:       2,
				SnapshotTimestamp: 3000,
				TotalSizeBytes:    147,
				Cycles:            42,
			},
		},
		{
			name: "no snapshot reports timestamp zero",
			state: &DirectoryState{
				SortedFiles: []FileInfo{{Size: 10}},
			},
			cycles: 3,
			want: Stats{
				SortedFiles:    1,
				TotalSizeBytes: 10,
				Cycles:         3,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := statsFromState(tt.state, tt.cycles)
			assert.Equal(t, tt.want, *got)
		})
	}
}
