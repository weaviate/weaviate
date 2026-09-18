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
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
)

func TestCompactor_IsolatedNode(t *testing.T) {
	for _, tc := range []struct {
		name  string
		id    uint64
		level uint16
	}{
		{name: "first HNSW object", id: 0},
		{name: "first HFresh centroid", id: 1},
		{name: "higher level node", id: 7, level: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			logger := logrus.New()
			compactor := NewCompactor(DefaultCompactorConfig(dir), logger)
			for round := 0; round < 3; round++ {
				name := fmt.Sprint(1000 + round)
				f, err := os.Create(filepath.Join(dir, name))
				require.NoError(t, err)
				writer := NewWALWriter(f)
				switch round {
				case 0:
					require.NoError(t, writer.WriteSetEntryPointMaxLevel(tc.id, tc.level))
					require.NoError(t, writer.WriteAddNode(tc.id, tc.level))
				case 1:
					// A later log has no AddNode. It must not lower the stored level.
					require.NoError(t, writer.WriteAddLinksAtLevel(tc.id, 0, nil))
				case 2:
					// Deletion must still win over the isolated node in the snapshot.
					require.NoError(t, writer.WriteDeleteNode(tc.id))
				}
				require.NoError(t, f.Close())
				createEmptyFile(t, dir, fmt.Sprint(1001+round))
				// Force another snapshot even when the delta is smaller than 20%.
				compactor.config.SnapshotThreshold = 0.000001
				action, err := compactor.RunCycle(nil)
				require.NoError(t, err)
				require.Equal(t, ActionCreateSnapshot, action)
				state, err := NewFileDiscovery(dir).Scan()
				require.NoError(t, err)
				require.NotNil(t, state.Snapshot)
				res, err := NewSnapshotReader(logger).ReadFromFile(state.Snapshot.Path)
				require.NoError(t, err)
				if round == 2 {
					if int(tc.id) < len(res.Graph.Nodes) {
						require.Nil(t, res.Graph.Nodes[tc.id])
					}
					continue
				}
				require.Greater(t, len(res.Graph.Nodes), int(tc.id))
				node := res.Graph.Nodes[tc.id]
				require.NotNil(t, node, "isolated node must survive compaction round %d", round)
				require.Equal(t, int(tc.level), node.Level)
				require.Equal(t, tc.id, res.Graph.Entrypoint)
			}
		})
	}
}

func TestCompactor_EmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	config := DefaultCompactorConfig(dir)
	compactor := NewCompactor(config, logger)

	action, err := compactor.RunCycle(nil)
	require.NoError(t, err)
	assert.Equal(t, ActionNone, action)
}

func TestCompactor_OnlyLiveFile(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create only a live file (highest timestamp raw file)
	createEmptyFile(t, dir, "1000")

	config := DefaultCompactorConfig(dir)
	compactor := NewCompactor(config, logger)

	action, err := compactor.RunCycle(nil)
	require.NoError(t, err)
	assert.Equal(t, ActionNone, action)

	// Live file should still exist
	_, err = os.Stat(filepath.Join(dir, "1000"))
	require.NoError(t, err)
}

func TestCompactor_CleanupTempFiles(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create orphaned temp files
	createEmptyFile(t, dir, "1000.sorted.tmp")
	createEmptyFile(t, dir, "2000.snapshot.tmp")

	// Create a live file
	createEmptyFile(t, dir, "3000")

	config := DefaultCompactorConfig(dir)
	compactor := NewCompactor(config, logger)

	_, err := compactor.RunCycle(nil)
	require.NoError(t, err)

	// Temp files should be cleaned up
	_, err = os.Stat(filepath.Join(dir, "1000.sorted.tmp"))
	assert.True(t, os.IsNotExist(err), "temp file should be deleted")

	_, err = os.Stat(filepath.Join(dir, "2000.snapshot.tmp"))
	assert.True(t, os.IsNotExist(err), "temp file should be deleted")
}

func TestCompactor_DecideAction_NoSnapshot(t *testing.T) {
	logger := logrus.New()
	config := DefaultCompactorConfig("/tmp")
	compactor := NewCompactor(config, logger)

	tests := []struct {
		name     string
		state    *DirectoryState
		expected Action
	}{
		{
			name: "no sorted files",
			state: &DirectoryState{
				SortedFiles: []FileInfo{},
			},
			expected: ActionNone,
		},
		{
			name: "one sorted file, create snapshot",
			state: &DirectoryState{
				SortedFiles: []FileInfo{
					{StartTS: 1000, Size: 1000},
				},
			},
			expected: ActionCreateSnapshot,
		},
		{
			name: "many sorted files, merge first",
			state: &DirectoryState{
				SortedFiles: []FileInfo{
					{StartTS: 1000, Size: 100},
					{StartTS: 2000, Size: 100},
					{StartTS: 3000, Size: 100},
					{StartTS: 4000, Size: 100},
					{StartTS: 5000, Size: 100},
					{StartTS: 6000, Size: 100}, // > MaxFilesPerMerge
				},
			},
			expected: ActionMergeSorted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			action := compactor.decideAction(tt.state)
			assert.Equal(t, tt.expected, action)
		})
	}
}

func TestCompactor_DecideAction_WithSnapshot(t *testing.T) {
	logger := logrus.New()
	config := DefaultCompactorConfig("/tmp")
	config.SnapshotThreshold = 0.20
	compactor := NewCompactor(config, logger)

	tests := []struct {
		name     string
		state    *DirectoryState
		expected Action
	}{
		{
			name: "sorted ratio below threshold, merge",
			state: &DirectoryState{
				Snapshot: &FileInfo{Size: 1000},
				SortedFiles: []FileInfo{
					{StartTS: 2000, Size: 100}, // 10% < 20%
					{StartTS: 3000, Size: 50},
				},
			},
			expected: ActionMergeSorted,
		},
		{
			name: "sorted ratio above threshold, create snapshot",
			state: &DirectoryState{
				Snapshot: &FileInfo{Size: 100},
				SortedFiles: []FileInfo{
					{StartTS: 2000, Size: 100}, // 50% > 20%
				},
			},
			expected: ActionCreateSnapshot,
		},
		{
			name: "no sorted files",
			state: &DirectoryState{
				Snapshot:    &FileInfo{Size: 1000},
				SortedFiles: []FileInfo{},
			},
			expected: ActionNone,
		},
		{
			name: "one sorted file below threshold",
			state: &DirectoryState{
				Snapshot: &FileInfo{Size: 1000},
				SortedFiles: []FileInfo{
					{StartTS: 2000, Size: 100}, // 10% < 20%, only 1 file
				},
			},
			expected: ActionNone, // Can't merge just one file
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			action := compactor.decideAction(tt.state)
			assert.Equal(t, tt.expected, action)
		})
	}
}

// RunCycle calls decideAction on every maintenance cycle, so it must not
// allocate log fields unless the logger keeps debug entries.
func TestCompactor_DecideAction_Logging(t *testing.T) {
	sorted := func(n int, size int64) []FileInfo {
		files := make([]FileInfo, n)
		for i := range files {
			files[i] = FileInfo{StartTS: int64(1000 * (i + 1)), Size: size}
		}
		return files
	}

	states := []struct {
		name     string
		state    *DirectoryState
		expected Action
	}{
		{name: "no data", state: &DirectoryState{}, expected: ActionNone},
		{name: "initial snapshot", state: &DirectoryState{SortedFiles: sorted(1, 1000)}, expected: ActionCreateSnapshot},
		{name: "merge before initial snapshot", state: &DirectoryState{SortedFiles: sorted(6, 100)}, expected: ActionMergeSorted},
		{
			name:     "merge before snapshot above threshold",
			state:    &DirectoryState{Snapshot: &FileInfo{Size: 100}, SortedFiles: sorted(6, 100)},
			expected: ActionMergeSorted,
		},
		{
			name:     "snapshot above threshold",
			state:    &DirectoryState{Snapshot: &FileInfo{Size: 100}, SortedFiles: sorted(1, 100)},
			expected: ActionCreateSnapshot,
		},
		{
			name:     "merge below threshold",
			state:    &DirectoryState{Snapshot: &FileInfo{Size: 1000}, SortedFiles: sorted(2, 50)},
			expected: ActionMergeSorted,
		},
		{
			name:     "one sorted file below threshold",
			state:    &DirectoryState{Snapshot: &FileInfo{Size: 1000}, SortedFiles: sorted(1, 100)},
			expected: ActionNone,
		},
	}

	for _, tc := range states {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("info level", func(t *testing.T) {
				logger, hook := test.NewNullLogger()
				logger.SetLevel(logrus.InfoLevel)
				compactor := NewCompactor(DefaultCompactorConfig(t.TempDir()), logger.WithField("id", "main"))

				var got Action
				allocs := testing.AllocsPerRun(10, func() {
					got = compactor.decideAction(tc.state)
				})
				assert.Equal(t, tc.expected, got)
				assert.Zero(t, allocs)
				assert.Empty(t, hook.AllEntries())
			})

			t.Run("debug level", func(t *testing.T) {
				logger, hook := test.NewNullLogger()
				logger.SetLevel(logrus.DebugLevel)
				compactor := NewCompactor(DefaultCompactorConfig(t.TempDir()), logger)

				assert.Equal(t, tc.expected, compactor.decideAction(tc.state))
				if tc.expected == ActionNone {
					assert.Empty(t, hook.AllEntries(), "no-op decisions repeat every idle cycle and must stay silent")
					return
				}
				require.Len(t, hook.AllEntries(), 1)
				entry := hook.LastEntry()
				assert.Equal(t, logrus.DebugLevel, entry.Level)
				assert.Equal(t, "decision: "+tc.expected.String(), entry.Message)
				assert.Equal(t, "hnsw_compactor_decide", entry.Data["action"])
				assert.NotEmpty(t, entry.Data["reason"])
				assert.Equal(t, len(tc.state.SortedFiles), entry.Data["sorted_count"])
			})
		})
	}
}

// The debug logs RunCycle reaches past decideAction must not build their fields
// unless the logger keeps debug entries. A logger whose level LevelEnabled cannot
// read still builds them, which gives each case its baseline.
func TestCompactor_DebugLogging(t *testing.T) {
	sortedInputs := func(t *testing.T, dir string) []FileInfo {
		files := []FileInfo{
			{Path: filepath.Join(dir, "1000.sorted"), StartTS: 1000, EndTS: 1000, Type: FileTypeSorted},
			{Path: filepath.Join(dir, "2000.sorted"), StartTS: 2000, EndTS: 2000, Type: FileTypeSorted},
		}
		for _, f := range files {
			file, err := os.Create(f.Path)
			require.NoError(t, err)
			w := NewWALWriter(file)
			require.NoError(t, w.WriteSetEntryPointMaxLevel(0, 0))
			require.NoError(t, w.WriteAddNode(0, 0))
			require.NoError(t, file.Close())
		}
		return files
	}

	testCases := []struct {
		name     string
		run      func(t *testing.T, c *Compactor)
		messages []string
	}{
		{
			name: "resolveOverlaps",
			run: func(t *testing.T, c *Compactor) {
				state := &DirectoryState{Overlaps: []Overlap{{
					MergedFile:    FileInfo{Path: filepath.Join(c.config.Dir, "1000_3000.sorted")},
					ContainedFile: FileInfo{Path: filepath.Join(c.config.Dir, "2000.sorted")},
				}}}
				require.NoError(t, c.resolveOverlaps(state))
			},
			messages: []string{"removing file contained in merged range"},
		},
		{
			name: "convertFileToSorted",
			run: func(t *testing.T, c *Compactor) {
				createEmptyFile(t, c.config.Dir, "1000")
				raw := FileInfo{Path: filepath.Join(c.config.Dir, "1000"), StartTS: 1000, EndTS: 1000, Type: FileTypeRaw}
				_, err := c.convertFileToSorted(raw)
				require.NoError(t, err)
			},
			messages: []string{"converting file to sorted format", "converted file to sorted format"},
		},
		{
			name: "mergeSorted",
			run: func(t *testing.T, c *Compactor) {
				state := &DirectoryState{SortedFiles: sortedInputs(t, c.config.Dir)}
				require.NoError(t, c.mergeSorted(state, nil))
			},
			messages: []string{"merging sorted files", "merged sorted files"},
		},
		{
			name: "createSnapshot",
			run: func(t *testing.T, c *Compactor) {
				state := &DirectoryState{SortedFiles: sortedInputs(t, c.config.Dir)}
				require.NoError(t, c.createSnapshot(state, nil))
			},
			messages: []string{"creating snapshot", "created snapshot"},
		},
		{
			name: "InMemoryReader.Do",
			run: func(t *testing.T, c *Compactor) {
				var buf bytes.Buffer
				require.NoError(t, NewWALWriter(&buf).WriteSetEntryPointMaxLevel(0, 0))
				_, err := NewInMemoryReader(NewWALCommitReader(&buf, c.logger), c.logger).Do(nil, false)
				require.NoError(t, err)
			},
			messages: []string{"hnsw commit logger " + SetEntryPointMaxLevel.String()},
		},
		{
			name: "growIndexToAccommodateNode",
			run: func(t *testing.T, c *Compactor) {
				_, grown, err := growIndexToAccommodateNode(nil, 0, c.logger)
				require.NoError(t, err)
				require.True(t, grown)
			},
			messages: []string{fmt.Sprintf("index grown from 0 to %d", cache.MinimumIndexGrowthDelta)},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			allocs := func(logger logrus.FieldLogger) float64 {
				c := NewCompactor(DefaultCompactorConfig(t.TempDir()), logger)
				return testing.AllocsPerRun(10, func() { tc.run(t, c) })
			}
			infoLogger, infoHook := test.NewNullLogger()
			infoLogger.SetLevel(logrus.InfoLevel)
			assert.Less(t, allocs(infoLogger), allocs(unreadableLogger{infoLogger}))
			assert.Empty(t, infoHook.AllEntries())

			debugLogger, debugHook := test.NewNullLogger()
			debugLogger.SetLevel(logrus.DebugLevel)
			tc.run(t, NewCompactor(DefaultCompactorConfig(t.TempDir()), debugLogger))
			var messages []string
			for _, entry := range debugHook.AllEntries() {
				assert.Equal(t, logrus.DebugLevel, entry.Level)
				messages = append(messages, entry.Message)
			}
			assert.Equal(t, tc.messages, messages)
		})
	}
}

// unreadableLogger is a FieldLogger whose level LevelEnabled cannot inspect.
type unreadableLogger struct{ logrus.FieldLogger }

func TestCompactor_ResolveOverlaps(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create overlapping files
	createEmptyFile(t, dir, "7_9.sorted")
	createEmptyFile(t, dir, "8.sorted")

	// Create live file
	createEmptyFile(t, dir, "10")

	config := DefaultCompactorConfig(dir)
	compactor := NewCompactor(config, logger)

	_, err := compactor.RunCycle(nil)
	require.NoError(t, err)

	// Contained file should be deleted
	_, err = os.Stat(filepath.Join(dir, "8.sorted"))
	assert.True(t, os.IsNotExist(err), "contained file should be deleted")

	// Merged file should still exist
	_, err = os.Stat(filepath.Join(dir, "7_9.sorted"))
	require.NoError(t, err)
}

func TestCompactor_ConvertRawToSorted(t *testing.T) {
	dir := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// Create a raw file with valid WAL content
	rawPath := filepath.Join(dir, "1000")
	createWALFile(t, rawPath)

	// Create a live file (higher timestamp)
	createEmptyFile(t, dir, "2000")

	config := DefaultCompactorConfig(dir)
	compactor := NewCompactor(config, logger)

	action, err := compactor.RunCycle(nil)
	require.NoError(t, err)

	// Original raw file should be converted and deleted
	_, err = os.Stat(rawPath)
	assert.True(t, os.IsNotExist(err), "raw file should be deleted after conversion")

	// The compactor converts raw → sorted → snapshot in one cycle
	// So we expect a snapshot file to be created
	assert.Equal(t, ActionCreateSnapshot, action)
	_, err = os.Stat(filepath.Join(dir, "1000.snapshot"))
	require.NoError(t, err, "snapshot file should be created")
}

func TestAction_String(t *testing.T) {
	assert.Equal(t, "none", ActionNone.String())
	assert.Equal(t, "merge_sorted", ActionMergeSorted.String())
	assert.Equal(t, "create_snapshot", ActionCreateSnapshot.String())
	assert.Equal(t, "unknown", Action(99).String())
}

func TestDefaultCompactorConfig(t *testing.T) {
	config := DefaultCompactorConfig("/some/path")

	assert.Equal(t, "/some/path", config.Dir)
	assert.Equal(t, 5, config.MaxFilesPerMerge)
	assert.Equal(t, 0.20, config.SnapshotThreshold)
	assert.Equal(t, DefaultBufferSize, config.BufferSize)
}

func TestNewCompactor_AppliesDefaults(t *testing.T) {
	logger := logrus.New()

	// Test with zero values
	config := CompactorConfig{Dir: "/tmp"}
	compactor := NewCompactor(config, logger)

	assert.Equal(t, 5, compactor.config.MaxFilesPerMerge)
	assert.Equal(t, 0.20, compactor.config.SnapshotThreshold)
	assert.Equal(t, DefaultBufferSize, compactor.config.BufferSize)
}

// createEmptyFile creates an empty file with the given name.
func createEmptyFile(t *testing.T, dir, name string) {
	t.Helper()
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	require.NoError(t, err)
	f.Close()
}

// createWALFile creates a minimal valid WAL file.
func createWALFile(t *testing.T, path string) {
	t.Helper()

	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	walWriter := NewWALWriter(f)

	// Write a simple node
	err = walWriter.WriteAddNode(0, 0)
	require.NoError(t, err)

	// Write a link
	err = walWriter.WriteReplaceLinksAtLevel(0, 0, []uint64{1, 2})
	require.NoError(t, err)

	// Write entrypoint
	err = walWriter.WriteSetEntryPointMaxLevel(0, 0)
	require.NoError(t, err)
}
