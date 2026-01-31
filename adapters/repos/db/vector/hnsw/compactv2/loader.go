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

package compactv2

import (
	"bufio"
	"io"
	"sort"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const (
	// DefaultLoaderBufferSize is the default buffer size for reading WAL files (512KB).
	DefaultLoaderBufferSize = 512 * 1024
)

// LoaderConfig contains configuration for the Loader.
type LoaderConfig struct {
	// Dir is the directory containing commit log files.
	Dir string

	// Logger is used for logging during load operations.
	Logger logrus.FieldLogger

	// BufferSize is the buffer size for reading WAL files.
	// If zero or negative, DefaultLoaderBufferSize is used.
	BufferSize int

	// FS is the filesystem interface to use for file operations.
	// If nil, defaults to common.NewOSFS().
	FS common.FS
}

// Loader reads all commit log files at startup and returns the accumulated
// HNSW graph state.
//
// The [Loader.Load] method executes the following steps:
//  1. Migrate snapshots from the old .hnsw.snapshot.d/ directory if needed
//  2. Cleanup orphaned temp files from interrupted operations
//  3. Discover files via [FileDiscovery] to get current [DirectoryState]
//  4. Load snapshot via [SnapshotReader] if one exists (provides base state)
//  5. Filter out WAL files already covered by the snapshot's timestamp range
//  6. Apply remaining WAL files in timestamp order via [InMemoryReader]
//
// Unlike [Compactor], the loader includes all files including the most recent
// commit log (LiveFile) since at startup nothing is being written yet.
//
// Returns nil state (not an error) if the directory is empty or doesn't exist.
type Loader struct {
	config LoaderConfig
	fs     common.FS
}

// NewLoader creates a new Loader with the given configuration.
func NewLoader(config LoaderConfig) *Loader {
	if config.BufferSize <= 0 {
		config.BufferSize = DefaultLoaderBufferSize
	}
	if config.FS == nil {
		config.FS = common.NewOSFS()
	}
	return &Loader{config: config, fs: config.FS}
}

// Load discovers and loads all commit log files, returning the accumulated state.
// Returns nil state (not error) if directory is empty or doesn't exist.
func (l *Loader) Load() (*ent.DeserializationResult, error) {
	// 0. Migrate snapshots from old directory FIRST (before any other operations)
	// This ensures that snapshots stored in the old .hnsw.snapshot.d/ directory
	// are moved to the new unified .hnsw.commitlog.d/ directory before we scan.
	migrator := NewMigratorWithFS(l.config.Dir, l.config.Logger, l.fs)
	if err := migrator.MigrateSnapshotDirectory(); err != nil {
		return nil, errors.Wrap(err, "migrate snapshot directory")
	}

	// 1. Cleanup orphaned temp files from previous incomplete operations
	if err := CleanupOrphanedTempFilesWithFS(l.config.Dir, l.fs); err != nil {
		return nil, errors.Wrap(err, "cleanup orphaned temp files")
	}

	// Also cleanup any orphaned .migrating files from incomplete migrations
	if err := CleanupMigratingFilesWithFS(l.config.Dir, l.fs); err != nil {
		return nil, errors.Wrap(err, "cleanup orphaned migrating files")
	}

	// Cleanup corrupt .condensed/.sorted files (when raw file with same timestamp exists)
	if err := CleanupCorruptCondensedFilesWithFS(l.config.Dir, l.fs); err != nil {
		return nil, errors.Wrap(err, "cleanup corrupt condensed files")
	}

	// 2. Discover files in the directory
	discovery := NewFileDiscoveryWithFS(l.config.Dir, l.fs)
	state, err := discovery.Scan()
	if err != nil {
		return nil, errors.Wrap(err, "scan directory")
	}

	// 3. Build list of files to load (including LiveFile for startup)
	walFiles := l.collectWALFiles(state)

	// 4. Filter out overlapped files
	walFiles = l.filterOverlappedFiles(walFiles, state.Overlaps)

	// 5. Sort by timestamp (oldest first)
	sort.Slice(walFiles, func(i, j int) bool {
		return walFiles[i].StartTS < walFiles[j].StartTS
	})

	// 6. Load snapshot if exists (starting point)
	var result *ent.DeserializationResult
	var snapshotEndTS int64

	if state.Snapshot != nil {
		snapshotReader := NewSnapshotReader(l.config.Logger)
		result, err = snapshotReader.ReadFromFileWithFS(state.Snapshot.Path, l.fs)
		if err != nil {
			return nil, errors.Wrapf(err, "read snapshot %s", state.Snapshot.Path)
		}
		snapshotEndTS = state.Snapshot.EndTS

		l.config.Logger.WithFields(logrus.Fields{
			"action":   "hnsw_loader",
			"snapshot": state.Snapshot.Path,
			"end_ts":   snapshotEndTS,
		}).Debug("loaded snapshot")
	}

	// 7. Filter WAL files that are already covered by snapshot
	if result != nil {
		walFiles = l.filterFilesAlreadyInSnapshot(walFiles, snapshotEndTS)
	}

	// 8. If no snapshot and no WAL files, return nil (empty directory)
	if result == nil && len(walFiles) == 0 {
		return nil, nil
	}

	// 9. Load WAL files sequentially (oldest to newest)
	for _, f := range walFiles {
		result, err = l.loadWALFile(f, result)
		if err != nil {
			return nil, errors.Wrapf(err, "load WAL file %s", f.Path)
		}
	}

	l.config.Logger.WithFields(logrus.Fields{
		"action":    "hnsw_loader",
		"wal_files": len(walFiles),
	}).Debug("loaded all WAL files")

	// If no meaningful data was loaded (e.g., only empty files), return nil
	// This maintains compatibility with the old behavior where an empty commit log
	// directory would result in no state being returned.
	if result != nil && result.IsEmpty() {
		return nil, nil
	}

	return result, nil
}

// collectWALFiles collects all WAL files that need to be loaded.
// This includes SortedFiles, CondensedFiles, RawFiles, and LiveFile.
func (l *Loader) collectWALFiles(state *DirectoryState) []FileInfo {
	files := make([]FileInfo, 0, len(state.SortedFiles)+len(state.CondensedFiles)+len(state.RawFiles)+1)

	files = append(files, state.SortedFiles...)
	files = append(files, state.CondensedFiles...)
	files = append(files, state.RawFiles...)

	// Include LiveFile at startup (unlike compaction)
	if state.LiveFile != nil {
		files = append(files, *state.LiveFile)
	}

	return files
}

// filterOverlappedFiles removes files that are contained within merged ranges.
func (l *Loader) filterOverlappedFiles(files []FileInfo, overlaps []Overlap) []FileInfo {
	if len(overlaps) == 0 {
		return files
	}

	// Build set of overlapped file paths
	overlappedPaths := make(map[string]struct{}, len(overlaps))
	for _, o := range overlaps {
		overlappedPaths[o.ContainedFile.Path] = struct{}{}
	}

	// Filter out overlapped files
	filtered := make([]FileInfo, 0, len(files))
	for _, f := range files {
		if _, isOverlapped := overlappedPaths[f.Path]; !isOverlapped {
			filtered = append(filtered, f)
		}
	}

	return filtered
}

// filterFilesAlreadyInSnapshot removes WAL files that are already covered by the snapshot.
func (l *Loader) filterFilesAlreadyInSnapshot(files []FileInfo, snapshotEndTS int64) []FileInfo {
	filtered := make([]FileInfo, 0, len(files))
	for _, f := range files {
		// Keep files that have data after the snapshot
		if f.EndTS > snapshotEndTS {
			filtered = append(filtered, f)
		}
	}
	return filtered
}

// loadWALFile reads a single WAL file and applies it to the current state.
func (l *Loader) loadWALFile(f FileInfo, state *ent.DeserializationResult) (*ent.DeserializationResult, error) {
	file, err := l.fs.Open(f.Path)
	if err != nil {
		return state, errors.Wrapf(err, "open file %s", f.Path)
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, l.config.BufferSize)
	walReader := NewWALCommitReader(reader, l.config.Logger)
	inMemReader := NewInMemoryReader(walReader, l.config.Logger)

	// keepLinkReplaceInfo=false at startup since we're building final state
	result, err := inMemReader.Do(state, false)
	if err != nil {
		// For EOF/UnexpectedEOF, log warning and continue with partial state
		// This can happen if the file was truncated due to crash
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			l.config.Logger.WithFields(logrus.Fields{
				"action": "hnsw_loader",
				"file":   f.Path,
				"error":  err.Error(),
			}).Warn("WAL file may be truncated, continuing with partial state")
			return result, nil
		}
		return result, err
	}

	l.config.Logger.WithFields(logrus.Fields{
		"action":   "hnsw_loader",
		"file":     f.Path,
		"type":     f.Type.String(),
		"start_ts": f.StartTS,
		"end_ts":   f.EndTS,
	}).Debug("loaded WAL file")

	return result, nil
}
