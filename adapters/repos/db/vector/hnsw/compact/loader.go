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

// LoadResult contains the result of loading commit logs.
type LoadResult struct {
	// State is the accumulated HNSW graph state, or nil if directory was empty.
	State *ent.DeserializationResult

	// RecoveredFromCrash is true if a truncated/corrupt WAL file was detected
	// and truncated during loading. When true, the caller should start a new
	// commit log file instead of appending to the existing one.
	RecoveredFromCrash bool
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
// Returns nil result (not error) if directory is empty or doesn't exist.
// If a truncated WAL file is detected (crash recovery), RecoveredFromCrash will be true.
func (l *Loader) Load() (*LoadResult, error) {
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

		// Bumped to INFO and enriched with node_count + entrypoint_changed for
		// the issues#200 investigation. We want to be able to grep CI logs and
		// see, for every snapshot loaded, what shape it had on disk. A snapshot
		// with node_count=0 is the panic-producing shape; if the producer-side
		// fix worked it should never appear here.
		nodeCount := 0
		entrypointChanged := false
		hasCompression := false
		if result != nil {
			nodeCount = len(result.Nodes())
			entrypointChanged = result.EntrypointChanged()
			hasCompression = result.HasCompression()
		}
		l.config.Logger.WithFields(logrus.Fields{
			"action":             "hnsw_loader_snapshot",
			"snapshot":           state.Snapshot.Path,
			"end_ts":             snapshotEndTS,
			"node_count":         nodeCount,
			"entrypoint_changed": entrypointChanged,
			"has_compression":    hasCompression,
		}).Info("loaded snapshot")
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
	var recoveredFromCrash bool
	for _, f := range walFiles {
		var crashed bool
		result, crashed, err = l.loadWALFile(f, result)
		if err != nil {
			return nil, errors.Wrapf(err, "load WAL file %s", f.Path)
		}
		if crashed {
			recoveredFromCrash = true
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

	// Diagnostic: a result that is "non-empty" by IsEmpty()'s definition but has
	// zero nodes is incoherent — IsEmpty() returns false because EntrypointChanged
	// or compression data is set, but downstream callers expect at least one node.
	// Loading such a result causes h.nodes to be a zero-length slice, which panics
	// on the next search via isEmptyUnlocked. See issue weaviate/0-weaviate-issues#200.
	if result != nil && len(result.Nodes()) == 0 {
		fields := logrus.Fields{
			"action":             "hnsw_loader_zero_node_result",
			"snapshot_path":      "",
			"snapshot_end_ts":    int64(0),
			"entrypoint_changed": result.EntrypointChanged(),
			"entrypoint_id":      result.Entrypoint(),
			"has_compression":    result.HasCompression(),
			"has_muvera":         result.HasMuvera(),
			"tombstone_count":    len(result.Tombstones()),
			"wal_files_loaded":   len(walFiles),
		}
		if state.Snapshot != nil {
			fields["snapshot_path"] = state.Snapshot.Path
			fields["snapshot_end_ts"] = state.Snapshot.EndTS
		}
		walPaths := make([]string, 0, len(walFiles))
		for _, f := range walFiles {
			walPaths = append(walPaths, f.Path)
		}
		fields["wal_paths"] = walPaths
		l.config.Logger.WithFields(fields).
			Warn("hnsw loader produced a non-empty result with zero nodes; downstream search will panic")
	}

	return &LoadResult{
		State:              result,
		RecoveredFromCrash: recoveredFromCrash,
	}, nil
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
// Returns the result, whether crash recovery occurred (truncation), and any error.
func (l *Loader) loadWALFile(f FileInfo, state *ent.DeserializationResult) (*ent.DeserializationResult, bool, error) {
	file, err := l.fs.Open(f.Path)
	if err != nil {
		return state, false, errors.Wrapf(err, "open file %s", f.Path)
	}
	defer file.Close()

	walReader := NewWALCommitReader(file, l.config.Logger)
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
			}).Warn("WAL file truncated - recovering from crash")

			// Truncate raw files to remove partial entries.
			// This prevents the partial entry from becoming corrupted on next write
			// and ensures clean compaction later.
			if f.Type == FileTypeRaw {
				validBytes := walReader.BytesRead()
				if truncErr := l.fs.Truncate(f.Path, validBytes); truncErr != nil {
					l.config.Logger.WithError(truncErr).
						WithField("file", f.Path).
						WithField("valid_bytes", validBytes).
						Error("failed to truncate corrupt WAL file")
				} else {
					l.config.Logger.WithFields(logrus.Fields{
						"action":      "hnsw_loader",
						"file":        f.Path,
						"valid_bytes": validBytes,
					}).Info("truncated corrupt WAL file")
				}
			}

			return result, true, nil // true = recovered from crash
		}
		return result, false, err
	}

	l.config.Logger.WithFields(logrus.Fields{
		"action":   "hnsw_loader",
		"file":     f.Path,
		"type":     f.Type.String(),
		"start_ts": f.StartTS,
		"end_ts":   f.EndTS,
	}).Debug("loaded WAL file")

	return result, false, nil
}
