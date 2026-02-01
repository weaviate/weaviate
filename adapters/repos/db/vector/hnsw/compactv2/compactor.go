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
	"os"
	"path/filepath"
	"sort"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

// Action represents the type of compaction action to perform.
type Action int

const (
	// ActionNone indicates no action is needed.
	ActionNone Action = iota
	// ActionMergeSorted indicates sorted files should be merged.
	ActionMergeSorted
	// ActionCreateSnapshot indicates a new snapshot should be created.
	ActionCreateSnapshot
)

// String returns a human-readable description of the action.
func (a Action) String() string {
	switch a {
	case ActionNone:
		return "none"
	case ActionMergeSorted:
		return "merge_sorted"
	case ActionCreateSnapshot:
		return "create_snapshot"
	default:
		return "unknown"
	}
}

// CompactorConfig contains configuration for the Compactor.
type CompactorConfig struct {
	// Dir is the directory containing commit log files.
	Dir string

	// MaxFilesPerMerge is the maximum number of files to merge in one operation.
	// Default: 5
	MaxFilesPerMerge int

	// SnapshotThreshold is the ratio of sorted file size to total size above which
	// we should create a new snapshot instead of just merging sorted files.
	// Default: 0.20 (20%)
	SnapshotThreshold float64

	// BufferSize is the buffer size for file I/O operations.
	// Default: 1MB (DefaultBufferSize)
	BufferSize int

	// FS is the filesystem interface to use for file operations.
	// If nil, defaults to common.NewOSFS().
	FS common.FS
}

// DefaultCompactorConfig returns the default configuration.
func DefaultCompactorConfig(dir string) CompactorConfig {
	return CompactorConfig{
		Dir:               dir,
		MaxFilesPerMerge:  5,
		SnapshotThreshold: 0.20,
		BufferSize:        DefaultBufferSize,
	}
}

// Compactor manages the compaction process for commit log files.
//
// Each call to [Compactor.RunCycle] performs one iteration of the compaction loop:
//  1. Cleanup orphaned temp files from interrupted operations
//  2. Discover files via [FileDiscovery] to get current [DirectoryState]
//  3. Resolve overlaps by deleting files contained within merged ranges
//  4. Convert raw and .condensed files to .sorted format
//  5. Decide on an [Action]: merge sorted files or create a snapshot
//  6. Execute the chosen action using [NWayMerger] and appropriate writers
//
// The decision logic balances write amplification against file count:
// when sorted files exceed SnapshotThreshold of total size, a new snapshot
// is created; otherwise, sorted files are merged to reduce count.
//
// External logic (typically a background goroutine) is responsible for
// calling RunCycle periodically. Each cycle is idempotent and crash-safe
// via [SafeFileWriter].
type Compactor struct {
	config CompactorConfig
	logger logrus.FieldLogger
	fs     common.FS
}

// NewCompactor creates a new Compactor.
func NewCompactor(config CompactorConfig, logger logrus.FieldLogger) *Compactor {
	// Apply defaults for zero values
	if config.MaxFilesPerMerge <= 0 {
		config.MaxFilesPerMerge = 5
	}
	if config.SnapshotThreshold <= 0 {
		config.SnapshotThreshold = 0.20
	}
	if config.BufferSize <= 0 {
		config.BufferSize = DefaultBufferSize
	}
	if config.FS == nil {
		config.FS = common.NewOSFS()
	}

	return &Compactor{
		config: config,
		logger: logger,
		fs:     config.FS,
	}
}

// RunCycle performs a single iteration of the compaction loop.
// It returns the action that was taken (or ActionNone if nothing was done).
func (c *Compactor) RunCycle() (Action, error) {
	// Step 1: Cleanup orphaned temp files and detect overlaps
	if err := c.cleanup(); err != nil {
		return ActionNone, errors.Wrap(err, "cleanup")
	}

	// Step 2: Discover files
	discovery := NewFileDiscoveryWithFS(c.config.Dir, c.fs)
	state, err := discovery.Scan()
	if err != nil {
		return ActionNone, errors.Wrap(err, "file discovery")
	}

	// Step 3: Resolve overlaps (delete contained files)
	if err := c.resolveOverlaps(state); err != nil {
		return ActionNone, errors.Wrap(err, "resolve overlaps")
	}

	// Step 4: Convert raw/condensed files to sorted
	if err := c.convertToSorted(state); err != nil {
		return ActionNone, errors.Wrap(err, "convert to sorted")
	}

	// Re-scan after conversions
	state, err = discovery.Scan()
	if err != nil {
		return ActionNone, errors.Wrap(err, "rescan after conversion")
	}

	// Step 5: Decide action
	action := c.decideAction(state)
	if action == ActionNone {
		return ActionNone, nil
	}

	// Step 6: Execute action
	switch action {
	case ActionNone:
		// Already handled above
	case ActionMergeSorted:
		if err := c.mergeSorted(state); err != nil {
			return ActionNone, errors.Wrap(err, "merge sorted files")
		}
	case ActionCreateSnapshot:
		if err := c.createSnapshot(state); err != nil {
			return ActionNone, errors.Wrap(err, "create snapshot")
		}
	}

	return action, nil
}

// cleanup removes orphaned temp files.
func (c *Compactor) cleanup() error {
	return CleanupOrphanedTempFilesWithFS(c.config.Dir, c.fs)
}

// resolveOverlaps removes files that are contained within merged ranges.
func (c *Compactor) resolveOverlaps(state *DirectoryState) error {
	for _, overlap := range state.Overlaps {
		c.logger.WithFields(logrus.Fields{
			"action":         "hnsw_compactor_resolve_overlap",
			"merged_file":    filepath.Base(overlap.MergedFile.Path),
			"contained_file": filepath.Base(overlap.ContainedFile.Path),
		}).Debug("removing file contained in merged range")

		if err := c.fs.Remove(overlap.ContainedFile.Path); err != nil && !os.IsNotExist(err) {
			return errors.Wrapf(err, "remove contained file %s", overlap.ContainedFile.Path)
		}
	}
	return nil
}

// convertToSorted converts raw and condensed files to sorted format.
func (c *Compactor) convertToSorted(state *DirectoryState) error {
	// Convert raw files (except live file)
	for _, f := range state.RawFiles {
		if err := c.convertFileToSorted(f); err != nil {
			return errors.Wrapf(err, "convert raw file %s", f.Path)
		}
	}

	// Convert condensed files
	for _, f := range state.CondensedFiles {
		if err := c.convertFileToSorted(f); err != nil {
			return errors.Wrapf(err, "convert condensed file %s", f.Path)
		}
	}

	return nil
}

// convertFileToSorted converts a single file (raw or condensed) to sorted format.
func (c *Compactor) convertFileToSorted(f FileInfo) error {
	c.logger.WithFields(logrus.Fields{
		"action": "hnsw_compactor_convert",
		"file":   filepath.Base(f.Path),
		"type":   f.Type.String(),
	}).Debug("converting file to sorted format")

	// Open source file
	srcFile, err := c.fs.Open(f.Path)
	if err != nil {
		return errors.Wrapf(err, "open source file")
	}
	defer srcFile.Close()

	// Read into memory using WALCommitReader + InMemoryReader
	walReader := NewWALCommitReader(srcFile, c.logger)
	inMemReader := NewInMemoryReader(walReader, c.logger)
	result, err := inMemReader.Do(nil, true) // keepLinkReplaceInformation = true
	if err != nil {
		return errors.Wrap(err, "read file into memory")
	}

	// Build output filename
	outFilename := BuildMergedFilename(f.StartTS, f.EndTS, FileTypeSorted)
	outPath := filepath.Join(c.config.Dir, outFilename)

	// Write using SortedWriter via SafeFileWriter
	sfw, err := NewSafeFileWriterWithFS(outPath, c.config.BufferSize, c.fs)
	if err != nil {
		return errors.Wrap(err, "create safe file writer")
	}
	defer sfw.Abort() // cleanup on error

	sortedWriter := NewSortedWriter(sfw.Writer(), c.logger)
	if err := sortedWriter.WriteAll(result); err != nil {
		return errors.Wrap(err, "write sorted file")
	}

	if err := sfw.Commit(); err != nil {
		return errors.Wrap(err, "commit sorted file")
	}

	// Delete original file after successful conversion
	if err := c.fs.Remove(f.Path); err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "remove original file")
	}

	c.logger.WithFields(logrus.Fields{
		"action":   "hnsw_compactor_convert",
		"original": filepath.Base(f.Path),
		"output":   outFilename,
	}).Debug("converted file to sorted format")

	return nil
}

// decideAction determines what compaction action to take based on current state.
func (c *Compactor) decideAction(state *DirectoryState) Action {
	snapshotSize := state.TotalSnapshotSize()
	sortedSize := state.TotalSortedSize()
	totalSize := snapshotSize + sortedSize
	sortedCount := len(state.SortedFiles)

	log := c.logger.WithFields(logrus.Fields{
		"action":        "hnsw_compactor_decide",
		"snapshot_size": snapshotSize,
		"sorted_size":   sortedSize,
		"sorted_count":  sortedCount,
		"has_snapshot":  state.Snapshot != nil,
		"threshold":     c.config.SnapshotThreshold,
		"max_files":     c.config.MaxFilesPerMerge,
	})

	if totalSize == 0 {
		log.Debug("decision: no action - no data to compact (total size is 0)")
		return ActionNone
	}

	sortedRatio := float64(sortedSize) / float64(totalSize)
	log = log.WithField("sorted_ratio", sortedRatio)

	if state.Snapshot == nil {
		// No snapshot exists yet
		if sortedCount > c.config.MaxFilesPerMerge {
			log.WithField("reason", "no snapshot exists, but too many sorted files to snapshot at once").
				Debugf("decision: merge sorted files first (%d files > max %d)", sortedCount, c.config.MaxFilesPerMerge)
			return ActionMergeSorted
		}
		if sortedCount > 0 {
			log.WithField("reason", "no snapshot exists, creating initial snapshot").
				Debugf("decision: create snapshot from %d sorted file(s)", sortedCount)
			return ActionCreateSnapshot
		}
		log.Debug("decision: no action - no snapshot and no sorted files")
		return ActionNone
	}

	// Snapshot exists - decide based on sorted ratio vs threshold
	if sortedRatio > c.config.SnapshotThreshold {
		// Sorted files are large relative to snapshot - worth creating new snapshot
		if sortedCount > c.config.MaxFilesPerMerge {
			log.WithField("reason", "sorted ratio exceeds threshold, but too many files to snapshot at once").
				Debugf("decision: merge sorted files first (%d files > max %d, ratio %.1f%% > threshold %.1f%%)",
					sortedCount, c.config.MaxFilesPerMerge, sortedRatio*100, c.config.SnapshotThreshold*100)
			return ActionMergeSorted
		}
		log.WithField("reason", "sorted ratio exceeds threshold, write amplification is acceptable").
			Debugf("decision: create snapshot (ratio %.1f%% > threshold %.1f%%)",
				sortedRatio*100, c.config.SnapshotThreshold*100)
		return ActionCreateSnapshot
	}

	// Sorted ratio is below threshold - write amplification not worth it
	if sortedCount > 1 {
		log.WithField("reason", "sorted ratio below threshold, merging sorted files to reduce count").
			Debugf("decision: merge %d sorted files (ratio %.1f%% <= threshold %.1f%%)",
				sortedCount, sortedRatio*100, c.config.SnapshotThreshold*100)
		return ActionMergeSorted
	}

	log.WithField("reason", "sorted ratio below threshold and only one sorted file").
		Debugf("decision: no action (ratio %.1f%% <= threshold %.1f%%, only %d sorted file)",
			sortedRatio*100, c.config.SnapshotThreshold*100, sortedCount)
	return ActionNone
}

// mergeSorted merges the oldest N sorted files into one.
func (c *Compactor) mergeSorted(state *DirectoryState) error {
	// Select files to merge (oldest N)
	filesToMerge := state.SortedFiles
	if len(filesToMerge) > c.config.MaxFilesPerMerge {
		filesToMerge = filesToMerge[:c.config.MaxFilesPerMerge]
	}

	if len(filesToMerge) < 2 {
		return nil // Nothing to merge
	}

	c.logger.WithFields(logrus.Fields{
		"action":     "hnsw_compactor_merge",
		"file_count": len(filesToMerge),
	}).Debug("merging sorted files")

	// Create iterators for each file
	// Track opened files for cleanup
	openedFiles := make([]common.File, 0, len(filesToMerge))
	closeFiles := func() {
		for _, f := range openedFiles {
			f.Close()
		}
	}
	defer closeFiles()

	iterators := make([]IteratorLike, 0, len(filesToMerge))
	for i, f := range filesToMerge {
		file, err := c.fs.Open(f.Path)
		if err != nil {
			return errors.Wrapf(err, "open file %s", f.Path)
		}
		openedFiles = append(openedFiles, file)

		walReader := NewWALCommitReader(file, c.logger)
		it, err := NewIterator(walReader, i, c.logger)
		if err != nil {
			return errors.Wrapf(err, "create iterator for %s", f.Path)
		}
		iterators = append(iterators, it)
	}

	// Create n-way merger
	merger, err := NewNWayMerger(iterators, c.logger)
	if err != nil {
		return errors.Wrap(err, "create n-way merger")
	}

	// Determine output filename
	startTS := filesToMerge[0].StartTS
	endTS := filesToMerge[len(filesToMerge)-1].EndTS
	outFilename := BuildMergedFilename(startTS, endTS, FileTypeSorted)
	outPath := filepath.Join(c.config.Dir, outFilename)

	// Write merged output using WALWriter via SafeFileWriter
	sfw, err := NewSafeFileWriterWithFS(outPath, c.config.BufferSize, c.fs)
	if err != nil {
		return errors.Wrap(err, "create safe file writer")
	}
	defer sfw.Abort()

	walWriter := NewWALWriter(sfw.Writer())

	// Write global commits first
	if err := c.writeGlobalCommits(walWriter, merger.GlobalCommits()); err != nil {
		return errors.Wrap(err, "write global commits")
	}

	// Write node commits
	for {
		nc, err := merger.Next()
		if err != nil {
			return errors.Wrap(err, "get next from merger")
		}
		if nc == nil {
			break
		}

		if err := c.writeNodeCommits(walWriter, nc); err != nil {
			return errors.Wrapf(err, "write commits for node %d", nc.NodeID)
		}
	}

	if err := sfw.Commit(); err != nil {
		return errors.Wrap(err, "commit merged file")
	}

	// Delete source files after successful merge
	for _, f := range filesToMerge {
		if err := c.fs.Remove(f.Path); err != nil && !os.IsNotExist(err) {
			c.logger.WithError(err).WithField("file", f.Path).Warn("failed to delete source file after merge")
		}
	}

	c.logger.WithFields(logrus.Fields{
		"action":       "hnsw_compactor_merge",
		"output":       outFilename,
		"merged_count": len(filesToMerge),
	}).Debug("merged sorted files")

	return nil
}

// createSnapshot creates a new snapshot from the current state.
func (c *Compactor) createSnapshot(state *DirectoryState) error {
	// Collect all inputs
	var allInputFiles []FileInfo

	// Include existing snapshot if present
	if state.Snapshot != nil {
		allInputFiles = append(allInputFiles, *state.Snapshot)
	}

	// Include all sorted files
	allInputFiles = append(allInputFiles, state.SortedFiles...)

	if len(allInputFiles) == 0 {
		return nil // Nothing to snapshot
	}

	// Sort by StartTS to ensure proper precedence
	sort.Slice(allInputFiles, func(i, j int) bool {
		return allInputFiles[i].StartTS < allInputFiles[j].StartTS
	})

	c.logger.WithFields(logrus.Fields{
		"action":       "hnsw_compactor_snapshot",
		"input_count":  len(allInputFiles),
		"has_snapshot": state.Snapshot != nil,
		"sorted_count": len(state.SortedFiles),
	}).Debug("creating snapshot")

	// Track opened files for cleanup
	openedFiles := make([]common.File, 0, len(allInputFiles))
	closeFiles := func() {
		for _, f := range openedFiles {
			f.Close()
		}
	}
	defer closeFiles()

	// Create iterators with proper precedence (older = lower ID)
	var iterators []IteratorLike
	for i, f := range allInputFiles {
		var it IteratorLike
		var err error

		if f.Type == FileTypeSnapshot {
			it, err = NewSnapshotIteratorWithFS(f.Path, i, c.logger, c.fs)
		} else {
			file, err2 := c.fs.Open(f.Path)
			if err2 != nil {
				return errors.Wrapf(err2, "open file %s", f.Path)
			}
			openedFiles = append(openedFiles, file)

			walReader := NewWALCommitReader(file, c.logger)
			it, err = NewIterator(walReader, i, c.logger)
		}

		if err != nil {
			return errors.Wrapf(err, "create iterator for %s", f.Path)
		}
		iterators = append(iterators, it)
	}

	// Create n-way merger
	merger, err := NewNWayMerger(iterators, c.logger)
	if err != nil {
		return errors.Wrap(err, "create n-way merger")
	}

	// Determine output filename
	startTS := allInputFiles[0].StartTS
	endTS := allInputFiles[len(allInputFiles)-1].EndTS
	outFilename := BuildMergedFilename(startTS, endTS, FileTypeSnapshot)
	outPath := filepath.Join(c.config.Dir, outFilename)

	// Write snapshot via SafeFileWriter
	sfw, err := NewSafeFileWriterWithFS(outPath, c.config.BufferSize, c.fs)
	if err != nil {
		return errors.Wrap(err, "create safe file writer")
	}
	defer sfw.Abort()

	snapshotWriter := NewSnapshotWriter(sfw.Writer())
	if err := snapshotWriter.WriteFromMerger(merger); err != nil {
		return errors.Wrap(err, "write snapshot from merger")
	}

	if err := sfw.Commit(); err != nil {
		return errors.Wrap(err, "commit snapshot file")
	}

	// Delete source files after successful snapshot creation
	for _, f := range allInputFiles {
		if err := c.fs.Remove(f.Path); err != nil && !os.IsNotExist(err) {
			c.logger.WithError(err).WithField("file", f.Path).Warn("failed to delete source file after snapshot")
		}
	}

	c.logger.WithFields(logrus.Fields{
		"action":      "hnsw_compactor_snapshot",
		"output":      outFilename,
		"input_count": len(allInputFiles),
	}).Debug("created snapshot")

	return nil
}

// writeGlobalCommits writes global commits using WALWriter.
func (c *Compactor) writeGlobalCommits(w *WALWriter, commits []Commit) error {
	for _, c := range commits {
		switch ct := c.(type) {
		case *SetEntryPointMaxLevelCommit:
			if err := w.WriteSetEntryPointMaxLevel(ct.Entrypoint, ct.Level); err != nil {
				return err
			}
		case *AddPQCommit:
			if err := w.WriteAddPQ(ct.Data); err != nil {
				return err
			}
		case *AddSQCommit:
			if err := w.WriteAddSQ(ct.Data); err != nil {
				return err
			}
		case *AddRQCommit:
			if err := w.WriteAddRQ(ct.Data); err != nil {
				return err
			}
		case *AddBRQCommit:
			if err := w.WriteAddBRQ(ct.Data); err != nil {
				return err
			}
		case *AddMuveraCommit:
			if err := w.WriteAddMuvera(ct.Data); err != nil {
				return err
			}
		}
	}
	return nil
}

// writeNodeCommits writes commits for a single node using WALWriter.
func (c *Compactor) writeNodeCommits(w *WALWriter, nc *NodeCommits) error {
	for _, c := range nc.Commits {
		switch ct := c.(type) {
		case *AddNodeCommit:
			if err := w.WriteAddNode(ct.ID, ct.Level); err != nil {
				return err
			}
		case *DeleteNodeCommit:
			if err := w.WriteDeleteNode(ct.ID); err != nil {
				return err
			}
		case *AddLinkAtLevelCommit:
			if err := w.WriteAddLinkAtLevel(ct.Source, ct.Level, ct.Target); err != nil {
				return err
			}
		case *AddLinksAtLevelCommit:
			if err := w.WriteAddLinksAtLevel(ct.Source, ct.Level, ct.Targets); err != nil {
				return err
			}
		case *ReplaceLinksAtLevelCommit:
			if err := w.WriteReplaceLinksAtLevel(ct.Source, ct.Level, ct.Targets); err != nil {
				return err
			}
		case *ClearLinksCommit:
			if err := w.WriteClearLinks(ct.ID); err != nil {
				return err
			}
		case *ClearLinksAtLevelCommit:
			if err := w.WriteClearLinksAtLevel(ct.ID, ct.Level); err != nil {
				return err
			}
		case *AddTombstoneCommit:
			if err := w.WriteAddTombstone(ct.ID); err != nil {
				return err
			}
		case *RemoveTombstoneCommit:
			if err := w.WriteRemoveTombstone(ct.ID); err != nil {
				return err
			}
		}
	}
	return nil
}
