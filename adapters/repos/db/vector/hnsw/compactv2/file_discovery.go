//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package compactv2

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

const (
	// File suffixes
	suffixCondensed = ".condensed"
	suffixSorted    = ".sorted"
	suffixSnapshot  = ".snapshot"
)

// Overlap represents a detected overlap between files.
// The merged file contains the contained file's range.
type Overlap struct {
	MergedFile    FileInfo
	ContainedFile FileInfo
}

// DirectoryState represents the current state of commit log files in a directory.
type DirectoryState struct {
	// Snapshot is the current snapshot file, or nil if none exists.
	Snapshot *FileInfo

	// SortedFiles are all .sorted files, sorted by StartTS (oldest first).
	SortedFiles []FileInfo

	// RawFiles are files without a suffix that need conversion (excluding the live file).
	RawFiles []FileInfo

	// CondensedFiles are .condensed files that need conversion.
	CondensedFiles []FileInfo

	// LiveFile is the highest timestamp file without a suffix.
	// This is the currently active file being written to and should never be touched.
	LiveFile *FileInfo

	// Overlaps are detected overlaps that need resolution.
	Overlaps []Overlap
}

// TotalSortedSize returns the sum of all sorted file sizes.
func (ds *DirectoryState) TotalSortedSize() int64 {
	var total int64
	for _, f := range ds.SortedFiles {
		total += f.Size
	}
	return total
}

// TotalSnapshotSize returns the snapshot file size, or 0 if no snapshot exists.
func (ds *DirectoryState) TotalSnapshotSize() int64 {
	if ds.Snapshot == nil {
		return 0
	}
	return ds.Snapshot.Size
}

// FileDiscovery scans directories for commit log files.
type FileDiscovery struct {
	dir string
	fs  common.FS
}

// NewFileDiscovery creates a new file discovery scanner for the given directory.
func NewFileDiscovery(dir string) *FileDiscovery {
	return NewFileDiscoveryWithFS(dir, common.NewOSFS())
}

// NewFileDiscoveryWithFS creates a new file discovery scanner with a custom filesystem.
func NewFileDiscoveryWithFS(dir string, fs common.FS) *FileDiscovery {
	return &FileDiscovery{dir: dir, fs: fs}
}

// Scan scans the directory and returns the current state of commit log files.
func (d *FileDiscovery) Scan() (*DirectoryState, error) {
	entries, err := d.fs.ReadDir(d.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return &DirectoryState{}, nil
		}
		return nil, errors.Wrapf(err, "read directory %s", d.dir)
	}

	state := &DirectoryState{
		SortedFiles:    make([]FileInfo, 0),
		RawFiles:       make([]FileInfo, 0),
		CondensedFiles: make([]FileInfo, 0),
		Overlaps:       make([]Overlap, 0),
	}

	var allFiles []FileInfo

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()

		// Skip temp files
		if strings.HasSuffix(name, tempFileSuffix) {
			continue
		}

		info, err := d.parseFilename(name)
		if err != nil {
			// Skip files that don't match our naming convention
			continue
		}

		// Get file size
		stat, err := d.fs.Stat(filepath.Join(d.dir, name))
		if err != nil {
			return nil, errors.Wrapf(err, "stat file %s", name)
		}
		info.Size = stat.Size()

		allFiles = append(allFiles, info)
	}

	// Sort all files by StartTS to identify the live file
	sort.Slice(allFiles, func(i, j int) bool {
		return allFiles[i].StartTS < allFiles[j].StartTS
	})

	// Find the live file: highest timestamp raw file
	var highestRawTS int64 = -1
	highestRawIdx := -1
	for i, f := range allFiles {
		if f.Type == FileTypeRaw && f.StartTS > highestRawTS {
			highestRawTS = f.StartTS
			highestRawIdx = i
		}
	}

	// Categorize files
	for i, f := range allFiles {
		switch f.Type {
		case FileTypeSnapshot:
			if state.Snapshot == nil || f.StartTS < state.Snapshot.StartTS {
				// Keep the oldest snapshot (root file)
				state.Snapshot = &allFiles[i]
			}
		case FileTypeSorted:
			state.SortedFiles = append(state.SortedFiles, f)
		case FileTypeCondensed:
			state.CondensedFiles = append(state.CondensedFiles, f)
		case FileTypeRaw:
			if i == highestRawIdx {
				state.LiveFile = &allFiles[i]
			} else {
				state.RawFiles = append(state.RawFiles, f)
			}
		}
	}

	// Sort sorted files by StartTS (oldest first)
	sort.Slice(state.SortedFiles, func(i, j int) bool {
		return state.SortedFiles[i].StartTS < state.SortedFiles[j].StartTS
	})

	// Detect overlaps
	state.Overlaps = d.detectOverlaps(state)

	return state, nil
}

// parseFilename parses a commit log filename and extracts file info.
// Valid formats:
//   - {timestamp} (raw file)
//   - {timestamp}.condensed
//   - {timestamp}.sorted
//   - {timestamp}.snapshot
//   - {start}_{end}.sorted (merged range)
//   - {start}_{end}.snapshot (merged range)
func (d *FileDiscovery) parseFilename(name string) (FileInfo, error) {
	path := filepath.Join(d.dir, name)
	info := FileInfo{Path: path}

	// Determine file type and get base name
	baseName := name
	switch {
	case strings.HasSuffix(name, suffixCondensed):
		info.Type = FileTypeCondensed
		baseName = strings.TrimSuffix(name, suffixCondensed)
	case strings.HasSuffix(name, suffixSorted):
		info.Type = FileTypeSorted
		baseName = strings.TrimSuffix(name, suffixSorted)
	case strings.HasSuffix(name, suffixSnapshot):
		info.Type = FileTypeSnapshot
		baseName = strings.TrimSuffix(name, suffixSnapshot)
	default:
		info.Type = FileTypeRaw
	}

	// Parse timestamp(s) from base name
	// Format: {timestamp} or {start}_{end}
	if idx := strings.Index(baseName, "_"); idx != -1 {
		// Merged range format: {start}_{end}
		startStr := baseName[:idx]
		endStr := baseName[idx+1:]

		startTS, err := strconv.ParseInt(startStr, 10, 64)
		if err != nil {
			return FileInfo{}, errors.Wrapf(err, "parse start timestamp from %s", name)
		}

		endTS, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil {
			return FileInfo{}, errors.Wrapf(err, "parse end timestamp from %s", name)
		}

		info.StartTS = startTS
		info.EndTS = endTS
	} else {
		// Single timestamp format
		ts, err := strconv.ParseInt(baseName, 10, 64)
		if err != nil {
			return FileInfo{}, errors.Wrapf(err, "parse timestamp from %s", name)
		}

		info.StartTS = ts
		info.EndTS = ts
	}

	return info, nil
}

// detectOverlaps detects files that are contained within merged ranges.
func (d *FileDiscovery) detectOverlaps(state *DirectoryState) []Overlap {
	overlaps := make([]Overlap, 0)

	// Collect all files with timestamp ranges
	allFiles := make([]FileInfo, 0)
	allFiles = append(allFiles, state.SortedFiles...)
	allFiles = append(allFiles, state.RawFiles...)
	allFiles = append(allFiles, state.CondensedFiles...)
	if state.Snapshot != nil {
		allFiles = append(allFiles, *state.Snapshot)
	}

	// Find merged files (those with ranges)
	for _, merged := range allFiles {
		if !merged.IsMergedRange() {
			continue
		}

		// Check if any other file is contained within this merged range
		for _, other := range allFiles {
			if other.Path == merged.Path {
				continue
			}

			if merged.Contains(other) {
				overlaps = append(overlaps, Overlap{
					MergedFile:    merged,
					ContainedFile: other,
				})
			}
		}
	}

	return overlaps
}

// BuildMergedFilename builds a filename for a merged file.
// For a single source, returns "{timestamp}.{suffix}".
// For multiple sources, returns "{firstTS}_{lastTS}.{suffix}".
func BuildMergedFilename(startTS, endTS int64, fileType FileType) string {
	var base string
	if startTS == endTS {
		base = strconv.FormatInt(startTS, 10)
	} else {
		base = strconv.FormatInt(startTS, 10) + "_" + strconv.FormatInt(endTS, 10)
	}
	return base + fileType.Suffix()
}
