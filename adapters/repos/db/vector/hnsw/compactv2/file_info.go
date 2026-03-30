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

// FileType represents the type of a commit log file in the compactv2 system.
type FileType int

const (
	// FileTypeRaw is a freshly flushed, unsorted file (no suffix).
	// These files cannot be merged directly and must first be converted to .sorted.
	FileTypeRaw FileType = iota

	// FileTypeCondensed is a legacy v1 format file (.condensed suffix).
	// These are partially sorted and can be merged via InMemoryReader → SortedWriter.
	FileTypeCondensed

	// FileTypeSorted is a v2 format file (.sorted suffix).
	// These are fully sorted by node ID and can be directly merged via Iterator.
	FileTypeSorted

	// FileTypeSnapshot is an absolute state checkpoint (.snapshot suffix).
	// These can be merged via SnapshotIterator.
	FileTypeSnapshot
)

// String returns the string representation of the file type.
func (ft FileType) String() string {
	switch ft {
	case FileTypeRaw:
		return "raw"
	case FileTypeCondensed:
		return "condensed"
	case FileTypeSorted:
		return "sorted"
	case FileTypeSnapshot:
		return "snapshot"
	default:
		return "unknown"
	}
}

// Suffix returns the file suffix for this file type.
func (ft FileType) Suffix() string {
	switch ft {
	case FileTypeRaw:
		return ""
	case FileTypeCondensed:
		return ".condensed"
	case FileTypeSorted:
		return ".sorted"
	case FileTypeSnapshot:
		return ".snapshot"
	default:
		return ""
	}
}

// FileInfo contains metadata about a commit log file.
type FileInfo struct {
	// Path is the full path to the file.
	Path string

	// Type is the type of the file (Raw, Condensed, Sorted, Snapshot).
	Type FileType

	// StartTS is the first timestamp in the file's range.
	// For single files: the timestamp from the filename.
	// For merged files: the first timestamp in the range (e.g., 7 from 7_9.sorted).
	StartTS int64

	// EndTS is the last timestamp in the file's range.
	// For single files: same as StartTS.
	// For merged files: the last timestamp in the range (e.g., 9 from 7_9.sorted).
	EndTS int64

	// Size is the file size in bytes.
	Size int64
}

// IsMergedRange returns true if this file represents a merged range of files.
func (fi FileInfo) IsMergedRange() bool {
	return fi.StartTS != fi.EndTS
}

// Contains returns true if this file's timestamp range contains the other file's range.
func (fi FileInfo) Contains(other FileInfo) bool {
	return fi.StartTS <= other.StartTS && fi.EndTS >= other.EndTS
}

// Overlaps returns true if this file's timestamp range overlaps with the other file's range.
func (fi FileInfo) Overlaps(other FileInfo) bool {
	return fi.StartTS <= other.EndTS && fi.EndTS >= other.StartTS
}
