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

// Stats is a summary of the commit log directory, published by the Compactor
// at the end of each completed cycle. A published Stats is immutable: readers
// on other goroutines receive it through [Compactor.Stats] and must not
// modify it. Publishing at cycle end (rather than scanning on demand) keeps
// API reads free of disk I/O and never observes a directory mid-rewrite.
type Stats struct {
	// RawFiles is the number of unsorted commit log files awaiting
	// conversion, excluding the file currently being written to.
	RawFiles int

	// CondensedFiles is the number of .condensed (v1 format) files awaiting
	// conversion.
	CondensedFiles int

	// SortedFiles is the number of .sorted files awaiting merge or snapshot.
	SortedFiles int

	// SnapshotTimestamp is the newest commit timestamp absorbed into the
	// current snapshot, taken from the snapshot filename (unix seconds).
	// 0 when no snapshot exists.
	SnapshotTimestamp int64

	// TotalSizeBytes is the combined size of all commit log files in the
	// directory, including the actively written file.
	TotalSizeBytes int64

	// Cycles is the number of compaction cycles completed since the
	// Compactor was created, i.e. since the vector index was loaded.
	Cycles uint64
}

func statsFromState(state *DirectoryState, cycles uint64) *Stats {
	s := &Stats{
		RawFiles:       len(state.RawFiles),
		CondensedFiles: len(state.CondensedFiles),
		SortedFiles:    len(state.SortedFiles),
		Cycles:         cycles,
	}

	if state.Snapshot != nil {
		s.SnapshotTimestamp = state.Snapshot.EndTS
		s.TotalSizeBytes += state.Snapshot.Size
	}
	if state.LiveFile != nil {
		s.TotalSizeBytes += state.LiveFile.Size
	}
	for _, f := range state.RawFiles {
		s.TotalSizeBytes += f.Size
	}
	for _, f := range state.CondensedFiles {
		s.TotalSizeBytes += f.Size
	}
	s.TotalSizeBytes += state.TotalSortedSize()

	return s
}
