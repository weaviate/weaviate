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

// Package compactv2 manages HNSW commit log storage: reading, writing, merging,
// and compacting commit logs into snapshots for efficient index persistence and
// fast startup times.
//
// # Philosophy
//
// The v1 compaction system could only condense files in-memory: read the entire
// file, deduplicate commits, write back to disk. The only way to merge multiple
// files was to append them ("combine") and then condense the result. This
// approach suffered from severe write amplification and did not scale for large
// graphs or clusters.
//
// The core goal of compactv2 is on-disk stream merging. The challenge is that
// stream merging requires sorted input, but a freshly flushed commit log has no
// particular order—commits arrive as operations happen.
//
// The solution is to limit the in-memory step to only the most recently flushed
// log, which has a fixed size regardless of graph size. This single in-memory
// pass converts the unordered raw log into a sorted .sorted file. From that
// point on, all operations are streaming: sorted files can be merged together
// like LSM segments without loading them into memory.
//
// Another goal was making snapshots first-class citizens. In v1, snapshots
// lived in a separate directory and were never the true source of truth—the
// original commit logs still existed, wasting storage. In compactv2, a snapshot
// is simply another output format of the [NWayMerger]. All files live in the
// same directory with no duplication.
//
// The package deliberately reuses existing formats where possible. Snapshots
// use the battle-tested V3 format. Commit types are shared with the .condensed
// format. The only new format is .sorted, which groups commits by node ID
// (see [NodeCommits]) to enable efficient stream merging.
//
// # Workflows
//
// There are two main workflows in this package:
//
// Startup (loading an index):
// [Loader] discovers files via [FileDiscovery], reads any existing snapshot
// with [SnapshotReader], then applies WAL files using [InMemoryReader] to
// build the complete in-memory state.
//
// Background compaction:
// [Compactor] periodically converts raw/condensed files to sorted format,
// merges sorted files via [NWayMerger], and creates snapshots when beneficial.
// The [Iterator] and [SnapshotIterator] types provide the [IteratorLike]
// interface for streaming node data into the merger.
//
// # Key Types
//
// High-level orchestration:
//   - [Loader] - Reads all commit log files at startup
//   - [Compactor] - Orchestrates background compaction cycles
//
// File discovery and metadata:
//   - [FileDiscovery] - Scans directories for commit log files
//   - [DirectoryState] - Current state of files in a directory
//   - [FileType] / [FileInfo] - File type classification and metadata
//
// Reading commits:
//   - [WALCommitReader] - Streams [Commit] values from any WAL file
//   - [InMemoryReader] - Deserializes commits into in-memory graph state
//   - [SnapshotReader] - Reads V3 snapshot files with concurrent block reading
//
// Writing:
//   - [WALWriter] - Low-level commit serialization
//   - [SortedWriter] - Writes node-sorted .sorted files from in-memory state
//   - [SnapshotWriter] - Writes V3 snapshot files from merged data
//   - [SafeFileWriter] - Crash-safe atomic file creation (temp + rename)
//
// Merging:
//   - [Iterator] - Provides node-level iteration over sorted WAL files
//   - [SnapshotIterator] - Adapts snapshots to the same iteration interface
//   - [IteratorLike] - Interface implemented by both iterator types
//   - [NWayMerger] - Merges multiple sorted streams by node ID
//
// Migration:
//   - [Migrator] - Handles legacy format migration (.condensed → .sorted)
//
// # File Formats
//
// The package works with four file formats:
//
// Raw files (no suffix): Freshly flushed from memory, time-ordered commits.
//
// Condensed files (.condensed): Legacy v1 format, partially sorted. Deprecated
// but supported for migration via [Migrator].
//
// Sorted files (.sorted): Fully sorted by node ID, enabling efficient stream
// merging. This is the preferred intermediate format.
//
// Snapshot files (.snapshot): Absolute state checkpoints. Contains the complete
// graph state at a point in time, with subsequent changes stored in WAL files.
// The V3 format uses checksummed blocks for concurrent reading.
//
// File ordering: Snapshot → WAL files (valid). WAL files can be merged together.
// Snapshots must always be the base state.
package compactv2
