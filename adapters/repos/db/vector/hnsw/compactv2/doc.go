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

// Package compactv2 provides tools for reading, writing, and managing HNSW
// commit logs in various formats.
//
// # File Formats
//
// This package works with four distinct file formats for storing HNSW commit logs:
//
// ## Raw Files
//
// Raw files are logs that have been freshly flushed from memory. They contain
// commits in the order they were written, which is essentially time-sorted order.
// Raw files contain no additional sorting or optimization and represent the most
// basic form of persistent commit storage.
//
// ## Condensed Files (.condensed suffix) - DEPRECATED
//
// Condensed files are produced by the legacy v1 "condensor" and are now deprecated.
// They are partially sorted with a specific structure:
//   - First, all addition commits (AddNode, AddLink, etc.) sorted by node ID
//   - Followed by tombstone commits
//   - Followed by other commit types
//
// While condensed files provide some level of organization, they have limitations
// when it comes to efficient stream-merging operations, which led to the development
// of the sorted format.
//
// ## Sorted Files (.sorted suffix)
//
// Sorted files are a new addition from the v2 package and represent an improvement
// over the condensed format. The entire file is sorted by node ID, which enables
// efficient stream-merging. This format allows for better performance when combining
// multiple log files and provides more consistent access patterns.
//
// Sorted files are the preferred format for new implementations and should be used
// instead of condensed files whenever possible.
//
// ## Snapshot Files
//
// Snapshot files represent a fundamentally different approach compared to the other
// formats. Unlike raw, condensed, and sorted files which contain delta operations
// (commits), a snapshot captures the absolute state of the HNSW index at a specific
// point in time.
//
// Key characteristics of snapshots:
//   - Contains no delta operations, only absolute state
//   - Can be followed by logs (raw, condensed, or sorted) to represent subsequent changes
//   - Cannot follow logs - snapshots must always be the base state
//   - Provides a clean checkpoint from which to rebuild or recover state
//   - Uses checkpoints to allow reading concurrently -> fast startup time
//
// # File Ordering Rules
//
// The ordering of different file types follows these rules:
//   - Snapshot → Raw/Condensed/Sorted logs (valid)
//   - Logs → Snapshot (invalid - snapshots cannot follow logs)
//   - Logs can be combined and merged with other logs
//
// This architecture allows for efficient incremental updates (via logs) on top of
// a stable baseline (snapshot), while preventing invalid state combinations.
package compactv2
