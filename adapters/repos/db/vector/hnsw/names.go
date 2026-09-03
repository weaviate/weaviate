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

package hnsw

import "fmt"

// A multivector index keeps one bucket of its own, named off the index ID:
// muvera encodings when muvera is on, node-to-doc mappings when it is off. The
// two are mutually exclusive.
//
// These live here rather than being concatenated at the point of use so the
// code that CREATES the bucket and the drop that removes it share one
// definition. Concatenated inline, a rename on the hnsw side compiles cleanly
// while the cleanup keeps deleting the old name: removeBucket no-ops and the
// leak returns silently. The db package's drop-artifact catalogue imports
// these directly, so it and the index that creates the bucket still cannot
// drift apart.

// MuveraBucketName is the bucket a muvera-encoded multivector index stores its
// encoded vectors in. indexID is the vector index's ID.
func MuveraBucketName(indexID string) string {
	return fmt.Sprintf("%s_muvera_vectors", indexID)
}

// MVMappingsBucketName is the bucket a multivector index WITHOUT muvera stores
// its node-to-doc mappings in. indexID is the vector index's ID.
func MVMappingsBucketName(indexID string) string {
	return fmt.Sprintf("%s_mv_mappings", indexID)
}

// CommitLogDirName is the on-disk directory name (not a path) of a physical
// index ID's commit log: "<physicalID>.hnsw.commitlog.d". commitLogDirectory
// defines the full path in terms of this, so the live commit logger and the
// drop-artifact catalogue in package db cannot disagree on the name.
func CommitLogDirName(physicalID string) string {
	return physicalID + ".hnsw.commitlog.d"
}

// SnapshotDirName is the on-disk directory name (not a path) of a physical
// index ID's legacy snapshot directory: "<physicalID>.hnsw.snapshot.d".
// Snapshots have lived inside the commit log directory since the migration in
// package compact; this name survives only so the drop-artifact catalogue can
// still clean up a directory left behind by a version that wrote it there.
func SnapshotDirName(physicalID string) string {
	return physicalID + ".hnsw.snapshot.d"
}
