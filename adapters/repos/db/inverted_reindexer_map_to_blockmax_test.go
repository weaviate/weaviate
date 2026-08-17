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

package db

// newFileMapToBlockmaxReindexTracker creates a file-based reindex tracker
// for the most recent searchable map-to-blockmax migration. Migration dirs
// carry a per-node generation suffix (`_<N>`); callers that don't know the
// generation get the highest one that exists on disk, or the first
// generation if there is no on-disk state yet.
func newFileMapToBlockmaxReindexTracker(lsmPath string, keyParser indexKeyParser) *fileReindexTracker {
	gen := maxMigrationGeneration(lsmPath, MigrationDirSearchableMapToBlockmax, "")
	if gen == 0 {
		gen = 1
	}
	return NewFileReindexTracker(lsmPath, MigrationDirSearchableMapToBlockmax+genSuffix(gen), keyParser)
}
