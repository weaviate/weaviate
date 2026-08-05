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

package backup

import (
	"path/filepath"
	"strings"
)

const (
	dbExt        = ".db"
	bloomExt     = ".bloom"
	cnaExt       = ".cna"
	metadataExt  = ".metadata"
	condensedExt = ".condensed"
	snapshotExt  = ".snapshot"
)

// IsImmutableFile reports whether a backup file (relative path) is guaranteed
// never to be modified in place after it is written. Such files are safe to
// hard-link during backup, and safe to deduplicate across incremental backups by
// comparing size+mtime. Every other file is copied and re-uploaded on each
// incremental: an in-place rewrite can change its content while keeping the same
// size, so size+mtime is not a reliable unchanged signal for it.
func IsImmutableFile(relPath string) bool {
	base := filepath.Base(relPath)
	ext := filepath.Ext(base)

	// LSM segment data files — written once during flush/compaction, never modified.
	// Excludes meta*.db (flat index BoltDB, mmap writes) and index.db (dynamic index BoltDB).
	if ext == dbExt && !strings.HasPrefix(base, "meta") && base != "index.db" {
		return true
	}
	// LSM segment companion files — written once during segment init, never modified.
	// .bloom = bloom filter, .cna = count net additions, .metadata = combined metadata.
	if ext == bloomExt || ext == cnaExt || ext == metadataExt {
		return true
	}
	// Condensed HNSW commitlogs — produced by compaction, never reopened for writes.
	if ext == condensedExt {
		return true
	}
	// HNSW snapshots — point-in-time captures, never modified after creation.
	if ext == snapshotExt {
		return true
	}
	return false
}
