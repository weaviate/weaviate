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

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// PrepareForBackup makes sure that the previously writeable commitlog is
// switched to a new one, thus making the existing file read-only.
//
// Note: this only seals the *previous* file. The new file is the writer's
// append target, and AddNode / Flush against it can land between this
// returning and ListFiles being called — so ListFiles must exclude the
// new file by identity, not by relying on it still being empty.
func (h *hnsw) PrepareForBackup(ctx context.Context) error {
	if err := h.commitLog.PrepareForBackup(true); err != nil {
		return fmt.Errorf("switch commitlogs: %w", err)
	}

	return nil
}

func (h *hnsw) ResumeAfterBackup(ctx context.Context) error {
	// Nothing to do here. The backup invariant ("listed files are immutable
	// for the duration of the copy") is upheld by ListFiles excluding the
	// active commit log file by path. The active file *does* receive writes
	// during a backup, but it is never part of the file set the backup
	// machinery copies.
	return nil
}

// SnapshotMutableFiles is a no-op for HNSW: the index only ever writes new
// files and never mutates existing ones in place (see ResumeAfterBackup), so
// there are no in-place-mutated files (e.g. bbolt meta.db) that need to be
// frozen into the staging directory during an active-shard backup. ListFiles
// already captures every immutable file that belongs in the backup.
func (h *hnsw) SnapshotMutableFiles(ctx context.Context, basePath, stagingDir string) ([]string, error) {
	return nil, nil
}

// ListFiles returns every commit-log file that is safe to copy as part of a
// backup — i.e. every file in the commitlog directory except the writer's
// active append target.
//
// Two filters are applied. They are NOT redundant:
//
//  1. exclude the active file by path (h.commitLog.ActiveFilePath()): this
//     is the file the writer is appending to. Even though the backup
//     orchestration calls PrepareForBackup before ListFiles, the queue's
//     `defer q.Resume()` re-arms workers before the index-level
//     PrepareForBackup runs, so a worker can land an AddNode + Flush
//     against the new file before we walk the directory. Excluding by
//     identity is the only reliable way to keep the active file out.
//  2. exclude zero-byte files: stale empty raw files left by a prior
//     startup that crashed before the first write are harmless in a
//     backup, but copying them around is just noise. (createNewCommitFile
//     prunes these on startup, but there is a small window where one can
//     coexist with a freshly opened active file.)
//
// ListFiles assumes maintenance is paused; otherwise concurrent compaction
// or switch cycles can rename / replace files between the WalkDir and the
// caller's read.
func (h *hnsw) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	var (
		logRoot = filepath.Join(h.commitLog.RootPath(), fmt.Sprintf("%s.hnsw.commitlog.d", h.commitLog.ID()))
		found   = make(map[string]struct{})
		files   []string
	)

	// Snapshot the active file path *before* the walk. The commit-logger
	// mutex is dropped immediately, but that's fine: rotations only happen
	// from PrepareForBackup (which the caller has already invoked) and from
	// the switchLogs cycle (deactivated by HaltForTransfer for the duration
	// of the backup). So the snapshot is stable for the duration of this
	// call, and we don't need to hold the lock across the WalkDir.
	activeAbs := h.commitLog.ActiveFilePath()
	var activeRel string
	if activeAbs != "" {
		rel, err := filepath.Rel(basePath, activeAbs)
		if err != nil {
			return nil, fmt.Errorf("active commit log path %q relative to %q: %w", activeAbs, basePath, err)
		}
		activeRel = rel
	}

	err := filepath.WalkDir(logRoot, func(pth string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}

		st, statErr := os.Stat(pth)
		if statErr != nil {
			return statErr
		}

		// Filter (2): drop empty files. See function godoc for why this is
		// kept alongside the by-path active-file exclusion.
		if st.Size() == 0 {
			return nil
		}

		rel, relErr := filepath.Rel(basePath, pth)
		if relErr != nil {
			return relErr
		}

		// Filter (1): drop the active append target. The size>0 filter
		// above does NOT subsume this: under load, the active file is
		// often non-empty by the time we get here.
		if rel == activeRel {
			return nil
		}

		found[rel] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list files for hnsw commitlog: %w", err)
	}

	files = make([]string, 0, len(found))
	for file := range found {
		files = append(files, file)
	}

	return files, nil
}
