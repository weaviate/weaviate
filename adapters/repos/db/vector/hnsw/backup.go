//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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

// SwitchCommitLogs makes sure that the previously writeable commitlog is
// switched to a new one, thus making the existing file read-only.
func (h *hnsw) SwitchCommitLogs(ctx context.Context) error {
	if err := h.commitLog.SwitchCommitLogs(true); err != nil {
		return fmt.Errorf("switch commitlogs: %w", err)
	}

	return nil
}

// ListFiles lists all files that are part of the part of the HNSW
// except the last commit-log which is writable. This operation is typically
// called immediately after calling SwitchCommitlogs which means that the
// latest (writeable) log file is typically empty.
// ListFiles errors if maintenance is not paused, as a stable state
// cannot be guaranteed with maintenance going on in the background.
func (h *hnsw) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	var (
		logRoot = filepath.Join(h.commitLog.RootPath(), fmt.Sprintf("%s.hnsw.commitlog.d", h.commitLog.ID()))
		found   = make(map[string]struct{})
		files   []string
	)

	err := filepath.WalkDir(logRoot, func(pth string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}

		st, statErr := os.Stat(pth)
		if statErr != nil {
			return statErr
		}

		// only list non-empty files
		if st.Size() > 0 {
			rel, relErr := filepath.Rel(basePath, pth)
			if relErr != nil {
				return relErr
			}
			found[rel] = struct{}{}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list files for hnsw commitlog: %w", err)
	}

	curr, _, err := getCurrentCommitLogFileName(logRoot)
	if err != nil {
		return nil, fmt.Errorf("current commitlog file name: %w", err)
	}

	// remove active log from list, as
	// it is not part of the backup
	path, err := filepath.Rel(basePath, filepath.Join(logRoot, curr))
	if err != nil {
		return nil, fmt.Errorf("delete active log: %w", err)
	}
	delete(found, path)

	files, i := make([]string, len(found)), 0
	for file := range found {
		files[i] = file
		i++
	}

	return files, nil
}
