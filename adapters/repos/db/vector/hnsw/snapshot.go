package hnsw

import (
	"context"
	"fmt"
	"io/fs"
	"path"
	"path/filepath"

	"github.com/pkg/errors"
)

// PauseMaintenance makes sure that no new background processes can be started.
// If a Combining or Condensing operation is already ongoing, the method blocks
// until the operation has either finished or the context expired
//
// If a Delete-Cleanup Cycle is running (TombstoneCleanupCycle), it is aborted,
// as it's not feasible to wait for such a cycle to complete, as it can take hours.
func (h *hnsw) PauseMaintenance(ctx context.Context) error {
	cleanupHalted := make(chan struct{})

	go func() {
		h.commitLog.PauseMaintenance()
		h.tombstoneCleanupCycle.Stop(ctx)
		cleanupHalted <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		// resume the cleanup cycle, as the
		// context deadline was exceeded
		defer h.tombstoneCleanupCycle.Start(h.cleanupInterval)
		return errors.Wrap(ctx.Err(), "long-running tombstone cleanup in progress")
	case <-cleanupHalted:
		return nil
	}
}

// SwitchCommitLogs makes sure that the previously writeable commitlog is
// switched to a new one, thus making the existing file read-only.
func (h *hnsw) SwitchCommitLogs(ctx context.Context) error {
	done := make(chan struct{})

	go func() {
		err := h.commitLog.SwitchCommitLogs(true)
		if err != nil {
			h.logger.Error("failed to switch commit logs")
		}
		done <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "switch commitlogs")
	case <-done:
		return nil
	}
}

// ListFiles lists all files that are part of the part of the HNSW
// except the last commit-log which is writable. This operation is typically
// called immediately after calling SwitchCommitlogs which means that the
// latest (writeable) log file is typically empty.
// ListFiles errors if maintenance is not paused, as a stable state
// cannot be guaranteed with maintenance going on in the background.
func (h *hnsw) ListFiles(ctx context.Context) ([]string, error) {
	var (
		logRoot = path.Join(h.commitLog.RootPath(), fmt.Sprintf("%s.hnsw.commitlog.d", h.commitLog.ID()))
		found   = make(map[string]struct{})
		files   []string
	)

	err := filepath.WalkDir(logRoot, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}
		found[path] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, errors.Errorf("failed to list files for hnsw commitlog: %s", err)
	}

	curr, _, err := getCurrentCommitLogFileName(logRoot)
	if err != nil {
		return nil, errors.Wrap(err, "current commitlog file name")
	}

	// remove active log from list, as
	// it is not part of the snapshot
	delete(found, path.Join(logRoot, curr))

	files, i := make([]string, len(found)), 0
	for file := range found {
		files[i] = file
		i++
	}

	return files, nil
}

// ResumeMaintenance starts all async cycles. It errors if the operations
// had not been paused prior.
func (h *hnsw) ResumeMaintenance(ctx context.Context) error {
	h.tombstoneCleanupCycle.Start(h.cleanupInterval)
	h.commitLog.Start()
	return nil
}
