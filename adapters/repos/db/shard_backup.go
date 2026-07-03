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

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/file"
)

// HaltForTransfer stops compaction, and flushing memtable and commit log to begin with backup or cloud offload.
// This method could be called multiple times with different inactivity timeouts,
// a zeroed `inactivityTimeout` implies no timeout.
// If inactivity timeout is reached it will resume maintenance cycle independently on how many halt request has been made.
func (s *Shard) HaltForTransfer(ctx context.Context, offloading bool, inactivityTimeout time.Duration) (err error) {
	s.haltForTransferMux.Lock()
	defer s.haltForTransferMux.Unlock()

	s.haltForTransferCount++

	defer func() {
		if err == nil && inactivityTimeout > 0 {
			s.mayUpdateInactivityTimeout(inactivityTimeout)
			s.mayInitInactivityMonitoring()
		}
	}()

	if offloading {
		// TODO: tenant offloading is calling HaltForTransfer but
		// if Shutdown is called this step is not needed
		s.mayStopAsyncReplication()
	}

	if s.haltForTransferCount > 1 {
		// shard was already halted
		return nil
	}

	defer func() {
		if err != nil {
			err = fmt.Errorf("pause compaction: %w", err)
			if err2 := s.mayForceResumeMaintenanceCycles(ctx, false); err2 != nil {
				err = fmt.Errorf("%w: resume maintenance: %w", err, err2)
			}
		}
	}()

	if err = s.store.PauseCompaction(ctx); err != nil {
		return fmt.Errorf("pause compaction: %w", err)
	}
	if err = s.store.FlushMemtables(ctx); err != nil {
		return fmt.Errorf("flush memtables: %w", err)
	}
	if err = s.cycleCallbacks.vectorCombinedCallbacksCtrl.Deactivate(ctx); err != nil {
		return fmt.Errorf("pause vector maintenance: %w", err)
	}
	if err = s.cycleCallbacks.geoPropsCombinedCallbacksCtrl.Deactivate(ctx); err != nil {
		return fmt.Errorf("pause geo props maintenance: %w", err)
	}

	// get the queues ready for backup (e.g. enable maintenance mode, switch to new chunks)
	_ = s.ForEachVectorQueue(func(targetVector string, q *VectorIndexQueue) error {
		if err = q.PrepareForBackup(ctx); err != nil {
			return fmt.Errorf("prepare for backup of vector %q: %w", targetVector, err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("flush vector index queues: %w", err)
	}
	err = s.ForEachGeoQueue(func(_ string, q *VectorIndexQueue) error {
		if err = q.PrepareForBackup(ctx); err != nil {
			return fmt.Errorf("prepare for backup of geo index: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("flush geo index queues: %w", err)
	}

	// get the index ready for backup (e.g switch commit logs, pause operation queues), ensuring all data is flushed to disk
	err = s.ForEachVectorIndex(func(targetVector string, index VectorIndex) error {
		if err = index.PrepareForBackup(ctx); err != nil {
			return fmt.Errorf("prepare for backup of vector %q: %w", targetVector, err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *Shard) mayUpdateInactivityTimeout(inactivityTimeout time.Duration) {
	if s.haltForTransferInactivityTimeout != 0 && s.haltForTransferInactivityTimeout <= inactivityTimeout {
		// no need to update current inactivity timeout
		return
	}

	s.haltForTransferInactivityTimeout = inactivityTimeout

	// restart any running monitor so the shorter timeout takes effect; the immediately-following
	// mayInitInactivityMonitoring respawns it. cancelling only stops the goroutine, not maintenance.
	s.mayStopInactivityMonitoring()
}

// mayStopInactivityMonitoring cancels the running inactivity monitor and clears the sentinel.
// Caller must hold haltForTransferMux; must not lock here (callers hold it across a wider section).
func (s *Shard) mayStopInactivityMonitoring() {
	if s.haltForTransferCtxCancel != nil {
		s.haltForTransferCtxCancel()
		s.haltForTransferCtxCancel = nil
	}
}

// mayResetInactivityDeadline records file activity by pushing the inactivity deadline forward.
// The monitor re-arms against this deadline, so a reset can never race a fire into a spurious resume.
func (s *Shard) mayResetInactivityDeadline() {
	if s.haltForTransferInactivityTimeout <= 0 {
		return
	}
	s.haltForTransferInactivityDeadline = time.Now().Add(s.haltForTransferInactivityTimeout)
}

func (s *Shard) mayInitInactivityMonitoring() {
	if s.haltForTransferCtxCancel != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.haltForTransferCtxCancel = cancel

	s.haltForTransferInactivityDeadline = time.Now().Add(s.haltForTransferInactivityTimeout)

	timer := time.NewTimer(s.haltForTransferInactivityTimeout)

	enterrors.GoWrapper(func() {
		// supersession and teardown cancel this ctx before any successor, so a stale fire is dropped.
		defer timer.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				if !s.handleInactivityFire(ctx, timer) {
					return
				}
			}
		}
	}, s.index.logger)
}

// handleInactivityFire resolves an inactivity-timer fire, returning true to keep watching
// (activity re-armed the timer) or false to stop (ctx cancelled, or the shard was resumed).
func (s *Shard) handleInactivityFire(ctx context.Context, timer *time.Timer) (keepWatching bool) {
	s.haltForTransferMux.Lock()
	defer s.haltForTransferMux.Unlock()

	if ctx.Err() != nil {
		// superseded or torn down while this fire waited on the mux; stop without resuming.
		return false
	}
	if remaining := time.Until(s.haltForTransferInactivityDeadline); remaining > 0 {
		// activity pushed the deadline forward; re-arm and keep watching.
		timer.Reset(remaining)
		return true
	}
	if err := s.mayForceResumeMaintenanceCycles(context.Background(), true); err != nil {
		s.index.logger.Error(err)
	}
	return false
}

// CreateBackupSnapshot halts compaction, lists backup files, hardlinks them into
// a staging directory, then immediately resumes compaction. This minimizes the
// compaction pause to just the time needed for enumeration and hardlink creation
// (typically 2-5s), rather than blocking for the entire upload duration.
func (s *Shard) CreateBackupSnapshot(ctx context.Context, sd *backup.ShardDescriptor, stagingRoot string) ([]string, error) {
	if err := s.HaltForTransfer(ctx, false, 0); err != nil {
		return nil, fmt.Errorf("halt for snapshot: %w", err)
	}
	defer s.resumeMaintenanceCycles(ctx)

	files, err := s.ListBackupFiles(ctx, sd)
	if err != nil {
		return nil, fmt.Errorf("list backup files: %w", err)
	}

	staged := make(map[string]struct{})

	err = s.ForEachVectorIndex(func(targetVector string, idx VectorIndex) error {
		relPaths, err := idx.SnapshotMutableFiles(ctx, s.index.Config.RootPath, stagingRoot)
		if err != nil {
			return fmt.Errorf("snapshot mutable files of vector %q: %w", targetVector, err)
		}
		for _, relPath := range relPaths {
			staged[relPath] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if s.dynamicVectorIndexDB != nil {
		relPath, err := dynamic.SnapshotSharedStateDB(s.dynamicVectorIndexDB, s.path(), s.index.Config.RootPath, stagingRoot)
		if err != nil {
			return nil, err
		}
		staged[relPath] = struct{}{}
	}

	listed := make(map[string]struct{}, len(files))
	for _, relPath := range files {
		listed[relPath] = struct{}{}
	}
	for relPath := range staged {
		if _, ok := listed[relPath]; !ok {
			files = append(files, relPath)
		}
	}

	for _, relPath := range files {
		if _, ok := staged[relPath]; ok {
			// already written as a consistent copy above; do not hardlink over it
			continue
		}
		src := filepath.Join(s.index.Config.RootPath, relPath)
		dst := filepath.Join(stagingRoot, relPath)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return nil, fmt.Errorf("create staging subdir for %s: %w", relPath, err)
		}
		if err := os.Link(src, dst); err != nil {
			return nil, fmt.Errorf("hardlink %s to staging: %w", relPath, err)
		}
	}

	return files, nil
}

// ListBackupFiles lists all files used to backup a shard
func (s *Shard) ListBackupFiles(ctx context.Context, ret *backup.ShardDescriptor) ([]string, error) {
	s.haltForTransferMux.Lock()
	defer s.haltForTransferMux.Unlock()

	if s.haltForTransferCount == 0 {
		return nil, fmt.Errorf("can not list files: illegal state: shard %q is not paused for transfer", s.name)
	}

	s.mayResetInactivityDeadline()

	if err := s.readBackupMetadata(ret); err != nil {
		return nil, err
	}

	files, err := s.store.ListFiles(ctx, s.index.Config.RootPath)
	if err != nil {
		return nil, err
	}

	err = s.ForEachVectorIndex(func(targetVector string, idx VectorIndex) error {
		filesIdx, err := idx.ListFiles(ctx, s.index.Config.RootPath)
		if err != nil {
			return fmt.Errorf("list files of vector %q: %w", targetVector, err)
		}
		files = append(files, filesIdx...)
		return nil
	})
	if err != nil {
		return nil, err
	}

	err = s.ForEachVectorQueue(func(targetVector string, queue *VectorIndexQueue) error {
		filesVq, err := queue.ForceSwitch(ctx, s.index.Config.RootPath)
		if err != nil {
			return fmt.Errorf("list files of queue %q: %w", targetVector, err)
		}
		files = append(files, filesVq...)
		return nil
	})
	if err != nil {
		return nil, err
	}

	err = s.ForEachGeoQueue(func(propName string, queue *VectorIndexQueue) error {
		filesGq, err := queue.ForceSwitch(ctx, s.index.Config.RootPath)
		if err != nil {
			return fmt.Errorf("list files of geo queue %q: %w", propName, err)
		}
		files = append(files, filesGq...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

func (s *Shard) resumeMaintenanceCycles(ctx context.Context) error {
	s.haltForTransferMux.Lock()
	defer s.haltForTransferMux.Unlock()

	return s.mayForceResumeMaintenanceCycles(ctx, false)
}

func (s *Shard) mayForceResumeMaintenanceCycles(ctx context.Context, forced bool) error {
	if s.haltForTransferCount == 0 {
		// noop, maintenance cycles not halted
		return nil
	}

	if forced {
		s.haltForTransferCount = 0
	} else {
		s.haltForTransferCount--

		if s.haltForTransferCount > 0 {
			// maintenance cycles are not resumed as there is at least one active halt request
			return nil
		}
	}

	// terminate the inactivity monitor synchronously under the mux, so a subsequent
	// HaltForTransfer reliably starts a new monitor.
	s.mayStopInactivityMonitoring()

	// fully resumed: reset so the next halt cycle uses its own timeout, not the shortest ever seen.
	s.haltForTransferInactivityTimeout = 0
	s.haltForTransferInactivityDeadline = time.Time{}

	g := enterrors.NewErrorGroupWrapper(s.index.logger)

	g.Go(func() error {
		return s.store.ResumeCompaction(ctx)
	})
	g.Go(func() error {
		return s.cycleCallbacks.vectorCombinedCallbacksCtrl.Activate()
	})
	g.Go(func() error {
		return s.cycleCallbacks.geoPropsCombinedCallbacksCtrl.Activate()
	})

	g.Go(func() error {
		return s.ForEachVectorQueue(func(_ string, q *VectorIndexQueue) error {
			if err := q.DisableMaintenanceMode(); err != nil {
				return fmt.Errorf("resuming after backup: %w", err)
			}

			return nil
		})
	})
	g.Go(func() error {
		return s.ForEachGeoQueue(func(_ string, q *VectorIndexQueue) error {
			if err := q.DisableMaintenanceMode(); err != nil {
				return fmt.Errorf("resuming after backup: %w", err)
			}
			return nil
		})
	})
	g.Go(func() error {
		return s.ForEachVectorIndex(func(_ string, index VectorIndex) error {
			if err := index.ResumeAfterBackup(ctx); err != nil {
				return fmt.Errorf("resuming after backup: %w", err)
			}
			return nil
		})
	})

	if err := g.Wait(); err != nil {
		return fmt.Errorf("failed to resume maintenance cycles for shard '%s': %w", s.name, err)
	}

	return nil
}

func (s *Shard) readBackupMetadata(d *backup.ShardDescriptor) (err error) {
	d.Name = s.name

	d.Node = s.index.getSchema.NodeName()

	fpath := s.counter.FileName()
	if d.DocIDCounter, err = os.ReadFile(fpath); err != nil {
		return fmt.Errorf("read shard doc-id-counter %s: %w", fpath, err)
	}
	d.DocIDCounterPath, err = filepath.Rel(s.index.Config.RootPath, fpath)
	if err != nil {
		return fmt.Errorf("docid counter path: %w", err)
	}
	fpath = s.GetPropertyLengthTracker().FileName()
	if d.PropLengthTracker, err = os.ReadFile(fpath); err != nil {
		return fmt.Errorf("read shard prop-lengths %s: %w", fpath, err)
	}
	d.PropLengthTrackerPath, err = filepath.Rel(s.index.Config.RootPath, fpath)
	if err != nil {
		return fmt.Errorf("proplength tracker path: %w", err)
	}
	fpath = s.versioner.path
	if d.Version, err = os.ReadFile(fpath); err != nil {
		return fmt.Errorf("read shard version %s: %w", fpath, err)
	}
	d.ShardVersionPath, err = filepath.Rel(s.index.Config.RootPath, fpath)
	if err != nil {
		return fmt.Errorf("shard version path: %w", err)
	}
	return nil
}

func (s *Shard) GetFileMetadata(ctx context.Context, relativeFilePath string) (file.FileMetadata, error) {
	s.haltForTransferMux.Lock()
	defer s.haltForTransferMux.Unlock()

	if s.haltForTransferCount == 0 {
		return file.FileMetadata{}, fmt.Errorf("can not open file %q for reading: illegal state: shard %q is not paused for transfer",
			relativeFilePath, s.name)
	}

	s.mayResetInactivityDeadline()

	finalPath, err := s.sanitizeFilePath(relativeFilePath)
	if err != nil {
		return file.FileMetadata{}, fmt.Errorf("sanitize file path %q: %w", relativeFilePath, err)
	}
	return file.GetFileMetadata(finalPath)
}

func (s *Shard) GetFile(ctx context.Context, relativeFilePath string) (io.ReadCloser, error) {
	s.haltForTransferMux.Lock()
	defer s.haltForTransferMux.Unlock()

	if s.haltForTransferCount == 0 {
		return nil, fmt.Errorf("can not open file %q for reading: illegal state: shard %q is not paused for transfer",
			relativeFilePath, s.name)
	}

	s.mayResetInactivityDeadline()

	finalPath, err := s.sanitizeFilePath(relativeFilePath)
	if err != nil {
		return nil, fmt.Errorf("sanitize file path %q: %w", relativeFilePath, err)
	}

	reader, err := os.Open(finalPath)
	if err != nil {
		return nil, fmt.Errorf("open file %q for reading: %w", relativeFilePath, err)
	}

	return reader, nil
}

func (s *Shard) sanitizeFilePath(relativeFilePath string) (string, error) {
	// clean the path to remove any ../ or ./ sequences
	cleanFilePath := filepath.Clean(relativeFilePath)
	if filepath.IsAbs(cleanFilePath) {
		return "", fmt.Errorf("relative file path %q is an absolute path", relativeFilePath)
	}
	combinedPath := filepath.Join(s.index.Config.RootPath, cleanFilePath)
	finalPath, err := filepath.EvalSymlinks(combinedPath)
	if err != nil {
		return "", fmt.Errorf("resolve symlinks for %q: %w", finalPath, err)
	}
	finalPath = filepath.Clean(finalPath)

	// Resolve symlinks in root path - this is important for testing on MacOs where /var is a symlink
	rootPath, err := filepath.EvalSymlinks(s.index.Config.RootPath)
	if err != nil {
		return "", fmt.Errorf("resolve symlinks for root path %q: %w", s.index.Config.RootPath, err)
	}

	rel, err := filepath.Rel(rootPath, finalPath)
	if err != nil {
		return "", fmt.Errorf("make %q relative to %q: %w", finalPath, rootPath, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("file path %q is outside shard root %q", finalPath, rootPath)
	}
	return finalPath, nil
}
