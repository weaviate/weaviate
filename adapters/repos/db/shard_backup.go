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
	"os"
	"path/filepath"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/file"
)

// HaltForTransfer stops compaction and flushes the memtable and commit log so
// a backup, replica snapshot, or cloud offload can read stable files. Halts
// nest: every call increments the halt count, every resume decrements it, and
// cycles restart only at zero.
// The preparation work (pausing compaction, flushing memtables, readying
// vector indexes and queues) is bounded by `HaltForTransferTimeout`; a zeroed
// `HaltForTransferTimeout` implies no bound.
func (s *Shard) HaltForTransfer(ctx context.Context, offloading bool) (err error) {
	innerCtx := ctx
	if timeout := s.index.Config.HaltForTransferTimeout; timeout > 0 {
		var cancel context.CancelFunc
		innerCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	s.haltForTransferMux.Lock()
	defer s.haltForTransferMux.Unlock()

	// Check before bumping haltForTransferCount so a rejection does not
	// leave the counter incremented; the error path would not run a
	// matching resume.
	if !offloading {
		if blockedErr := s.index.refuseIfReindexInFlight(s.name); blockedErr != nil {
			return blockedErr
		}
		if busy, reason := s.structuralVectorOpInFlight(); busy {
			return fmt.Errorf("%w: shard %q: %s; transfer deferred until it completes",
				enterrors.ErrShardBusyStructuralOp, s.name, reason)
		}
	}

	s.haltForTransferCount++

	if offloading {
		// TODO: tenant offloading is calling HaltForTransfer but
		// if Shutdown is called this step is not needed
		s.mayStopAsyncReplication()
	}

	// Placed before the pause branch so it also covers count>1 callers: on error
	// resumeMaintenanceCyclesLocked decrements our own increment and only truly
	// resumes at count==0, so a failed count>1 caller rolls back 2→1 without
	// unhalting the shard the first op still holds.
	defer func() {
		if err != nil {
			// each preparation step below wraps its own error; only append
			// the outcome of the cleanup attempt here
			if err2 := s.resumeMaintenanceCyclesLocked(ctx); err2 != nil {
				err = fmt.Errorf("%w: resume maintenance: %w", err, err2)
			}
		}
	}()

	// Pause steps run only on the first halt. Re-pausing per halt would strand the
	// per-bucket pause-timer refcount (1 pause : 1 stop) and never observe the
	// Prometheus pause-duration timer.
	if s.haltForTransferCount == 1 {
		if err = s.store.PauseCompaction(innerCtx); err != nil {
			return fmt.Errorf("pause compaction: %w", err)
		}
		if err = s.cycleCallbacks.vectorCombinedCallbacksCtrl.Deactivate(innerCtx); err != nil {
			return fmt.Errorf("pause vector maintenance: %w", err)
		}
		if err = s.cycleCallbacks.geoPropsCombinedCallbacksCtrl.Deactivate(innerCtx); err != nil {
			return fmt.Errorf("pause geo props maintenance: %w", err)
		}
	} else {
		s.index.logger.WithField("shard", s.name).
			Debugf("shard already halted for transfer (count=%d); re-sealing state on shared halt", s.haltForTransferCount)
	}

	// Seal steps run on EVERY halt: a second consumer's snapshot deliberately
	// excludes the active memtable/WAL and the active HNSW commit-log, so any
	// write that landed after the first consumer's flush must be re-sealed here or
	// it is silently dropped from the second consumer's snapshot.

	// get the queues ready for backup (e.g. enable maintenance mode, switch to new chunks)
	_ = s.ForEachVectorQueue(func(targetVector string, q *VectorIndexQueue) error {
		if err = q.PrepareForBackup(innerCtx); err != nil {
			return fmt.Errorf("prepare for backup of vector %q: %w", targetVector, err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("flush vector index queues: %w", err)
	}
	err = s.ForEachGeoQueue(func(_ string, q *VectorIndexQueue) error {
		if err = q.PrepareForBackup(innerCtx); err != nil {
			return fmt.Errorf("prepare for backup of geo index: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("flush geo index queues: %w", err)
	}

	// get the index ready for backup (e.g switch commit logs, pause operation queues), ensuring all data is flushed to disk
	err = s.ForEachVectorIndex(func(targetVector string, index VectorIndex) error {
		if err = index.PrepareForBackup(innerCtx); err != nil {
			return fmt.Errorf("prepare for backup of vector %q: %w", targetVector, err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Flush memtables after draining the queues and preparing the indexes.
	// Queue tasks (e.g. HNSW insertions) and index PrepareForBackup (e.g.
	// HFresh queue drains) may have written compressed vectors to the LSM
	// store after the initial FlushMemtables call above. Without this flush
	// those compressed vectors stay in the memtable (WAL only) and are absent
	// from the backup while the HNSW commit log references them — including
	// potentially as the entrypoint. On restore this leads to "entrypoint was
	// deleted in the object store" errors on every search.
	if err = s.store.FlushMemtables(innerCtx); err != nil {
		return fmt.Errorf("flush memtables after queue drain: %w", err)
	}

	return nil
}

// structuralVectorOpInFlight reports whether any vector index is mid-restructure
// (HNSW compression or dynamic flat→HNSW upgrade) — a snapshot taken now would
// be structurally inconsistent. reason names the first offending index.
func (s *Shard) structuralVectorOpInFlight() (busy bool, reason string) {
	_ = s.ForEachVectorIndex(func(name string, vi VectorIndex) error {
		if u, ok := vi.(upgradableIndexer); ok && u.UpgradeInProgress() {
			busy = true
			reason = fmt.Sprintf("vector %q: compression or flat→HNSW upgrade in progress", name)
		}
		return nil
	})
	return
}

// CreateBackupSnapshot halts compaction, lists backup files, hardlinks them into
// a staging directory, then immediately resumes compaction. This minimizes the
// compaction pause to just the time needed for enumeration and hardlink creation
// (typically 2-5s), rather than blocking for the entire upload duration.
func (s *Shard) CreateBackupSnapshot(ctx context.Context, sd *backup.ShardDescriptor, stagingRoot string) ([]string, error) {
	if err := s.HaltForTransfer(ctx, false); err != nil {
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

	pairs := make([]file.HardlinkPair, 0, len(files))
	for _, relPath := range files {
		if _, ok := staged[relPath]; ok {
			// already written as a consistent copy above; do not hardlink over it
			continue
		}
		pairs = append(pairs, file.HardlinkPair{
			Src: filepath.Join(s.index.Config.RootPath, relPath),
			Dst: filepath.Join(stagingRoot, relPath),
		})
	}
	if err := file.HardlinkFiles(pairs); err != nil {
		return nil, fmt.Errorf("hardlink backup files to staging: %w", err)
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

	return s.resumeMaintenanceCyclesLocked(ctx)
}

// resumeMaintenanceCyclesLocked decrements the halt count and restarts the
// paused cycles when it reaches zero. Callers hold haltForTransferMux.
func (s *Shard) resumeMaintenanceCyclesLocked(ctx context.Context) error {
	if s.haltForTransferCount == 0 {
		// noop, maintenance cycles not halted
		return nil
	}

	s.haltForTransferCount--

	if s.haltForTransferCount > 0 {
		// maintenance cycles are not resumed as there is at least one active halt request
		return nil
	}

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
