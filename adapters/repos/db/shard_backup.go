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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/geo"
	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/file"
)

// haltedForTransfer is a lock-free probe: never parks behind backup prep, never misreads a concurrent reader as halted.
func (s *Shard) haltedForTransfer() bool {
	return s.haltForTransferCount.Load() > 0
}

// HaltForTransfer stops compaction, and flushing memtable and commit log to begin with backup or cloud offload.
// This method could be called multiple times with different inactivity timeouts,
// a zeroed `inactivityTimeout` implies no timeout.
// When the inactivity timeout expires, the watchdog clears only the holds that
// asked for a timeout; owned holds and holds without a timeout survive and
// keep the shard paused.
// The preparation work (pausing compaction, flushing memtables, readying vector indexes and queues)
// is additionally bounded by `HaltForTransferTimeout`, independent of `inactivityTimeout`;
// a zeroed `HaltForTransferTimeout` implies no bound.
func (s *Shard) HaltForTransfer(ctx context.Context, offloading bool, inactivityTimeout time.Duration) (err error) {
	return s.haltForTransfer(ctx, offloading, inactivityTimeout, false)
}

func (s *Shard) haltForTransfer(ctx context.Context, offloading bool, inactivityTimeout time.Duration, owned bool) (err error) {
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

	s.haltForTransferCount.Add(1)
	if owned {
		s.haltForTransferOwnedCount++
	}

	defer func() {
		if err != nil {
			return
		}
		if inactivityTimeout > 0 {
			s.haltForTransferArmedCount++
			s.mayUpdateInactivityTimeout(inactivityTimeout)
			s.mayInitInactivityMonitoring()
		}
		s.mayResetInactivityDeadline()
	}()

	if offloading {
		// TODO: tenant offloading is calling HaltForTransfer but
		// if Shutdown is called this step is not needed.
		// persistHashtree=false: shard stays live and its .ht is not offloaded.
		s.mayStopAsyncReplication(false)
	}

	// Placed before the pause branch so it also covers count>1 callers: on error
	// the rollback decrements our own increment (and, for an owned hold, the owned count) and
	// only truly resumes at count==0, so a failed count>1 caller rolls back 2->1
	// without unhalting the shard the first op still holds.
	defer func() {
		if err != nil {
			if owned {
				s.haltForTransferOwnedCount--
			}
			if err2 := s.mayForceResumeMaintenanceCycles(ctx, false); err2 != nil {
				err = fmt.Errorf("%w: resume maintenance: %w", err, err2)
			}
		}
	}()

	// Pause steps run only on the first halt. Re-pausing per halt would strand the
	// per-bucket pause-timer refcount (1 pause : 1 stop) and never observe the
	// Prometheus pause-duration timer.
	if s.haltForTransferCount.Load() == 1 {
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
			Debugf("shard already halted for transfer (count=%d); re-sealing state on shared halt", s.haltForTransferCount.Load())
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

	if err := s.ForEachGeoIndex(func(propName string, index *geo.Index) error {
		if err := index.PrepareForBackup(innerCtx); err != nil {
			return fmt.Errorf("prepare for backup of geo index %q: %w", propName, err)
		}
		return nil
	}); err != nil {
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

// haltForTransferOwned acquires an owned halt-for-transfer hold. An owned hold
// can only be released by the returned closure; anonymous resumes
// (resumeMaintenanceCycles) and watchdog force-resumes cannot consume it.
// The release closure is idempotent.
func (s *Shard) haltForTransferOwned(ctx context.Context) (release func(context.Context) error, err error) {
	if err := s.haltForTransfer(ctx, false, 0, true); err != nil {
		return nil, err
	}

	var released bool
	return func(ctx context.Context) error {
		s.haltForTransferMux.Lock()

		if released {
			s.haltForTransferMux.Unlock()
			return nil
		}
		released = true
		s.haltForTransferOwnedCount--
		s.haltForTransferCount.Add(-1)

		fullyResumed := s.haltForTransferCount.Load() == 0
		if !fullyResumed {
			s.haltForTransferMux.Unlock()
			return nil
		}

		// doPhysicalResume requires the mux to be held.
		s.mayStopInactivityMonitoring()
		s.haltForTransferInactivityTimeout = 0
		s.haltForTransferInactivityDeadline = time.Time{}
		resumeErr := s.doPhysicalResume(ctx)
		s.haltForTransferMux.Unlock()

		s.reapplyAsyncReplicationAfterResume(ctx)
		return resumeErr
	}, nil
}

// MayResetTransferInactivityTimer counts an in-flight transfer RPC as
// activity so the halt watchdog doesn't force-resume mid-stream.
func (s *Shard) MayResetTransferInactivityTimer() {
	s.haltForTransferMux.Lock()
	defer s.haltForTransferMux.Unlock()
	s.mayResetInactivityDeadline()
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
	forceResumed := false
	// Registered before the unlock defer so it runs after the mux is released.
	defer func() {
		if forceResumed {
			s.reapplyAsyncReplicationAfterResume(context.Background())
		}
	}()
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
	// Reapply async replication only when the fire fully resumed the shard.
	forceResumed = s.haltForTransferCount.Load() == 0
	return false
}

// CreateBackupSnapshot halts compaction, lists backup files, hardlinks them into
// a staging directory, then immediately resumes compaction. This minimizes the
// compaction pause to just the time needed for enumeration and hardlink creation
// (typically 2-5s), rather than blocking for the entire upload duration.
//
// The halt is acquired as an owned hold so that anonymous resumes and watchdog
// fires from other callers cannot consume it.
func (s *Shard) CreateBackupSnapshot(ctx context.Context, sd *backup.ShardDescriptor, stagingRoot string) ([]string, error) {
	release, err := s.haltForTransferOwned(ctx)
	if err != nil {
		return nil, fmt.Errorf("halt for snapshot: %w", err)
	}
	defer func() {
		if err := release(ctx); err != nil {
			s.index.logger.WithField("shard", s.name).
				Warnf("releasing owned halt after snapshot: %v", err)
		}
	}()

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

	if s.haltForTransferCount.Load() == 0 {
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

	if err := s.ForEachGeoIndex(func(propName string, index *geo.Index) error {
		filesGi, err := index.ListFiles(ctx, s.index.Config.RootPath)
		if err != nil {
			return fmt.Errorf("list files of geo index %q: %w", propName, err)
		}
		files = append(files, filesGi...)
		return nil
	}); err != nil {
		return nil, err
	}
	return files, nil
}

func (s *Shard) resumeMaintenanceCycles(ctx context.Context) error {
	s.haltForTransferMux.Lock()
	err := s.mayForceResumeMaintenanceCycles(ctx, false)
	fullyResumed := s.haltForTransferCount.Load() == 0
	s.haltForTransferMux.Unlock()

	// Enables skipped while halted have no other re-derive path for plain halts.
	if fullyResumed {
		s.reapplyAsyncReplicationAfterResume(ctx)
	}
	return err
}

// reapplyAsyncReplicationAfterResume re-derives a skipped enable once a transfer halt fully lifts; disables are never skipped, so this only ever enables.
func (s *Shard) reapplyAsyncReplicationAfterResume(ctx context.Context) {
	if s.shutOrDropped() || s.shutdownRequested.Load() {
		return
	}
	if err := s.index.withAsyncReplicationApply(func() error {
		if s.shutOrDropped() || s.haltedForTransfer() {
			return nil // re-halted or tearing down; the next resume re-derives
		}
		enabled, config := s.index.asyncReplicationStateForShard(s.name)
		if !enabled {
			if !s.hasActiveAsyncReplicationTargetOverrides() {
				return nil
			}
			config = s.index.AsyncReplicationConfig()
		}
		return s.enableAsyncReplication(ctx, config)
	}); err != nil {
		s.index.logger.
			WithField("action", "async_replication").
			WithField("class_name", s.class.Class).
			WithField("shard_name", s.name).
			Errorf("re-applying async replication after transfer resume: %v", err)
	}
}

// mayForceResumeMaintenanceCycles decrements or clears halt holds and runs the
// physical resume steps when the count reaches zero. Caller must hold haltForTransferMux.
//
// forced=false (anonymous release): decrements one non-owned hold; does
// nothing when only owned holds remain (count - owned == 0). Lowers armed if
// it now exceeds the non-owned count.
//
// forced=true (watchdog fire): clears exactly the armed holds (those acquired
// with inactivityTimeout > 0); the metric increments only when armed > 0.
// Owned holds and holds without a timeout survive.
func (s *Shard) mayForceResumeMaintenanceCycles(ctx context.Context, forced bool) error {
	if s.haltForTransferCount.Load() == 0 {
		// noop, maintenance cycles not halted
		return nil
	}

	if forced {
		n := s.haltForTransferArmedCount
		s.haltForTransferCount.Add(int64(-n))
		s.haltForTransferArmedCount = 0

		if n > 0 && s.promMetrics != nil && s.promMetrics.ShardHaltForTransferForceResume != nil {
			s.promMetrics.ShardHaltForTransferForceResume.
				WithLabelValues().
				Inc()
		}

		// Runs even when n == 0: an anonymous release may have already
		// zeroed the armed count while the monitor goroutine is exiting.
		// haltForTransferCtxCancel must end up nil, or a future armed halt
		// cannot start a new monitor.
		s.mayStopInactivityMonitoring()
		s.haltForTransferInactivityTimeout = 0
		s.haltForTransferInactivityDeadline = time.Time{}

		if s.haltForTransferCount.Load() > 0 {
			// Remaining holds (owned, or anonymous without a timeout) keep the pause.
			return nil
		}
	} else {
		nonOwned := s.haltForTransferCount.Load() - int64(s.haltForTransferOwnedCount)
		if nonOwned <= 0 {
			// Only owned holds remain; an anonymous release cannot consume them.
			return nil
		}
		s.haltForTransferCount.Add(-1)

		// An anonymous release cannot tell which hold it consumed, so lower
		// armed if it now exceeds the non-owned count.
		newNonOwned := s.haltForTransferCount.Load() - int64(s.haltForTransferOwnedCount)
		if int64(s.haltForTransferArmedCount) > newNonOwned {
			s.haltForTransferArmedCount = int(newNonOwned)
		}

		if s.haltForTransferCount.Load() > 0 {
			// maintenance cycles are not resumed as there is at least one active halt request
			return nil
		}

		// terminate the inactivity monitor synchronously under the mux, so a subsequent
		// HaltForTransfer reliably starts a new monitor.
		s.mayStopInactivityMonitoring()

		// fully resumed: reset so the next halt cycle uses its own timeout, not the shortest ever seen.
		s.haltForTransferInactivityTimeout = 0
		s.haltForTransferInactivityDeadline = time.Time{}
	}

	return s.doPhysicalResume(ctx)
}

// doPhysicalResume runs the errgroup that re-enables compaction, cycle callbacks,
// maintenance modes, and vector/geo indexes. Caller must hold haltForTransferMux.
func (s *Shard) doPhysicalResume(ctx context.Context) error {
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
	g.Go(func() error {
		return s.ForEachGeoIndex(func(_ string, index *geo.Index) error {
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

	if s.haltForTransferCount.Load() == 0 {
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

	if s.haltForTransferCount.Load() == 0 {
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
