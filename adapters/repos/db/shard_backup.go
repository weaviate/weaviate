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

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/geo"
	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/file"
)

func replicaHaltOwner(opID string) string { return "replica:" + opID }

func offloadHaltOwner(shard string) string { return "offload:" + shard }

// haltTotalLocked returns the summed refcount across all owners. Caller must
// hold haltForTransferMux.
func (s *Shard) haltTotalLocked() int {
	total := 0
	for _, n := range s.haltForTransferOwners {
		total += n
	}
	return total
}

// publishHaltTotalLocked mirrors the owner map's total into haltForTransferTotal,
// which haltedForTransfer reads without the mux. Every mutation of
// haltForTransferOwners must call it. Caller must hold haltForTransferMux.
func (s *Shard) publishHaltTotalLocked() int {
	total := s.haltTotalLocked()
	s.haltForTransferTotal.Store(int64(total))
	return total
}

// haltedForTransfer is a lock-free probe: never parks behind backup prep, never misreads a concurrent reader as halted.
func (s *Shard) haltedForTransfer() bool {
	return s.haltForTransferTotal.Load() > 0
}

// haltAddOwnerLocked records one more halt held by owner and returns the new
// total. Caller must hold haltForTransferMux.
func (s *Shard) haltAddOwnerLocked(owner string) int {
	if s.haltForTransferOwners == nil {
		s.haltForTransferOwners = map[string]int{}
	}
	s.haltForTransferOwners[owner]++
	return s.publishHaltTotalLocked()
}

// haltRemoveOwnerLocked drops one halt held by owner, deleting the entry at
// zero, and reports whether owner is now fully gone. Caller must hold haltForTransferMux.
func (s *Shard) haltRemoveOwnerLocked(owner string) (gone bool) {
	n, ok := s.haltForTransferOwners[owner]
	if !ok {
		return false
	}
	if n <= 1 {
		delete(s.haltForTransferOwners, owner)
		s.publishHaltTotalLocked()
		return true
	}
	s.haltForTransferOwners[owner] = n - 1
	s.publishHaltTotalLocked()
	return false
}

// haltDropOwnerLocked removes every halt held by owner and reports whether any
// existed. Caller must hold haltForTransferMux.
func (s *Shard) haltDropOwnerLocked(owner string) (held bool) {
	if _, ok := s.haltForTransferOwners[owner]; !ok {
		return false
	}
	delete(s.haltForTransferOwners, owner)
	s.publishHaltTotalLocked()
	return true
}

// clearHaltForTransferStateLocked drops every trace of halt-for-transfer state at
// shard teardown: a shard whose store is closed must not keep answering "paused for
// transfer", nor keep claiming an outstanding resume. Both owner maps are cleared
// together; clearing only haltForTransferOwners would leave an armed set that
// resumeOwnerLocked's zero-total branch can never clean up. Caller must hold
// haltForTransferMux.
func (s *Shard) clearHaltForTransferStateLocked() {
	// also drops an already-fired monitor waiting on the mux, so it can't resume mid-teardown.
	s.mayStopInactivityMonitoring()
	s.haltForTransferOwners = nil
	s.publishHaltTotalLocked()
	s.haltForTransferInactivityOwners = nil
	s.haltForTransferInactivityTimeout = 0
	s.haltForTransferInactivityDeadline = time.Time{}
	s.maintenanceResumePending = false
}

// HaltForTransfer stops compaction, and flushing memtable and commit log to begin with backup or cloud offload.
// This method could be called multiple times with different inactivity timeouts,
// a zeroed `inactivityTimeout` implies no timeout. Each caller names itself via
// owner; a resume removes only that owner's halt.
// If inactivity timeout is reached it will resume the maintenance cycle for the
// timing-out transfers independently of how many halt requests they made or of
// the total live-halt count; healthy co-holders that never armed a timeout survive.
// The preparation work (pausing compaction, flushing memtables, readying vector indexes and queues)
// is additionally bounded by `HaltForTransferTimeout`, independent of `inactivityTimeout`;
// a zeroed `HaltForTransferTimeout` implies no bound.
func (s *Shard) HaltForTransfer(ctx context.Context, owner string, offloading bool, inactivityTimeout time.Duration) (err error) {
	innerCtx := ctx
	if timeout := s.index.Config.HaltForTransferTimeout; timeout > 0 {
		var cancel context.CancelFunc
		innerCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	s.haltForTransferMux.Lock()
	defer s.haltForTransferMux.Unlock()

	// Check before registering owner so a rejection does not leave a halt
	// recorded; the error path would not run a matching resume.
	if !offloading {
		if blockedErr := s.index.refuseIfReindexInFlight(s.name); blockedErr != nil {
			return blockedErr
		}
		if busy, reason := s.structuralVectorOpInFlight(); busy {
			return fmt.Errorf("%w: shard %q: %s; transfer deferred until it completes",
				enterrors.ErrShardBusyStructuralOp, s.name, reason)
		}
	}

	newTotal := s.haltAddOwnerLocked(owner)

	defer func() {
		if err != nil {
			return
		}
		if inactivityTimeout > 0 {
			if s.haltForTransferInactivityOwners == nil {
				s.haltForTransferInactivityOwners = map[string]struct{}{}
			}
			s.haltForTransferInactivityOwners[owner] = struct{}{}
			s.mayUpdateInactivityTimeout(inactivityTimeout)
			s.mayInitInactivityMonitoring()
		}
		// A halt that returns successfully is itself liveness, and neither call above
		// moves an existing deadline: mayUpdateInactivityTimeout only shortens the
		// timeout, and mayInitInactivityMonitoring returns early once a monitor is
		// running. Without this reset an overlapping consumer inherits a deadline the
		// first consumer's seal steps already spent. No-ops when no timeout is armed.
		s.mayResetInactivityDeadline()
	}()

	if offloading {
		// TODO: tenant offloading is calling HaltForTransfer but
		// if Shutdown is called this step is not needed.
		//
		// capture=false: do NOT persist the hashtree. A persisted tree is accepted as
		// authoritative on the next load after a height-only check, which is sound only
		// when the shard serves no further writes. This shard still takes internal
		// replica writes during the halt, and an aborted freeze returns it to HOT. With
		// no .ht on disk the next activation rebuilds by full scan.
		s.mayStopAsyncReplication(false)
	}

	// Placed before the pause branch so it also covers total>1 callers: on error
	// resumeOwnerLocked removes only our own owner's halt and truly resumes only
	// at total==0, so a failed total>1 caller rolls back without unhalting the
	// shard another op still holds.
	defer func() {
		if err != nil {
			// each preparation step below wraps its own error; only append
			// the outcome of the cleanup attempt here
			if err2 := s.resumeOwnerLocked(ctx, owner); err2 != nil {
				err = fmt.Errorf("%w: resume maintenance: %w", err, err2)
			}
		}
	}()

	// Pause steps run only on the first halt. Re-pausing per halt would strand the
	// per-bucket pause-timer refcount (1 pause : 1 stop) and never observe the
	// Prometheus pause-duration timer.
	if newTotal == 1 {
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
			Debugf("shard already halted for transfer by %q (owner count=%d, total=%d); re-sealing state on shared halt",
				owner, s.haltForTransferOwners[owner], newTotal)
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
	if err := s.forceResumeArmedLocked(context.Background()); err != nil {
		s.index.logger.Error(err)
	}
	forceResumed = true
	return false
}

// CreateBackupSnapshot halts compaction, lists backup files, hardlinks them into
// a staging directory, then immediately resumes compaction. This minimizes the
// compaction pause to just the time needed for enumeration and hardlink creation
// (typically 2-5s), rather than blocking for the entire upload duration.
func (s *Shard) CreateBackupSnapshot(ctx context.Context, owner string, sd *backup.ShardDescriptor, stagingRoot string) ([]string, error) {
	if err := s.HaltForTransfer(ctx, owner, false, 0); err != nil {
		return nil, fmt.Errorf("halt for snapshot: %w", err)
	}
	defer s.resumeMaintenanceCycles(ctx, owner)

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

	if s.haltTotalLocked() == 0 {
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

func (s *Shard) resumeMaintenanceCycles(ctx context.Context, owner string) error {
	s.haltForTransferMux.Lock()
	err := s.resumeOwnerLocked(ctx, owner)
	fullyResumed := s.haltTotalLocked() == 0
	s.haltForTransferMux.Unlock()

	// Enables skipped while halted have no other re-derive path for plain halts.
	if fullyResumed {
		s.reapplyAsyncReplicationAfterResume(ctx)
	}
	return err
}

// resumeHaltOwner drops all of owner's halts and resumes maintenance when no other
// halt remains. Reports whether owner held anything, so the caller can skip recovery
// work on a shard that was never halted.
//
// It drops the owner outright rather than decrementing once, because it recovers a
// halt whose placer is gone: two aborted freeze rounds leave two halts under the same
// owner and no actor is left to match them one for one.
//
// Unlike resumeMaintenanceCycles it does not re-derive async replication: its only
// caller follows a held halt with rebuildAsyncReplicationAfterOffloadHalt, which
// restores the tree by full scan.
func (s *Shard) resumeHaltOwner(ctx context.Context, owner string) (wasHeld bool, err error) {
	s.haltForTransferMux.Lock()
	defer s.haltForTransferMux.Unlock()

	if !s.haltDropOwnerLocked(owner) {
		return false, nil
	}
	delete(s.haltForTransferInactivityOwners, owner)

	return true, s.completeResumeLocked(ctx)
}

// resumeOwnerLocked removes one of owner's halts and, once the owner is fully
// gone, un-arms it; maintenance physically resumes only when no live halt
// remains. A pending retry from an earlier failed resume is deferred while a
// reindex is in flight on this shard. Caller must hold haltForTransferMux.
func (s *Shard) resumeOwnerLocked(ctx context.Context, owner string) error {
	if s.haltTotalLocked() == 0 {
		if s.maintenanceResumePending {
			// The backup release sweep reaches every loaded shard, so this retry can
			// arrive uninvited. The store's compaction pause is a shared boolean, so
			// retrying while the reindex/orphan-audit machinery holds that pause would
			// restart compaction underneath its file removals. Skipping keeps the flag
			// set for the next attempt.
			if blockedErr := s.index.refuseIfReindexInFlight(s.name); blockedErr != nil {
				s.index.logger.WithFields(logrus.Fields{
					"action": "resume_maintenance",
					"shard":  s.name,
					"reason": "reindex_in_flight",
				}).Debugf("deferring pending maintenance resume: %v", blockedErr)
				return nil
			}

			// A prior resume removed the owner bookkeeping below BEFORE the fallible
			// completeResumeLocked ran and then failed; without this retry every later
			// attempt reports success on a shard whose maintenance never restarted. The
			// state being repaired is a queue or vector index left in backup mode, not
			// compaction left off: store.ResumeCompaction cannot fail and the error
			// group runs every leg regardless of the first error.
			//
			// The retry handle is this flag rather than a retained owner claim, because
			// haltForTransferOwners is a gate: haltTotalLocked answers "is this shard
			// paused for transfer" and decides whether the next halt runs the pause
			// steps. A failed resume has already restarted compaction, so a retained
			// owner would report "paused" while compaction physically runs.
			return s.completeResumeLocked(ctx)
		}
		// noop, maintenance cycles not halted
		return nil
	}

	if s.haltRemoveOwnerLocked(owner) {
		delete(s.haltForTransferInactivityOwners, owner)
	}

	return s.completeResumeLocked(ctx)
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

func (s *Shard) forceResumeArmedLocked(ctx context.Context) error {
	if s.haltTotalLocked() == 0 {
		// noop, maintenance cycles not halted
		return nil
	}

	forced := make([]string, 0, len(s.haltForTransferInactivityOwners))
	for a := range s.haltForTransferInactivityOwners {
		delete(s.haltForTransferOwners, a)
		forced = append(forced, a)
	}
	s.publishHaltTotalLocked()
	s.haltForTransferInactivityOwners = nil
	s.index.logger.WithField("shard", s.name).
		Warnf("halt-for-transfer inactivity watchdog fired; force-resuming armed owners %v", forced)
	// A tick in steady state means a transfer was force-resumed mid-stream —
	// i.e. the read-path timer reset isn't reaching us.
	if s.promMetrics != nil && s.promMetrics.ShardHaltForTransferForceResume != nil {
		s.promMetrics.ShardHaltForTransferForceResume.
			WithLabelValues().
			Inc()
	}

	return s.completeResumeLocked(ctx)
}

func (s *Shard) completeResumeLocked(ctx context.Context) error {
	// Tear the monitor down once no armed owner remains — whether via a fire that
	// cleared the set or the last armed transfer resuming normally.
	if len(s.haltForTransferInactivityOwners) == 0 {
		// terminate the inactivity monitor synchronously under the mux, so a subsequent
		// HaltForTransfer reliably starts a new monitor.
		s.mayStopInactivityMonitoring()

		// reset so the next halt cycle uses its own timeout, not the shortest ever seen.
		s.haltForTransferInactivityTimeout = 0
		s.haltForTransferInactivityDeadline = time.Time{}
	}

	if s.haltTotalLocked() > 0 {
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
	g.Go(func() error {
		return s.ForEachGeoIndex(func(_ string, index *geo.Index) error {
			if err := index.ResumeAfterBackup(ctx); err != nil {
				return fmt.Errorf("resuming after backup: %w", err)
			}
			return nil
		})
	})

	if err := g.Wait(); err != nil {
		s.maintenanceResumePending = true
		return fmt.Errorf("failed to resume maintenance cycles for shard '%s': %w", s.name, err)
	}

	s.maintenanceResumePending = false
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

	if s.haltTotalLocked() == 0 {
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

	if s.haltTotalLocked() == 0 {
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
