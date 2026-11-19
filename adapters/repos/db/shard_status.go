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
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storagestate"
)

// Standardized reasons for shard status changes.  These are stored in
// ShardStatus.Reason and used by the recovery logic to decide whether a
// READONLY shard can be automatically transitioned back to READY.
const (
	statusReasonResourcePressure  = "resource pressure"
	statusReasonResourceRecovery  = "resource usage below threshold"
	statusReasonManualUpdate      = "manually set by user"
	statusReasonVectorIndexUpdate = "update vector index config"
	statusReasonShutdown          = "shutdown"
	statusReasonNotifyReady       = "notify ready"
	statusReasonMirrorUnarmed     = "a reindex migration could not re-arm its double-write mirror"
)

type ShardStatus struct {
	Status storagestate.Status
	Reason string
}

func (s *Shard) registerCountedStatus(status storagestate.Status) {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	s.index.metrics.UpdateShardStatus("", status.String())
	s.countedStatus = status.String()
}

// moveCountedStatusUnlocked follows a status change in the gauge; an uncounted
// (released) shard stays uncounted.
func (s *Shard) moveCountedStatusUnlocked(next storagestate.Status) {
	if s.countedStatus == "" || s.countedStatus == next.String() {
		return
	}

	s.index.metrics.UpdateShardStatus(s.countedStatus, next.String())
	s.countedStatus = next.String()
}

// releaseCountedStatus is idempotent and safe on a never-counted shard.
func (s *Shard) releaseCountedStatus() {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()
	s.migrationMirrors.stopCountingHeld()

	if s.countedStatus == "" {
		return
	}

	s.index.metrics.UpdateShardStatus(s.countedStatus, "")
	s.countedStatus = ""
}

// GetStatus recomputes READY/INDEXING from the vector queues. The gauge is not
// moved by the recompute, only by status writes.
func (s *Shard) GetStatus() storagestate.Status {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	return s.getStatusUnlocked()
}

func (s *Shard) getStatusUnlocked() storagestate.Status {
	if s.status.Status != storagestate.StatusReady && s.status.Status != storagestate.StatusIndexing {
		return s.status.Status
	}

	if !s.hasAnyVectorIndex() {
		return s.status.Status
	}

	status := storagestate.StatusReady
	_ = s.ForEachVectorQueue(func(_ string, queue *VectorIndexQueue) error {
		if queue.Size() > 0 {
			status = storagestate.StatusIndexing
		}
		return nil
	})
	_ = s.ForEachGeoQueue(func(_ string, queue *VectorIndexQueue) error {
		if queue.Size() > 0 {
			status = storagestate.StatusIndexing
		}
		return nil
	})
	s.status.Status = status
	return status
}

func (s *Shard) GetStatusReason() string {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()
	return s.status.Reason
}

// isReadOnly returns an error if shard is readOnly and nil otherwise
func (s *Shard) isReadOnly() error {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	if s.status.Status == storagestate.StatusReadOnly {
		return storagestate.ErrStatusReadOnlyWithReason(s.status.Reason)
	}
	return nil
}

func (s *Shard) SetStatusReadonly(reason string) error {
	return s.UpdateStatus(storagestate.StatusReadOnly.String(), reason)
}

func (s *Shard) UpdateStatus(in, reason string) error {
	if err := s.retryHeldMigrations(in); err != nil {
		return err
	}
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	return s.updateStatusUnlocked(in, reason)
}

// UpdateStatusIf writes the new status only if cond holds for the current one,
// evaluating cond and writing under a single statusLock acquisition so a status
// another writer sets in between cannot be overwritten. cond runs with the lock
// held and must not call back into the shard.
func (s *Shard) UpdateStatusIf(cond func(ShardStatus) bool, in, reason string) error {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	current := ShardStatus{Status: s.getStatusUnlocked(), Reason: s.status.Reason}
	if !cond(current) {
		return nil
	}
	return s.updateStatusUnlocked(in, reason)
}

// updateStatusUnlocked updates the status without locking the statusLock.
// Warning: Use UpdateStatus instead.
func (s *Shard) updateStatusUnlocked(in, reason string) error {
	targetStatus, err := storagestate.ValidateStatus(strings.ToUpper(in))
	if err != nil {
		return errors.Wrap(err, in)
	}
	// isReadOnly lets every status but READONLY take writes, so the gate refuses each of them until a retry arms or stamps every held task.
	if targetStatus != storagestate.StatusReadOnly && len(s.migrationMirrors.heldUnarmed()) > 0 {
		return s.heldReadOnlyErr(errors.New("no recovery retry has re-armed its mirror or stamped it Unmirrored"))
	}
	oldStatus := s.status.Status
	s.status.Status = targetStatus
	s.status.Reason = reason

	logger := s.index.logger.WithFields(logrus.Fields{
		"action": "update_shard_status",
		"class":  s.index.Config.ClassName,
		"shard":  s.name,
		"status": targetStatus.String(),
		"prev":   oldStatus.String(),
		"reason": reason,
	})
	if err = s.store.UpdateBucketsStatus(targetStatus); err != nil {
		logger.WithError(err).Error("shard status change failed")
		return err
	}

	if targetStatus != storagestate.StatusReadOnly {
		s.migrationMirrors.releaseHeld()
	}
	s.moveCountedStatusUnlocked(targetStatus)

	lvl := logrus.DebugLevel
	if targetStatus == storagestate.StatusReadOnly {
		lvl = logrus.WarnLevel
	}
	logger.Log(lvl, "shard status changed")
	return nil
}

// retryHeldMigrations starts the arm and refuses at once: a status change can run inside a RAFT apply,
// which must never wait on the disk.
// A shutdown waits for the arm's shard reference, and a drop waits about 30 s for it.
func (s *Shard) retryHeldMigrations(in string) error {
	target, err := storagestate.ValidateStatus(strings.ToUpper(in))
	held := s.migrationMirrors.heldUnarmed()
	// performShutdown writes SHUTDOWN while holding shutdownLock, which preventShutdown would wait on forever.
	if err != nil || target == storagestate.StatusReadOnly || len(held) == 0 || s.shut.Load() {
		return nil
	}
	release, err := s.preventShutdown()
	if err != nil {
		return s.heldReadOnlyErr(fmt.Errorf("the shard is shutting down: %w", err))
	}
	if !s.liftRetry.TryLock() {
		release()
		return s.heldReadOnlyErr(errors.New("a recovery retry is already running"))
	}
	enterrors.GoWrapper(func() {
		defer release()
		defer s.liftRetry.Unlock()
		if err := s.rearmHeld(); err != nil {
			s.index.logger.WithField("shard", s.name).Errorf("recovery retry of a held shard failed: %v", err)
		}
	}, s.index.logger)
	return s.heldReadOnlyErr(errors.New("a recovery retry started; this node's log names a failure"))
}

// rearmHeld reads the held set under liftRetry, because an earlier lift's arm may have armed or stamped it since.
// It arms only units ClaimUnits claimed, which a cleanup's seal waits for, so a teardown cannot remove what the arm re-creates.
func (s *Shard) rearmHeld() error {
	held := s.migrationMirrors.heldUnarmed()
	if len(held) == 0 {
		return nil
	}
	claimed, skipped, releaseClaims := s.reindexer.ClaimUnits(held)
	defer releaseClaims()
	unarmed, err := s.armRecoveredMigrations(context.WithoutCancel(s.shutCtx), claimed)
	ec := errorcompounder.New()
	ec.Add(err)
	// Buckets the arm opened go back to READONLY before the hold changes, so no lift races the reset.
	ec.Add(s.store.UpdateBucketsStatus(storagestate.StatusReadOnly))
	if len(skipped) > 0 {
		ec.Add(fmt.Errorf("a teardown holds the unit of %d migration(s); set the status again once it ends", len(skipped)))
	}
	if err != nil || len(skipped) > 0 {
		s.migrationMirrors.holdUnarmed(append(unarmed, skipped...))
	} else {
		s.migrationMirrors.markHeldSettled()
		ec.Add(s.index.db.liftHeldShard(s))
	}
	return ec.ToError()
}

func (s *Shard) heldReadOnlyErr(cause error) error {
	return fmt.Errorf("shard %q: a reindex migration's double-write mirror is not armed and "+
		"cannot be stamped Unmirrored: %w", s.name, cause)
}
