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
	"time"

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
	// Same test as the write guards: every status but READONLY takes writes.
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

// A status change can run inside a RAFT apply.
const liftRetryTimeout = 30 * time.Second

// retryHeldMigrations runs before the status lock so reads never wait on the
// disk. Only the caller's wait is bounded: a stamp is permanent, so the arm runs
// to its end under its own shard reference, which shutdown waits for.
func (s *Shard) retryHeldMigrations(in string) error {
	target, err := storagestate.ValidateStatus(strings.ToUpper(in))
	held := s.migrationMirrors.heldUnarmed()
	// The shutdown's own status write runs under the lock preventShutdown takes.
	if err != nil || target == storagestate.StatusReadOnly || len(held) == 0 || s.shut.Load() {
		return nil
	}
	release, err := s.preventShutdown()
	if err != nil {
		return nil
	}
	if !s.liftRetry.TryLock() {
		release()
		return s.heldReadOnlyErr(errors.New("a recovery retry is already running"))
	}
	done := make(chan error, 1)
	enterrors.GoWrapper(func() {
		defer release()
		defer s.liftRetry.Unlock()
		_, err := s.armRecoveredMigrations(context.WithoutCancel(s.shutCtx), held)
		// The arm may open buckets after its caller left, while the shard is still READONLY.
		ec := errorcompounder.New()
		ec.Add(err)
		ec.Add(s.store.UpdateBucketsStatus(storagestate.StatusReadOnly))
		// Marked last, so no lift races the reset. The steps keep waiting until a status write ends the hold.
		if err == nil {
			s.migrationMirrors.markHeldArmed()
		}
		done <- ec.ToError()
	}, s.index.logger)
	wait := time.NewTimer(liftRetryTimeout)
	defer wait.Stop()
	select {
	case err := <-done:
		if err != nil {
			return s.heldReadOnlyErr(err)
		}
		return nil
	case <-wait.C:
		return s.heldReadOnlyErr(fmt.Errorf("the disk did not answer within %s", liftRetryTimeout))
	case <-s.shutCtx.Done():
		return s.heldReadOnlyErr(fmt.Errorf("the shard is shutting down: %w", context.Cause(s.shutCtx)))
	}
}

func (s *Shard) heldReadOnlyErr(cause error) error {
	return fmt.Errorf("shard %q stays %s: a reindex migration's double-write mirror is not armed and "+
		"cannot be stamped Unmirrored: %w", s.name, storagestate.StatusReadOnly, cause)
}
