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
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/storagestate"
)

// Standardized reasons for shard status changes.  These are stored in
// ShardStatus.Reason and used by the recovery logic to decide whether a
// READONLY shard can be automatically transitioned back to READY.
const (
	statusReasonResourcePressure = "resource pressure"
	statusReasonResourceRecovery = "resource usage below threshold"
	statusReasonManualUpdate     = "manually set by user"
	statusReasonShutdown         = "shutdown"
	statusReasonNotifyReady      = "notify ready"
)

type ShardStatus struct {
	Status storagestate.Status
	Reason string
}

// setCountedStatus moves this shard's entry in the per-status shard gauge to
// next, or releases it entirely when next is "". It is idempotent: re-releasing
// an already-released shard is a no-op, which is what lets both teardown paths
// (shutdown and drop, in either order, plus the partial-init cleanup) call it
// unconditionally without double-decrementing.
func (s *Shard) setCountedStatus(next string) {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	s.setCountedStatusLocked(next)
}

// setCountedStatusLocked is setCountedStatus for callers already holding
// statusLock.
func (s *Shard) setCountedStatusLocked(next string) {
	if s.countedStatus == next {
		return
	}

	s.index.metrics.UpdateShardStatus(s.countedStatus, next)
	s.countedStatus = next
}

func (s *Shard) GetStatus() storagestate.Status {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

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
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	return s.updateStatusUnlocked(in, reason)
}

// updateStatusUnlocked updates the status without locking the statusLock.
// Warning: Use UpdateStatus instead.
func (s *Shard) updateStatusUnlocked(in, reason string) error {
	targetStatus, err := storagestate.ValidateStatus(strings.ToUpper(in))
	if err != nil {
		return errors.Wrap(err, in)
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

	s.setCountedStatusLocked(targetStatus.String())

	lvl := logrus.DebugLevel
	if targetStatus == storagestate.StatusReadOnly {
		lvl = logrus.WarnLevel
	}
	logger.Log(lvl, "shard status changed")
	return nil
}
