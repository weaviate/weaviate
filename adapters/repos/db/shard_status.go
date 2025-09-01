//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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

type ShardStatus struct {
	Status storagestate.Status
	Reason string
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
	s.status.Status = status
	return status
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

	s.index.metrics.UpdateShardStatus(oldStatus.String(), targetStatus.String())

	lvl := logrus.DebugLevel
	if targetStatus == storagestate.StatusReadOnly {
		lvl = logrus.WarnLevel
	}
	logger.Log(lvl, "shard status changed")
	return nil
}
