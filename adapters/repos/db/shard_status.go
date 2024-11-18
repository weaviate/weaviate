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

package db

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/storagestate"
)

type ShardStatus struct {
	Status storagestate.Status
	Reason string
}

func NewShardStatus() ShardStatus {
	return ShardStatus{Status: storagestate.StatusLoading}
}

func (s *ShardStatus) Init() {
	s.Status = storagestate.StatusReady
	s.Reason = ""
}

func (s *Shard) initStatus() {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	s.status.Init()
}

func (s *Shard) GetStatus() storagestate.Status {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	return s.status.Status
}

// Same implem for for a regular shard, this only differ in lazy loaded shards
func (s *Shard) GetStatusNoLoad() storagestate.Status {
	return s.GetStatus()
}

// isReadOnly returns an error if shard is readOnly and nil otherwise
func (s *Shard) isReadOnly() error {
	if s.status.Status == storagestate.StatusReadOnly {
		return storagestate.ErrStatusReadOnlyWithReason(s.status.Reason)
	}
	return nil
}

func (s *Shard) compareAndSwapStatusIndexingAndReady(old, new string) (storagestate.Status, error) {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	if (old != storagestate.StatusIndexing.String() && old != storagestate.StatusReady.String()) ||
		(new != storagestate.StatusIndexing.String() && new != storagestate.StatusReady.String()) {
		return s.status.Status, fmt.Errorf("can only swap between indexing and ready, got %v and %v", old, new)
	}

	if s.status.Status.String() != old {
		return s.status.Status, nil
	}

	return s.status.Status, s.updateStatusUnlocked(new, "")
}

func (s *Shard) SetStatusReadonly(reason string) error {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	return s.updateStatusUnlocked(storagestate.StatusReadOnly.String(), reason)
}

func (s *Shard) UpdateStatus(in string) error {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	reason := ""
	if in == storagestate.StatusReadOnly.String() {
		reason = "manually set by user"
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

	s.status.Status = targetStatus
	s.status.Reason = reason

	err = s.updateStoreStatus(targetStatus)
	if err != nil {
		return err
	}

	s.index.logger.
		WithField("action", "update shard status").
		WithField("class", s.index.Config.ClassName).
		WithField("shard", s.name).
		WithField("status", in).
		WithField("readOnlyReason", reason)

	return nil
}

func (s *Shard) updateStoreStatus(targetStatus storagestate.Status) error {
	return s.store.UpdateBucketsStatus(targetStatus)
}
