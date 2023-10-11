//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/storagestate"
)

func (s *Shard) initStatus() {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	s.status = storagestate.StatusReady
}

func (s *Shard) getStatus() storagestate.Status {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	return s.status
}

func (s *Shard) isReadOnly() bool {
	return s.getStatus() == storagestate.StatusReadOnly
}

func (s *Shard) compareAndSwapStatus(old, new string) (storagestate.Status, error) {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	if s.status.String() != old {
		return s.status, nil
	}

	return s.status, s.updateStatusUnlocked(new)
}

func (s *Shard) updateStatus(in string) error {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	return s.updateStatusUnlocked(in)
}

// updateStatusUnlocked updates the status without locking the statusLock.
// Warning: Use updateStatus instead.
func (s *Shard) updateStatusUnlocked(in string) error {
	targetStatus, err := storagestate.ValidateStatus(strings.ToUpper(in))
	if err != nil {
		return errors.Wrap(err, in)
	}

	s.status = targetStatus
	s.updateStoreStatus(targetStatus)

	return nil
}

func (s *Shard) updateStoreStatus(targetStatus storagestate.Status) {
	s.store.UpdateBucketsStatus(targetStatus)
}
