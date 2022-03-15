//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/storagestate"
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

func (s *Shard) updateStatus(in string) error {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

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
