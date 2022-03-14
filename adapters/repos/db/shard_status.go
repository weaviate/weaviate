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
)

const (
	ShardStatusReadOnly = "READONLY"
	ShardStatusReady    = "READY"
)

var (
	ErrShardReadOnly = errors.New("shard is read-only")
)

func (s *Shard) initStatus() {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	s.status = ShardStatusReady
}

func (s *Shard) getStatus() string {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	return s.status
}

func (s *Shard) isReadOnly() bool {
	return s.getStatus() == ShardStatusReadOnly
}

func (s *Shard) updateStatus(in string) error {
	s.statusLock.Lock()
	defer s.statusLock.Unlock()

	targetStatus := strings.ToUpper(in)

	if !isValidShardStatus(targetStatus) {
		return errors.Errorf("'%s' is not a valid shard status", in)
	}

	s.status = targetStatus
	return nil
}

func isValidShardStatus(in string) bool {
	switch in {
	case ShardStatusReadOnly, ShardStatusReady:
		return true
	default:
		return false
	}
}
