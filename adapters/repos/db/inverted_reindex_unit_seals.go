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
	"sync"

	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// migrationUnitSeals holds the seal every teardown takes before it removes a
// migration's directories. With no builder installed, SealUnit grants every
// seal until the cutover installs one.
type migrationUnitSeals struct {
	mu      sync.RWMutex
	builder ReindexUnitSealBuilder
}

func (s *migrationUnitSeals) Install(builder ReindexUnitSealBuilder) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.builder = builder
}

// SealUnit reserves a (task, unit) for teardown. The returned bool is false
// only when a worker still holds the unit; every uninstalled or nil-seal case
// reports success with a no-op release.
func (s *migrationUnitSeals) SealUnit(desc distributedtask.TaskDescriptor, unitID string) (func(), bool) {
	builder := func() ReindexUnitSealBuilder {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.builder
	}()

	if builder == nil {
		return func() {}, true
	}
	seal := builder()
	if seal == nil {
		return func() {}, true
	}
	return seal(desc, unitID)
}

// SetReindexUnitSeal installs the seal a teardown takes before it removes a
// migration's directories.
func (db *DB) SetReindexUnitSeal(builder ReindexUnitSealBuilder) {
	db.migrationSeals.Install(builder)
}
