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

import "sync"

// migrationMirrorRegistry holds the handles that disarm a migration's
// double-write mirror, keyed by (record key, property) rather than task
// instance: whoever disarms (successor retirement, cancel, swap completion,
// task-cache eviction) is never whoever armed. Per-property so disarming a
// predecessor can't silently drop mirroring for properties its successor
// hasn't taken over yet.
type migrationMirrorRegistry struct {
	mu      sync.Mutex
	disarms map[migrationMirrorKey]func()
}

type migrationMirrorKey struct {
	record   MigrationRecordKey
	property string
}

// ArmMigrationMirror records the handle that disarms the mirror for one
// (record, property). Re-arming an already-armed pair disarms the handle it
// replaces, so a resume can't leave two callbacks registered for one property.
func (r *migrationMirrorRegistry) ArmMigrationMirror(key MigrationRecordKey, prop string, disarm func()) {
	if disarm == nil {
		return
	}
	mirrorKey := migrationMirrorKey{record: key, property: prop}

	r.mu.Lock()
	if r.disarms == nil {
		r.disarms = map[migrationMirrorKey]func(){}
	}
	previous := r.disarms[mirrorKey]
	r.disarms[mirrorKey] = disarm
	r.mu.Unlock()

	if previous != nil {
		previous()
	}
}

// DisarmMigrationMirror runs and forgets the handle for one (record,
// property); a no-op if unarmed, since every disarm edge is re-derived at
// each load and must be safe to re-run.
func (r *migrationMirrorRegistry) DisarmMigrationMirror(key MigrationRecordKey, prop string) {
	mirrorKey := migrationMirrorKey{record: key, property: prop}

	r.mu.Lock()
	disarm := r.disarms[mirrorKey]
	delete(r.disarms, mirrorKey)
	r.mu.Unlock()

	// Run outside the lock: disarm reaches into shard write-path state, and
	// calling it locked would pin a lock ordering on every other caller.
	if disarm != nil {
		disarm()
	}
}

// DisarmMigrationMirrors disarms every property of one record. It is what the
// cancel edge and process exit need, where the record goes away whole.
func (r *migrationMirrorRegistry) DisarmMigrationMirrors(key MigrationRecordKey) {
	r.mu.Lock()
	var disarms []func()
	for mirrorKey, disarm := range r.disarms {
		if mirrorKey.record != key {
			continue
		}
		disarms = append(disarms, disarm)
		delete(r.disarms, mirrorKey)
	}
	r.mu.Unlock()

	for _, disarm := range disarms {
		disarm()
	}
}

// Lets reconciliation reach the handles without knowing about shards.
var _ migrationMirrorDisarmer = (*Shard)(nil)

func (s *Shard) DisarmMigrationMirror(key MigrationRecordKey, prop string) {
	s.migrationMirrors.DisarmMigrationMirror(key, prop)
}
