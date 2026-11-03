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

type migrationMirrorRegistry struct {
	mu      sync.Mutex
	disarms map[migrationMirrorKey]func()
	// Neither armed nor recorded as unarmed; the shard stays read-only for them.
	held []*ShardReindexTaskGeneric
}

type migrationMirrorKey struct {
	record   MigrationRecordKey
	property string
}

func (r *migrationMirrorRegistry) ArmMigrationMirror(key MigrationRecordKey, prop string, disarm func()) {
	if disarm == nil {
		return
	}
	if previous := r.swap(migrationMirrorKey{record: key, property: prop}, disarm); previous != nil {
		previous()
	}
}

func (r *migrationMirrorRegistry) DisarmMigrationMirror(key MigrationRecordKey, prop string) {
	// Run outside the lock: disarm reaches into shard write-path state.
	if disarm := r.take(migrationMirrorKey{record: key, property: prop}); disarm != nil {
		disarm()
	}
}

func (r *migrationMirrorRegistry) DisarmMigrationMirrors(key MigrationRecordKey) {
	for _, disarm := range r.takeAll(key) {
		disarm()
	}
}

func (r *migrationMirrorRegistry) swap(mirrorKey migrationMirrorKey, disarm func()) func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.disarms == nil {
		r.disarms = map[migrationMirrorKey]func(){}
	}
	previous := r.disarms[mirrorKey]
	r.disarms[mirrorKey] = disarm
	return previous
}

func (r *migrationMirrorRegistry) take(mirrorKey migrationMirrorKey) func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	disarm := r.disarms[mirrorKey]
	delete(r.disarms, mirrorKey)
	return disarm
}

func (r *migrationMirrorRegistry) takeAll(key MigrationRecordKey) []func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	var disarms []func()
	for mirrorKey, disarm := range r.disarms {
		if mirrorKey.record != key {
			continue
		}
		disarms = append(disarms, disarm)
		delete(r.disarms, mirrorKey)
	}
	return disarms
}

func (r *migrationMirrorRegistry) holdUnarmed(tasks []*ShardReindexTaskGeneric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.held = tasks
}

func (r *migrationMirrorRegistry) heldUnarmed() []*ShardReindexTaskGeneric {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.held
}

func (r *migrationMirrorRegistry) releaseHeld() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.held = nil
}

func (r *migrationMirrorRegistry) isHeld(key MigrationRecordKey) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, t := range r.held {
		if t.migrationRecordKey() == key {
			return true
		}
	}
	return false
}

var _ migrationMirrorDisarmer = (*Shard)(nil)

func (s *Shard) DisarmMigrationMirror(key MigrationRecordKey, prop string) {
	s.migrationMirrors.DisarmMigrationMirror(key, prop)
}
