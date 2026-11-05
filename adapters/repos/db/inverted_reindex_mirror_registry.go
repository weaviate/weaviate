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
	// held lists the tasks a load or lift could neither arm nor stamp Unmirrored, kept after a retry settles them so steps wait until a status write.
	held []*ShardReindexTaskGeneric
	// The steps still wait after a retry settled the held set, until a status write makes the shard writable.
	heldSettled bool
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
	r.held, r.heldSettled = tasks, false
}

func (r *migrationMirrorRegistry) heldUnarmed() []*ShardReindexTaskGeneric {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.heldSettled {
		return nil
	}
	return r.held
}

func (r *migrationMirrorRegistry) markHeldSettled() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.heldSettled = len(r.held) > 0
}

func (r *migrationMirrorRegistry) releaseHeld() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.held, r.heldSettled = nil, false
}

func (r *migrationMirrorRegistry) holding() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.held) > 0
}

var _ migrationMirrorDisarmer = (*Shard)(nil)

func (s *Shard) DisarmMigrationMirror(key MigrationRecordKey, prop string) {
	s.migrationMirrors.DisarmMigrationMirror(key, prop)
}
