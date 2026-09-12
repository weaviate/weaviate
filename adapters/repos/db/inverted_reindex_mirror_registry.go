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
}

type migrationMirrorKey struct {
	record   MigrationRecordKey
	property string
}

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

func (r *migrationMirrorRegistry) DisarmMigrationMirror(key MigrationRecordKey, prop string) {
	mirrorKey := migrationMirrorKey{record: key, property: prop}

	r.mu.Lock()
	disarm := r.disarms[mirrorKey]
	delete(r.disarms, mirrorKey)
	r.mu.Unlock()

	// Run outside the lock: disarm reaches into shard write-path state.
	if disarm != nil {
		disarm()
	}
}

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

var _ migrationMirrorDisarmer = (*Shard)(nil)

func (s *Shard) DisarmMigrationMirror(key MigrationRecordKey, prop string) {
	s.migrationMirrors.DisarmMigrationMirror(key, prop)
}
