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
// double-write mirror, keyed by (record key, property).
//
// The key is what makes it necessary. A mirror lives exactly as long as its
// record's staged data can still become the live data, so it is disarmed on
// the edges that end that possibility — a successor's retirement, the cancel
// edge, swap completion — and none of those actors is the one that armed it.
// Handles collected on the arming task instance are unreachable from all
// three, and the provider clears a terminal task's instance cache outright.
//
// Per property rather than per record because the relation that disarms is
// itself per property: a successor's property set can partially overlap a
// committed predecessor's, and disarming the predecessor wholesale would stop
// mirroring properties the successor never took over.
//
// The zero value is usable, so a shard can hold one as a plain field.
type migrationMirrorRegistry struct {
	mu      sync.Mutex
	disarms map[migrationMirrorKey]func()
}

type migrationMirrorKey struct {
	record   MigrationRecordKey
	property string
}

// ArmMigrationMirror records the handle that disarms the mirror for one
// (record, property). Arming a pair that is already armed disarms the handle
// it replaces, so a re-registration after a resume cannot leave the write path
// carrying two callbacks for one property.
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
// property). Disarming a pair that is not armed is a no-op: every edge that
// disarms is re-derived at each load and must be safe to re-run, and after a
// restart there is nothing to disarm at all because mirrors live only in the
// process that armed them.
func (r *migrationMirrorRegistry) DisarmMigrationMirror(key MigrationRecordKey, prop string) {
	mirrorKey := migrationMirrorKey{record: key, property: prop}

	r.mu.Lock()
	disarm := r.disarms[mirrorKey]
	delete(r.disarms, mirrorKey)
	r.mu.Unlock()

	// Outside the lock: the handle reaches into the shard's write-path
	// callback state, and holding two locks in one order here would pin an
	// ordering on every other caller of that state.
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

// ArmedMigrationMirrors reports how many mirrors are armed. Two generations on
// one property is a steady state while a failed one waits to be superseded, so
// the count is the one observable that tells a leak from that overlap.
func (r *migrationMirrorRegistry) ArmedMigrationMirrors() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.disarms)
}

// A shard is the registry's owner, and satisfying the disarmer interface is
// what lets reconciliation reach the handles without knowing about shards.
var _ migrationMirrorDisarmer = (*Shard)(nil)

func (s *Shard) DisarmMigrationMirror(key MigrationRecordKey, prop string) {
	s.migrationMirrors.DisarmMigrationMirror(key, prop)
}
