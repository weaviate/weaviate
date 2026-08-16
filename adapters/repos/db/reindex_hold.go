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
	"fmt"
	"strings"
	"sync"
)

type ReindexHold int

const (
	ReindexHoldNone ReindexHold = iota
	ReindexHoldCleanup
)

func (h ReindexHold) String() string {
	switch h {
	case ReindexHoldNone:
		return "none"
	case ReindexHoldCleanup:
		return "cleanup"
	default:
		return fmt.Sprintf("unrecognized_hold_%d", int(h))
	}
}

// ReindexHoldRegistry tracks in-progress cleanup sweeps, collection-wide because a sweep walks every local shard.
// Refcounted, so overlapping teardowns cannot let the first to finish reopen the gate under the second.
type ReindexHoldRegistry struct {
	mu    sync.RWMutex
	holds map[string]map[ReindexHold]int
}

// Returns an idempotent release: calling it twice must not drop another hold.
func (r *ReindexHoldRegistry) acquire(collection string, kind ReindexHold) func() {
	key := strings.ToLower(collection)
	r.mu.Lock()
	if r.holds == nil {
		r.holds = make(map[string]map[ReindexHold]int, 1)
	}
	byKind := r.holds[key]
	if byKind == nil {
		byKind = make(map[ReindexHold]int, 1)
		r.holds[key] = byKind
	}
	byKind[kind]++
	r.mu.Unlock()
	return sync.OnceFunc(func() { r.release(key, kind) })
}

func (r *ReindexHoldRegistry) release(key string, kind ReindexHold) {
	r.mu.Lock()
	defer r.mu.Unlock()
	byKind := r.holds[key]
	byKind[kind]--
	if byKind[kind] <= 0 {
		delete(byKind, kind)
	}
	if len(byKind) == 0 {
		delete(r.holds, key)
	}
}

// Hold runs fn with the collection held; the release is deferred so a panicking sweep cannot leak it.
func (r *ReindexHoldRegistry) Hold(collection string, kind ReindexHold, fn func()) {
	release := r.acquire(collection, kind)
	defer release()
	fn()
}

// HoldReindexCleanup runs fn with the collection's backup and restore gates shut,
// so a teardown spanning several index types is covered end to end rather than
// re-taken per sweep. Nesting is safe: holds are refcounted.
func (db *DB) HoldReindexCleanup(collection string, fn func()) {
	db.reindexHolds.Hold(collection, ReindexHoldCleanup, fn)
}

// HoldFor returns the strongest hold on the named collections, or on all of them when none are named.
func (r *ReindexHoldRegistry) HoldFor(collections ...string) ReindexHold {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(collections) == 0 {
		strongest := ReindexHoldNone
		for _, byKind := range r.holds {
			strongest = max(strongest, strongestOf(byKind))
		}
		return strongest
	}
	strongest := ReindexHoldNone
	for _, collection := range collections {
		strongest = max(strongest, strongestOf(r.holds[strings.ToLower(collection)]))
	}
	return strongest
}

func strongestOf(byKind map[ReindexHold]int) ReindexHold {
	strongest := ReindexHoldNone
	for kind := range byKind {
		strongest = max(strongest, kind)
	}
	return strongest
}

// ReindexHoldFor reports the strongest cleanup hold on the given collections.
// Flag off it reports none even while a sweep started under an earlier flag on
// still holds one. Won't-fix: RUNTIME_REINDEX_ENABLED is preview-only, removed at GA.
func (db *DB) ReindexHoldFor(collections ...string) ReindexHold {
	if db.config.RuntimeReindexDisabled {
		return ReindexHoldNone
	}
	return db.reindexHolds.HoldFor(collections...)
}
