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

// Refcounted, because overlapping teardowns on one collection would let the
// first to finish reopen the gate under the second. Collection-wide, because
// the sweep it guards walks every local shard. It covers a sweep from the
// moment it is taken and no earlier: a terminal task whose drain times out is
// never swept, and so never held.
type ReindexHoldRegistry struct {
	mu    sync.RWMutex
	holds map[string]map[ReindexHold]int
}

// Idempotent: releasing twice must not drop another hold.
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

// Deferred, so a panicking sweep cannot hold the collection until restart.
func (r *ReindexHoldRegistry) Hold(collection string, kind ReindexHold, fn func()) {
	release := r.acquire(collection, kind)
	defer release()
	fn()
}

// No collection means every one of them. The highest value wins so map order
// cannot decide the answer.
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

// The registry is a field on this same DB, so there is nothing to install
// and no window in which the gates read it before it exists.
func (db *DB) ReindexHoldFor(collections ...string) ReindexHold {
	if db.config.RuntimeReindexDisabled {
		return ReindexHoldNone
	}
	return db.reindexHolds.HoldFor(collections...)
}
