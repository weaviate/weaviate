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

package cluster

import (
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// dbLoader owns the background shard load. Its zero value is idle.
//
// While a load runs, commands still apply to the schema in full; only their DB
// writes are deferred, and the loader applies those on a later pass.
type dbLoader struct {
	// Read by every Apply, so it stays outside the lock. A load never restarts
	// once finished, keeping the common path lock-free.
	inFlight atomic.Bool

	mu      sync.Mutex
	started bool
	stale   bool // a command deferred a DB write during this pass
	// Classes a deferred command deleted, and whether each had frozen tenants.
	// A pass rebuilds what the schema still lists, so a deferred addition needs
	// no record; a deletion does, being absent from that list.
	deletes map[string]bool

	wg sync.WaitGroup
}

// start runs load in the background unless a load has already run, and reports
// whether it did. Scheduling lives here so that load itself has one body and
// the call site is what says how it runs.
func (l *dbLoader) start(load func(), log logrus.FieldLogger) bool {
	if !l.begin() {
		return false
	}
	enterrors.GoWrapper(func() {
		defer l.wg.Done()
		load()
	}, log)
	return true
}

// begin reports whether this call owns the load. One-shot: inFlight clears just
// before dbLoaded is published and Apply's guard is !dbLoaded, so a command
// landing in between would otherwise start a second concurrent loader.
func (l *dbLoader) begin() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.started {
		return false
	}
	l.started = true
	l.inFlight.Store(true)
	l.wg.Add(1)
	return true
}

// deferWrite reports whether a command must skip its DB write because a load is
// mid-pass, recording deletedClass so the loader can finish the job.
//
// The record shares the lock with stale: taken separately, a class recorded
// just after the loader drained would have no pass left to act on it, and its
// shards would stay on disk for good.
func (l *dbLoader) deferWrite(deletedClass string, hasFrozen bool) bool {
	if !l.inFlight.Load() {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.inFlight.Load() {
		return false
	}
	l.stale = true
	if deletedClass != "" {
		if l.deletes == nil {
			l.deletes = map[string]bool{}
		}
		// OR, never overwrite: a class deleted frozen, re-added, then deleted
		// hot still has the first incarnation's cloud data to clean up.
		l.deletes[deletedClass] = l.deletes[deletedClass] || hasFrozen
	}
	return true
}

// finish ends the load unless a command deferred a write during the pass, in
// which case the loader owes another and gets the classes to drop first.
func (l *dbLoader) finish() (deletes map[string]bool, done bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.stale {
		l.inFlight.Store(false)
		return nil, true
	}
	l.stale = false
	deletes, l.deletes = l.deletes, nil
	return deletes, false
}
