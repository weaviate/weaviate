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
	"context"
	"time"
)

// unloadPhase values; see LazyLoadShard.unloadPhase.
const (
	unloadPhaseNone int32 = iota
	unloadPhaseDeciding
	unloadPhaseTearing
)

// unloader is the index's unloading policy, nil when unloading is off. Bare
// test wrappers carry no shardOpts, so it reads as off for them.
func (l *LazyLoadShard) unloader() ShardUnloader {
	if l.shardOpts == nil || l.shardOpts.index == nil {
		return nil
	}
	return l.shardOpts.index.Config.ShardUnloader
}

func (l *LazyLoadShard) namespace() string {
	return l.shardOpts.index.namespace
}

// Namespace is the namespace of the wrapper's collection, "" outside namespaces.
func (l *LazyLoadShard) Namespace() string { return l.namespace() }

// LastAccess is the access clock tick of the wrapper's last pin.
func (l *LazyLoadShard) LastAccess() uint32 { return l.lastAccess.Load() }

// UnloadedAt is when TryUnload last closed the shard; zero once it loads again.
func (l *LazyLoadShard) UnloadedAt() time.Time {
	if ns := l.unloadedAt.Load(); ns != 0 {
		return time.Unix(0, ns)
	}
	return time.Time{}
}

// TryMarkUnloadPending claims the wrapper for one unload handoff.
func (l *LazyLoadShard) TryMarkUnloadPending() bool {
	return l.unloadPending.CompareAndSwap(false, true)
}

// ClearUnloadPending releases the claim TryMarkUnloadPending took.
func (l *LazyLoadShard) ClearUnloadPending() { l.unloadPending.Store(false) }

// UnloadPending reports whether a handoff claimed the wrapper.
func (l *LazyLoadShard) UnloadPending() bool { return l.unloadPending.Load() }

// UnloadCandidate is a lock-free pre-check for victim sampling; TryUnload decides.
func (l *LazyLoadShard) UnloadCandidate() bool {
	s := l.currentShard()
	return s != nil && !l.unloadPending.Load() && !s.shut.Load()
}

// recordAccess stamps the current tick; a stalled clock ticker cannot make it stale.
func (l *LazyLoadShard) recordAccess() { l.stampAccess(accessClockReading()) }

// stampAccess raises the clock, then lastAccess, to tick, never back and only
// when they moved, so a hot shard's cache line is written at most once a tick.
func (l *LazyLoadShard) stampAccess(tick uint32) {
	raiseUint32(&accessClock, tick)
	raiseUint32(&l.lastAccess, tick)
}

// markLoaded registers a load that just published shard; callers hold l.mutex.
func (l *LazyLoadShard) markLoaded(u ShardUnloader, shard *Shard) {
	l.unloadedAt.Store(0)
	l.recordAccess()
	u.Registry().add(l, shard)
}

// markUnloaded deregisters the wrapper; callers hold l.mutex. Idempotent.
func (l *LazyLoadShard) markUnloaded() {
	u := l.unloader()
	if u == nil {
		return
	}
	u.Registry().remove(l)
}

// deregisterShard is markUnloaded for shard, which a deferred shutdown just
// shut on the last release and so must not wait on l.mutex. It is a no-op once
// the wrapper is registered for another shard.
func (l *LazyLoadShard) deregisterShard(shard *Shard) {
	if u := l.unloader(); u != nil {
		u.Registry().removeShard(l, shard)
	}
}

// TryUnload tries to close the loaded shard and leave the wrapper cold in the
// shard map, and reports whether it did. Tenant status is untouched. Every lock
// it takes is a try-lock, so it never waits on a request, a RAFT apply or a
// load; a refusal leaves the shard exactly as it was.
//
// Lock order is the documented one: tryEnterRead, shardCreateLocks, l.mutex,
// shutdownLock. shardCreateLocks and tryEnterRead are released once the shard is
// marked shut, so a COLD apply, a drop or an index close waits only on l.mutex
// for this one teardown (LazyLoadShard.Shutdown and drop take it), and a
// request for this shard reloads it once the teardown finished. l.mutex and
// shut keep any reload out meanwhile. It clears the unloadPending claim on return.
func (l *LazyLoadShard) TryUnload(ctx context.Context, req UnloadRequest) bool {
	idx := l.shardOpts.index
	name := l.shardOpts.name
	defer l.ClearUnloadPending()

	if !idx.tryEnterRead() {
		return false
	}
	readEntered := true
	releaseRead := func() {
		if readEntered {
			readEntered = false
			idx.exitRead()
		}
	}
	defer releaseRead()
	if idx.closeRequestedCause() != nil {
		return false
	}

	if !idx.shardCreateLocks.TryLock(name) {
		return false
	}
	createLocked := true
	releaseCreateLock := func() {
		if createLocked {
			createLocked = false
			idx.shardCreateLocks.Unlock(name)
		}
	}
	defer releaseCreateLock()

	if entry, ok := idx.shards.Load(name).(*LazyLoadShard); !ok || entry != l {
		return false
	}

	if !l.mutex.TryLock() {
		return false
	}
	defer l.mutex.Unlock()
	l.unloadPhase.Store(unloadPhaseDeciding)
	defer l.unloadPhase.Store(unloadPhaseNone)
	if l.loadWaiters.Load() > 0 || l.loadBlocked {
		return false
	}
	shard := l.currentShard()
	if shard == nil {
		return false
	}
	if req.IdleTTL > 0 && !AccessClockIdle(AccessClockNow(), l.lastAccess.Load(), req.IdleTTL) {
		return false
	}

	registry := l.unloadRegistry()
	if registry != nil {
		registry.unloading.Add(1)
		defer registry.unloading.Add(-1)
	}

	unloaded, err := shard.tryShutdownIdle(ctx, func() {
		l.unloadPhase.Store(unloadPhaseTearing)
		releaseCreateLock()
		releaseRead()
		if l.unloadMarkedHook != nil {
			l.unloadMarkedHook()
		}
	})
	switch {
	case unloaded:
		l.shard.Store(nil)
		l.markUnloaded()
		l.unloadedAt.Store(time.Now().UnixNano())
	case err != nil:
		// The torn pointer stays published as the last reference to its handles, like shardTeardownError.
		l.markUnloaded()
		idx.logger.WithField("action", "lazy_shard_unload").WithField("shard", name).
			Errorf("teardown failed mid-way; torn shard retained (unavailable until restart): %v", err)
	case shard.shut.Load():
		// A deferred shutdown completed on the published shard; it no longer counts as loaded.
		l.markUnloaded()
	default:
	}
	return unloaded
}

func (l *LazyLoadShard) unloadRegistry() *LoadedShardRegistry {
	if u := l.unloader(); u != nil {
		return u.Registry()
	}
	return nil
}
