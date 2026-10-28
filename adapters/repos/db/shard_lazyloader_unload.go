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

	"github.com/weaviate/weaviate/entities/storagestate"
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

// LastAccess is the [AccessClockNow] reading of the wrapper's last pin.
func (l *LazyLoadShard) LastAccess() uint32 { return l.lastAccess.Load() }

// LoadedAt is when the current load finished; zero while cold.
func (l *LazyLoadShard) LoadedAt() time.Time {
	if ns := l.loadedAt.Load(); ns != 0 {
		return time.Unix(0, ns)
	}
	return time.Time{}
}

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
	return s != nil && !l.unloadPending.Load() &&
		s.inUseCounter.Load() == 0 && s.haltForTransferCount.Load() == 0 &&
		!s.shut.Load() && !s.shutdownRequested.Load() && !s.dropRequested.Load() &&
		s.changeLogs.Load().Len() == 0
}

// recordAccess stores the access clock only when it moved, so a hot shard's
// cache line is written at most once a tick.
func (l *LazyLoadShard) recordAccess() {
	if now := accessClock.Load(); l.lastAccess.Load() != now {
		l.lastAccess.Store(now)
	}
}

// markLoaded registers a load that just published shard; callers hold l.mutex.
func (l *LazyLoadShard) markLoaded(u ShardUnloader) {
	l.loadedAt.Store(time.Now().UnixNano())
	l.unloadedAt.Store(0)
	l.recordAccess()
	u.Registry().add(l)
}

// markUnloaded deregisters the wrapper; callers hold l.mutex. Idempotent.
func (l *LazyLoadShard) markUnloaded() {
	u := l.unloader()
	if u == nil {
		return
	}
	l.loadedAt.Store(0)
	u.Registry().remove(l)
}

// deregister is markUnloaded for a shard a deferred shutdown just shut on the
// last release, which must not wait on l.mutex. It cannot interleave with an
// add: the shut shard stays published, so this wrapper never loads again.
func (l *LazyLoadShard) deregister() { l.markUnloaded() }

// TryUnload closes the loaded shard and leaves the wrapper cold in the shard
// map. Tenant status is untouched. Every lock it takes is a try-lock, so it
// never waits on a request, a RAFT apply or a load; any refusal leaves the
// shard exactly as it was. The reindex clause reads the caller's snapshot
// (req.Reindex), never DTM: that list may reach the leader.
//
// Lock order is the documented one: enterRead, shardCreateLocks, l.mutex,
// shutdownLock. shardCreateLocks is released once the shard is marked shut, so
// a COLD apply or a drop waits only on l.mutex for this one teardown, and a
// request for this shard reloads it once the teardown finished. It clears the
// unloadPending claim on return.
func (l *LazyLoadShard) TryUnload(ctx context.Context, req UnloadRequest) UnloadOutcome {
	idx := l.shardOpts.index
	name := l.shardOpts.name
	defer l.ClearUnloadPending()

	if err := idx.enterRead(); err != nil {
		return UnloadOutcomeIndexClosing
	}
	defer idx.exitRead()
	if idx.closeRequestedCause() != nil {
		return UnloadOutcomeIndexClosing
	}
	if idx.db != nil && !idx.db.config.RuntimeReindexDisabled {
		if o := req.Reindex.blocks(idx.Config.ClassName.String(), name); o != "" {
			return o
		}
	}

	if !idx.shardCreateLocks.TryLock(name) {
		return UnloadOutcomeLockBusy
	}
	createLocked := true
	releaseCreateLock := func() {
		if createLocked {
			createLocked = false
			idx.shardCreateLocks.Unlock(name)
		}
	}
	defer releaseCreateLock()

	switch entry := idx.shards.Load(name).(type) {
	case *LazyLoadShard:
		if entry != l {
			return UnloadOutcomeNotInMap
		}
	case *RecoveringShard:
		return UnloadOutcomeRecovering
	default:
		return UnloadOutcomeNotInMap
	}

	if !l.mutex.TryLock() {
		return UnloadOutcomeLockBusy
	}
	defer l.mutex.Unlock()
	l.unloadPhase.Store(unloadPhaseDeciding)
	defer l.unloadPhase.Store(unloadPhaseNone)
	if l.loadWaiters.Load() > 0 {
		return UnloadOutcomeLockBusy
	}

	if l.loadBlocked {
		return UnloadOutcomeRecovering
	}
	shard := l.currentShard()
	if shard == nil {
		return UnloadOutcomeAlreadyCold
	}
	if o := l.unloadBlocker(shard, req); o != "" {
		return o
	}

	registry := l.unloadRegistry()
	if registry != nil {
		registry.unloading.Add(1)
		defer registry.unloading.Add(-1)
	}

	outcome, err := shard.tryShutdownIdle(ctx, func() {
		l.unloadPhase.Store(unloadPhaseTearing)
		releaseCreateLock()
		if l.unloadMarkedHook != nil {
			l.unloadMarkedHook()
		}
	})
	switch outcome {
	case UnloadOutcomeOK:
		l.shard.Store(nil)
		l.markUnloaded()
		l.unloadedAt.Store(time.Now().UnixNano())
	case UnloadOutcomeTorn:
		// The torn pointer stays published as the last reference to its handles, like shardTeardownError.
		l.markUnloaded()
		if err != nil {
			idx.logger.WithField("action", "lazy_shard_unload").WithField("shard", name).
				Errorf("teardown failed mid-way; torn shard retained (unavailable until restart): %v", err)
		}
	case UnloadOutcomeAlreadyCold:
		// A deferred shutdown completed on the published shard; it no longer counts as loaded.
		l.markUnloaded()
	default:
	}
	return outcome
}

func (l *LazyLoadShard) unloadRegistry() *LoadedShardRegistry {
	if u := l.unloader(); u != nil {
		return u.Registry()
	}
	return nil
}

// unloadBlocker evaluates every clause before the teardown; callers hold
// shardCreateLocks and l.mutex.
func (l *LazyLoadShard) unloadBlocker(s *Shard, req UnloadRequest) UnloadOutcome {
	if s.shut.Load() {
		return ""
	}
	if o := s.idleUnloadBlocker(); o != "" {
		return o
	}
	if o := s.vectorOpUnloadBlocker(); o != "" {
		return o
	}
	if req.Mode == UnloadModeBackground {
		return l.softUnloadBlocker(s, req)
	}
	return ""
}

// softUnloadBlocker holds the clauses a background unload honours and a make-room unload ignores.
func (l *LazyLoadShard) softUnloadBlocker(s *Shard, req UnloadRequest) UnloadOutcome {
	if req.MinResidency > 0 {
		if loadedAt := l.loadedAt.Load(); loadedAt != 0 && time.Since(time.Unix(0, loadedAt)) < req.MinResidency {
			return UnloadOutcomeMinResidency
		}
	}
	if req.IdleTTL > 0 {
		now, last := AccessClockNow(), l.lastAccess.Load()
		if now < last || time.Duration(now-last)*time.Second < req.IdleTTL {
			return UnloadOutcomeNotIdle
		}
	}
	if o := s.queueUnloadBlocker(); o != "" {
		return o
	}
	if !s.asyncReplicationRWMux.TryRLock() {
		return UnloadOutcomeLockBusy
	}
	initialising := s.hashtree != nil && !s.hashtreeFullyInitialized
	s.asyncReplicationRWMux.RUnlock()
	if initialising {
		return UnloadOutcomeHashtreeInit
	}
	return ""
}

// tryUpgradableIndexer is a vector index whose upgrade probe can refuse instead
// of waiting on the index lock.
type tryUpgradableIndexer interface {
	TryUpgradeInProgress() (inProgress, ok bool)
}

// vectorOpUnloadBlocker is structuralVectorOpInFlight with try-locks only.
func (s *Shard) vectorOpUnloadBlocker() UnloadOutcome {
	outcome := UnloadOutcome("")
	acquired, err := s.vectors.TryForEach(func(_ string, index VectorIndex, _ *VectorIndexQueue) error {
		if outcome != "" {
			return nil
		}
		switch vi := index.(type) {
		case tryUpgradableIndexer:
			if busy, ok := vi.TryUpgradeInProgress(); !ok {
				outcome = UnloadOutcomeLockBusy
			} else if busy {
				outcome = UnloadOutcomeVectorOp
			}
		case upgradableIndexer:
			if vi.UpgradeInProgress() {
				outcome = UnloadOutcomeVectorOp
			}
		default:
		}
		return nil
	})
	if !acquired || err != nil {
		return UnloadOutcomeLockBusy
	}
	return outcome
}

// queueUnloadBlocker refuses while a vector or geo queue holds work or is locked.
func (s *Shard) queueUnloadBlocker() UnloadOutcome {
	outcome := UnloadOutcome("")
	check := func(q *VectorIndexQueue) {
		if outcome != "" || q == nil {
			return
		}
		if size, ok := q.TrySize(); !ok {
			outcome = UnloadOutcomeLockBusy
		} else if size > 0 {
			outcome = UnloadOutcomeQueueNotEmpty
		}
	}
	acquired, err := s.vectors.TryForEach(func(_ string, _ VectorIndex, q *VectorIndexQueue) error {
		check(q)
		return nil
	})
	if !acquired || err != nil {
		return UnloadOutcomeLockBusy
	}
	if outcome != "" {
		return outcome
	}
	if !s.propertyIndicesLock.TryRLock() {
		return UnloadOutcomeLockBusy
	}
	defer s.propertyIndicesLock.RUnlock()
	for _, q := range s.geoQueues {
		check(q)
	}
	return outcome
}

// idleUnloadBlocker is the in-memory part of the predicate. It uses atomics and
// try-locks only, so tryShutdownIdle re-runs it under shutdownLock, which every
// pin takes: a pin or any state a pinned caller left behind is then seen.
func (s *Shard) idleUnloadBlocker() UnloadOutcome {
	if s.inUseCounter.Load() != 0 || s.shutdownRequested.Load() || s.dropRequested.Load() {
		return UnloadOutcomeInUse
	}
	if s.haltForTransferCount.Load() > 0 {
		return UnloadOutcomeHalted
	}
	if !s.replicationMap.TryLock() {
		return UnloadOutcomeLockBusy
	}
	pending := len(s.replicationMap.Tasks) > 0
	s.replicationMap.Unlock()
	if pending {
		return UnloadOutcomePending2PC
	}
	if s.changeLogs.Load().Len() > 0 {
		return UnloadOutcomeChangeLog
	}
	if !s.asyncReplicationRWMux.TryRLock() {
		return UnloadOutcomeLockBusy
	}
	overrides := len(s.targetNodeOverrides) > 0
	checkpoint := s.asyncCheckpointHashtree != nil
	s.asyncReplicationRWMux.RUnlock()
	if overrides {
		return UnloadOutcomeAsyncOverride
	}
	if checkpoint {
		return UnloadOutcomeAsyncCheckpoint
	}
	if !s.statusLock.TryRLock() {
		return UnloadOutcomeLockBusy
	}
	manual := s.status.Status == storagestate.StatusReadOnly && s.status.Reason == statusReasonManualUpdate
	s.statusLock.RUnlock()
	if manual {
		return UnloadOutcomeManualReadOnly
	}
	return ""
}
