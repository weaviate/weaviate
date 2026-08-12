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
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/storagestate"
)

// shardKnownShut reports whether a map entry points at a shard that CLEANLY
// completed a shutdown — the only state a reactivation may evict. Distinct
// from !shardStillAlive: an unloaded LazyLoadShard is the normal steady state
// of every not-yet-loaded shard, and evicting it would race a concurrent
// Load() on the old wrapper into a second instance over the same directory.
// A torn shard (teardownErr set) is NOT evictable either: it may still hold
// open handles/flocks, so the map entry is the only reference keeping them
// reachable — see shardTeardownError.
func shardKnownShut(s ShardLike) bool {
	switch sh := s.(type) {
	case *Shard:
		return sh.shut.Load() && sh.teardownError() == nil
	case *LazyLoadShard:
		sh.mutex.Lock()
		defer sh.mutex.Unlock()
		return sh.loaded && sh.shard.shut.Load() && sh.shard.teardownError() == nil
	default:
		return false
	}
}

// teardownError returns the sticky deep-teardown failure, nil when the shard
// is live or cleanly shut.
func (s *Shard) teardownError() error {
	s.shutdownLock.RLock()
	defer s.shutdownLock.RUnlock()
	if !s.shut.Load() {
		return nil
	}
	return s.teardownErr
}

// shardTeardownError surfaces a map entry's sticky teardown failure (nil for
// live, cleanly-shut, or unloaded entries). Torn shards stay in the map on
// purpose: the entry is the last reference to their possibly-still-open
// handles, and serving the sticky error is cheaper and clearer than letting
// every reactivation re-init into a bucket-registry collision. Heals on
// process restart.
func shardTeardownError(s ShardLike) error {
	switch sh := s.(type) {
	case *Shard:
		return sh.teardownError()
	case *LazyLoadShard:
		sh.mutex.Lock()
		defer sh.mutex.Unlock()
		if !sh.loaded {
			return nil
		}
		return sh.shard.teardownError()
	default:
		return nil
	}
}

// shardStillAlive reports whether a shard instance remains operational after a
// failed Shutdown. performShutdown refuses BEFORE marking the shard shut when
// it is still in use, so a failed close usually leaves a fully live instance —
// the caller must then restore it to the shard map rather than orphan it (an
// orphaned live instance lets a reactivation double-open the same directory).
func shardStillAlive(s ShardLike) bool {
	switch sh := s.(type) {
	case *Shard:
		return !sh.shut.Load()
	case *LazyLoadShard:
		sh.mutex.Lock()
		defer sh.mutex.Unlock()
		return sh.loaded && !sh.shard.shut.Load()
	default:
		// Unknown wrapper: restoring a live shard is the safe direction — a
		// dead map entry fails requests loudly, an orphaned live instance
		// corrupts silently.
		return true
	}
}

// releaseShardLifecycleMetrics gives back the shard-lifecycle count of a shard
// that has left the shard map for good. Shutting a shard down only moves it from
// the loaded to the unloaded bucket — that is the right bookkeeping for a shard
// still on this node, but a shard evicted from the map is on it no longer, and
// nothing downstream would ever take it back out of unloaded.
//
// Call it only once the shard is known to have stayed out of the map: a failed
// shutdown puts live and torn shards back (see restoreShardIfStillAlive), and
// those are still counted. Idempotent for both shard kinds, so a later drop of
// the same instance does not double-decrement.
func releaseShardLifecycleMetrics(s ShardLike) {
	switch sh := s.(type) {
	case *Shard:
		sh.releaseShardMetrics()
	case *LazyLoadShard:
		sh.releaseShardMetrics()
	}
}

func (s *Shard) Shutdown(ctx context.Context) (err error) {
	s.shutdownRequested.Store(true)
	var lastAttemptErr error
	err = backoff.Retry(func() error {
		// this retry to make sure it's retried in case
		// the performShutdown() returned shard still in use
		lastAttemptErr = s.performShutdown(ctx)
		if errors.Is(lastAttemptErr, errTeardownFailed) {
			// Sticky: no amount of retrying un-tears the shard, and callers
			// hold shardCreateLocks across this call — fail immediately
			// instead of burning the full backoff window.
			return backoff.Permanent(lastAttemptErr)
		}
		return lastAttemptErr
	}, backoff.WithContext(backoff.WithMaxRetries(
		// this will try with max 2 seconds could be configurable later on
		backoff.NewConstantBackOff(200*time.Millisecond), 10), ctx))
	// ctx cancellation makes backoff return ctx.Err(), swallowing the attempt
	// error — a cancelled wait on a still-in-use shard is still the in-use
	// case (refs pending, releases complete the shutdown), not an abort.
	stillInUse := errors.Is(err, errShardStillInUse) || errors.Is(lastAttemptErr, errShardStillInUse)
	if err != nil && !errors.Is(err, errAlreadyShutdown) && !stillInUse {
		// Aborted shutdown: clear the request flag, or the shard callers
		// restore (restoreShardIfStillAlive) stays gated by preventShutdown
		// and refCountSub self-completes the shutdown once refs drain. If
		// that deferred shutdown wins the race, the shard is marked shut and
		// shardStillAlive refuses the restore.
		//
		// Still-in-use is the one abort that must KEEP the flag: pending
		// refs exist by definition, and the last release completing the
		// shutdown is the designed eventual-shutdown contract
		// (TestShardShutdownWhenIdleEventually pins it).
		s.shutdownRequested.Store(false)
	}
	return err
}

// shutdownOrRestoreShard closes a shard already removed from the shard map
// and, when the close fails with the instance still live, puts it back —
// leaving a live instance out of the map lets a later (re)load double-open
// the directory. Callers hold the shard's create lock and classify
// errAlreadyShutdown themselves (terminal, not a failure).
//
// It is also where a shard that stays evicted gives back its shard-lifecycle
// count, because this is the only place that knows which way that went. Doing it
// here rather than in each caller is deliberate: the eviction sites are spread
// across tenant deactivation, replica teardown and index reconciliation, and
// every one of them that forgot would leak a shard in shards_unloaded until the
// process restarted.
func shutdownOrRestoreShard(ctx context.Context, shards *shardMap, name string, shard ShardLike, logger logrus.FieldLogger) error {
	// Deferred rather than called on each outcome so a panic in Shutdown is
	// covered too: the caller has already removed the shard from the map, and a
	// panic means the restore below never ran, so the shard is evicted for good.
	// Callers run inside error-group wrappers that recover, so the process
	// carries on and the stranded count would outlive the shard.
	restored := false
	defer func() {
		if !restored {
			releaseShardLifecycleMetrics(shard)
		}
	}()

	err := shard.Shutdown(ctx)
	if err == nil || errors.Is(err, errAlreadyShutdown) {
		return err
	}
	if restoreShardIfStillAlive(shards, name, shard) {
		restored = true
		if terr := shardTeardownError(shard); terr != nil {
			logger.WithField("action", "shard_shutdown").
				WithField("shard", name).
				Errorf("teardown failed mid-way; torn shard retained in the map (holds its leaked handles, unavailable until restart): %v", err)
		} else {
			logger.WithField("action", "shard_shutdown").
				WithField("shard", name).
				Errorf("shutdown failed; live shard restored to the active map to prevent a duplicate instance: %v", err)
		}
		return err
	}
	// Not restored and not torn: the shard is CLEANLY shut — the concurrent
	// deferred completion (last ref release) won the race while this attempt
	// timed out or saw it still in use. The attempt error is stale; the
	// outcome the caller asked for happened. Report it as the benign
	// already-shut case, not a failure (a cold-tenant batch would otherwise
	// fail whole on one racy tenant).
	return errAlreadyShutdown
}

// restoreShardIfStillAlive puts a shard whose Shutdown failed back into the
// shard map (under the caller's shardCreateLock). Two cases restore: a live
// instance (a failed close usually means "still in use" — leaving it out of
// the map would let a later (re)load double-open the same directory), and a
// TORN one (deep teardown failure: the entry is the last reference to its
// possibly-still-open handles, and it fails fast with the sticky teardownErr
// — see shardTeardownError). Only a cleanly-shut shard is left out.
func restoreShardIfStillAlive(shards *shardMap, name string, shard ShardLike) bool {
	if !shardStillAlive(shard) && shardTeardownError(shard) == nil {
		return false
	}
	shards.Store(name, shard)
	return true
}

/*

	batch
		shut
		false
			in_use ++
			defer in_use --
		true
			fail request

	shutdown
		loop + time:
		if shut == true
			fail request
		in_use == 0 && shut == false
			shut = true

*/
// Shutdown needs to be idempotent, so it can also deal with a partial
// initialization. In some parts, it relies on the underlying structs to have
// idempotent Shutdown methods. In other parts, it explicitly checks if a
// component was initialized. If not, it turns it into a noop to prevent
// blocking.
func (s *Shard) performShutdown(ctx context.Context) (err error) {
	s.shutdownLock.Lock()
	defer s.shutdownLock.Unlock()
	defer func() {
		// A teardown that fails AFTER the shut mark must stay visible: the
		// idempotent short-circuit below would otherwise convert the retry
		// into a silent nil, callers would treat the partially-torn shard as
		// cleanly closed, and its still-open buckets (whose registry entries
		// never cleared) would fail every future re-init of the tenant.
		if err != nil && s.shut.Load() && s.teardownErr == nil {
			s.teardownErr = err
		}
	}()

	if s.shut.Load() {
		s.shutdownRequested.Store(false)
		if s.teardownErr != nil {
			return fmt.Errorf("%w: %w", errTeardownFailed, s.teardownErr)
		}
		s.index.logger.
			WithField("action", "shutdown").
			Debugf("shard %q is already shut down", s.name)
			// shutdown is idempotent
		return nil
	}
	if s.inUseCounter.Load() > 0 {
		s.index.logger.
			WithField("action", "shutdown").
			Debugf("shard %q is still in use", s.name)
		return fmt.Errorf("shard %q: %w", s.name, errShardStillInUse)
	}
	s.shut.Store(true)
	s.shutdownRequested.Store(false)
	s.shutCtxCancel(fmt.Errorf("shutdown %q", s.ID()))

	// Track shard unloading: loaded -> unloading
	// Only update metrics if the shard was properly registered (prevents double-counting
	// during partial initialization cleanup)
	if s.metricsRegistered.Load() {
		s.metrics.baseMetrics.StartUnloadingShard()
		// Deferred, not tail-called: the teardown below routes its failures into
		// an error compounder rather than returning, but a panic in any of the
		// closes would unwind past a plain statement and pin shards_unloading at
		// +1 with shards_loaded already decremented.
		defer func() {
			s.metrics.baseMetrics.FinishUnloadingShard()
			// The shard now sits in the unloaded bucket, so a drop that follows
			// must release that one rather than decrementing loaded a second time.
			s.metricsUnloaded.Store(true)
		}()
	}

	// Release the per-status gauge from here on out, not at the end: everything
	// below reports through the error compounder rather than returning, but a
	// teardown that panics or grows an early return later must still not strand
	// this shard's bucket. Deferred after the point of no return, so the
	// still-in-use rejection above (which leaves the shard live) never reaches it.
	//
	// This releases on shutdown rather than on eviction, so a shard that is
	// restored to the map by shutdownOrRestoreShard — a torn one holding its
	// leaked handles, or one whose deferred ref-drain completed it in place —
	// stops being counted while it is technically still in the map. That is
	// deliberate: the gauge counts shards this node can serve, and a shut shard
	// serves nothing but errAlreadyShutdown / errTeardownFailed. Tying the
	// release to eviction instead would mean repeating it at every site that
	// evicts a known-shut entry (both re-init paths, each drop path), which is
	// the scattered bookkeeping that leaked in the first place.
	defer s.setCountedStatus("")

	start := time.Now()
	defer func() {
		s.index.metrics.ObserveUpdateShardStatus(storagestate.StatusShutdown.String(), time.Since(start))
	}()

	s.reindexer.Stop(s, fmt.Errorf("shard shutdown"))

	s.haltForTransferMux.Lock()
	// also drops an already-fired monitor waiting on the mux, so it can't resume mid-teardown.
	s.mayStopInactivityMonitoring()
	s.haltForTransferMux.Unlock()

	ec := errorcompounder.New()

	err = s.GetPropertyLengthTracker().Close()
	ec.AddWrapf(err, "close prop length tracker")

	// unregister all callbacks at once, in parallel
	err = cyclemanager.NewCombinedCallbackCtrl(0, s.index.logger,
		s.cycleCallbacks.compactionCallbacksCtrl,
		s.cycleCallbacks.compactionAuxCallbacksCtrl,
		s.cycleCallbacks.flushCallbacksCtrl,
		s.cycleCallbacks.vectorCombinedCallbacksCtrl,
		s.cycleCallbacks.geoPropsCombinedCallbacksCtrl,
	).Unregister(ctx)
	ec.Add(err)

	s.mayStopAsyncReplication()

	_ = s.ForEachVectorQueue(func(targetVector string, queue *VectorIndexQueue) error {
		if err = queue.Flush(); err != nil {
			ec.Add(fmt.Errorf("flush vector index queue commitlog of vector %q: %w", targetVector, err))
		}

		if err = queue.Close(ctx); err != nil {
			ec.Add(fmt.Errorf("shut down vector index queue of vector %q: %w", targetVector, err))
		}

		return nil
	})

	_ = s.ForEachGeoQueue(func(propName string, queue *VectorIndexQueue) error {
		if err = queue.Flush(); err != nil {
			ec.Add(fmt.Errorf("flush geo index queue commitlog of prop %q: %w", propName, err))
		}

		if err = queue.Close(ctx); err != nil {
			ec.Add(fmt.Errorf("shut down geo index queue of prop %q: %w", propName, err))
		}

		return nil
	})

	s.propertyIndicesLock.RLock()
	err = s.propertyIndices.ShutdownGeoIndices(ctx)
	s.propertyIndicesLock.RUnlock()
	ec.AddWrapf(err, "shutdown geo property indices")

	_ = s.ForEachVectorIndex(func(targetVector string, index VectorIndex) error {
		// to ensure that all commitlog entries are written to disk.
		// otherwise in some cases the tombstone cleanup process'
		// 'RemoveTombstone' entry is not picked up on restarts
		// resulting in perpetually attempting to remove a tombstone
		// which doesn't actually exist anymore
		if err = index.Flush(); err != nil {
			ec.Add(fmt.Errorf("flush vector index commitlog of vector %q: %w", targetVector, err))
		}

		if err = index.Shutdown(ctx); err != nil {
			ec.Add(fmt.Errorf("shut down vector index of vector %q: %w", targetVector, err))
		}

		return nil
	})

	if s.store != nil {
		s.UpdateStatus(storagestate.StatusShutdown.String(), statusReasonShutdown)

		// store would be nil if loading the objects bucket failed, as we would
		// only return the store on success from s.initLSMStore()
		err = s.store.Shutdown(ctx)
		ec.AddWrapf(err, "stop lsmkv store")
	}

	if s.dynamicVectorIndexDB != nil {
		err = s.dynamicVectorIndexDB.Close()
		ec.AddWrapf(err, "stop dynamic vector index db")
	}

	return ec.ToError()
}

// drainRefsForDrop blocks new pins and waits for the in-flight ones to finish,
// so a drop does not tear the store down underneath a running request.
// Note: this will keep drainRefsForDrop running for 30 seconds.
func (s *Shard) drainRefsForDrop() error {
	s.dropRequested.Store(true)

	return backoff.Retry(func() error {
		s.shutdownLock.Lock()
		defer s.shutdownLock.Unlock()

		if inUse := s.inUseCounter.Load(); inUse > 0 {
			return fmt.Errorf("shard %q holds %d reference(s): %w", s.name, inUse, errShardStillInUse)
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewConstantBackOff(300*time.Millisecond), 100)) // 30 seconds
}

const msgReleasedMoreThanOnce = "shard reference released more than once per acquire"

func (s *Shard) preventShutdown() (release func(), err error) {
	if s.shutdownRequested.Load() {
		return func() {}, errShutdownInProgress
	}
	s.shutdownLock.RLock()
	defer s.shutdownLock.RUnlock()

	if s.dropRequested.Load() {
		return func() {}, errDropInProgress
	}

	if s.shut.Load() {
		return func() {}, errAlreadyShutdown
	}

	s.refCountAdd()
	// Releasing more than once per acquire would drive the counter negative and
	// disable the in-use guard in performShutdown, so absorb it and report it.
	var released atomic.Bool
	return func() {
		if !released.CompareAndSwap(false, true) {
			s.index.logger.
				WithField("action", "shard_ref_count").
				WithField("shard", s.name).
				Error(msgReleasedMoreThanOnce)
			return
		}
		s.refCountSub()
	}, nil
}

func (s *Shard) refCountAdd() {
	s.inUseCounter.Add(1)
}

// refCountSub, and hence preventShutdown's release func, must not be called
// while holding s.shutdownLock: performShutdown takes the write lock.
func (s *Shard) refCountSub() {
	// a shutdown requested while the shard was in use runs once the last
	// reference drops
	if s.inUseCounter.Add(-1) == 0 && s.shutdownRequested.Load() {
		if err := s.performShutdown(context.TODO()); err != nil {
			// No caller to receive this: the deferred completion runs on
			// whichever request releases the last ref. teardownErr keeps deep
			// failures sticky for later Shutdown/reactivation attempts, but
			// the failure must be visible when it happens, not only then.
			s.index.logger.WithField("action", "shard_shutdown").
				WithField("shard", s.ID()).
				Errorf("deferred shutdown on last reference release failed: %v", err)
		}
	}
}

// // cleanupPartialInit is called when the shard was only partially initialized.
// // Internally it just uses [Shutdown], but also adds some logging.
// func (s *Shard) cleanupPartialInit(ctx context.Context) {
// 	log := s.index.logger.WithField("action", "cleanup_partial_initialization")
// 	if err := s.Shutdown(ctx); err != nil {
// 		log.WithError(err).Error("failed to shutdown store")
// 	}

// 	log.Debug("successfully cleaned up partially initialized shard")
// }

// func (s *Shard) NotifyReady() {
// 	s.initStatus()
// 	s.index.logger.
// 		WithField("action", "startup").
// 		Debugf("shard=%s is ready", s.name)
// }
