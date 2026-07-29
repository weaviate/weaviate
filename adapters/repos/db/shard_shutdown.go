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

	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/storagestate"
)

// shardKnownShut reports whether a map entry points at a shard that already
// COMPLETED a shutdown — the only state a reactivation may evict. Distinct
// from !shardStillAlive: an unloaded LazyLoadShard is the normal steady state
// of every not-yet-loaded shard, and evicting it would race a concurrent
// Load() on the old wrapper into a second instance over the same directory.
func shardKnownShut(s ShardLike) bool {
	switch sh := s.(type) {
	case *Shard:
		return sh.shut.Load()
	case *LazyLoadShard:
		sh.mutex.Lock()
		defer sh.mutex.Unlock()
		return sh.loaded && sh.shard.shut.Load()
	default:
		return false
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

func (s *Shard) Shutdown(ctx context.Context) (err error) {
	s.shutdownRequested.Store(true)
	var lastAttemptErr error
	err = backoff.Retry(func() error {
		// this retry to make sure it's retried in case
		// the performShutdown() returned shard still in use
		lastAttemptErr = s.performShutdown(ctx)
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

// restoreShardIfStillAlive puts a shard whose Shutdown failed back into the
// shard map (under the caller's shardCreateLock): a failed close usually
// means "still in use" — leaving the live instance out of the map would let
// a later (re)load double-open the same directory. Deep teardown failures
// never restore: the shard is already marked shut (shardStillAlive false) and
// the sticky teardownErr keeps the failure visible on every retry.
func restoreShardIfStillAlive(shards *shardMap, name string, shard ShardLike) bool {
	if !shardStillAlive(shard) {
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
			return fmt.Errorf("previous shutdown attempt failed mid-teardown: %w", s.teardownErr)
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
	}

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

	// Track shard unloaded: unloading -> unloaded
	if s.metricsRegistered.Load() {
		s.metrics.baseMetrics.FinishUnloadingShard()
	}

	return ec.ToError()
}

const msgReleasedMoreThanOnce = "shard reference released more than once per acquire"

func (s *Shard) preventShutdown() (release func(), err error) {
	if s.shutdownRequested.Load() {
		return func() {}, errShutdownInProgress
	}
	s.shutdownLock.RLock()
	defer s.shutdownLock.RUnlock()

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
		s.performShutdown(context.TODO())
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
