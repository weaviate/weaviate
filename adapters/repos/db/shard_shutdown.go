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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storagestate"
)

// Shutdown drains in-flight users, then tears the in-memory state down, leaving
// everything on disk. It is idempotent. On drain timeout it errors but leaves
// the teardown pending for the last user to release (see [Shard.releaseRef]),
// so a busy shard cannot be unmapped yet never torn down.
func (s *Shard) Shutdown(ctx context.Context) error {
	s.lifecycle.requestTeardown(shardUnloading)

	claimed, err := s.awaitTeardown(ctx, shardUnloading)
	if err != nil {
		return err
	}
	if !claimed {
		s.index.logger.
			WithField("action", "shutdown").
			Debugf("shard %q is already shut down", s.name)
		return nil
	}

	return s.performShutdown(ctx)
}

// awaitTeardown polls until this caller owns the teardown for kind; claimed ==
// false with a nil error means it already ran. Polling rather than parking keeps
// the lifecycle one atomic word with no companion primitive to sync.
func (s *Shard) awaitTeardown(ctx context.Context, kind shardPhase) (claimed bool, err error) {
	err = backoff.Retry(func() error {
		var current shardPhase
		claimed, current = s.lifecycle.claimTeardown(kind)
		if claimed || current == shardClosed {
			return nil
		}
		s.index.logger.
			WithField("action", "shutdown").
			Debugf("shard %q is still in use", s.name)
		return fmt.Errorf("shard %q is still in use", s.name)
	}, backoff.WithContext(backoff.WithMaxRetries(
		// this will try with max 2 seconds could be configurable later on
		backoff.NewConstantBackOff(200*time.Millisecond), 10), ctx))
	return claimed, err
}

// performShutdown runs the unload teardown body. The caller must own the
// teardown via [Shard.awaitTeardown], which is what guarantees no user is in
// flight and no other goroutine is here.
//
// It needs to be idempotent, so it can also deal with a partial initialization.
// In some parts, it relies on the underlying structs to have idempotent Shutdown
// methods. In other parts, it explicitly checks if a component was initialized.
// If not, it turns it into a noop to prevent blocking.
func (s *Shard) performShutdown(ctx context.Context) (err error) {
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

	// Safe: the teardown below collects errors from parallel goroutines.
	ec := errorcompounder.NewSafe()

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

	// A shard can carry many named vectors, and each flush and close is its own
	// disk round-trip; run them concurrently. Errors go to ec rather than the
	// group, so one failing queue does not skip the rest.
	//
	// Note the closures use their own err: the outer one is this function's named
	// return, and writing it from several goroutines would be a data race.
	queueEg := enterrors.NewErrorGroupWrapper(s.index.logger)
	queueEg.SetLimit(_NUMCPU)

	_ = s.ForEachVectorQueue(func(targetVector string, queue *VectorIndexQueue) error {
		queueEg.Go(func() error {
			if err := queue.Flush(); err != nil {
				ec.Add(fmt.Errorf("flush vector index queue commitlog of vector %q: %w", targetVector, err))
			}

			if err := queue.Close(ctx); err != nil {
				ec.Add(fmt.Errorf("shut down vector index queue of vector %q: %w", targetVector, err))
			}

			return nil
		})
		return nil
	})

	_ = s.ForEachGeoQueue(func(propName string, queue *VectorIndexQueue) error {
		queueEg.Go(func() error {
			if err := queue.Flush(); err != nil {
				ec.Add(fmt.Errorf("flush geo index queue commitlog of prop %q: %w", propName, err))
			}

			if err := queue.Close(ctx); err != nil {
				ec.Add(fmt.Errorf("shut down geo index queue of prop %q: %w", propName, err))
			}

			return nil
		})
		return nil
	})

	// Every queue must be closed before any vector index is torn down: a queue
	// still draining writes into the index it feeds.
	_ = queueEg.Wait()

	s.propertyIndicesLock.RLock()
	err = s.propertyIndices.ShutdownGeoIndices(ctx)
	s.propertyIndicesLock.RUnlock()
	ec.AddWrapf(err, "shutdown geo property indices")

	indexEg := enterrors.NewErrorGroupWrapper(s.index.logger)
	indexEg.SetLimit(_NUMCPU)

	_ = s.ForEachVectorIndex(func(targetVector string, index VectorIndex) error {
		indexEg.Go(func() error {
			// to ensure that all commitlog entries are written to disk.
			// otherwise in some cases the tombstone cleanup process'
			// 'RemoveTombstone' entry is not picked up on restarts
			// resulting in perpetually attempting to remove a tombstone
			// which doesn't actually exist anymore
			if err := index.Flush(); err != nil {
				ec.Add(fmt.Errorf("flush vector index commitlog of vector %q: %w", targetVector, err))
			}

			if err := index.Shutdown(ctx); err != nil {
				ec.Add(fmt.Errorf("shut down vector index of vector %q: %w", targetVector, err))
			}

			return nil
		})
		return nil
	})
	_ = indexEg.Wait()

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

// preventShutdown blocks teardown — unload and drop alike — until release is
// called, so the holder is guaranteed the store, buckets and vector indexes stay
// alive. release is never nil and must be called exactly once.
func (s *Shard) preventShutdown() (release func(), err error) {
	if err := s.lifecycle.acquire(); err != nil {
		return func() {}, err
	}
	// Per closure, not just against the count reaching zero: with several holders
	// a double release would otherwise consume someone else's reference.
	var released atomic.Bool
	return func() {
		if !released.CompareAndSwap(false, true) {
			s.index.logger.
				WithField("action", "shard_ref_count").
				WithField("shard", s.name).
				Error(msgReleasedMoreThanOnce)
			return
		}
		s.releaseRef()
	}, nil
}

// releaseRef returns one [Shard.preventShutdown] reference and, if it was the
// last holding up a pending unload, completes it. A pending drop is left to
// [Shard.drop]'s caller, which holds the keepFiles argument this path lacks.
func (s *Shard) releaseRef() {
	drained, err := s.lifecycle.release()
	if err != nil {
		s.index.logger.
			WithField("action", "shard_ref_count").
			WithField("shard", s.name).
			Errorf("shard reference bookkeeping: %v", err)
		return
	}
	if !drained {
		return
	}

	claimed, _ := s.lifecycle.claimTeardown(shardUnloading)
	if !claimed {
		return
	}
	if err := s.performShutdown(context.TODO()); err != nil {
		s.index.logger.
			WithField("action", "shutdown").
			WithField("shard", s.name).
			Errorf("deferred shutdown after last reference released: %v", err)
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
