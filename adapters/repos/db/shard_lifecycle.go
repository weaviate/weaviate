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
	"errors"
	"fmt"
	"sync/atomic"
)

// shardPhase only moves forward: shardLive -> (shardUnloading | shardDropping)
// -> shardClosed, plus an upgrade from shardUnloading to shardDropping.
type shardPhase uint8

const (
	shardLive shardPhase = iota
	// shardUnloading is completed by whichever goroutine releases last;
	// shardDropping stays owned by [Shard.drop]'s caller, which holds keepFiles.
	shardUnloading
	shardDropping
	shardClosed
)

func (p shardPhase) String() string {
	switch p {
	case shardLive:
		return "live"
	case shardUnloading:
		return "unloading"
	case shardDropping:
		return "dropping"
	case shardClosed:
		return "closed"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(p))
	}
}

const (
	shardPhaseShift = 56
	shardRefsMask   = uint64(1)<<shardPhaseShift - 1
)

// shardLifecycle packs the teardown phase (top 8 bits) and the in-flight user
// count (low 56) into one word, so teardown observes "no new users AND all users
// gone" as one value. Split across variables that pair needs a lock, and a path
// that forgets it — [Shard.drop] did — tears the store down under a live request.
//
// Once the phase leaves shardLive acquire refuses, so the refcount only
// decreases: a CAS seeing refs == 0 cannot lose to an increment.
type shardLifecycle struct {
	state atomic.Uint64
}

func packShardState(p shardPhase, refs uint64) uint64 {
	return uint64(p)<<shardPhaseShift | refs
}

func unpackShardState(v uint64) (shardPhase, uint64) {
	return shardPhase(v >> shardPhaseShift), v & shardRefsMask
}

func (l *shardLifecycle) phase() shardPhase {
	p, _ := unpackShardState(l.state.Load())
	return p
}

// inUse is for diagnostics; acting on it is racy.
func (l *shardLifecycle) inUse() uint64 {
	_, refs := unpackShardState(l.state.Load())
	return refs
}

// isTeardownRefusal reports whether err is [shardLifecycle.acquire] refusing
// because the shard has left shardLive — draining, or already torn down. It
// lives next to acquire so that adding a phase updates the predicate and its
// producer together, instead of leaving call sites enumerating sentinels.
func isTeardownRefusal(err error) bool {
	return errors.Is(err, errShutdownInProgress) || errors.Is(err, errAlreadyShutdown)
}

// acquire must be paired with exactly one release.
func (l *shardLifecycle) acquire() error {
	for {
		v := l.state.Load()
		p, refs := unpackShardState(v)
		switch p {
		case shardLive:
		case shardClosed:
			return errAlreadyShutdown
		default:
			return errShutdownInProgress
		}
		if l.state.CompareAndSwap(v, packShardState(shardLive, refs+1)) {
			return nil
		}
	}
}

// release reports whether it drained the shard with a teardown pending, i.e.
// whether the caller now owes that teardown a completion attempt. An unmatched
// release is reported, not applied: wrapping past zero disables the drain guard.
func (l *shardLifecycle) release() (drained bool, err error) {
	for {
		v := l.state.Load()
		p, refs := unpackShardState(v)
		if refs == 0 {
			return false, fmt.Errorf("%s (phase %s)", msgReleasedMoreThanOnce, p)
		}
		if l.state.CompareAndSwap(v, packShardState(p, refs-1)) {
			return refs == 1 && (p == shardUnloading || p == shardDropping), nil
		}
	}
}

// requestTeardown stops the shard admitting users and returns the phase now in
// effect. It neither drains nor tears down; see [shardLifecycle.claimTeardown].
func (l *shardLifecycle) requestTeardown(kind shardPhase) shardPhase {
	for {
		v := l.state.Load()
		p, refs := unpackShardState(v)
		switch {
		// a pending drop outranks an unload request
		case p == shardClosed, p == kind, p == shardDropping:
			return p
		}
		if l.state.CompareAndSwap(v, packShardState(kind, refs)) {
			return kind
		}
	}
}

// claimTeardown gives exactly one caller the right to run the teardown body for
// kind, once every user has released. On failure the returned phase separates
// "still draining" (kind) from "already done" (shardClosed) from "superseded".
func (l *shardLifecycle) claimTeardown(kind shardPhase) (claimed bool, current shardPhase) {
	for {
		v := l.state.Load()
		p, refs := unpackShardState(v)
		if p != kind || refs != 0 {
			return false, p
		}
		if l.state.CompareAndSwap(v, packShardState(shardClosed, 0)) {
			return true, p
		}
	}
}
