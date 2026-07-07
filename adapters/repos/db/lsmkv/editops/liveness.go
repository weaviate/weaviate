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

package editops

import (
	"context"
	"sync"
	"time"
)

// LivenessProvider returns the set of edit-op IDs that still have a live
// (non-terminal) task. Consulted on shard load so an op whose task is gone is
// swept instead of re-armed — a re-armed orphan would strip a re-created
// same-name vector.
type LivenessProvider func(ctx context.Context) (map[string]struct{}, error)

// livenessCacheTTL bounds the leader round-trips a mass shard load can trigger:
// every shard loading within the window shares one lookup.
const livenessCacheTTL = 5 * time.Second

var (
	livenessMu       sync.RWMutex
	livenessProvider LivenessProvider
	livenessCached   map[string]struct{}
	livenessFetched  time.Time
)

// SetLivenessProvider installs the cluster-level live-op lookup. Like the
// transformers registry, it is package-level so per-bucket wiring stays free of
// task-specific plumbing. Must be installed before shards load (in production:
// before the RAFT store opens), or restore-time loads skip the sweep.
//
// The provider must return the live op IDs of EVERY task type that registers
// edit ops (today: drop-vector only); an op type it does not cover would be
// swept as an orphan. The sweep additionally protects op types other than
// remove_target_vectors, so a future producer fails safe until it extends the
// provider.
func SetLivenessProvider(p LivenessProvider) {
	livenessMu.Lock()
	livenessProvider = p
	livenessCached = nil
	livenessFetched = time.Time{}
	livenessMu.Unlock()
}

// LivenessProviderInstalled reports whether a provider is wired; callers use it
// to warn when a sweep is silently disabled.
func LivenessProviderInstalled() bool {
	livenessMu.RLock()
	defer livenessMu.RUnlock()
	return livenessProvider != nil
}

// LiveOps resolves the live-op set, or (nil, nil) when no provider is installed
// — callers treat nil as "sweep disabled". Results are cached briefly so
// per-shard-load callers don't fan a mass load out into per-shard leader RPCs.
//
// A cached set may PREDATE an op's task commit, so it must never be the basis
// for deleting an op — use LiveOpsFresh to confirm before destroying. Returns a
// defensive copy: callers may extend it (Recover's type protection) without
// poisoning the cache.
func LiveOps(ctx context.Context) (map[string]struct{}, error) {
	livenessMu.RLock()
	p := livenessProvider
	cached, fetched := livenessCached, livenessFetched
	livenessMu.RUnlock()
	if p == nil {
		return nil, nil
	}
	if cached != nil && time.Since(fetched) < livenessCacheTTL {
		return copySet(cached), nil
	}
	return LiveOpsFresh(ctx)
}

// LiveOpsFresh always asks the provider, refreshing the cache. Destructive
// decisions (sweeping an op) must use this: an op observed in a sidecar implies
// its task already committed, so a read taken NOW is authoritative, while a
// cached set fetched before the commit would wrongly report it dead.
func LiveOpsFresh(ctx context.Context) (map[string]struct{}, error) {
	livenessMu.RLock()
	p := livenessProvider
	livenessMu.RUnlock()
	if p == nil {
		return nil, nil
	}
	live, err := p(ctx)
	if err != nil {
		return nil, err
	}
	livenessMu.Lock()
	livenessCached, livenessFetched = live, time.Now()
	livenessMu.Unlock()
	return copySet(live), nil
}

func copySet(in map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for k := range in {
		out[k] = struct{}{}
	}
	return out
}
