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
)

// LivenessProvider returns the set of edit-op IDs that still have a live
// (non-terminal) task. Consulted on shard load so an op whose task is gone is
// swept instead of re-armed — a re-armed orphan would strip a re-created
// same-name vector.
type LivenessProvider func(ctx context.Context) (map[string]struct{}, error)

var (
	livenessMu       sync.RWMutex
	livenessProvider LivenessProvider
)

// SetLivenessProvider installs the cluster-level live-op lookup. Like the
// transformers registry, it is package-level so per-bucket wiring stays free of
// task-specific plumbing. Must be installed before shards load (in production:
// before the RAFT store opens), or restore-time loads skip the sweep.
func SetLivenessProvider(p LivenessProvider) {
	livenessMu.Lock()
	livenessProvider = p
	livenessMu.Unlock()
}

// LiveOps resolves the live-op set, or (nil, nil) when no provider is installed
// — callers treat nil as "sweep disabled".
func LiveOps(ctx context.Context) (map[string]struct{}, error) {
	livenessMu.RLock()
	p := livenessProvider
	livenessMu.RUnlock()
	if p == nil {
		return nil, nil
	}
	return p(ctx)
}
