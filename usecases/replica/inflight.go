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

package replica

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// inflightWrites tracks, per shard, the write coordinations this node is
// currently driving. It exists to close a replica-movement lost-write race:
// during a COPY/MOVE, a coordinator whose op-state view is still FINALIZING
// routes a write to the source only; if that write lands after the source's
// change-capture log is sealed, it is captured by neither the log nor the
// target.
//
// A write is registered here BEFORE it routes (reads op-state). So any write
// that routed under the pre-INTEGRATING view was registered before the
// INTEGRATING RAFT apply. WaitForDrain, called once a node has applied
// INTEGRATING, snapshots the set and waits for exactly those writes — which is
// provably every source-only write — so the node only reports INTEGRATING (and
// the consumer only seals the log) once they have all committed into the still-
// open log. No coordinator/routing changes are needed for this to be exact.
type inflightWrites struct {
	mu     sync.RWMutex
	nextID atomic.Uint64
	// byShard maps a shard to the set of in-flight registration ids for it.
	byShard map[string]map[uint64]struct{}
}

func newInflightWrites() *inflightWrites {
	return &inflightWrites{byShard: make(map[string]map[uint64]struct{})}
}

// register records a new in-flight write to shard and returns a release closure
// that removes it. The release must be called exactly once (via defer) when the
// write coordination returns. Safe for concurrent callers.
func (w *inflightWrites) register(shard string) (release func()) {
	id := w.nextID.Add(1)

	w.mu.Lock()
	defer w.mu.Unlock()

	set := w.byShard[shard]
	if set == nil {
		set = make(map[uint64]struct{})
		w.byShard[shard] = set
	}
	set[id] = struct{}{}

	return func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		if set := w.byShard[shard]; set != nil {
			delete(set, id)
			if len(set) == 0 {
				delete(w.byShard, shard)
			}
		}
	}
}

// snapshot returns the registration ids currently in flight for shard.
func (w *inflightWrites) snapshot(shard string) []uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	set := w.byShard[shard]
	if len(set) == 0 {
		return nil
	}
	ids := make([]uint64, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	return ids
}

// stillPending reports whether any of ids is still in flight for shard.
func (w *inflightWrites) stillPending(shard string, ids []uint64) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	set := w.byShard[shard]
	if len(set) == 0 {
		return false
	}
	for _, id := range ids {
		if _, ok := set[id]; ok {
			return true
		}
	}
	return false
}

// WaitForDrain blocks until every write in flight to shard at call time has
// completed, or ctx is done. Writes registered after the snapshot are ignored,
// so it terminates under sustained write load: post-cutover writes do not hold the drain.
func (w *inflightWrites) WaitForDrain(ctx context.Context, shard string) error {
	pending := w.snapshot(shard)
	if len(pending) == 0 {
		return nil
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if !w.stillPending(shard, pending) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
