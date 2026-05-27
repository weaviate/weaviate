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

package distributedtask

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
)

// DefaultThrottleInterval is the production cap on per-unit progress writes
// to the RAFT log. [Scheduler.Start] passes this constant to
// [NewThrottledRecorder]. Pinned by `TestThrottledRecorder_DefaultInterval_*`
// — if you change the value, update [Scheduler.Start]'s rationale comment
// and the matching prose in `doc.go` ("Progress throttling" section) too.
const DefaultThrottleInterval = 3 * time.Second

// ThrottledRecorder wraps a [TaskCompletionRecorder] to prevent progress updates from
// flooding Raft consensus. Each unit's progress is forwarded at most once per the
// interval given to [NewThrottledRecorder] ([DefaultThrottleInterval] in production);
// intermediate updates are silently dropped. Completion and failure calls always pass
// through immediately — they are never throttled.
//
// Throttle entries are cleaned up when a unit reaches a terminal state (completion or
// failure), so the internal map does not grow beyond the number of active units.
//
// Two non-negotiable carve-outs (weaviate/0-weaviate-issues#240 Symptom B):
//   - progress == 0.0 is never throttled — it is the per-unit worker's
//     first call (the "claim") and the path that lands Unit.NodeID.
//   - lastSent is updated only after a successful forward; a failed
//     forward leaves no entry so the retry isn't blocked.
type ThrottledRecorder struct {
	inner    TaskCompletionRecorder
	interval time.Duration
	clock    clockwork.Clock
	mu       sync.Mutex
	lastSent map[string]time.Time // key: "namespace/taskID/version/unitID"
}

func NewThrottledRecorder(inner TaskCompletionRecorder, interval time.Duration, clock clockwork.Clock) *ThrottledRecorder {
	if clock == nil {
		clock = clockwork.NewRealClock()
	}
	return &ThrottledRecorder{
		inner:    inner,
		interval: interval,
		clock:    clock,
		lastSent: make(map[string]time.Time),
	}
}

func (r *ThrottledRecorder) RecordDistributedTaskUnitCompletion(ctx context.Context, namespace, taskID string, version uint64, nodeID, unitID string) error {
	r.cleanupThrottleEntry(namespace, taskID, version, unitID)
	return r.inner.RecordDistributedTaskUnitCompletion(ctx, namespace, taskID, version, nodeID, unitID)
}

func (r *ThrottledRecorder) RecordDistributedTaskUnitFailure(ctx context.Context, namespace, taskID string, version uint64, nodeID, unitID, errMsg string) error {
	r.cleanupThrottleEntry(namespace, taskID, version, unitID)
	return r.inner.RecordDistributedTaskUnitFailure(ctx, namespace, taskID, version, nodeID, unitID, errMsg)
}

func (r *ThrottledRecorder) cleanupThrottleEntry(namespace, taskID string, version uint64, unitID string) {
	key := fmt.Sprintf("%s/%s/%d/%s", namespace, taskID, version, unitID)
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.lastSent, key)
}

func (r *ThrottledRecorder) UpdateDistributedTaskUnitProgress(ctx context.Context, namespace, taskID string, version uint64, nodeID, unitID string, progress float32) error {
	// CLAIM bypass: progress == 0.0 is the per-unit worker's first
	// call (the claim) that lands Unit.NodeID via the FSM. Throttling
	// risks deduplicating the assignment and orphaning the unit.
	// Non-CLAIM 0.0 emissions (e.g. composeProgressEnvelope on the
	// first sub-task) also hit this bypass — harmless extra forward,
	// RAFT applies are idempotent on (unitID, progress).
	if progress == 0.0 {
		return r.inner.UpdateDistributedTaskUnitProgress(ctx, namespace, taskID, version, nodeID, unitID, progress)
	}

	key := fmt.Sprintf("%s/%s/%d/%s", namespace, taskID, version, unitID)
	now := r.clock.Now()

	throttled := func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		last, ok := r.lastSent[key]
		return ok && now.Sub(last) < r.interval
	}()
	if throttled {
		return nil
	}

	if err := r.inner.UpdateDistributedTaskUnitProgress(ctx, namespace, taskID, version, nodeID, unitID, progress); err != nil {
		return err
	}

	func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if cur, ok := r.lastSent[key]; !ok || cur.Before(now) {
			r.lastSent[key] = now
		}
	}()
	return nil
}
