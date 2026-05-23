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

// ThrottledRecorder wraps a [TaskCompletionRecorder] to prevent progress updates from
// flooding Raft consensus. Each unit's progress is forwarded at most once per interval
// (default 30s); intermediate updates are silently dropped. Completion and failure calls
// always pass through immediately — they are never throttled.
//
// Throttle entries are cleaned up when a unit reaches a terminal state (completion or
// failure), so the internal map does not grow beyond the number of active units.
//
// Two carve-outs are non-negotiable, both pinned by tests in
// throttled_recorder_test.go and motivated by
// weaviate/0-weaviate-issues#240 Symptom B:
//
//   - The CLAIM call (progress == 0.0) is never throttled. It is the
//     only path that sets Unit.NodeID; deduplicating it risks
//     orphaning the unit.
//   - lastSent is updated only AFTER a successful forward. A failed
//     forward leaves no entry so the caller's retry is not blocked.
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
	delete(r.lastSent, key)
	r.mu.Unlock()
}

func (r *ThrottledRecorder) UpdateDistributedTaskUnitProgress(ctx context.Context, namespace, taskID string, version uint64, nodeID, unitID string, progress float32) error {
	// CLAIM bypass: progress == 0.0 is the only path that sets
	// Unit.NodeID, so it must never be deduplicated.
	if progress == 0.0 {
		return r.inner.UpdateDistributedTaskUnitProgress(ctx, namespace, taskID, version, nodeID, unitID, progress)
	}

	key := fmt.Sprintf("%s/%s/%d/%s", namespace, taskID, version, unitID)

	r.mu.Lock()
	last, ok := r.lastSent[key]
	now := r.clock.Now()
	if ok && now.Sub(last) < r.interval {
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()

	if err := r.inner.UpdateDistributedTaskUnitProgress(ctx, namespace, taskID, version, nodeID, unitID, progress); err != nil {
		return err
	}

	r.mu.Lock()
	if cur, ok := r.lastSent[key]; !ok || cur.Before(now) {
		r.lastSent[key] = now
	}
	r.mu.Unlock()
	return nil
}
