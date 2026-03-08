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
// flooding Raft consensus. Each sub-unit's progress is forwarded at most once per interval
// (default 30s); intermediate updates are silently dropped. Completion and failure calls
// always pass through immediately — they are never throttled.
//
// Throttle entries are cleaned up when a sub-unit reaches a terminal state (completion or
// failure), so the internal map does not grow beyond the number of active sub-units.
type ThrottledRecorder struct {
	inner    TaskCompletionRecorder
	interval time.Duration
	clock    clockwork.Clock
	mu       sync.Mutex
	lastSent map[string]time.Time // key: "namespace/taskID/version/subUnitID"
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

func (r *ThrottledRecorder) RecordDistributedTaskNodeCompletion(ctx context.Context, namespace, taskID string, version uint64) error {
	return r.inner.RecordDistributedTaskNodeCompletion(ctx, namespace, taskID, version)
}

func (r *ThrottledRecorder) RecordDistributedTaskNodeFailure(ctx context.Context, namespace, taskID string, version uint64, errMsg string) error {
	return r.inner.RecordDistributedTaskNodeFailure(ctx, namespace, taskID, version, errMsg)
}

func (r *ThrottledRecorder) RecordDistributedTaskSubUnitCompletion(ctx context.Context, namespace, taskID string, version uint64, nodeID, subUnitID string) error {
	r.cleanupThrottleEntry(namespace, taskID, version, subUnitID)
	return r.inner.RecordDistributedTaskSubUnitCompletion(ctx, namespace, taskID, version, nodeID, subUnitID)
}

func (r *ThrottledRecorder) RecordDistributedTaskSubUnitFailure(ctx context.Context, namespace, taskID string, version uint64, nodeID, subUnitID, errMsg string) error {
	r.cleanupThrottleEntry(namespace, taskID, version, subUnitID)
	return r.inner.RecordDistributedTaskSubUnitFailure(ctx, namespace, taskID, version, nodeID, subUnitID, errMsg)
}

func (r *ThrottledRecorder) cleanupThrottleEntry(namespace, taskID string, version uint64, subUnitID string) {
	key := fmt.Sprintf("%s/%s/%d/%s", namespace, taskID, version, subUnitID)
	r.mu.Lock()
	delete(r.lastSent, key)
	r.mu.Unlock()
}

func (r *ThrottledRecorder) UpdateDistributedTaskSubUnitProgress(ctx context.Context, namespace, taskID string, version uint64, nodeID, subUnitID string, progress float32) error {
	key := fmt.Sprintf("%s/%s/%d/%s", namespace, taskID, version, subUnitID)

	r.mu.Lock()
	last, ok := r.lastSent[key]
	now := r.clock.Now()
	if ok && now.Sub(last) < r.interval {
		r.mu.Unlock()
		return nil
	}
	r.lastSent[key] = now
	r.mu.Unlock()

	return r.inner.UpdateDistributedTaskSubUnitProgress(ctx, namespace, taskID, version, nodeID, subUnitID, progress)
}
