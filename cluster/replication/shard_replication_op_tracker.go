package replication

import (
	"sync"
	"time"
)

// OpTiming tracks the start and end times for a replication operation.
type OpTiming struct {
	StartTime time.Time
	EndTime   time.Time
}

// OpTracker is responsible for tracking ongoing replication operations by their IDs and times.
type OpTracker struct {
	ops          map[uint64]OpTiming
	timeProvider TimeProvider
	mu           sync.RWMutex
}

// NewOpTracker creates a new OpTracker instance.
func NewOpTracker(timeProvider TimeProvider) *OpTracker {
	return &OpTracker{
		ops:          make(map[uint64]OpTiming),
		timeProvider: timeProvider,
	}
}

// AddOp adds a new operation to the tracker, recording its start time.
func (t *OpTracker) AddOp(opId uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.ops[opId]; !exists {
		t.ops[opId] = OpTiming{
			StartTime: t.timeProvider.Now(),
		}
	}
}

// CompleteOp marks the operation as completed and records its end time.
func (t *OpTracker) CompleteOp(opId uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if timing, exists := t.ops[opId]; exists && timing.EndTime.IsZero() {
		timing.EndTime = t.timeProvider.Now()
		t.ops[opId] = timing
	}
}

// IsOpInProgress checks if an operation is currently being processed (not yet completed).
func (t *OpTracker) IsOpInProgress(opId uint64) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	timing, exists := t.ops[opId]
	return exists && timing.EndTime.IsZero()
}

// IsOpCompleted checks if an operation has been completed (has an end time).
func (t *OpTracker) IsOpCompleted(opId uint64) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	timing, exists := t.ops[opId]
	return exists && !timing.EndTime.IsZero()
}

// CleanUpOp removes the operation from the osp being tracked.
// Removing ops that are not completed yet (without EndTime) is allowed to make sure
// that retrying failed ops is allowed.
func (t *OpTracker) CleanUpOp(opId uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	_, exists := t.ops[opId]
	if !exists {
		return
	}

	delete(t.ops, opId)
}
