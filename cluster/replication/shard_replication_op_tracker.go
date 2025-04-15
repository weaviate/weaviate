package replication

import (
	"sync"
	"time"
)

// OpTiming tracks the start and end times for a replication operation.
// StartTime is set when an operation is first added to the tracker.
// EndTime is set when an operation is marked as completed.
type OpTiming struct {
	StartTime time.Time
	EndTime   time.Time
}

// OpTracker is responsible for tracking the lifecycle of replication operations.
// It provides thread-safe tracking using a sync.Map of operation states using their
// unique IDs and maintaining start and end timing information.
type OpTracker struct {
	ops          sync.Map
	timeProvider TimeProvider
}

// NewOpTracker creates a new OpTracker instance with the specified time provider.
// The time provider allows for more testable code by enabling time manipulation in tests.
func NewOpTracker(timeProvider TimeProvider) *OpTracker {
	return &OpTracker{
		timeProvider: timeProvider,
	}
}

// AddOp adds a new operation to the tracker, recording its start time.
// If the operation is already being tracked, it preserves the original start time
// to maintain accurate timing of the operation's lifecycle.
func (t *OpTracker) AddOp(opId uint64) {
	// Use LoadOrStore to atomically check if the operation exists and add it if it doesn't.
	t.ops.LoadOrStore(opId, OpTiming{
		StartTime: t.timeProvider.Now(),
	})
}

// CompleteOp marks the operation as completed by recording its end time.
// If the operation doesn't exist or is already completed, this method is a no-op.
func (t *OpTracker) CompleteOp(opId uint64) {
	value, exists := t.ops.Load(opId)
	if !exists {
		return
	}

	timing := value.(OpTiming)
	if timing.EndTime.IsZero() {
		timing.EndTime = t.timeProvider.Now()
		t.ops.Store(opId, timing)
	}
}

// IsOpInProgress checks if an operation is currently in progress.
// Returns true if the operation exists and has not been completed (no end time),
// false otherwise.
func (t *OpTracker) IsOpInProgress(opId uint64) bool {
	value, exists := t.ops.Load(opId)
	if !exists {
		return false
	}

	timing := value.(OpTiming)
	return timing.EndTime.IsZero()
}

// IsOpCompleted checks if an operation has been completed.
// Returns true if the operation exists and has been completed (has an end time),
// false otherwise.
func (t *OpTracker) IsOpCompleted(opId uint64) bool {
	value, exists := t.ops.Load(opId)
	if !exists {
		return false
	}

	timing := value.(OpTiming)
	return !timing.EndTime.IsZero()
}

// CleanUpOp removes the operation from the tracker.
// This allows operations to be retried if needed and supports the operation
// retention policy. It's safe to call on operations that don't exist.
func (t *OpTracker) CleanUpOp(opId uint64) {
	t.ops.Delete(opId)
}
