//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package queue

import (
	"context"
	"sync"
)

// A Task represents a unit of work to be processed by the workers.
type Task interface {
	// Op returns a uint8 representing the operation type of the task.
	// It is used for compressing tasks that implement the optional TaskGrouper interface.
	Op() uint8
	// Key returns a uint64 that can be used to influence task partitioning.
	// Tasks are assigned to workers based on the hash of their key, to ensure
	// that tasks with the same key are always processed by the same worker.
	// Implementations can also return a constant value to make sure all tasks are
	// processed by the same worker (e.g. for operations that must be serialized, like HFresh Merge operation).
	Key() uint64
	// Execute is called by a worker to process the task.
	// If a task returns a transient error from Execute (e.g. canceled context, not enough memory, etc.),
	// it will be retried using an exponential backoff strategy.
	// Otherwise, the error will be considered permanent and the task will not be retried.
	Execute(ctx context.Context) error
}

// TaskGrouper is an optional interface that can be implemented by a Task
// to provide custom grouping logic.
type TaskGrouper interface {
	NewGroup(op uint8, tasks ...Task) Task
}

// A Batch represents a group of tasks dequeued together.
// The Scheduler will call Done() when all tasks have been processed,
// or Cancel() if the batch processing was canceled.
// The Queue implementation can use the OnDone and OnCanceled callbacks to
// perform any necessary actions when the batch is done or canceled.
type Batch struct {
	Tasks      []Task
	Ctx        context.Context
	OnDone     func()
	OnCanceled func()
	once       sync.Once
}

// Called by the worker when all tasks in the batch have been processed.
// It will execute the OnDone callback if it is set.
func (b *Batch) Done() {
	if b.OnDone != nil {
		b.once.Do(b.OnDone)
	}
}

// Called by the worker if the batch processing was canceled.
// It will execute the OnCanceled callback if it is set.
func (b *Batch) Cancel() {
	if b.OnCanceled != nil {
		b.OnCanceled()
	}
}

// MergeBatches merges multiple batches into a single batch.
// It will ignore nil batches.
// It will execute the OnDone and OnCanceled functions of all batches.
func MergeBatches(batches ...*Batch) *Batch {
	// count the number of tasks
	var numTasks int
	for _, batch := range batches {
		if batch == nil {
			continue
		}

		numTasks += len(batch.Tasks)
	}

	tasks := make([]Task, 0, numTasks)
	onDoneFns := make([]func(), 0, len(batches))
	onCanceledFns := make([]func(), 0, len(batches))

	for _, batch := range batches {
		if batch == nil {
			continue
		}

		if len(batch.Tasks) > 0 {
			tasks = append(tasks, batch.Tasks...)
		}
		if batch.OnDone != nil {
			onDoneFns = append(onDoneFns, batch.OnDone)
		}
		if batch.OnCanceled != nil {
			onCanceledFns = append(onCanceledFns, batch.OnCanceled)
		}
	}

	return &Batch{
		Tasks: tasks,
		OnDone: func() {
			for _, fn := range onDoneFns {
				fn()
			}
		},
		OnCanceled: func() {
			for _, fn := range onCanceledFns {
				fn()
			}
		},
	}
}

type TaskDecoder interface {
	DecodeTask([]byte) (Task, error)
}
