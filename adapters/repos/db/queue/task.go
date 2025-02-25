//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package queue

import (
	"context"
	"sync"
)

type Task interface {
	Op() uint8
	Key() uint64
	Execute(ctx context.Context) error
}

type TaskGrouper interface {
	NewGroup(op uint8, tasks ...Task) Task
}

type Batch struct {
	Tasks      []Task
	Ctx        context.Context
	onDone     func()
	onCanceled func()
	once       sync.Once
}

func (b *Batch) Done() {
	if b.onDone != nil {
		b.once.Do(b.onDone)
	}
}

func (b *Batch) Cancel() {
	if b.onCanceled != nil {
		b.onCanceled()
	}
}

// MergeBatches merges multiple batches into a single batch.
// It will ignore nil batches.
// It will execute the onDone and onCanceled functions of all batches.
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
		if batch.onDone != nil {
			onDoneFns = append(onDoneFns, batch.onDone)
		}
		if batch.onCanceled != nil {
			onCanceledFns = append(onCanceledFns, batch.onCanceled)
		}
	}

	return &Batch{
		Tasks: tasks,
		onDone: func() {
			for _, fn := range onDoneFns {
				fn()
			}
		},
		onCanceled: func() {
			for _, fn := range onCanceledFns {
				fn()
			}
		},
	}
}

type TaskDecoder interface {
	DecodeTask([]byte) (Task, error)
}
