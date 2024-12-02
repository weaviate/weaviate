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

type TaskDecoder interface {
	DecodeTask([]byte) (Task, error)
}
