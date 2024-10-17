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

import "context"

type Task struct {
	Op       uint8
	DocIDs   []uint64
	executor TaskExecutor
}

func (t *Task) Key() uint64 {
	return t.DocIDs[0]
}

func (t *Task) Execute(ctx context.Context) error {
	return t.executor.Execute(ctx, t.Op, t.DocIDs...)
}

type Batch struct {
	Tasks []*Task
	Ctx   context.Context
	Done  func()
}

type TaskExecutor interface {
	Execute(ctx context.Context, op uint8, keys ...uint64) error
}

type TaskExecutorFunc func(ctx context.Context, op uint8, keys ...uint64) error

func (f TaskExecutorFunc) Execute(ctx context.Context, op uint8, keys ...uint64) error {
	return f(ctx, op, keys...)
}
