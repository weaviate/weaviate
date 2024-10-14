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
