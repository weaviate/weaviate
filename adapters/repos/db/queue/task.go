package queue

import "context"

type Task struct {
	Op     uint8
	DocIDs []uint64
	exec   TaskExecutor
}

func (t *Task) Key() uint64 {
	return t.DocIDs[0]
}

func (t *Task) Execute(ctx context.Context) error {
	return t.exec(ctx, t.Op, t.DocIDs...)
}

type Batch struct {
	Tasks []*Task
	Ctx   context.Context
	Done  func()
}
