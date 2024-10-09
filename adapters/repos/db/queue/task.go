package queue

import "context"

type TaskRecord struct {
	Op  uint8
	Key uint64
}

type Task interface {
	Key() uint64
	Op() uint8
	Execute(context.Context) error
}

type TaskGroup interface {
	Task

	AddTask(op uint64, task Task) bool
}

type Batch struct {
	Tasks []Task
	Ctx   context.Context
	Done  func()
}
