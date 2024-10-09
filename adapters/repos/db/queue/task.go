package queue

import "context"

type Task interface {
	Key() uint64
	Op() uint64
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
