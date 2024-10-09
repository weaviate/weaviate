package queue

import "io"

type QueueDecoder interface {
	ID() string
	Path() string
	DecodeTask(io.Reader) (Task, error)
}
