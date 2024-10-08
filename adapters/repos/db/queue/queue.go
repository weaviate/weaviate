package queue

type QueueDecoder interface {
	ID() string
	Path() string
	DecodeTask([]byte) (Task, int, error)
}
