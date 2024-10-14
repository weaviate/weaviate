package queue

import (
	"bufio"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Queue struct {
	id         string
	path       string
	logger     logrus.FieldLogger
	enc        *Encoder
	exec       TaskExecutor
	lastPushed atomic.Pointer[time.Time]
	closed     atomic.Bool
}

func NewQueue(logger logrus.FieldLogger, id, path string, exec TaskExecutor) (*Queue, error) {
	logger = logger.WithField("queue", id)
	enc, err := NewEncoder(path, logger)
	if err != nil {
		return nil, err
	}

	q := Queue{
		logger: logger,
		id:     id,
		path:   path,
		enc:    enc,
		exec:   exec,
	}

	return &q, nil
}

// Close the queue, prevent further pushes and unregister it from the scheduler.
func (q *Queue) Close() error {
	if q.closed.Swap(true) {
		return errors.New("queue already closed")
	}

	err := q.enc.Close()
	if err != nil {
		return errors.Wrap(err, "failed to flush encoder")
	}

	return nil
}

func (q *Queue) Push(op uint8, keys ...uint64) error {
	if q.closed.Load() {
		return errors.New("queue closed")
	}

	now := time.Now()

	q.lastPushed.Store(&now)

	for _, key := range keys {
		err := q.enc.Encode(op, key)
		if err != nil {
			return errors.Wrap(err, "failed to encode task")
		}
	}

	return q.enc.Flush()
}

func (q *Queue) ID() string {
	return q.id
}

func (q *Queue) Path() string {
	return q.path
}

func (q *Queue) Encoder() *Encoder {
	return q.enc
}

func (q *Queue) LastPushed() time.Time {
	t := q.lastPushed.Load()
	if t == nil {
		return time.Time{}
	}

	return *t
}

func (q *Queue) DecodeTask(r *bufio.Reader) (*Task, error) {
	op, key, err := Decode(r)
	if err != nil {
		return nil, err
	}

	return &Task{
		Op:       op,
		DocIDs:   []uint64{key},
		executor: q.exec,
	}, nil
}

func (q *Queue) Size() int {
	return q.enc.RecordCount()
}

func (q *Queue) Drop() error {
	_ = q.Close()

	return q.enc.Drop()
}
