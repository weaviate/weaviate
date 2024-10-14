package queue

import (
	"bufio"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Queue struct {
	ID   string
	Path string

	logger     logrus.FieldLogger
	s          *Scheduler
	enc        *Encoder
	exec       TaskExecutor
	lastPushed atomic.Pointer[time.Time]
	closed     atomic.Bool
}

func NewQueue(s *Scheduler, logger logrus.FieldLogger, id, path string, exec TaskExecutor) (*Queue, error) {
	logger = logger.WithField("queue", id)
	enc, err := NewEncoder(path, logger)
	if err != nil {
		return nil, err
	}

	q := Queue{
		logger: logger,
		ID:     id,
		Path:   path,
		s:      s,
		enc:    enc,
		exec:   exec,
	}

	s.RegisterQueue(&q)

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

	q.s.UnregisterQueue(q.ID)

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
