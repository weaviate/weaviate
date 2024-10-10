package queue

import (
	"bufio"
	"context"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Queue struct {
	ID   string
	Path string

	s          *Scheduler
	enc        *Encoder
	exec       TaskExecutor
	lastPushed atomic.Pointer[time.Time]
}

type TaskExecutor func(ctx context.Context, op uint8, keys ...uint64) error

func NewQueue(s *Scheduler, logger logrus.FieldLogger, id, path string, execFn TaskExecutor) (*Queue, error) {
	logger = logger.WithField("queue", id)
	enc, err := NewEncoder(path, logger)
	if err != nil {
		return nil, err
	}

	q := Queue{
		ID:   id,
		Path: path,
		s:    s,
		enc:  enc,
		exec: execFn,
	}

	s.RegisterQueue(&q)

	return &q, nil
}

func (q *Queue) Push(op uint8, keys ...uint64) error {
	now := time.Now()

	q.lastPushed.Store(&now)

	for _, key := range keys {
		_, err := q.enc.Encode(op, key)
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
		Op:     op,
		DocIDs: []uint64{key},
		exec:   q.exec,
	}, nil
}
