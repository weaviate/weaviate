package queue

import (
	"bufio"
	"context"

	"github.com/pkg/errors"
)

type Queue struct {
	ID   string
	Path string

	s    *Scheduler
	enc  *Encoder
	exec TaskExecutor
}

type TaskExecutor func(ctx context.Context, op uint8, keys ...uint64) error

func NewQueue(s *Scheduler, id, path string, execFn TaskExecutor) (*Queue, error) {
	enc, err := NewEncoder(path)
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
	for _, key := range keys {
		_, err := q.enc.Encode(op, key)
		if err != nil {
			return errors.Wrap(err, "failed to encode task")
		}
	}

	return nil
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
