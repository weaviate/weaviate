package queue

import (
	"bufio"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Queue struct {
	// Logger for the queue. Wrappers of this queue should use this logger.
	Logger logrus.FieldLogger
	// BeforeScheduleFn is a hook that is called before the queue is scheduled.
	BeforeScheduleFn func() (skip bool)

	scheduler    *Scheduler
	id           string
	path         string
	enc          *Encoder
	exec         TaskExecutor
	lastPushTime atomic.Pointer[time.Time]
	closed       atomic.Bool
}

func New(s *Scheduler, id, path string, exec TaskExecutor) (*Queue, error) {
	logger := logrus.New().WithField("queue_id", id)

	enc, err := NewEncoder(path, logger)
	if err != nil {
		return nil, err
	}

	q := Queue{
		Logger:    logger,
		scheduler: s,
		id:        id,
		path:      path,
		enc:       enc,
		exec:      exec,
	}

	s.RegisterQueue(&q)

	return &q, nil
}

// Close the queue, prevent further pushes and unregister it from the scheduler.
func (q *Queue) Close() error {
	if q.closed.Swap(true) {
		return errors.New("queue already closed")
	}

	q.scheduler.UnregisterQueue(q.id)

	err := q.enc.Close()
	if err != nil {
		return errors.Wrap(err, "failed to flush encoder")
	}

	return nil
}

func (q *Queue) ID() string {
	return q.id
}

func (q *Queue) Push(op uint8, keys ...uint64) error {
	if q.closed.Load() {
		return errors.New("queue closed")
	}

	now := time.Now()

	q.lastPushTime.Store(&now)

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

func (q *Queue) Size() int64 {
	return q.enc.RecordCount()
}

func (q *Queue) Pause() {
	q.scheduler.PauseQueue(q.id)
}

func (q *Queue) Resume() {
	q.scheduler.ResumeQueue(q.id)
}

func (q *Queue) Wait() {
	q.scheduler.Wait(q.id)
}

func (q *Queue) Drop() error {
	_ = q.Close()

	return q.enc.Drop()
}

func (q *Queue) BeforeSchedule() bool {
	if q.BeforeScheduleFn != nil {
		return q.BeforeScheduleFn()
	}

	return false
}
