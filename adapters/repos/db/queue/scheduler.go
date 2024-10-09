package queue

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

type Scheduler struct {
	logger logrus.FieldLogger

	queues struct {
		sync.Mutex

		m map[string]*queueState
	}

	// context used to close pending tasks
	ctx      context.Context
	cancelFn context.CancelFunc

	workers         []chan Batch
	tasksProcessing *common.SharedGauge

	wg sync.WaitGroup
}

type SchedulerOptions struct {
	Logger  logrus.FieldLogger
	Workers []chan Batch
}

func NewScheduler(opts SchedulerOptions) (*Scheduler, error) {
	if len(opts.Workers) == 0 {
		return nil, errors.New("at least one worker is required")
	}

	if opts.Logger == nil {
		opts.Logger = logrus.New()
	}
	opts.Logger = opts.Logger.WithField("component", "queue-scheduler")

	s := Scheduler{
		logger:          opts.Logger,
		tasksProcessing: common.NewSharedGauge(),
		workers:         opts.Workers,
	}

	s.ctx, s.cancelFn = context.WithCancel(context.Background())

	return &s, nil
}

func (s *Scheduler) RegisterQueue(q QueueDecoder) {
	s.queues.Lock()
	defer s.queues.Unlock()

	s.queues.m[q.ID()] = &queueState{
		q: q,
	}

	s.logger.WithField("id", q.ID()).Debug("queue registered")
}

func (s *Scheduler) UnregisterQueue(id string) {
	s.queues.Lock()
	defer s.queues.Unlock()

	delete(s.queues.m, id)

	s.logger.WithField("id", id).Debug("queue unregistered")
}

func (s *Scheduler) Start() {
	s.wg.Add(1)

	f := func() {
		defer s.wg.Done()

		s.scheduler()
	}
	enterrors.GoWrapper(f, s.logger)
}

func (s *Scheduler) Close() error {
	if s == nil {
		// scheduler not initialized. No op.
		return nil
	}

	// check if the scheduler is already closed
	if s.ctx.Err() != nil {
		return nil
	}

	// stop scheduling
	s.cancelFn()

	// wait for the scheduler to stop
	s.wg.Wait()

	// wait for the workers to finish processing tasks
	s.tasksProcessing.Wait()

	s.logger.Debug("scheduler closed")

	return nil
}

func (s *Scheduler) PauseQueue(id string) {
	s.queues.Lock()
	defer s.queues.Unlock()

	q, ok := s.queues.m[id]
	if !ok {
		return
	}

	q.paused = true

	s.logger.WithField("id", id).Debug("queue paused")
}

func (s *Scheduler) ResumeQueue(id string) {
	s.queues.Lock()
	defer s.queues.Unlock()

	q, ok := s.queues.m[id]
	if !ok {
		return
	}

	q.paused = false

	s.logger.WithField("id", id).Debug("queue resumed")
}

type queueState struct {
	q         QueueDecoder
	paused    bool
	readFiles []string
	cursor    int
}

func (s *Scheduler) scheduler() {

}

func (s *Scheduler) schedule() {
	s.queues.Lock()
	defer s.queues.Unlock()

	// loop over the queues in random order
	for id, q := range s.queues.m {
		if q.paused {
			continue
		}

		err := s.scheduleQueue(q)
		if err != nil {
			s.logger.WithError(err).WithField("id", id).Error("failed to schedule queue")
		}
	}
}

func (s *Scheduler) scheduleQueue(q *queueState) error {
	f, path, err := s.readNextChunk(q)
	if err != nil || f == nil {
		return err
	}

	batches := make([][]Task, len(s.workers))

	for {
		t, err := q.q.DecodeTask(f)
		if err == io.EOF {
			break
		}
		if err != nil || t == nil {
			_ = f.Close()
			return err
		}

		slot := t.Key() % uint64(len(s.workers))
		batches[slot] = append(batches[slot], t)
	}

	err = f.Close()
	if err != nil {
		s.logger.WithField("file", path).WithError(err).Warn("failed to close chunk file")
	}

	var counter atomic.Int32
	counter.Store(int32(len(batches)))

	for i, batch := range batches {
		if len(batch) == 0 {
			continue
		}

		s.tasksProcessing.Incr()

		select {
		case <-s.ctx.Done():
			s.tasksProcessing.Decr()
			return nil
		case s.workers[i] <- Batch{
			Tasks: batch,
			Ctx:   s.ctx,
			Done: func() {
				defer s.tasksProcessing.Decr()

				if counter.Add(-1) == 0 {
					s.removeChunk(path)
				}
			},
		}:
		}
	}

	return nil
}

func (s *Scheduler) readNextChunk(q *queueState) (*os.File, string, error) {
	if q.cursor+1 < len(q.readFiles) {
		q.cursor++

		f, err := os.Open(q.readFiles[q.cursor])
		if err != nil {
			return nil, "", err
		}

		return f, q.readFiles[q.cursor], nil
	}

	q.readFiles = nil
	q.cursor = 0

	// read the directory
	entries, err := os.ReadDir(q.q.Path())
	if err != nil {
		return nil, "", err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		q.readFiles = append(q.readFiles, filepath.Join(q.q.Path(), entry.Name()))
	}

	if len(q.readFiles) == 0 {
		return nil, "", nil
	}

	f, err := os.Open(q.readFiles[q.cursor])
	if err != nil {
		return nil, "", err
	}

	return f, q.readFiles[q.cursor], nil
}

func (s *Scheduler) removeChunk(path string) {
	err := os.Remove(path)
	if err != nil {
		s.logger.WithError(err).WithField("file", path).Error("failed to remove chunk")
		return
	}

	s.logger.WithField("file", path).Debug("chunk removed")
}
