package queue

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

type Scheduler struct {
	SchedulerOptions

	queues struct {
		sync.Mutex

		m map[string]*queueState
	}

	// context used to close pending tasks
	ctx      context.Context
	cancelFn context.CancelFunc

	activeTasks *common.SharedGauge

	wg sync.WaitGroup
}

type SchedulerOptions struct {
	Logger           logrus.FieldLogger
	Workers          []chan Batch
	ScheduleInterval time.Duration
	// If a queue does not receive any tasks for this duration, it is considered stale
	// and must be scheduled.
	StaleTimeout time.Duration
}

func NewScheduler(opts SchedulerOptions) (*Scheduler, error) {
	if len(opts.Workers) == 0 {
		return nil, errors.New("at least one worker is required")
	}

	if opts.Logger == nil {
		opts.Logger = logrus.New()
	}
	opts.Logger = opts.Logger.WithField("component", "queue-scheduler")

	if opts.ScheduleInterval == 0 {
		opts.ScheduleInterval = time.Second
	}

	if opts.StaleTimeout == 0 {
		opts.StaleTimeout = 5 * time.Second
	}

	s := Scheduler{
		SchedulerOptions: opts,
		activeTasks:      common.NewSharedGauge(),
	}
	s.queues.m = make(map[string]*queueState)

	s.ctx, s.cancelFn = context.WithCancel(context.Background())

	return &s, nil
}

func (s *Scheduler) RegisterQueue(q *Queue) {
	s.queues.Lock()
	defer s.queues.Unlock()

	s.queues.m[q.ID] = &queueState{
		q: q,
	}

	s.Logger.WithField("id", q.ID).Debug("queue registered")
}

func (s *Scheduler) UnregisterQueue(id string) {
	s.queues.Lock()
	defer s.queues.Unlock()

	delete(s.queues.m, id)

	s.Logger.WithField("id", id).Debug("queue unregistered")
}

func (s *Scheduler) Start() {
	s.wg.Add(1)

	f := func() {
		defer s.wg.Done()

		s.runScheduler()
	}
	enterrors.GoWrapper(f, s.Logger)
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
	s.activeTasks.Wait()

	s.Logger.Debug("scheduler closed")

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

	s.Logger.WithField("id", id).Debug("queue paused")
}

func (s *Scheduler) ResumeQueue(id string) {
	s.queues.Lock()
	defer s.queues.Unlock()

	q, ok := s.queues.m[id]
	if !ok {
		return
	}

	q.paused = false

	s.Logger.WithField("id", id).Debug("queue resumed")
}

type queueState struct {
	q         *Queue
	paused    bool
	readFiles []string
	cursor    int
}

func (s *Scheduler) runScheduler() {
	t := time.NewTicker(s.ScheduleInterval)

	for {
		select {
		case <-s.ctx.Done():
			// stop the ticker
			t.Stop()
			return
		case <-t.C:
			s.schedule()
		}
	}
}

func (s *Scheduler) schedule() {
	// loop over the queues in random order
	s.queues.Lock()
	ids := make([]string, 0, len(s.queues.m))
	for id := range s.queues.m {
		ids = append(ids, id)
	}
	s.queues.Unlock()

	for _, id := range ids {
		s.queues.Lock()
		q, ok := s.queues.m[id]
		if !ok {
			// queue was unregistered
			s.queues.Unlock()
			continue
		}

		if q.paused {
			s.queues.Unlock()
			continue
		}

		s.queues.Unlock()

		s.Logger.WithField("id", id).Debug("scheduling queue")
		err := s.dispatchQueue(q)
		if err != nil {
			s.Logger.WithError(err).WithField("id", id).Error("failed to schedule queue")
		}
	}
}

func (s *Scheduler) dispatchQueue(q *queueState) error {
	f, path, err := s.readQueueChunk(q)
	if err != nil {
		return err
	}
	// if there are no more chunks to read,
	// check if the partial chunk is stale (e.g no tasks were pushed for a while)
	if f == nil {
		f, path, err = s.checkIfStale(q)
		if err != nil || f == nil {
			return err
		}
	}

	// decode all tasks from the chunk
	// and partition them by worker
	r := bufio.NewReader(f)
	partitions := make([][]*Task, len(s.Workers))

	for {
		t, err := q.q.DecodeTask(r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil || t == nil {
			_ = f.Close()
			return err
		}

		// TODO: introduce other partitioning strategies if needed
		slot := t.Key() % uint64(len(s.Workers))
		partitions[slot] = append(partitions[slot], t)
	}

	err = f.Close()
	if err != nil {
		s.Logger.WithField("file", path).WithError(err).Warn("failed to close chunk file")
	}

	if len(partitions) == 0 {
		s.Logger.WithField("file", path).Warn("read chunk is empty. removing file")
		q.q.enc.removeChunk(path)
		return nil
	}

	// TODO: compress the tasks before sending them to the workers

	// keep track of the number of active tasks
	// for this chunk to remove it when all tasks are done
	var counter atomic.Int32
	counter.Store(int32(len(partitions)))

	for i, batch := range partitions {
		if len(batch) == 0 {
			continue
		}

		s.activeTasks.Incr()

		select {
		case <-s.ctx.Done():
			s.activeTasks.Decr()
			return nil
		case s.Workers[i] <- Batch{
			Tasks: batch,
			Ctx:   s.ctx,
			Done: func() {
				defer s.activeTasks.Decr()

				if counter.Add(-1) == 0 {
					q.q.enc.removeChunk(path)
				}
			},
		}:
		}
	}

	return nil
}

func (s *Scheduler) readQueueChunk(q *queueState) (*os.File, string, error) {
	if q.cursor+1 < len(q.readFiles) {
		q.cursor++

		f, err := os.Open(q.readFiles[q.cursor])
		if err != nil {
			return nil, "", err
		}

		return f, q.readFiles[q.cursor], nil
	}

	q.readFiles = q.readFiles[:0]
	q.cursor = 0

	// read the directory
	entries, err := os.ReadDir(q.q.Path)
	if err != nil {
		return nil, "", err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// check if the entry name matches the regex pattern of a chunk file
		if !chunkFilePattern.Match([]byte(entry.Name())) {
			continue
		}

		q.readFiles = append(q.readFiles, filepath.Join(q.q.Path, entry.Name()))
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

func (s *Scheduler) checkIfStale(q *queueState) (*os.File, string, error) {
	lastPushed := q.q.lastPushed.Load()
	if lastPushed == nil {
		return nil, "", nil
	}

	if time.Since(*lastPushed) < s.StaleTimeout {
		return nil, "", nil
	}

	s.Logger.WithField("id", q.q.ID).Debug("partial chunk is stale, scheduling")

	err := q.q.enc.promoteChunk()
	if err != nil {
		return nil, "", err
	}

	return s.readQueueChunk(q)
}
