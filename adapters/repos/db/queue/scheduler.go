//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package queue

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
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
	Logger logrus.FieldLogger
	// Channels to send tasks to the workers. At least one worker is required.
	Workers []chan Batch
	// The interval at which the scheduler checks the queues for tasks. Defaults to 1 second.
	ScheduleInterval time.Duration
	// If a queue does not receive any tasks for this duration, it is considered stale
	// and must be scheduled. Defaults to 5 seconds.
	StaleTimeout time.Duration
}

func NewScheduler(opts SchedulerOptions) *Scheduler {
	if len(opts.Workers) <= 0 {
		panic("at least one worker is required")
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

	return &s
}

func (s *Scheduler) RegisterQueue(q *Queue) {
	s.queues.Lock()
	defer s.queues.Unlock()

	s.queues.m[q.id] = newQueueState(q)

	s.Logger.WithField("id", q.id).Debug("queue registered")
}

func (s *Scheduler) UnregisterQueue(id string) {
	s.PauseQueue(id)

	q := s.getQueue(id)
	if q == nil {
		return
	}

	// wait for the workers to finish processing the queue's tasks
	q.activeTasks.Wait()

	// the queue is paused, so it's safe to remove it
	s.queues.Lock()
	delete(s.queues.m, id)
	s.queues.Unlock()

	s.Logger.WithField("id", id).Debug("queue unregistered")
}

func (s *Scheduler) Start() {
	if s.ctx != nil {
		// scheduler already started
		return
	}

	s.ctx, s.cancelFn = context.WithCancel(context.Background())

	s.wg.Add(1)

	f := func() {
		defer s.wg.Done()

		s.runScheduler()
	}
	enterrors.GoWrapper(f, s.Logger)
}

func (s *Scheduler) Close() error {
	if s == nil || s.ctx == nil {
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

	// close the channels
	for _, ch := range s.Workers {
		close(ch)
	}

	s.Logger.Debug("scheduler closed")

	return nil
}

func (s *Scheduler) PauseQueue(id string) {
	q := s.getQueue(id)
	if q == nil {
		return
	}

	q.m.Lock()
	q.paused = true
	q.m.Unlock()

	s.Logger.WithField("id", id).Debug("queue paused")
}

func (s *Scheduler) ResumeQueue(id string) {
	q := s.getQueue(id)
	if q == nil {
		return
	}

	q.m.Lock()
	q.paused = false
	q.m.Unlock()

	s.Logger.WithField("id", id).Debug("queue resumed")
}

func (s *Scheduler) Wait(id string) {
	q := s.getQueue(id)
	if q == nil {
		return
	}

	q.activeTasks.Wait()
}

func (s *Scheduler) getQueue(id string) *queueState {
	s.queues.Lock()
	defer s.queues.Unlock()

	return s.queues.m[id]
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
		q := s.getQueue(id)
		if q == nil {
			continue
		}

		// run the before-schedule hook if it is implemented
		if skip := q.q.BeforeSchedule(); skip {
			continue
		}

		if q.Paused() {
			continue
		}

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
	if f == nil && q.q.Size() > 0 {
		s.Logger.Debug("no chunks to read, checking if partial chunk is stale")
		f, path, err = s.checkIfStale(q)
		if err != nil || f == nil {
			return err
		}
	}
	if f == nil {
		return nil
	}

	// decode all tasks from the chunk
	// and partition them by worker
	r := bufio.NewReader(f)
	partitions := make([][]*Task, len(s.Workers))

	var taskCount int64
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
		taskCount++
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
	counter := len(partitions)

	for i, batch := range partitions {
		if len(batch) == 0 {
			continue
		}

		// increment the global active tasks counter
		s.activeTasks.Incr()
		// increment the queue's active tasks counter
		q.activeTasks.Incr()

		select {
		case <-s.ctx.Done():
			s.activeTasks.Decr()
			return nil
		case s.Workers[i] <- Batch{
			Tasks: batch,
			Ctx:   s.ctx,
			Done: func() {
				defer s.activeTasks.Decr()
				defer q.activeTasks.Decr()

				q.m.Lock()
				counter--
				if counter == 0 {
					q.q.enc.removeChunk(path)
				}
				q.m.Unlock()
			},
		}:
		}
	}

	s.logQueueStats(q.q, taskCount)

	return nil
}

func (s *Scheduler) readQueueChunk(q *queueState) (*os.File, string, error) {
	if q.cursor+1 < len(q.readFiles) {
		q.cursor++

		f, err := os.Open(q.readFiles[q.cursor])
		if err != nil {
			return nil, "", err
		}

		s.Logger.WithField("file", q.readFiles[q.cursor]).Debug("reading chunk file")

		return f, q.readFiles[q.cursor], nil
	}

	q.readFiles = q.readFiles[:0]
	q.cursor = 0

	// read the directory
	entries, err := os.ReadDir(q.q.path)
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

		q.readFiles = append(q.readFiles, filepath.Join(q.q.path, entry.Name()))
	}

	if len(q.readFiles) == 0 {
		return nil, "", nil
	}

	f, err := os.Open(q.readFiles[q.cursor])
	if err != nil {
		return nil, "", err
	}

	s.Logger.WithField("file", q.readFiles[q.cursor]).Debug("reading chunk file")

	return f, q.readFiles[q.cursor], nil
}

func (s *Scheduler) checkIfStale(q *queueState) (*os.File, string, error) {
	lastPushed := q.q.lastPushTime.Load()
	if lastPushed == nil {
		return nil, "", nil
	}

	if time.Since(*lastPushed) < s.StaleTimeout {
		return nil, "", nil
	}

	s.Logger.WithField("id", q.q.id).Debug("partial chunk is stale, scheduling")

	err := q.q.enc.promoteChunk()
	if err != nil {
		return nil, "", err
	}

	return s.readQueueChunk(q)
}

func (s *Scheduler) logQueueStats(q *Queue, vectorsSent int64) {
	// q.metrics.VectorsDequeued(vectorsSent)

	s.Logger.
		WithField("queue_id", q.ID()).
		WithField("queue_size", q.Size()).
		WithField("vectors_sent", vectorsSent).
		Debug("queue stats")
}

type queueState struct {
	m           sync.RWMutex
	q           *Queue
	paused      bool
	readFiles   []string
	cursor      int
	activeTasks *common.SharedGauge
}

func newQueueState(q *Queue) *queueState {
	return &queueState{
		q:           q,
		activeTasks: common.NewSharedGauge(),
	}
}

func (qs *queueState) Paused() bool {
	qs.m.RLock()
	defer qs.m.RUnlock()

	return qs.paused
}
