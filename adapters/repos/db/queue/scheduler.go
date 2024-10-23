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
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
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

func (s *Scheduler) RegisterQueue(q Queue) {
	s.queues.Lock()
	defer s.queues.Unlock()

	s.queues.m[q.ID()] = newQueueState(q)

	s.Logger.WithField("id", q.ID()).Debug("queue registered")
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
	// as long as there are tasks to schedule, keep running
	// in a tight loop
	for {
		if s.ctx.Err() != nil {
			return
		}

		if nothingScheduled := s.scheduleQueues(); nothingScheduled {
			return
		}
	}
}

func (s *Scheduler) scheduleQueues() (nothingScheduled bool) {
	// loop over the queues in random order
	s.queues.Lock()
	ids := make([]string, 0, len(s.queues.m))
	for id := range s.queues.m {
		ids = append(ids, id)
	}
	s.queues.Unlock()

	nothingScheduled = true

	for _, id := range ids {
		if s.ctx.Err() != nil {
			return
		}

		q := s.getQueue(id)
		if q == nil {
			continue
		}

		// run the before-schedule hook if it is implemented
		if hook, ok := q.q.(BeforeScheduleHook); ok {
			if skip := hook.BeforeSchedule(); skip {
				continue
			}
		}

		if q.Paused() {
			continue
		}

		if q.q.Size() == 0 {
			continue
		}

		nothingScheduled = false

		err := s.dispatchQueue(q)
		if err != nil {
			s.Logger.WithError(err).WithField("id", id).Error("failed to schedule queue")
		}
	}

	return
}

func (s *Scheduler) dispatchQueue(q *queueState) error {
	batch, err := q.q.DequeueBatch()
	if err != nil {
		return errors.Wrap(err, "failed to dequeue batch")
	}
	if batch == nil || len(batch.Tasks) == 0 {
		return nil
	}

	partitions := make([][]Task, len(s.Workers))

	var taskCount int64
	for _, t := range batch.Tasks {
		// TODO: introduce other partitioning strategies if needed
		slot := t.Key() % uint64(len(s.Workers))
		partitions[slot] = append(partitions[slot], t)
		taskCount++
	}

	// compress the tasks before sending them to the workers
	// i.e. group consecutive tasks with the same operation as a single task
	for i := range partitions {
		partitions[i] = s.compressTasks(partitions[i])
	}

	// keep track of the number of active tasks
	// for this chunk to remove it when all tasks are done
	counter := len(partitions)

	for i, partition := range partitions {
		if len(partition) == 0 {
			continue
		}

		// increment the global active tasks counter
		s.activeTasks.Incr()
		// increment the queue's active tasks counter
		q.activeTasks.Incr()

		select {
		case <-s.ctx.Done():
			s.activeTasks.Decr()
			q.activeTasks.Decr()
			return nil
		case s.Workers[i] <- Batch{
			Tasks: partition,
			Ctx:   s.ctx,
			Done: func() {
				defer s.activeTasks.Decr()
				defer q.activeTasks.Decr()

				q.m.Lock()
				counter--
				if counter == 0 {
					batch.Done()
				}
				q.m.Unlock()
			},
		}:
		}
	}

	s.logQueueStats(q.q, taskCount)

	return nil
}

func (s *Scheduler) logQueueStats(q Queue, vectorsSent int64) {
	// q.metrics.VectorsDequeued(vectorsSent)

	s.Logger.
		WithField("queue_id", q.ID()).
		WithField("queue_size", q.Size()).
		WithField("vectors_sent", vectorsSent).
		Debug("queue stats")
}

func (s *Scheduler) compressTasks(tasks []Task) []Task {
	if len(tasks) == 0 {
		return tasks
	}
	grouper, ok := tasks[0].(TaskGrouper)
	if !ok {
		return tasks
	}

	var cur uint8
	var group []Task
	var compressed []Task

	for i, t := range tasks {
		if i == 0 {
			cur = t.Op()
			group = append(group, t)
			continue
		}

		if t.Op() == cur {
			group = append(group, t)
			continue
		}

		compressed = append(compressed, grouper.NewGroup(cur, group...))

		cur = t.Op()
		group = []Task{t}
	}

	compressed = append(compressed, grouper.NewGroup(cur, group...))

	return compressed
}

type queueState struct {
	m           sync.RWMutex
	q           Queue
	paused      bool
	activeTasks *common.SharedGauge
}

func newQueueState(q Queue) *queueState {
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
