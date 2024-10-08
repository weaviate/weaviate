package queue

import (
	"sync"

	"github.com/sirupsen/logrus"
)

type Scheduler struct {
	logger logrus.FieldLogger

	queues struct {
		sync.Mutex

		m map[string]*queueState
	}

	workers []chan Task
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

}

func (s *Scheduler) Close() {
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
	q      QueueDecoder
	paused bool
}

func (s *Scheduler) schedule() {
	s.queues.Lock()
	defer s.queues.Unlock()

	// loop over the queues in a random order
	for id, q := range s.queues.m {
		if q.paused {
			continue
		}

		s.scheduleQueue(q)
	}
}

func (s *Scheduler) scheduleQueue(q *queueState) {

}
