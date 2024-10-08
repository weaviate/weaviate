package queue

import (
	"sync"

	"github.com/sirupsen/logrus"
)

type Scheduler struct {
	logger logrus.FieldLogger

	queues struct {
		sync.RWMutex

		m map[string]*queueState
	}
}

func (m *Scheduler) RegisterQueue(id string, config QueueConfig) {
	m.queues.Lock()
	defer m.queues.Unlock()

	m.queues.m[id] = &queueState{
		config: config,
	}

	m.logger.WithField("id", id).Debug("queue registered")
}

func (m *Scheduler) UnregisterQueue(id string) {
	m.queues.Lock()
	defer m.queues.Unlock()

	delete(m.queues.m, id)

	m.logger.WithField("id", id).Debug("queue unregistered")
}

func (m *Scheduler) Start() {

}

func (m *Scheduler) Close() {
}

func (m *Scheduler) PauseQueue(id string) {
	m.queues.Lock()
	defer m.queues.Unlock()

	q, ok := m.queues.m[id]
	if !ok {
		return
	}

	q.paused = true

	m.logger.WithField("id", id).Debug("queue paused")
}

func (m *Scheduler) ResumeQueue(id string) {
	m.queues.Lock()
	defer m.queues.Unlock()

	q, ok := m.queues.m[id]
	if !ok {
		return
	}

	q.paused = false

	m.logger.WithField("id", id).Debug("queue resumed")
}

type QueueConfig struct {
	Path string
}

type queueState struct {
	config QueueConfig
	paused bool
}
