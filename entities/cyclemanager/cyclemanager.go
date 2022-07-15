package cyclemanager

import (
	"context"
	"sync"
	"time"
)

type CycleManager struct {
	sync.RWMutex

	description string
	running     bool
	cycleFunc   func(duration time.Duration)

	Stopped chan struct{}
}

func New(cycleFunc func(duration time.Duration), description string) *CycleManager {
	return &CycleManager{
		description: description,
		cycleFunc:   cycleFunc,
		Stopped:     make(chan struct{}),
	}
}

func (c *CycleManager) Start(interval time.Duration) {
	c.Lock()
	defer c.Unlock()

	// prevent spawning multiple cycleFunc routines
	if c.running {
		return
	}

	c.cycleFunc(interval)
	c.running = true
}

func (c *CycleManager) Stop(ctx context.Context) {
	c.Lock()
	defer c.Unlock()

	if !c.running {
		return
	}

	c.Stopped <- struct{}{}
	c.running = false
}
