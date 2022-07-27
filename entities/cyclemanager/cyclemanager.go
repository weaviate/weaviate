//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

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
