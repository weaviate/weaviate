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

package logrusext

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// Sampler is a very simple log sampler which prints up to N log messages
// every tick duration.
type Sampler struct {
	l logrus.FieldLogger

	mu        sync.Mutex
	counter   int
	limit     int
	tick      time.Duration
	lastReset time.Time
}

func NewSampler(l logrus.FieldLogger, n int, tick time.Duration) *Sampler {
	return &Sampler{
		l:     l,
		limit: n,
		tick:  tick,
	}
}

func (s *Sampler) WithSampling(fn func(l logrus.FieldLogger)) {
	now := time.Now()

	s.mu.Lock()
	counter := s.counter
	if now.Sub(s.lastReset) > s.tick {
		counter = 0
		s.lastReset = now
	}
	counter++
	s.counter = counter
	s.mu.Unlock()

	if counter <= s.limit {
		fn(s.l)
	}
}
