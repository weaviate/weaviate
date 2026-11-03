//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import "sync"

// alreadyDrained is handed to waiters that arrive while the latch is idle; receive-only so nothing can close or send on it.
var alreadyDrained <-chan struct{} = func() chan struct{} { ch := make(chan struct{}); close(ch); return ch }()

// drainLatch counts in-flight work like sync.WaitGroup but wakes waiters by closing a per-episode channel, so an Add can never race a returning Wait.
type drainLatch struct {
	mu    sync.Mutex
	count int64
	ch    chan struct{} // non-nil exactly while count > 0
}

// Add adjusts the in-flight counter, closing the episode channel when it reaches zero.
func (l *drainLatch) Add(delta int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Validate before mutating: a recovered panic must not leave a negative count that silently swallows the next episode.
	next := l.count + int64(delta)
	if next < 0 {
		panic("negative async replication drain counter")
	}
	l.count = next
	if l.count == 0 {
		if l.ch != nil {
			close(l.ch)
			l.ch = nil
		}
		return
	}
	if l.ch == nil {
		l.ch = make(chan struct{})
	}
}

// Done settles one unit of in-flight work.
func (l *drainLatch) Done() { l.Add(-1) }

// Drained returns a channel closed when the counter next reaches zero; already closed when idle.
func (l *drainLatch) Drained() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.ch == nil {
		return alreadyDrained
	}
	return l.ch
}

// Wait blocks until the counter reaches zero.
func (l *drainLatch) Wait() { <-l.Drained() }
