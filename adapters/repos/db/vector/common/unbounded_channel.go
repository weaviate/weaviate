//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package common

import (
	"context"
	"sync"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// MakeUnboundedChannel creates a channel with unbounded capacity.
// Any push is guaranteed to succeed without blocking, regardless of the
// number of pending reads on `out`. The implementation uses a background goroutine
// that continuously drains an internal queue to the output channel.
// If the channel is empty, the goroutine sleeps until a new item is added.
// This channel doesn't implement backpressure and should be used with caution
// to avoid excessive memory usage.
func MakeUnboundedChannel[T any]() *UnboundedChannel[T] {
	u := UnboundedChannel[T]{
		ch: make(chan T, 64),
	}
	u.cond = sync.NewCond(&u.mu)

	enterrors.GoWrapper(u.run, logrus.New())
	return &u
}

type UnboundedChannel[T any] struct {
	ch        chan T
	ctx       context.Context
	mu        sync.RWMutex
	q         []T
	cond      *sync.Cond
	closeOnce sync.Once
	closed    bool
}

// Push adds a value to the channel. This operation does not wait for
// a corresponding read and buffers the value internally until it can be sent.
// If the channel has been closed, Push returns false.
func (u *UnboundedChannel[T]) Push(v T) bool {
	u.mu.Lock()
	if u.closed {
		u.mu.Unlock()
		return false
	}

	wasEmpty := len(u.q) == 0
	u.q = append(u.q, v)
	u.mu.Unlock()
	if wasEmpty {
		u.cond.Signal()
	}

	return true
}

// Out returns a read-only channel from which values can be received.
func (u *UnboundedChannel[T]) Out() <-chan T {
	return u.ch
}

// Close the channel and initiates the shutdown of the background
// goroutine. The provided context can be used to cancel the draining of
// remaining items to the output channel.
func (u *UnboundedChannel[T]) Close(ctx context.Context) {
	u.closeOnce.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}
		u.mu.Lock()
		u.ctx = ctx
		u.closed = true
		u.cond.Broadcast()
		u.mu.Unlock()
	})
}

func (u *UnboundedChannel[T]) run() {
	defer close(u.ch)

	bgCtx := context.Background()

	for {
		// sleep until we have something to send
		u.mu.Lock()
		for len(u.q) == 0 && !u.closed {
			u.cond.Wait()
		}
		// If closed and empty, we're done
		if len(u.q) == 0 && u.closed {
			u.mu.Unlock()
			return
		}

		// pop head
		v := u.pop()

		ctx := u.ctx
		if ctx == nil {
			ctx = bgCtx
		}
		u.mu.Unlock()

		select {
		case u.ch <- v:
		case <-ctx.Done():
			return
		}
	}
}

func (u *UnboundedChannel[T]) pop() T {
	var zero T

	// pop head
	v := u.q[0]
	u.q[0] = zero // prevent memory leak
	u.q = u.q[1:]

	// shrink the queue if necessary
	if len(u.q) == 0 {
		if cap(u.q) > 1024 {
			u.q = nil // release underlying array
		} else {
			u.q = u.q[:0] // reset to zero length
		}
	} else if cap(u.q) > 1024 && len(u.q) < cap(u.q)/4 {
		// if the queue is less than 25% full, shrink it to half the capacity
		newQ := make([]T, len(u.q), cap(u.q)/2)
		copy(newQ, u.q)
		u.q = newQ
	}

	return v
}

// Len returns the number of items currently buffered in the channel.
func (u *UnboundedChannel[T]) Len() int {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return len(u.q) + len(u.ch)
}
