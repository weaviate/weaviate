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

package shared

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/cluster"
)

var (
	ErrQueueFull = errors.New("request queue full")
	ErrShutdown  = errors.New("request queue shutting down")
)

// RequestQueue is a generic buffered work queue with N workers.
// It is used by both the REST and gRPC cluster API servers to
// rate-limit replication requests identically.
type RequestQueue[T any] struct {
	queue      chan T
	config     cluster.RequestQueueConfig
	handler    func(T)
	isExpired  func(T) bool
	onExpired  func(T)
	startOnce  sync.Once
	workerWg   sync.WaitGroup
	isShutdown atomic.Bool
	mu         sync.RWMutex
	logger     logrus.FieldLogger
}

// NewRequestQueue creates a new RequestQueue. If the queue is initially enabled,
// workers are started eagerly.
func NewRequestQueue[T any](
	config cluster.RequestQueueConfig,
	logger logrus.FieldLogger,
	handler func(T),
	isExpired func(T) bool,
	onExpired func(T),
) *RequestQueue[T] {
	rq := &RequestQueue[T]{
		queue:     make(chan T, config.QueueSize),
		config:    config,
		handler:   handler,
		isExpired: isExpired,
		onExpired: onExpired,
		logger:    logger,
	}

	if rq.Enabled() {
		rq.startOnce.Do(rq.startWorkers)
	}

	return rq
}

// IsShutdown returns true if the queue has been closed.
func (rq *RequestQueue[T]) IsShutdown() bool {
	return rq.isShutdown.Load()
}

// Enabled returns true if the queue is dynamically enabled.
func (rq *RequestQueue[T]) Enabled() bool {
	return rq.config.IsEnabled != nil && rq.config.IsEnabled.Get()
}

// EnsureStarted starts workers if they haven't been started yet.
// Safe to call multiple times.
func (rq *RequestQueue[T]) EnsureStarted() {
	rq.startOnce.Do(rq.startWorkers)
}

// Enqueue adds an item to the queue. Returns ErrQueueFull if the queue
// is at capacity, or ErrShutdown if the queue is shutting down.
func (rq *RequestQueue[T]) Enqueue(item T) error {
	rq.mu.RLock()
	defer rq.mu.RUnlock()

	if rq.isShutdown.Load() {
		return ErrShutdown
	}

	select {
	case rq.queue <- item:
		return nil
	default:
		return ErrQueueFull
	}
}

// Close gracefully shuts down the queue: marks shutdown, closes the channel,
// waits for workers with a timeout, and drains remaining items via onExpired.
func (rq *RequestQueue[T]) Close(ctx context.Context) error {
	if !rq.isShutdown.CompareAndSwap(false, true) {
		return nil
	}

	rq.logger.WithField("action", "close_request_queue").Debug("shutting down request queue")

	shutdownTimeout := rq.config.QueueShutdownTimeoutSeconds
	if shutdownTimeout == 0 {
		shutdownTimeout = cluster.DefaultRequestQueueShutdownTimeoutSeconds
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, time.Duration(shutdownTimeout)*time.Second)
	defer cancel()

	done := make(chan struct{})
	enterrors.GoWrapper(func() {
		rq.workerWg.Wait()
		close(done)
	}, rq.logger)

	rq.mu.Lock()
	close(rq.queue)
	rq.mu.Unlock()

	select {
	case <-done:
		rq.logger.WithField("action", "close_request_queue").Debug("workers finished gracefully")
		return nil
	case <-shutdownCtx.Done():
		err := fmt.Errorf("shutdown timeout reached, some workers may still be running")
		rq.logger.WithField("action", "close_request_queue").WithError(err).Warn("shutdown timeout reached, draining queue")
		for {
			select {
			case item, ok := <-rq.queue:
				if !ok {
					return err
				}
				rq.onExpired(item)
			default:
				return err
			}
		}
	}
}

func (rq *RequestQueue[T]) startWorkers() {
	numWorkers := max(1, rq.config.NumWorkers)
	for j := 0; j < numWorkers; j++ {
		rq.workerWg.Add(1)
		enterrors.GoWrapper(func() {
			defer rq.workerWg.Done()
			for item := range rq.queue {
				if rq.isExpired(item) {
					rq.onExpired(item)
					continue
				}
				rq.handler(item)
			}
		}, rq.logger)
	}
}
