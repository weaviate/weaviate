package spfresh

import (
	"context"
	"sync"

	"github.com/sirupsen/logrus"
)

type BackgroundOpType string

const (
	BackgroundOpSplit    BackgroundOpType = "split"
	BackgroundOpMerge    BackgroundOpType = "merge"
	BackgroundOpReassign BackgroundOpType = "reassign"
)

type Operation struct {
	PostingID uint64
	OpType    BackgroundOpType
}

type LocalRebuilder struct {
	ch         chan Operation
	ctx        context.Context
	cancel     context.CancelFunc
	concurency int
	wg         sync.WaitGroup
	logger     *logrus.Entry
}

func NewLocalRebuilder(concurency int, logger *logrus.Logger) *LocalRebuilder {
	r := LocalRebuilder{
		ch:         make(chan Operation),
		concurency: concurency,
		logger:     logger.WithField("component", "LocalRebuilder"),
	}

	return &r
}

func (r *LocalRebuilder) Start(ctx context.Context) {
	r.ctx, r.cancel = context.WithCancel(context.Background())

	for i := 0; i < r.concurency; i++ {
		r.wg.Add(1)
		go r.worker()
	}
}

func (r *LocalRebuilder) Enqueue(ctx context.Context, op Operation, postingID uint64) error {
	if r.ctx == nil {
		return nil // Not started yet
	}

	if r.ctx.Err() != nil {
		return r.ctx.Err() // Context already cancelled
	}

	// Enqueue the operation to the channel
	select {
	case r.ch <- op:
	case <-ctx.Done():
		// Context cancelled, do not enqueue
		return ctx.Err()
	}

	return nil
}

func (r *LocalRebuilder) worker() {
	defer r.wg.Done()

	for op := range r.ch {
		if r.ctx.Err() != nil {
			return // Stop processing if context is cancelled
		}

		// Process the operation
		switch op.OpType {
		case BackgroundOpSplit:
			// Handle split operation
		case BackgroundOpMerge:
			// Handle merge operation
		case BackgroundOpReassign:
			// Handle reassign operation
		default:
			r.logger.Warnf("Unknown operation type: %s for posting ID: %d", op.OpType, op.PostingID)
		}
	}
}

func (r *LocalRebuilder) Close(ctx context.Context) error {
	if r.ctx == nil {
		return nil // Already closed or not started
	}

	if r.ctx.Err() != nil {
		return r.ctx.Err() // Context already cancelled
	}

	// Cancel the context to prevent new operations from being enqueued
	r.cancel()

	// Close the channel to signal workers to stop
	close(r.ch)

	r.wg.Wait() // Wait for all workers to finish
	return nil
}
