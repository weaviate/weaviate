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

package queue

import (
	"context"
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	entsentry "github.com/weaviate/weaviate/entities/sentry"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type Worker struct {
	logger logrus.FieldLogger
	ch     chan *Batch
}

func NewWorker(logger logrus.FieldLogger) (*Worker, chan *Batch) {
	ch := make(chan *Batch)

	return &Worker{
		logger: logger.WithField("action", "queue_worker"),
		ch:     ch,
	}, ch
}

func (w *Worker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case batch := <-w.ch:
			_ = w.do(batch)
		}
	}
}

func (w *Worker) do(batch *Batch) (err error) {
	// Run blocks idle on the channel and only calls do() once a batch arrives,
	// so this wraps a real async-indexing burst, not idle polling.
	defer monitoring.GetBackgroundProcessMetrics().Started(monitoring.ProcessAsyncIndexing)()

	defer func() {
		// a panicking task must not kill the worker goroutine: it is never
		// restarted, and the batch would never be marked done or canceled,
		// leaving the queue's active tasks gauge stuck so the queue is never
		// scheduled again.
		if r := recover(); r != nil {
			w.logger.Errorf("recovered from panic while executing batch: %v", r)
			entsentry.Recover(r)
			enterrors.PrintStack(w.logger)
			err = fmt.Errorf("panic while executing batch: %v", r)
			batch.Cancel()
			return
		}
		if err != nil {
			batch.Cancel()
		} else {
			batch.Done()
		}
	}()

	var discarded []error
	for _, t := range batch.Tasks {
		execErr := t.Execute(batch.Ctx)
		if execErr == nil {
			continue
		}

		// the batch was canceled, e.g. the queue is being closed
		if batch.Ctx.Err() != nil {
			return execErr
		}

		// the whole batch is replayed on the queue's next turn, so that a
		// failing queue does not hold the worker
		if !isPermanent(execErr) {
			w.logger.WithField("tasks", len(batch.Tasks)).
				Warnf("recoverable error, the batch will be retried: %v", execErr)
			return execErr
		}

		discarded = append(discarded, execErr)
	}

	if len(discarded) > 0 {
		w.logger.WithField("failed", len(discarded)).
			Errorf("permanent errors, discarding the failed tasks: %v", errors.Join(discarded...))
	}

	return nil
}

// isPermanent reports whether a failed task must be discarded rather than
// retried. Timeouts are deliberately retried.
func isPermanent(err error) bool {
	return !enterrors.IsTransient(err) &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded)
}
