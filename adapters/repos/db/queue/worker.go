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
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	entsentry "github.com/weaviate/weaviate/entities/sentry"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

const (
	maxBackoffDuration = 30 * time.Second
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

	attempts := 1

	// keep track of failed tasks
	var failed []Task
	var errs []error

	for {
		tasks := batch.Tasks

		if len(failed) > 0 {
			tasks = failed
			failed = nil // reset failed tasks for the next iteration
			errs = nil
		}

		for i, t := range tasks {
			err = t.Execute(batch.Ctx)
			// check if the full batch was canceled
			if errors.Is(err, context.Canceled) {
				return err
			}
			if errors.Is(err, common.ErrWrongDimensions) {
				w.logger.
					WithError(err).
					Error("task failed due to wrong dimensions, discarding")
				continue // skip this task
			}

			// if the task failed, add it to the failed list
			if err != nil {
				errs = append(errs, err)
				failed = append(failed, tasks[i])
			}
		}

		if len(failed) == 0 {
			return nil // all tasks succeeded
		}

		hasPermanentErrs := hasPermanentErrors(errs)
		if hasPermanentErrs {
			w.logger.WithError(errors.Join(errs...)).
				WithField("failed", len(failed)).Error("permanent errors detected, discarding batch")
			return nil
		}

		// the remaining errors are recoverable: transient errors, or timeouts,
		// which are deliberately excluded from the permanent classification.
		// Retry the failed tasks with a backoff. Every failure must either
		// discard the batch, return, or sleep before the next attempt,
		// otherwise this loop spins at full speed.
		retryIn := w.calculateBackoff(attempts)
		w.logger.
			WithError(errors.Join(errs...)).
			WithField("failed", len(failed)).
			WithField("attempts", attempts).
			WithField("retry_in", retryIn).
			Warnf("recoverable errors detected, retrying batch in %s", retryIn)

		attempts++
		retryTimer := time.NewTimer(retryIn)
		select {
		case <-batch.Ctx.Done():
			if !retryTimer.Stop() {
				<-retryTimer.C
			}
			return batch.Ctx.Err()
		case <-retryTimer.C:
			// try again
		}
	}
}

func (w *Worker) calculateBackoff(attempts int) time.Duration {
	// Cap attempts to prevent bit-shift overflow
	const maxAttemptsBeforeCap = 5

	if attempts > maxAttemptsBeforeCap {
		return maxBackoffDuration
	}

	return time.Second << (attempts - 1)
}

func hasPermanentErrors(errs []error) bool {
	for _, err := range errs {
		if !enterrors.IsTransient(err) &&
			!errors.Is(err, context.Canceled) &&
			!errors.Is(err, context.DeadlineExceeded) {
			return true
		}
	}
	return false
}
