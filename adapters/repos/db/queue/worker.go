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

package queue

import (
	"context"
	"errors"
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"

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

		hasTransientErrs := hasTransientErrors(errs)
		if hasTransientErrs {
			retryIn := w.calculateBackoff(attempts)
			w.logger.
				WithError(errors.Join(errs...)).
				WithField("failed", len(failed)).
				WithField("attempts", attempts).
				WithField("retry_in", retryIn).
				Warnf("transient errors detected, retrying batch in %s", retryIn)

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
}

func (w *Worker) calculateBackoff(attempts int) time.Duration {
	backoff := time.Duration(1<<uint(attempts-1)) * time.Second
	return min(backoff, maxBackoffDuration)
}

func hasTransientErrors(errs []error) bool {
	for _, err := range errs {
		if enterrors.IsTransient(err) {
			return true
		}
	}

	return false
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
