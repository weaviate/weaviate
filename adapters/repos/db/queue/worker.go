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
	"github.com/weaviate/weaviate/usecases/monitoring"
)

const (
	maxBackoffDuration = 30 * time.Second

	// maxAttemptsBeforeCap is the number of rungs on the backoff ladder before it caps
	maxAttemptsBeforeCap = 5

	// maxMemoryPressureAttempts bounds a batch failing only with memory sheds, which retries cannot fix
	maxMemoryPressureAttempts = maxAttemptsBeforeCap + 1

	// memoryPressurePauseInterval is long on purpose: freeing memory takes minutes
	memoryPressurePauseInterval = 5 * time.Minute
)

// errMemoryPressurePause signals that do() parked the batch; never a task error.
var errMemoryPressurePause = errors.New("batch parked: too many consecutive memory-guard failures")

type Worker struct {
	logger logrus.FieldLogger
	ch     chan *Batch

	// test seams; zero means "use the production default"
	backoffFn              func(attempts int) time.Duration
	maxMemPressureAttempts int
	memPressurePause       time.Duration
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
		switch {
		case errors.Is(err, errMemoryPressurePause):
			// Done() is deliberately not called: it would let the disk queue delete the chunk
			batch.Requeue(w.memoryPressurePause())
		case err != nil:
			batch.Cancel()
		default:
			batch.Done()
		}
	}()

	attempts := 1

	// consecutive rounds in which every remaining error was a memory shed
	memPressureRounds := 0

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
					Errorf("task failed due to wrong dimensions, discarding: %v", err)
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
			w.logger.
				WithField("failed", len(failed)).
				Errorf("permanent errors detected, discarding batch: %v", errors.Join(errs...))
			return nil
		}

		if allMemoryPressure(errs) {
			memPressureRounds++
		} else {
			memPressureRounds = 0
		}

		if memPressureRounds >= w.maxMemoryPressureAttempts() {
			// only the tasks that are still outstanding need to be re-run
			batch.Tasks = failed

			pause := w.memoryPressurePause()
			w.logger.
				WithField("failed", len(failed)).
				WithField("attempts", attempts).
				WithField("resume_in", pause).
				Warnf("batch failed the memory guard %d times in a row; parking it for %s instead of retrying, the tasks stay queued: %v",
					memPressureRounds, pause, errors.Join(errs...))

			return errMemoryPressurePause
		}

		// the remaining errors are recoverable: transient errors, or timeouts,
		// which are deliberately excluded from the permanent classification.
		// Retry the failed tasks with a backoff. Every failure must either
		// discard the batch, return, or sleep before the next attempt,
		// otherwise this loop spins at full speed.
		retryIn := w.calculateBackoff(attempts)
		w.logger.
			WithField("failed", len(failed)).
			WithField("attempts", attempts).
			WithField("retry_in", retryIn).
			Warnf("recoverable errors detected, retrying batch in %s: %v", retryIn, errors.Join(errs...))

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
	if w.backoffFn != nil {
		return w.backoffFn(attempts)
	}

	// Cap attempts to prevent bit-shift overflow
	if attempts > maxAttemptsBeforeCap {
		return maxBackoffDuration
	}

	return time.Second << (attempts - 1)
}

func (w *Worker) maxMemoryPressureAttempts() int {
	if w.maxMemPressureAttempts > 0 {
		return w.maxMemPressureAttempts
	}

	return maxMemoryPressureAttempts
}

func (w *Worker) memoryPressurePause() time.Duration {
	if w.memPressurePause > 0 {
		return w.memPressurePause
	}

	return memoryPressurePauseInterval
}

// allMemoryPressure reports whether errs is non-empty and every error is a memory shed
func allMemoryPressure(errs []error) bool {
	if len(errs) == 0 {
		return false
	}

	for _, err := range errs {
		if !enterrors.IsMemoryPressure(err) {
			return false
		}
	}

	return true
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
