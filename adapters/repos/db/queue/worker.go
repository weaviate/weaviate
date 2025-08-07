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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

const (
	maxRetry = 3
)

type Worker struct {
	logger        logrus.FieldLogger
	retryInterval time.Duration
	ch            chan *Batch
}

func NewWorker(logger logrus.FieldLogger, retryInterval time.Duration) (*Worker, chan *Batch) {
	ch := make(chan *Batch)

	return &Worker{
		logger:        logger,
		retryInterval: retryInterval,
		ch:            ch,
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
			if errors.Is(err, common.WrongDimensionsError) {
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

		if attempts >= maxRetry {
			w.logger.
				WithError(errors.Join(errs...)).
				WithField("failed", len(failed)).
				WithField("attempts", attempts).
				Error("failed to process task, discarding")
			return nil
		}

		w.logger.
			WithError(errors.Join(errs...)).
			WithField("failed", len(failed)).
			WithField("attempts", attempts).
			Infof("failed to process task, retrying in %s", w.retryInterval.String())
		attempts++

		t := time.NewTimer(w.retryInterval)
		select {
		case <-batch.Ctx.Done():
			// drain the timer
			if !t.Stop() {
				<-t.C
			}

			return batch.Ctx.Err()
		case <-t.C:
		}
	}
}
