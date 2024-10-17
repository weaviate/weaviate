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
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Worker struct {
	logger        logrus.FieldLogger
	retryInterval time.Duration
	ch            chan Batch
}

func NewWorker(logger logrus.FieldLogger, retryInterval time.Duration) (*Worker, chan Batch) {
	ch := make(chan Batch)

	return &Worker{
		logger:        logger,
		retryInterval: retryInterval,
		ch:            ch,
	}, ch
}

func (w *Worker) Run() {
	for batch := range w.ch {
		stop := w.do(&batch)

		if stop {
			return
		}
	}
}

func (w *Worker) do(batch *Batch) (stop bool) {
	defer batch.Done()

	for _, t := range batch.Tasks {
		if w.processTask(batch.Ctx, t) {
			return true
		}
	}

	return false
}

func (w *Worker) processTask(ctx context.Context, task *Task) (stop bool) {
	for {
		err := task.Execute(ctx)
		if err == nil {
			return false
		}

		if errors.Is(err, context.Canceled) {
			w.logger.WithError(err).Debug("skipping processing task due to context cancellation")
			return true
		}

		w.logger.WithError(err).Infof("failed to process task, retrying in %s", w.retryInterval.String())

		t := time.NewTimer(w.retryInterval)
		select {
		case <-ctx.Done():
			// drain the timer
			if !t.Stop() {
				<-t.C
			}

			return true
		case <-t.C:
		}
	}
}
