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

	for _, t := range batch.Tasks {
		err = w.processTask(batch.Ctx, t)
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Worker) processTask(ctx context.Context, task Task) error {
	for {
		err := task.Execute(ctx)
		if err == nil {
			return nil
		}

		if errors.Is(err, context.Canceled) {
			return err
		}

		w.logger.WithError(err).Infof("failed to process task, retrying in %s", w.retryInterval.String())

		t := time.NewTimer(w.retryInterval)
		select {
		case <-ctx.Done():
			// drain the timer
			if !t.Stop() {
				<-t.C
			}

			return ctx.Err()
		case <-t.C:
		}
	}
}
