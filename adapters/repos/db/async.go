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

package db

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
)

type AsyncWorker struct {
	logger        logrus.FieldLogger
	retryInterval time.Duration
	ch            chan queue.Batch
}

func NewAsyncWorker(logger logrus.FieldLogger, retryInterval time.Duration) (*AsyncWorker, chan queue.Batch) {
	ch := make(chan queue.Batch)

	return &AsyncWorker{
		logger:        logger,
		retryInterval: retryInterval,
		ch:            ch,
	}, ch
}

func (a *AsyncWorker) Run() {
	for batch := range a.ch {
		stop := a.do(&batch)

		if stop {
			return
		}
	}
}

func (a *AsyncWorker) do(batch *queue.Batch) (stop bool) {
	defer batch.Done()

	for _, t := range batch.Tasks {
		if a.processTask(batch.Ctx, t) {
			return true
		}
	}

	return false
}

func (a *AsyncWorker) processTask(ctx context.Context, task *queue.Task) (stop bool) {
	for {
		err := task.Execute(ctx)
		if err == nil {
			return false
		}

		if errors.Is(err, context.Canceled) {
			a.logger.WithError(err).Debug("skipping indexing batch due to context cancellation")
			return true
		}

		a.logger.WithError(err).Infof("failed to index vectors, retrying in %s", a.retryInterval.String())

		t := time.NewTimer(a.retryInterval)
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
